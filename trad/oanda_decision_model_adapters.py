#!/usr/bin/env python3
"""Offline-only contextual-bandit and RL allocation adapters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np


ACTION_NAMES = ("short", "flat", "long")


def validate_offline_arrays(
    contexts: np.ndarray,
    action_rewards: np.ndarray,
    min_rows: int = 8,
) -> tuple[np.ndarray, np.ndarray]:
    x = np.asarray(contexts, dtype=np.float32)
    rewards = np.asarray(action_rewards, dtype=np.float32)
    if x.ndim != 2:
        raise ValueError("contexts must have shape (time, features)")
    if rewards.shape != (len(x), len(ACTION_NAMES)):
        raise ValueError(
            f"action_rewards must have shape (time, {len(ACTION_NAMES)}) in {ACTION_NAMES} order"
        )
    if len(x) < min_rows:
        raise ValueError(
            f"offline decision data requires at least {min_rows} chronological rows"
        )
    if not np.isfinite(x).all() or not np.isfinite(rewards).all():
        raise ValueError("offline decision arrays contain non-finite values")
    return x, rewards


class OfflineActionEnv:
    """Finite chronological replay; actions cannot affect data or reach a broker."""

    metadata = {"render_modes": []}

    def __new__(cls, *args: Any, **kwargs: Any):
        import gymnasium as gym

        if cls is OfflineActionEnv:
            dynamic = type("_GymOfflineActionEnv", (cls, gym.Env), {})
            return object.__new__(dynamic)
        return object.__new__(cls)

    def __init__(
        self,
        contexts: np.ndarray,
        action_rewards: np.ndarray,
        continuous: bool = False,
        max_drawdown: float = 8.0,
    ) -> None:
        import gymnasium as gym

        self.contexts, self.action_rewards = validate_offline_arrays(
            contexts, action_rewards, min_rows=2
        )
        if max_drawdown <= 0:
            raise ValueError("max_drawdown must be positive")
        self.continuous = continuous
        self.max_drawdown = float(max_drawdown)
        self.observation_space = gym.spaces.Box(
            low=-np.inf,
            high=np.inf,
            shape=(self.contexts.shape[1],),
            dtype=np.float32,
        )
        if continuous:
            self.action_space = gym.spaces.Box(-1.0, 1.0, shape=(1,), dtype=np.float32)
        else:
            self.action_space = gym.spaces.Discrete(len(ACTION_NAMES))
        self.index = 0
        self.equity = 0.0
        self.peak = 0.0

    def reset(self, *, seed: int | None = None, options: dict[str, Any] | None = None):
        try:
            super().reset(seed=seed)
        except AttributeError:
            pass
        self.index = 0
        self.equity = 0.0
        self.peak = 0.0
        return self.contexts[0], {"shadow_only": True}

    def _reward(self, action: Any) -> tuple[float, float]:
        rewards = self.action_rewards[self.index]
        if not self.continuous:
            action_index = int(np.asarray(action).item())
            if action_index not in range(len(ACTION_NAMES)):
                raise ValueError("discrete action is out of range")
            return float(rewards[action_index]), float(action_index - 1)
        exposure = float(np.clip(np.asarray(action).reshape(-1)[0], -1.0, 1.0))
        if exposure >= 0.0:
            reward = (1.0 - exposure) * rewards[1] + exposure * rewards[2]
        else:
            reward = (1.0 + exposure) * rewards[1] + (-exposure) * rewards[0]
        return float(reward), exposure

    def step(self, action: Any):
        reward, exposure = self._reward(action)
        self.equity += reward
        self.peak = max(self.peak, self.equity)
        drawdown = self.peak - self.equity
        self.index += 1
        exhausted = self.index >= len(self.contexts)
        drawdown_stop = drawdown >= self.max_drawdown
        terminated = bool(exhausted or drawdown_stop)
        if drawdown_stop:
            reward -= self.max_drawdown
        observation = self.contexts[min(self.index, len(self.contexts) - 1)]
        info = {
            "shadow_only": True,
            "equity": self.equity,
            "drawdown": drawdown,
            "drawdown_stop": drawdown_stop,
            "exposure": exposure,
        }
        return observation, float(reward), terminated, False, info


def make_sb3_agent(name: str, env: OfflineActionEnv, seed: int = 42) -> Any:
    from stable_baselines3 import DQN, PPO, SAC

    common = {"env": env, "verbose": 0, "seed": seed, "device": "cpu"}
    if name == "ppo":
        if env.continuous:
            raise ValueError("PPO qualification uses the fixed discrete action contract")
        return PPO(
            "MlpPolicy",
            n_steps=16,
            batch_size=16,
            n_epochs=1,
            policy_kwargs={"net_arch": [16, 16]},
            **common,
        )
    if name == "dqn":
        if env.continuous:
            raise ValueError("DQN requires discrete actions")
        return DQN(
            "MlpPolicy",
            learning_starts=8,
            buffer_size=256,
            batch_size=16,
            train_freq=4,
            gradient_steps=1,
            target_update_interval=16,
            policy_kwargs={"net_arch": [16, 16]},
            **common,
        )
    if name == "sac":
        if not env.continuous:
            raise ValueError("SAC requires the bounded continuous exposure contract")
        return SAC(
            "MlpPolicy",
            learning_starts=8,
            buffer_size=256,
            batch_size=16,
            train_freq=4,
            gradient_steps=1,
            policy_kwargs={"net_arch": [16, 16]},
            **common,
        )
    raise ValueError(f"unsupported Stable-Baselines3 agent: {name}")


def evaluate_agent(agent: Any, env: OfflineActionEnv) -> dict[str, float | int]:
    observation, _ = env.reset()
    rewards: list[float] = []
    exposures: list[float] = []
    while True:
        action, _ = agent.predict(observation, deterministic=True)
        observation, reward, terminated, truncated, info = env.step(action)
        rewards.append(float(reward))
        exposures.append(float(info["exposure"]))
        if terminated or truncated:
            break
    equity = np.cumsum(rewards)
    peaks = np.maximum.accumulate(np.maximum(equity, 0.0))
    return {
        "steps": len(rewards),
        "net_reward": float(np.sum(rewards)),
        "mean_reward": float(np.mean(rewards)),
        "max_drawdown": float(np.max(peaks - equity)),
        "mean_abs_exposure": float(np.mean(np.abs(exposures))),
    }


def qualify_sb3_agent(
    name: str,
    contexts: np.ndarray,
    action_rewards: np.ndarray,
    train_fraction: float = 0.7,
    total_timesteps: int = 48,
) -> dict[str, Any]:
    x, rewards = validate_offline_arrays(contexts, action_rewards, min_rows=32)
    split = int(len(x) * train_fraction)
    if split < 24 or len(x) - split < 8:
        raise ValueError("chronological train and holdout partitions are too small")
    continuous = name == "sac"
    train_env = OfflineActionEnv(x[:split], rewards[:split], continuous=continuous)
    holdout_env = OfflineActionEnv(x[split:], rewards[split:], continuous=continuous)
    agent = make_sb3_agent(name, train_env)
    agent.learn(total_timesteps=total_timesteps, progress_bar=False)
    metrics = evaluate_agent(agent, holdout_env)
    return {
        "model": name,
        "status": "synthetic_qualified",
        "train_rows": split,
        "holdout_rows": len(x) - split,
        "action_contract": "continuous_exposure_-1_to_1" if continuous else list(ACTION_NAMES),
        "account_wired": False,
        "production_eligible": False,
        **metrics,
    }


class VowpalContextualBanditAdapter:
    """Thin full-provenance wrapper around Vowpal Wabbit CB-ADF."""

    model_id = "contextual_bandit"

    def __init__(self, epsilon: float = 0.05) -> None:
        if not 0.0 <= epsilon <= 1.0:
            raise ValueError("epsilon must be between zero and one")
        try:
            import vowpalwabbit
        except ImportError as exc:
            raise RuntimeError("vowpalwabbit is not installed in this runtime") from exc
        self.workspace = vowpalwabbit.Workspace(
            f"--cb_explore_adf --epsilon {epsilon} --quiet"
        )

    @staticmethod
    def _features(values: np.ndarray) -> str:
        return " ".join(f"f{index}:{float(value):.9g}" for index, value in enumerate(values))

    def learn(
        self,
        context: np.ndarray,
        chosen_action: int,
        reward: float,
        propensity: float,
    ) -> None:
        if chosen_action not in range(len(ACTION_NAMES)):
            raise ValueError("chosen_action is out of range")
        if not 0.0 < propensity <= 1.0:
            raise ValueError("propensity must be in (0, 1]")
        shared = f"shared |context {self._features(np.asarray(context))}"
        lines = [shared]
        for index, name in enumerate(ACTION_NAMES):
            label = f"0:{-float(reward)}:{float(propensity)} " if index == chosen_action else ""
            lines.append(f"{label}|action name={name}")
        self.workspace.learn("\n".join(lines))

    def predict(self, context: np.ndarray) -> list[float]:
        lines = [f"shared |context {self._features(np.asarray(context))}"]
        lines.extend(f"|action name={name}" for name in ACTION_NAMES)
        return list(self.workspace.predict("\n".join(lines)))


def qualify_contextual_bandit(
    contexts: np.ndarray,
    action_rewards: np.ndarray,
    train_fraction: float = 0.7,
) -> dict[str, Any]:
    x, rewards = validate_offline_arrays(contexts, action_rewards, min_rows=32)
    split = int(len(x) * train_fraction)
    if split < 24 or len(x) - split < 8:
        raise ValueError("chronological train and holdout partitions are too small")
    model = VowpalContextualBanditAdapter(epsilon=0.05)
    for index in range(split):
        chosen = index % len(ACTION_NAMES)
        model.learn(x[index], chosen, float(rewards[index, chosen]), 1.0 / len(ACTION_NAMES))
    holdout_rewards: list[float] = []
    for context, row in zip(x[split:], rewards[split:]):
        probabilities = np.asarray(model.predict(context), dtype=float)
        action = int(np.argmax(probabilities))
        holdout_rewards.append(float(row[action]))
    return {
        "model": "contextual_bandit",
        "status": "synthetic_qualified",
        "train_rows": split,
        "holdout_rows": len(x) - split,
        "logged_propensity": 1.0 / len(ACTION_NAMES),
        "action_contract": list(ACTION_NAMES),
        "net_reward": float(np.sum(holdout_rewards)),
        "mean_reward": float(np.mean(holdout_rewards)),
        "account_wired": False,
        "production_eligible": False,
    }
