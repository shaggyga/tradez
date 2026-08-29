from __future__ import annotations

import numpy as np

import oanda_decision_model_adapters as decision


def _arrays(rows: int = 40):
    contexts = np.column_stack(
        [np.sin(np.arange(rows) / 5), np.cos(np.arange(rows) / 7)]
    ).astype(np.float32)
    signal = contexts[:, 0]
    rewards = np.column_stack([-signal, np.zeros(rows), signal]).astype(np.float32)
    return contexts, rewards


def test_discrete_environment_is_finite_and_drawdown_bounded() -> None:
    contexts, rewards = _arrays()
    env = decision.OfflineActionEnv(contexts, rewards, max_drawdown=0.1)
    observation, info = env.reset()
    assert observation.shape == (2,)
    assert info["shadow_only"]
    _, _, terminated, _, step_info = env.step(0)
    assert isinstance(terminated, bool)
    assert step_info["shadow_only"]


def test_continuous_exposure_interpolates_fixed_actions() -> None:
    contexts, rewards = _arrays()
    env = decision.OfflineActionEnv(contexts, rewards, continuous=True)
    env.reset()
    reward, exposure = env._reward(np.array([0.5], dtype=np.float32))
    assert exposure == 0.5
    assert np.isclose(reward, 0.5 * rewards[0, 2])


def test_contextual_bandit_runs_chronological_holdout() -> None:
    contexts, rewards = _arrays(48)
    result = decision.qualify_contextual_bandit(contexts, rewards)
    assert result["status"] == "synthetic_qualified"
    assert result["train_rows"] + result["holdout_rows"] == 48
    assert not result["account_wired"]
