#!/usr/bin/env python3
"""Leakage-safe representation, transfer, multi-task, and distillation adapters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass(frozen=True)
class WindowSplit:
    train_x: np.ndarray
    train_return: np.ndarray
    holdout_x: np.ndarray
    holdout_return: np.ndarray
    train_target_max_index: int
    holdout_target_min_index: int
    normalization_mean: np.ndarray
    normalization_std: np.ndarray


def chronological_windows(
    values: np.ndarray,
    context_steps: int = 16,
    horizon_steps: int = 2,
    split_fraction: float = 0.7,
) -> WindowSplit:
    frame = np.asarray(values, dtype=np.float32)
    if frame.ndim == 1:
        frame = frame[:, None]
    if frame.ndim != 2 or len(frame) < 48:
        raise ValueError("values must have shape (time, features) with at least 48 rows")
    if not np.isfinite(frame).all():
        raise ValueError("values contain non-finite entries")
    if context_steps < 4 or horizon_steps < 1:
        raise ValueError("invalid context or horizon")
    split_index = int(len(frame) * split_fraction)
    windows: list[np.ndarray] = []
    returns: list[np.ndarray] = []
    targets: list[int] = []
    for target_index in range(context_steps + horizon_steps - 1, len(frame)):
        context_end = target_index - horizon_steps + 1
        context_start = context_end - context_steps
        context = frame[context_start:context_end]
        base = frame[context_end - 1]
        target = frame[target_index]
        windows.append(context)
        returns.append(target - base)
        targets.append(target_index)
    x = np.stack(windows)
    y = np.stack(returns)
    target_indices = np.asarray(targets)
    train_mask = target_indices < split_index
    holdout_mask = target_indices >= split_index
    if train_mask.sum() < 16 or holdout_mask.sum() < 8:
        raise ValueError("chronological partitions are too small")
    mean = x[train_mask].mean(axis=(0, 1), keepdims=True)
    std = x[train_mask].std(axis=(0, 1), keepdims=True)
    std[std < 1e-6] = 1.0
    normalized = (x - mean) / std
    return WindowSplit(
        train_x=normalized[train_mask],
        train_return=y[train_mask],
        holdout_x=normalized[holdout_mask],
        holdout_return=y[holdout_mask],
        train_target_max_index=int(target_indices[train_mask].max()),
        holdout_target_min_index=int(target_indices[holdout_mask].min()),
        normalization_mean=mean.squeeze(0),
        normalization_std=std.squeeze(0),
    )


def _torch():
    import torch
    from torch import nn

    return torch, nn


class SequenceEncoder:
    """Factory wrapper so importing the module does not require PyTorch."""

    @staticmethod
    def build(input_features: int, hidden_size: int = 16):
        torch, nn = _torch()

        class _Encoder(nn.Module):
            def __init__(self) -> None:
                super().__init__()
                self.gru = nn.GRU(input_features, hidden_size, batch_first=True)

            def forward(self, x):
                sequence, state = self.gru(x)
                return sequence, state[-1]

        return _Encoder()


def _tensor(value: np.ndarray):
    torch, _ = _torch()
    return torch.as_tensor(value, dtype=torch.float32)


def _finite_loss(loss: Any) -> float:
    value = float(loss.detach().cpu())
    if not np.isfinite(value):
        raise RuntimeError("training objective became non-finite")
    return value


def qualify_masked_pretraining(split: WindowSplit) -> dict[str, Any]:
    torch, nn = _torch()
    x = _tensor(split.train_x[:32])
    encoder = SequenceEncoder.build(x.shape[-1])
    decoder = nn.Linear(16, x.shape[-1])
    optimizer = torch.optim.Adam([*encoder.parameters(), *decoder.parameters()], lr=1e-3)
    generator = torch.Generator().manual_seed(42)
    mask = torch.rand(x.shape[:2], generator=generator) < 0.2
    corrupted = x.clone()
    corrupted[mask] = 0.0
    sequence, _ = encoder(corrupted)
    reconstruction = decoder(sequence)
    loss = ((reconstruction - x)[mask]).pow(2).mean()
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()
    return {"model": "self_supervised_pretraining", "loss": _finite_loss(loss)}


def _augment(x: Any, seed: int) -> Any:
    torch, _ = _torch()
    generator = torch.Generator().manual_seed(seed)
    noise = torch.randn(x.shape, generator=generator, dtype=x.dtype) * 0.01
    mask = torch.rand(x.shape[:2], generator=generator) < 0.1
    result = x + noise
    result[mask] = 0.0
    return result


def qualify_contrastive_learning(split: WindowSplit) -> dict[str, Any]:
    torch, nn = _torch()
    x = _tensor(split.train_x[:32])
    encoder = SequenceEncoder.build(x.shape[-1])
    optimizer = torch.optim.Adam(encoder.parameters(), lr=1e-3)
    _, left = encoder(_augment(x, 41))
    _, right = encoder(_augment(x, 43))
    left = nn.functional.normalize(left, dim=1)
    right = nn.functional.normalize(right, dim=1)
    logits = left @ right.T / 0.1
    labels = torch.arange(len(x))
    loss = 0.5 * (
        nn.functional.cross_entropy(logits, labels)
        + nn.functional.cross_entropy(logits.T, labels)
    )
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()
    return {"model": "contrastive_learning", "loss": _finite_loss(loss)}


def _multitask_model(features: int, hidden: int = 16):
    torch, nn = _torch()

    class _Model(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.encoder = SequenceEncoder.build(features, hidden)
            self.direction = nn.Linear(hidden, 1)
            self.return_head = nn.Linear(hidden, 1)
            self.movement = nn.Linear(hidden, 1)

        def forward(self, x):
            _, embedding = self.encoder(x)
            return (
                self.direction(embedding).squeeze(-1),
                self.return_head(embedding).squeeze(-1),
                nn.functional.softplus(self.movement(embedding).squeeze(-1)),
            )

    return _Model()


def _multitask_loss(model: Any, x: Any, returns: Any) -> Any:
    torch, nn = _torch()
    signed = returns[:, 0]
    direction, predicted_return, movement = model(x)
    return (
        nn.functional.binary_cross_entropy_with_logits(direction, (signed > 0).float())
        + nn.functional.smooth_l1_loss(predicted_return, signed)
        + nn.functional.smooth_l1_loss(movement, signed.abs())
    )


def qualify_multi_task_learning(split: WindowSplit) -> dict[str, Any]:
    torch, _ = _torch()
    x = _tensor(split.train_x[:32])
    y = _tensor(split.train_return[:32])
    model = _multitask_model(x.shape[-1])
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    loss = _multitask_loss(model, x, y)
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()
    return {"model": "multi_task_learning", "loss": _finite_loss(loss)}


def qualify_transfer_learning(source: WindowSplit, target: WindowSplit) -> dict[str, Any]:
    torch, nn = _torch()
    source_x = _tensor(source.train_x[:32])
    source_y = _tensor(source.train_return[:32, 0])
    target_x = _tensor(target.train_x[:32])
    target_y = _tensor(target.train_return[:32, 0])
    encoder = SequenceEncoder.build(source_x.shape[-1])
    source_head = nn.Linear(16, 1)
    optimizer = torch.optim.Adam([*encoder.parameters(), *source_head.parameters()], lr=1e-3)
    _, source_embedding = encoder(source_x)
    source_loss = nn.functional.smooth_l1_loss(source_head(source_embedding).squeeze(-1), source_y)
    optimizer.zero_grad()
    source_loss.backward()
    optimizer.step()
    for parameter in encoder.parameters():
        parameter.requires_grad = False
    target_head = nn.Linear(16, 1)
    target_optimizer = torch.optim.Adam(target_head.parameters(), lr=1e-3)
    with torch.no_grad():
        _, target_embedding = encoder(target_x)
    target_loss = nn.functional.smooth_l1_loss(
        target_head(target_embedding).squeeze(-1), target_y
    )
    target_optimizer.zero_grad()
    target_loss.backward()
    target_optimizer.step()
    return {
        "model": "transfer_learning",
        "source_loss": _finite_loss(source_loss),
        "target_loss": _finite_loss(target_loss),
        "encoder_frozen_for_target_step": True,
    }


def qualify_knowledge_distillation(split: WindowSplit) -> dict[str, Any]:
    torch, nn = _torch()
    x = _tensor(split.train_x[:32])
    y = _tensor(split.train_return[:32, 0])
    teacher = _multitask_model(x.shape[-1], hidden=32)
    teacher_optimizer = torch.optim.Adam(teacher.parameters(), lr=1e-3)
    teacher_loss = _multitask_loss(teacher, x, _tensor(split.train_return[:32]))
    teacher_optimizer.zero_grad()
    teacher_loss.backward()
    teacher_optimizer.step()
    teacher.eval()
    for parameter in teacher.parameters():
        parameter.requires_grad = False
    student = _multitask_model(x.shape[-1], hidden=8)
    student_optimizer = torch.optim.Adam(student.parameters(), lr=1e-3)
    with torch.no_grad():
        teacher_logits, teacher_return, _ = teacher(x)
    student_logits, student_return, _ = student(x)
    temperature = 2.0
    soft_targets = torch.sigmoid(teacher_logits / temperature)
    loss = (
        nn.functional.binary_cross_entropy_with_logits(
            student_logits / temperature, soft_targets
        )
        * temperature**2
        + nn.functional.smooth_l1_loss(student_return, teacher_return)
        + 0.25 * nn.functional.smooth_l1_loss(student_return, y)
    )
    student_optimizer.zero_grad()
    loss.backward()
    student_optimizer.step()
    return {
        "model": "knowledge_distillation",
        "teacher_frozen": True,
        "teacher_loss": _finite_loss(teacher_loss),
        "student_loss": _finite_loss(loss),
    }


def synthetic_qualification(points: int = 96) -> dict[str, Any]:
    time = np.arange(points, dtype=np.float32)
    source_values = np.column_stack(
        [np.sin(time / 5.0), np.cos(time / 9.0), time / points]
    )
    target_values = np.column_stack(
        [np.sin(time / 5.0 + 0.3), np.cos(time / 9.0 - 0.2), time / points]
    )
    source = chronological_windows(source_values)
    target = chronological_windows(target_values)
    results = [
        qualify_masked_pretraining(source),
        qualify_contrastive_learning(source),
        qualify_multi_task_learning(source),
        qualify_transfer_learning(source, target),
        qualify_knowledge_distillation(source),
    ]
    for row in results:
        row.update(
            {
                "status": "synthetic_qualified",
                "account_wired": False,
                "production_eligible": False,
            }
        )
    return {
        "schema_version": 1,
        "execution_policy": "shadow_only_no_account_wiring",
        "split": {
            "train_target_max_index": source.train_target_max_index,
            "holdout_target_min_index": source.holdout_target_min_index,
            "chronological": source.train_target_max_index < source.holdout_target_min_index,
            "normalization_fit": "train_windows_only",
        },
        "results": results,
    }
