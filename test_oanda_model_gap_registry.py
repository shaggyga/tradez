from __future__ import annotations

import platform

import oanda_model_gap_registry as registry


def test_registry_contains_every_gap_family_and_no_account_wiring() -> None:
    report = registry.build_probe_report()
    models = {row["model_id"] for row in report["models"]}
    assert {
        "patchtst",
        "nbeats",
        "nhits",
        "lstm",
        "gru",
        "tcn",
        "chronos_2",
        "timesfm_2_5",
        "timesfm_icf",
        "moirai",
        "moirai_moe",
        "tiny_time_mixer",
        "toto_2",
        "lag_llama",
        "temporal_cross_pair_gnn",
        "s4",
        "mamba",
        "contextual_bandit",
        "ppo",
        "sac",
        "dqn",
        "self_supervised_pretraining",
        "contrastive_learning",
        "multi_task_learning",
        "transfer_learning",
        "knowledge_distillation",
    } <= models
    assert all(not row["account_wired"] for row in report["models"])


def test_evidence_order_is_strict() -> None:
    assert registry.evidence_at_least("runtime_available", "adapter_implemented")
    assert not registry.evidence_at_least("adapter_implemented", "synthetic_qualified")


def test_mamba_reports_the_official_host_constraint() -> None:
    spec = next(spec for spec in registry.MODEL_SPECS if spec.model_id == "mamba")
    probe = registry.probe_spec(spec)
    if platform.system() != "Linux":
        assert probe["runtime_status"] == "blocked"
        assert "Linux" in probe["runtime_reason"]
