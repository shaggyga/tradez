#!/usr/bin/env python3
"""Materialize an append-only genealogy of governed FX experiments.

This registry references the existing lifecycle/cohort databases rather than
replacing them.  Definitions never change; later results are appended as
observations.  The module is research-only and has no broker imports.
"""

from __future__ import annotations

import argparse
from collections import Counter
import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORTS = ROOT / "data" / "oanda_training_manager" / "reports" / "research_genealogy"
DEFAULT_DATABASE = STATE / "research_genealogy_v1.sqlite"
DEFAULT_STATE = STATE / "research_genealogy_v1.json"
DEFAULT_REPORT = REPORTS / "RESEARCH_GENEALOGY_CURRENT.md"


# This is intentionally duplicated from the frozen v4 engine instead of being
# imported from it.  The genealogy registrar is an independent trust boundary:
# changing the engine's own idea of its manifest cannot make an altered path
# acceptable to the append-only registry.
AFTER_COST_V4_MANIFEST_RELATIVE_PATH = (
    "config/currency_state_after_cost_counterfactual_v4_manifest.json"
)
AFTER_COST_V4_FROZEN_MANIFEST_FILE_SHA256 = (
    "3a2fe88ba8e330c92082324de307db075ce63b69b7c7b7e08a1e7dfca464b906"
)
AFTER_COST_V4_FROZEN_MANIFEST_SEMANTIC_SHA256 = (
    "e76780792f8e56960df668dd2fe032bce284481deb8a1ad45dbf1e74b97bef49"
)
AFTER_COST_V4_FROZEN_MODULE_SHA256 = (
    "98010415f01fb49d82d1be1bcc873e8070a46627bc5c43ae0198df4e2c496f5e"
)
AFTER_COST_V4_FROZEN_CONFIG_SHA256 = (
    "72c7cf64e2eaa5510e981239801393a1bb512cdb1feb1ae3c4ae2daa1745a03d"
)
AFTER_COST_V4_FROZEN_CLI_SHA256 = (
    "971a8d94502d5f23b7c43b299c491b3c295f63fe5583265fc569dcee7d9de825"
)
AFTER_COST_V4_EXPECTED_ARTIFACT_PATHS = {
    "counterfactual_v4_module": "src/forex_system/research/currency_state_after_cost_counterfactual_v4.py",
    "counterfactual_v4_config": "config/currency_state_after_cost_counterfactual_v4.json",
    "counterfactual_v4_cli": "oanda_currency_state_after_cost_counterfactual_v4.py",
    "counterfactual_v3_module": "src/forex_system/research/currency_state_after_cost_counterfactual_v3.py",
    "counterfactual_v3_config": "config/currency_state_after_cost_counterfactual_v3.json",
    "counterfactual_v3_cli": "oanda_currency_state_after_cost_counterfactual_v3.py",
    "counterfactual_v3_manifest": "config/currency_state_after_cost_counterfactual_v3_manifest.json",
    "counterfactual_v2_engine_module": "src/forex_system/research/currency_state_after_cost_counterfactual_v2.py",
    "input_envelope_module": "src/forex_system/research/after_cost_input_envelopes_v3.py",
    "input_envelope_config": "config/after_cost_input_envelopes_v3.json",
    "response_module": "src/forex_system/research/currency_state_response_timing_arms.py",
    "response_config": "config/currency_state_response_timing_arms_v1.json",
    "response_cli": "oanda_currency_state_response_timing_arms.py",
    "currency_state_module": "src/forex_system/features/currency_state_engine.py",
    "currency_state_config": "config/currency_state_engine_v2.json",
    "currency_state_cli": "oanda_currency_state_engine.py",
    "official_context_module": "src/forex_system/features/currency_state_official_context.py",
    "official_context_cli": "oanda_currency_state_official_context.py",
    "official_fact_adapter_module": "src/forex_system/ingestion/official_fact_adapter.py",
    "official_fact_adapter_cli": "oanda_official_fact_adapter.py",
    "official_fact_adapter_contract_manifest": "config/official_fact_adapter_contract_v1.json",
    "signed_exposure_module": "src/forex_system/contracts/signed_currency_exposure.py",
    "independent_verifier_producer": "oanda_independent_evidence_verifier.py",
    "quote_producer": "oanda_practice_quote_stream.py",
    "account_producer": "oanda_account_snapshot_writer.py",
}


SEQUENTIAL_ALL68_GENEALOGY_COHORT_SPECS = {
    "sequential_all68_portfolio_batch_replay_v1.ea3dd3438d15cea1a24b": {
        "source_pack_id": "sequential_replay_source_pack_v1.52259f543a3be80f3d3a",
        "result": "historical_engineering_diagnostic_nonhomogeneous_source_pack",
        "source_pack_state": "superseded_nonhomogeneous_historical_pack",
        "acceptance_schema": "legacy_pre_hardening_v1",
        "state_semantic_sha256": "3313c53d7dc4486927620a661007a3fe9574fd3cf4a9eeb56c4cfa2f6e4a15c2",
        "verifier_semantic_sha256": "f011a766e0a8cf86e774426fd00a2028191d620754ca007da796193beb4f4d19",
        "dataset_roots_sha256": "11aa35d0e23fdbc8cbf4980294f5cc29a63a848095a4e4e221932b3bdf882d8a",
    },
    "sequential_all68_portfolio_batch_replay_v1.445ddc96477b7ac284ed": {
        "source_pack_id": "sequential_replay_source_pack_v1.1bddc89d33ec30767c96",
        "result": "verified_historical_all68_batch_replay_corrected_source_pack",
        "source_pack_state": "corrected_homogeneous_exact_window_pack",
        "acceptance_schema": "legacy_pre_hardening_v1",
        "state_semantic_sha256": "8d67ca85c329391021e45c49547a5af66a55056a70da44674fbfa7ee621a8f6a",
        "verifier_semantic_sha256": "548a4a534786b8cccf920db27c73ff980ac6f8ea9d25f8794c94352d3f50f7a5",
        "dataset_roots_sha256": "ec80c858d71360a8165f52631363b322d819bcec134735535bc63891f95a925a",
    },
    "sequential_all68_portfolio_batch_replay_v1.7681e61bd31ac8877bd8": {
        "source_pack_id": "sequential_replay_source_pack_v1.ee6d6fd4d38744ecc1da",
        "result": "superseded_all68_replay_mutable_generation_timestamp",
        "source_pack_state": "explicit_safety_pack_pre_timestamp_immutability",
        "acceptance_schema": "strict_v2",
        "state_semantic_sha256": "96bf4d9453aaf23d565ae39b9208fd0c2ab799b20e2f376b5bad45a65a7a2408",
        "verifier_semantic_sha256": "d8fe14ae232b62bd9ddd6b52e8a7bcb0d214644cecc999fd57bbcb6aac99cf95",
        "dataset_roots_sha256": "0fd8997dc9ef453f7f09670bfe4ea055d9c1a7222a5550d399a4b69c09db82b6",
    },
    "sequential_all68_portfolio_batch_replay_v1.9d6e7e0f27ad289c4c83": {
        "source_pack_id": "sequential_replay_source_pack_v1.ee6d6fd4d38744ecc1da",
        "result": "verified_historical_all68_batch_replay_immutable_timestamp",
        "source_pack_state": "explicit_safety_exact_window_immutable_timestamp",
        "acceptance_schema": "strict_v3",
        "state_semantic_sha256": "bb50cbc9d51730c5b082465a92dca144a871fbf6f431695591686f5656a1cde8",
        "verifier_semantic_sha256": "4e98bd3c7ecace2700e505a924546c954c8ee75ace8490b3344bf4b109184dde",
        "dataset_roots_sha256": "26b3a71eedcbc79710a9b481002e9b270b732e7ef31848d35d214df006313569",
        "state_sha256": "a243c06d2d4d0647874f56fe732f88b63d288599f3341555e31146eaf04d3420",
        "verifier_sha256": "e78ce4ff7d45956124f41079aef7e2be681ca2dd6d6c7d92d692019154acb860",
    },
    "sequential_all68_portfolio_batch_replay_v1.efda27295d5107241262": {
        "source_pack_id": "sequential_replay_source_pack_v1.b9d1526f3dfa057bd06d",
        "result": "verified_historical_all68_batch_replay_canonical_gzip",
        "source_pack_state": "cross_runtime_canonical_gzip",
        "acceptance_schema": "strict_v3",
        "state_semantic_sha256": "c53f4ab530322257730ace9917c2a136dac687266a94b2af79c0dda04420bd93",
        "verifier_semantic_sha256": "e29548bd01b018e177256f89739859aa968f0521fb60d310e152eca29dbe55c5",
        "dataset_roots_sha256": "623430fa6acc16248427b6fdfaba1ffd99ef8931d79d2ac3dfcfcdcfb2d37dd0",
        "state_sha256": "5d9c76c4e03fa9c3878a17689aaac8643775729fdac526c4cabd802e376a0e8f",
        "verifier_sha256": "5e9e84e3dd8286b0f02ba5730994668bfcff8b7574a96e09e67fe53a3b9974cd",
    },
}


SEQUENTIAL_ALL68_MISTAKE_GENEALOGY_COHORT_SPECS = {
    "sequential_all68_mistake_curriculum_v1.283926823bce8ac804d7": {
        "result": "superseded_all68_mistake_curriculum_pre_timestamp_immutability",
        "require_current": False,
        "acceptance_schema": "semantic_parent_binding_v1",
        "parent_cohort_id": "sequential_all68_portfolio_batch_replay_v1.7681e61bd31ac8877bd8",
        "report_id": "a68mistakecurriculum_415aedfd5a98785ba425fc057d26",
        "report_content_sha256": "415aedfd5a98785ba425fc057d26db3ca07fca6271aa41b96e2ffb29a8363da9",
        "report_file_sha256": "750fa5dec4e5399479eb6529bf9dc9385c92c6ee01aa16fdbf6e22dc3d3f8d0d",
    },
    "sequential_all68_mistake_curriculum_v1.7712eb2214e1ebfe8013": {
        "result": "superseded_all68_mistake_curriculum_pre_raw_source_binding",
        "require_current": False,
        "acceptance_schema": "semantic_parent_binding_v2",
        "parent_cohort_id": "sequential_all68_portfolio_batch_replay_v1.9d6e7e0f27ad289c4c83",
        "report_id": "a68mistakecurriculum_cc17f527b71c69d5a743ed052ea7",
        "report_content_sha256": "cc17f527b71c69d5a743ed052ea78e9b91cb710992de87f3957190f7eed265bb",
        "report_file_sha256": "ca06b62380eebb1254aedb4bf180f6b0977bb6cbf0c18b647a796b0d10bcb8ea",
    },
    "sequential_all68_mistake_curriculum_v1.98755c2c2f9bd26b014a": {
        "result": "verified_historical_all68_mistake_curriculum_raw_source_bound",
        "require_current": False,
        "acceptance_schema": "strict_raw_parent_binding_v3",
        "parent_cohort_id": "sequential_all68_portfolio_batch_replay_v1.9d6e7e0f27ad289c4c83",
        "report_id": "a68mistakecurriculum_ce03dbb9ca777dacc82304c9b3f0",
        "report_content_sha256": "ce03dbb9ca777dacc82304c9b3f0620da840b93e6b6b26585be9857c1c3ba3ff",
        "report_file_sha256": "c5a63fe311b03f5bc59e25fa015233f9b5f1d64fe5efaac5375f69f41ec74842",
    },
    "sequential_all68_mistake_curriculum_v1.26b13486af3803244125": {
        "result": "verified_historical_all68_mistake_curriculum_canonical_gzip",
        "require_current": True,
        "acceptance_schema": "strict_raw_parent_binding_v3",
        "parent_cohort_id": "sequential_all68_portfolio_batch_replay_v1.efda27295d5107241262",
        "report_id": "a68mistakecurriculum_a3b4d3a281c74afcbb245816f572",
        "report_content_sha256": "a3b4d3a281c74afcbb245816f57290e751b09fc858a6eb62014a4a58726caa87",
        "report_file_sha256": "c7dd485cf5154b39bb56c8fc2aec1849cb3d0e17c7a40f46124864634e61b6fc",
    },
}


SEQUENTIAL_ALL68_POLICY_CHALLENGER_GENEALOGY_COHORT_SPECS = {
    "sequential_all68_policy_challenger_v1.efafd05f9f14a83ac946": {
        "result": "superseded_policy_challenger_pre_bounded_dataset_contract",
        "require_current": False,
        "material_sha256": "efafd05f9f14a83ac9467ca8c8ee2e18a535fead3f57b0b99cb4258bd317494d",
        "state_sha256": "db6558bc32d80a750d3195751176360c129ff769a7a5fbe6b85b32ba6e867cf8",
        "verifier_sha256": "468685b6ce757b7f9841015b213f9d73676564df8efbb42b31be0919616356d6",
        "report_sha256": "e211eaecbf5a9351f2e5fcd5b429327c7f1632a881b9ffd4736a7988cb5c75f4",
        "material_file_sha256": "5b200ab403684962b9c907f41847a44821cd3d6cc6dabb43bd295d1469d65c71",
        "dataset_manifest_sha256": "ca5e7e46119a8b3322382cac8b6b86c0e62dcbfc98e9a6cff0c8484f357bae99",
        "bounded_datasets_required": False,
    },
    "sequential_all68_policy_challenger_v1.ebb8b63d36ada93dca64": {
        "result": "superseded_policy_challenger_pre_complete_bound_source_link_contract",
        "require_current": False,
        "material_sha256": "ebb8b63d36ada93dca641b0888b93afc675a8cbbc6387a56cdf871f9ecd72f12",
        "state_sha256": "69c3e7171de480d88ae5b34b32a48598ed5a660180e4970ac5778e15f7eae99a",
        "verifier_sha256": "9ce13ea7c9679dbfd368341f2a4663fd07af6374dbb4b9f6f803fad10404d4f4",
        "report_sha256": "2374227a0e8f362acb92e499b3a21557c93251fcaf1016de0f8233ad65a246ec",
        "material_file_sha256": "cc85b2f9a8ed8e1f9b1d4a90c40c9110294d196241dd464ec0c09c787f4c0572",
        "dataset_manifest_sha256": "ce3ab77d8bbabc4e64f3c15d1e18ee4908fa470317815309a45631382ea9527e",
        "bounded_datasets_required": True,
    },
    "sequential_all68_policy_challenger_v1.a02365972fb81cba1f5f": {
        "result": "verified_historical_all68_policy_challenger_bounded_and_source_bound",
        "require_current": False,
        "material_sha256": "a02365972fb81cba1f5f2799ac75d295f5a246f2763be9514f6a82d5f91454c3",
        "state_sha256": "90c9f0d48d026aaf111272ada8c370c116be8390054ef58e0658289694ebbda7",
        "verifier_sha256": "c150eb071ae0b6227b6544f31dc0c465d959a6b56632daf6510939cee6808d80",
        "report_sha256": "84336cb1873d40bb3217de12367ae6efd4daf53a6dceec487093446fa69a3d81",
        "material_file_sha256": "8285f825f776c8f4dcf249f8b38ef3c532571b517b4e451cf261807333016026",
        "dataset_manifest_sha256": "53d2078f081837bbe9ab891d507c81cc2160dee2e3dd623e5a0e4170b0febe6f",
        "bounded_datasets_required": True,
    },
    "sequential_all68_policy_challenger_v1.0b5267c7a5c730a150cf": {
        "result": "verified_historical_all68_policy_challenger_canonical_gzip",
        "require_current": True,
        "material_sha256": "0b5267c7a5c730a150cf3c823c23516aade5655372d21d4964d4b532556fb9d2",
        "state_sha256": "653034521b9496db81bbf4a8ece59b2c52f1868860c90247f63a47e29aa5468e",
        "verifier_sha256": "9c55432b259e2255d9754e37e44c1b1b76925e15bfeff87189b674d1ec63b8b5",
        "report_sha256": "f1a512c9394922a191aa240a49e79bd11afdbe427ec6691fe22e1e512149a18e",
        "material_file_sha256": "8feb9801311ffd8391e10a685c3e97fce6f572999d73400458756abae65126d0",
        "dataset_manifest_sha256": "d8f7fe0c94fb59378575c622064135a4880937c1403a461a90070570834879c6",
        "bounded_datasets_required": True,
        "source_cohort_id": "sequential_all68_portfolio_batch_replay_v1.efda27295d5107241262",
        "source_material_sha256": "efda27295d510724126266eccf3c26137f5c5b078941e73a76f72b38359f0842",
        "source_pack_id": "sequential_replay_source_pack_v1.b9d1526f3dfa057bd06d",
        "mistake_cohort_id": "sequential_all68_mistake_curriculum_v1.26b13486af3803244125",
        "mistake_material_sha256": "26b13486af3803244125af4a46df0c3c18b0ae313ddcfa786839a562f5e9229a",
        "bound_files": {
            "bound_state.json": "5d9c76c4e03fa9c3878a17689aaac8643775729fdac526c4cabd802e376a0e8f",
            "bound_verifier_receipt.json": "5e9e84e3dd8286b0f02ba5730994668bfcff8b7574a96e09e67fe53a3b9974cd",
            "bound_source_pack_manifest.json": "71531c0f9beb5c62d88824814acb52fc820a098558becd81bf5bd47c3ee36c92",
            "bound_source_pack_clean_verifier_receipt.json": "660db3e6121977de7a31772dba7516c3b79f100a2211d83d303ed63fa9f0c124",
        },
    },
}

SEQUENTIAL_ALL68_POLICY_BOUND_FILE_SHA256 = {
    "bound_state.json": "a243c06d2d4d0647874f56fe732f88b63d288599f3341555e31146eaf04d3420",
    "bound_verifier_receipt.json": "e78ce4ff7d45956124f41079aef7e2be681ca2dd6d6c7d92d692019154acb860",
    "bound_source_pack_manifest.json": "7e7c1660be000f2c16f84c8616db9da3dd975fb7cb8d10feb1c926d3c6dedc74",
    "bound_source_pack_clean_verifier_receipt.json": "3a94d0aba8230c2d4f5f5d02ceb8ad20b30144ae70a7ac573e4bf2e851670549",
}

SEQUENTIAL_ALL68_POLICY_EXPANSION_SPEC = {
    "cohort_id": "sequential_all68_policy_expansion_v1.bd9d43742d8ee85ba52b",
    "require_current": False,
    "material_sha256": "bd9d43742d8ee85ba52b9f18d49ba3ce67fcb09fee8e6e9ae47e45e7df0427a5",
    "state_sha256": "5ccab539a97bde94a3074f3e93d7732e159026cd96a255184ee00977196b6b40",
    "verifier_sha256": "b39f4dd4726095ba77c7b6a911cd8586ac4af3a976fab49d076ec6699723f23e",
    "report_sha256": "72ac5b4665cf35b3eb67103d113300581d70b272a976173f2ca0d47dc2f5d5d7",
    "material_file_sha256": "cd4ddacb5f8f338821013828b815bf383f5319ff45514520be2cdf2943cb4312",
    "dataset_manifest_sha256": "abc2d8efbcb40326aed4d2daf6bcc78c7802b391ad765263dce4f59ed5f11410",
    "source_cohort_id": "seq_a68_wed_exp_v1.92bb0e964d2639b7e897",
    "source_material_sha256": "92bb0e964d2639b7e8976e1dbe439d60f1221630db5dc7149bb2d5358738d4d2",
    "source_pack_id": "sequential_replay_source_pack_v1.5cfcd2011d3b34fe4bb2",
    "source_pack_material_sha256": "5cfcd2011d3b34fe4bb2ff1da6d62fba54d87b68f70fbf9212a51b98dc0624bb",
    "bound_files": {
        "bound_state.json": "6ed030abcc4a3a4394ebe036d46c3a97ef22530226d862b20d139c318237d1f4",
        "bound_verifier_receipt.json": "8c30988d4dfad83de478080f1e7be3147a3cc5fda6c894c21be57a4a4ae52638",
        "bound_source_pack_manifest.json": "4a31de5abde2a05506c6d703f5f1fbe5a7bd1c32a60466373d9ddcc11f45b11b",
        "bound_source_pack_clean_verifier_receipt.json": "c1990bcf415147c425856dd2fb5e98d96afb92428abfc2484b48d73034e065e0",
    },
}
SEQUENTIAL_ALL68_POLICY_EXPANSION_SPECS = {
    SEQUENTIAL_ALL68_POLICY_EXPANSION_SPEC["cohort_id"]: SEQUENTIAL_ALL68_POLICY_EXPANSION_SPEC,
    "sequential_all68_policy_expansion_v1.15a3aa5d039df7df9a7d": {
        "cohort_id": "sequential_all68_policy_expansion_v1.15a3aa5d039df7df9a7d",
        "require_current": True,
        "material_sha256": "15a3aa5d039df7df9a7d41ccf3368252ffb85d9d2c0f14578b1fbc52bfd82c8c",
        "state_sha256": "4058a64c398a482858759f663d674c7d7fc7a637a85d31dbf43467dc8f7947c3",
        "verifier_sha256": "230d69b87f6cefbebd3bd27e08d38a9844ca211130436c85acbdf3dcc9928202",
        "report_sha256": "26f91f871bc1be7ec9ca73decf980ed70bda9587cb59da153788df0e0a33821c",
        "material_file_sha256": "26c6305135497c22217e722db36306e41f55165dc99f7e36e0de6acc116749de",
        "dataset_manifest_sha256": "74226deb3900637077cf2b7c3d7aa36626855df53e93d875087ce2a1a0804647",
        "source_cohort_id": "seq_a68_wed_exp_v1.e7de4ecdd0255eb13306",
        "source_material_sha256": "e7de4ecdd0255eb133066895abca348861681077d0c309e1a57d1820276eebea",
        "source_pack_id": "sequential_replay_source_pack_v1.554c8f74212202aa9b86",
        "source_pack_material_sha256": "554c8f74212202aa9b86e31cedf186dbee3d65e25aa7bda1ca737ec2d3c47300",
        "bound_files": {
            "bound_state.json": "da2c46f6bb086ee751ccb62ab7a5218e12a7457920d26a5b7ae85a098a884338",
            "bound_verifier_receipt.json": "bb0cbe65aec82a0fcffb4f2bdb32c84e48ff8eba2047d84fd80276732e0b3f2c",
            "bound_source_pack_manifest.json": "9c9a386474917cc50ff98967339d6a30f8d313d820b5b1b79c08123033252574",
            "bound_source_pack_clean_verifier_receipt.json": "e80e35d9b6f417409306208b4df1d772f34976e38eddce37d17dfa705a6dc16f",
        },
    },
}


# These two packs predate the explicit proof/broker/account safety schema.  They
# remain useful only to preserve the exact parents of already reviewed replay
# diagnostics.  Their bytes are pinned here; they never pass the current pack
# acceptance gate and are registered as quarantined historical lineage.
SEQUENTIAL_LEGACY_SOURCE_PACK_SPECS = {
    "sequential_replay_source_pack_v1.52259f543a3be80f3d3a": {
        "manifest_sha256": "73eb35c678ee9801133b57fa1fc8622756033e7242abb083b146529188758bb8",
        "lineage_state": "superseded_nonhomogeneous_pre_safety_schema",
    },
    "sequential_replay_source_pack_v1.1bddc89d33ec30767c96": {
        "manifest_sha256": "edeffa2e5a824b8690c9320a19394e8b1581afad4057f5cc436b48e78fe6ba4d",
        "lineage_state": "superseded_homogeneous_pre_safety_schema",
    },
}


FULL_RESEARCH_ONLY_SAFETY = {
    "research_only": True,
    "execution_eligible": False,
    "proof_eligible": False,
    "can_promote": False,
    "can_place_orders": False,
    "can_authorize": False,
    "broker_access": False,
    "account_access": False,
    "supported_decision": "no_trade",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    _replace_with_retry(temporary, path)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    _replace_with_retry(temporary, path)


def _replace_with_retry(temporary: Path, path: Path) -> None:
    for attempt in range(8):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt == 7:
                temporary.unlink(missing_ok=True)
                raise
            time.sleep(min(0.4, 0.025 * (2**attempt)))


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS experiments (
            hypothesis_id TEXT PRIMARY KEY,
            parent_hypothesis_id TEXT,
            experiment_kind TEXT NOT NULL,
            research_generation TEXT NOT NULL,
            idea_origin TEXT NOT NULL,
            pre_registered INTEGER NOT NULL,
            data_sources_json TEXT NOT NULL,
            feature_contract_json TEXT NOT NULL,
            label_contract_json TEXT NOT NULL,
            model_contract_json TEXT NOT NULL,
            cost_contract_json TEXT NOT NULL,
            allocator_contract_json TEXT NOT NULL,
            training_period_json TEXT NOT NULL,
            selection_period_json TEXT NOT NULL,
            confirmation_period_json TEXT NOT NULL,
            all_parameters_tried_json TEXT NOT NULL,
            selection_rule TEXT NOT NULL,
            holdouts_touched_json TEXT NOT NULL,
            source_code_hash TEXT NOT NULL,
            data_snapshot_hash TEXT NOT NULL,
            definition_sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            definition_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS experiment_observations (
            observation_id TEXT PRIMARY KEY,
            hypothesis_id TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            source_system TEXT NOT NULL,
            result TEXT NOT NULL,
            retirement_reason TEXT,
            retired_at TEXT,
            evidence_sha256 TEXT NOT NULL,
            evidence_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_genealogy_observations_latest
            ON experiment_observations(hypothesis_id,observed_at DESC);
        CREATE TABLE IF NOT EXISTS genealogy_imports (
            import_id TEXT PRIMARY KEY,
            imported_at TEXT NOT NULL,
            source_database TEXT NOT NULL,
            source_fingerprint TEXT NOT NULL,
            definition_count INTEGER NOT NULL,
            observation_count INTEGER NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS experiments_no_update
            BEFORE UPDATE ON experiments BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS experiments_no_delete
            BEFORE DELETE ON experiments BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS experiment_observations_no_update
            BEFORE UPDATE ON experiment_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        CREATE TRIGGER IF NOT EXISTS experiment_observations_no_delete
            BEFORE DELETE ON experiment_observations BEGIN SELECT RAISE(ABORT,'append_only'); END;
        """
    )
    connection.commit()
    return connection


EXPERIMENT_COLUMNS = (
    "hypothesis_id", "parent_hypothesis_id", "experiment_kind",
    "research_generation", "idea_origin", "pre_registered",
    "data_sources_json", "feature_contract_json", "label_contract_json",
    "model_contract_json", "cost_contract_json", "allocator_contract_json",
    "training_period_json", "selection_period_json", "confirmation_period_json",
    "all_parameters_tried_json", "selection_rule", "holdouts_touched_json",
    "source_code_hash", "data_snapshot_hash", "definition_sha256", "created_at",
    "definition_json",
)

OBSERVATION_COLUMNS = (
    "observation_id", "hypothesis_id", "observed_at", "source_system",
    "result", "retirement_reason", "retired_at", "evidence_sha256",
    "evidence_json",
)


def insert_experiment(connection: sqlite3.Connection, value: dict[str, Any]) -> bool:
    existing = connection.execute(
        "SELECT definition_sha256 FROM experiments WHERE hypothesis_id=?",
        (value.get("hypothesis_id"),),
    ).fetchone()
    if existing is not None:
        if str(existing[0]) != str(value.get("definition_sha256")):
            raise ValueError("hypothesis_id definition conflict")
        return False
    before = connection.total_changes
    connection.execute(
        f"INSERT OR IGNORE INTO experiments ({','.join(EXPERIMENT_COLUMNS)}) VALUES ({','.join('?' for _ in EXPERIMENT_COLUMNS)})",
        tuple(value.get(column) for column in EXPERIMENT_COLUMNS),
    )
    return connection.total_changes > before


def observe(
    connection: sqlite3.Connection, *, hypothesis_id: str, observed_at: str,
    source_system: str, result: str, evidence: dict[str, Any],
    retirement_reason: str | None = None, retired_at: str | None = None,
) -> bool:
    evidence_sha = stable_hash(evidence)
    observation_id = "genealogy_observation_" + stable_hash(
        (hypothesis_id, observed_at, source_system, result, evidence_sha)
    )[:28]
    before = connection.total_changes
    connection.execute(
        "INSERT OR IGNORE INTO experiment_observations VALUES (?,?,?,?,?,?,?,?,?)",
        (
            observation_id, hypothesis_id, observed_at, source_system, result,
            retirement_reason, retired_at, evidence_sha, canonical_json(evidence),
        ),
    )
    return connection.total_changes > before


def ro(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=10.0
    )
    connection.row_factory = sqlite3.Row
    return connection


def cohort_parent_map(connection: sqlite3.Connection, table: str) -> dict[str, str | None]:
    output: dict[str, str | None] = {}
    for row in connection.execute(
        f"SELECT previous_cohort_id,next_cohort_id FROM {table} ORDER BY observed_utc"
    ):
        output[str(row["next_cohort_id"])] = (
            str(row["previous_cohort_id"]) if row["previous_cohort_id"] else None
        )
    return output


def import_proof_registry(
    target: sqlite3.Connection, source: Path, *, kind: str, imported_at: str
) -> tuple[int, int]:
    if not source.is_file():
        return 0, 0
    connection = ro(source)
    definitions = observations = 0
    try:
        parents = cohort_parent_map(connection, "proof_cohort_transitions")
        for row in connection.execute("SELECT * FROM proof_cohorts ORDER BY cohort_start_utc"):
            contract = json.loads(str(row["contract_json"] or "{}"))
            cohort_id = str(row["cohort_id"])
            definition = {
                "hypothesis_id": cohort_id,
                "parent_hypothesis_id": parents.get(cohort_id),
                "experiment_kind": kind,
                "research_generation": "prospective_proof_generation_20260806",
                "idea_origin": str(row["family"]),
                "pre_registered": 1,
                "data_sources_json": canonical_json(["OANDA_executable_bid_ask", "OANDA_completed_candles"]),
                "feature_contract_json": canonical_json({"feature_schema_version": row["feature_schema_version"]}),
                "label_contract_json": canonical_json({"forecast_contract_version": row["forecast_contract_version"]}),
                "model_contract_json": canonical_json(contract.get("model_specification") or contract.get("hyperparameters") or {}),
                "cost_contract_json": canonical_json({"cost_model_version": row["cost_model_version"]}),
                "allocator_contract_json": "{}",
                "training_period_json": canonical_json({"cutoff": row["training_cutoff_utc"]}),
                "selection_period_json": canonical_json({"starts": row["cohort_start_utc"]}),
                "confirmation_period_json": canonical_json({"untouched_later_cohort_required": True}),
                "all_parameters_tried_json": canonical_json(contract.get("hyperparameters") or {}),
                "selection_rule": "frozen_material_contract; no interim retuning",
                "holdouts_touched_json": canonical_json([]),
                "source_code_hash": str(row["source_sha256"]),
                "data_snapshot_hash": str(row["training_dataset_sha256"]),
                "definition_sha256": str(row["material_contract_sha256"]),
                "created_at": str(row["cohort_start_utc"]),
                "definition_json": canonical_json(contract),
            }
            definitions += int(insert_experiment(target, definition))
            observations += int(observe(
                target, hypothesis_id=cohort_id, observed_at=str(row["cohort_start_utc"]),
                source_system=source.name, result="continue_collecting",
                evidence={"family": row["family"], "cohort_id": cohort_id},
            ))
    finally:
        connection.close()
    return definitions, observations


def import_allocator(
    target: sqlite3.Connection, source: Path, *, imported_at: str
) -> tuple[int, int]:
    if not source.is_file():
        return 0, 0
    connection = ro(source)
    definitions = observations = 0
    try:
        parents = cohort_parent_map(connection, "allocator_cohort_transitions")
        latest = {
            str(row["cohort_id"]): dict(row)
            for row in connection.execute(
                """SELECT event.cohort_id,event.next_state,event.observed_utc FROM allocator_lifecycle_events event
                   JOIN (SELECT cohort_id,MAX(rowid) rowid FROM allocator_lifecycle_events GROUP BY cohort_id) last
                     ON last.rowid=event.rowid"""
            )
        }
        for row in connection.execute("SELECT * FROM allocator_cohorts ORDER BY cohort_start_utc"):
            contract = json.loads(str(row["contract_json"] or "{}"))
            cohort_id = str(row["cohort_id"])
            definition = {
                "hypothesis_id": cohort_id, "parent_hypothesis_id": parents.get(cohort_id),
                "experiment_kind": "allocator_policy", "research_generation": "allocator_proof_generation_20260806",
                "idea_origin": str(row["policy_id"]), "pre_registered": 1,
                "data_sources_json": canonical_json(["governed_candidate_set", "OANDA_executable_bid_ask"]),
                "feature_contract_json": "{}", "label_contract_json": canonical_json({"policy_outcome": "executable_bid_ask_at_declared_horizon"}),
                "model_contract_json": "{}", "cost_contract_json": canonical_json(contract.get("cost_contract") or {}),
                "allocator_contract_json": canonical_json(contract), "training_period_json": "{}",
                "selection_period_json": canonical_json({"starts": row["cohort_start_utc"]}),
                "confirmation_period_json": canonical_json({"later_untouched_confirmation_required": True}),
                "all_parameters_tried_json": canonical_json(contract),
                "selection_rule": "frozen top-one allocator with six comparator arms",
                "holdouts_touched_json": canonical_json([]), "source_code_hash": str(row["source_sha256"]),
                "data_snapshot_hash": "prospective_decision_stream", "definition_sha256": str(row["material_contract_sha256"]),
                "created_at": str(row["cohort_start_utc"]), "definition_json": canonical_json(contract),
            }
            definitions += int(insert_experiment(target, definition))
            current = latest.get(cohort_id)
            observations += int(observe(
                target, hypothesis_id=cohort_id,
                observed_at=(str(current["observed_utc"]) if current else str(row["cohort_start_utc"])),
                source_system=source.name,
                result=(str(current["next_state"]) if current else "continue_collecting"),
                evidence={"phase": row["phase"], "cohort_id": cohort_id},
            ))
    finally:
        connection.close()
    return definitions, observations


def import_lifecycle(
    target: sqlite3.Connection, source: Path, *, imported_at: str
) -> tuple[int, int]:
    if not source.is_file():
        return 0, 0
    connection = ro(source)
    definitions = observations = 0
    try:
        # The lifecycle contains tens of thousands of governed cells.  The
        # former importer issued one target lookup per definition and one
        # duplicate insert per current observation on every pass.  On the live
        # Windows database that exceeded the supervision window, so genealogy
        # never caught up.  Load the immutable append-only keys once and only
        # touch rows that are actually new or changed.
        existing_definitions = {
            str(row[0]): str(row[1])
            for row in target.execute(
                "SELECT hypothesis_id,definition_sha256 FROM experiments "
                "WHERE experiment_kind='governed_cell'"
            )
        }
        existing_observations = {
            (str(row[0]), str(row[1]), str(row[2]), str(row[3]))
            for row in target.execute(
                "SELECT hypothesis_id,observed_at,result,evidence_sha256 "
                "FROM experiment_observations WHERE source_system=?",
                (source.name,),
            )
        }
        retirement = {
            str(row["hypothesis_id"]): dict(row)
            for row in connection.execute("SELECT * FROM futility_retirements")
        }
        latest = {
            str(row["hypothesis_id"]): dict(row)
            for row in connection.execute(
                """SELECT event.* FROM lifecycle_events event
                   JOIN (SELECT hypothesis_id,MAX(rowid) rowid FROM lifecycle_events GROUP BY hypothesis_id) last
                     ON last.rowid=event.rowid"""
            )
        }
        for row in connection.execute("SELECT * FROM hypotheses ORDER BY first_seen_utc"):
            hypothesis_id = str(row["hypothesis_id"])
            definition_sha256 = str(row["definition_sha256"])
            definition_json = str(row["definition_json"] or "{}")
            existing_sha = existing_definitions.get(hypothesis_id)
            if existing_sha is not None and existing_sha != definition_sha256:
                raise ValueError("hypothesis_id definition conflict")
            if existing_sha is None:
                definition = {
                    "hypothesis_id": hypothesis_id, "parent_hypothesis_id": None,
                    "experiment_kind": "governed_cell", "research_generation": str(row["evidence_contract_id"]),
                    "idea_origin": str(row["family"]), "pre_registered": int(bool(row["cohort_id"])),
                    "data_sources_json": canonical_json(["canonical_forecast_and_outcome_ledger"]),
                    "feature_contract_json": canonical_json({"cell_id": row["cell_id"]}),
                    "label_contract_json": canonical_json({"horizon_sec": row["horizon_sec"]}),
                    "model_contract_json": canonical_json({"family": row["family"], "cohort_id": row["cohort_id"]}),
                    "cost_contract_json": canonical_json({"liquidity_bucket": row["liquidity_bucket"]}),
                    "allocator_contract_json": "{}", "training_period_json": "{}",
                    "selection_period_json": canonical_json({"first_seen": row["first_seen_utc"]}),
                    "confirmation_period_json": canonical_json({"untouched_confirmation_required": True}),
                    "all_parameters_tried_json": canonical_json({"historical_parameter_inventory": "not_reconstructed"}),
                    "selection_rule": "family x pair x horizon x session x liquidity",
                    "holdouts_touched_json": canonical_json([]), "source_code_hash": "referenced_by_evidence_contract",
                    "data_snapshot_hash": definition_sha256, "definition_sha256": definition_sha256,
                    "created_at": str(row["first_seen_utc"]), "definition_json": definition_json,
                }
                definitions += int(insert_experiment(target, definition))
                existing_definitions[hypothesis_id] = definition_sha256
            current = latest.get(hypothesis_id)
            if current:
                retired = retirement.get(hypothesis_id)
                evidence = json.loads(str(current.get("evidence_json") or "{}"))
                observation_key = (
                    hypothesis_id,
                    str(current["observed_utc"]),
                    str(current["next_state"]),
                    stable_hash(evidence),
                )
                if observation_key not in existing_observations:
                    observations += int(observe(
                        target, hypothesis_id=hypothesis_id,
                        observed_at=str(current["observed_utc"]), source_system=source.name,
                        result=str(current["next_state"]), evidence=evidence,
                        retirement_reason=(str(retired["boundary_method"]) if retired else None),
                        retired_at=(str(retired["retired_utc"]) if retired else None),
                    ))
                    existing_observations.add(observation_key)
    finally:
        connection.close()
    return definitions, observations


def import_external_discovery_report(
    target: sqlite3.Connection,
    source: Path,
    *,
    source_family: str,
) -> tuple[int, int]:
    """Register each fixed rule/horizon from a historical discovery report.

    These reports can reject or nominate a new prospective hypothesis, but
    they can never create lifecycle confirmation or execution eligibility.
    The immutable source/candle fingerprint is part of the hypothesis ID so a
    materially different replay cannot overwrite this experiment.
    """
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    summaries = payload.get("summaries") or []
    if not isinstance(summaries, list):
        return 0, 0
    source_hashes = payload.get("source_hashes") or {}
    candle_hashes = payload.get("candle_hashes") or {}
    data_snapshot_hash = stable_hash(
        {"source_hashes": source_hashes, "candle_hashes": candle_hashes}
    )
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for raw in summaries:
        if not isinstance(raw, dict):
            continue
        rule = str(raw.get("rule") or "")
        try:
            horizon = int(raw.get("horizon_trading_days"))
        except (TypeError, ValueError):
            continue
        if rule:
            grouped.setdefault((rule, horizon), []).append(dict(raw))
    definitions = observations = 0
    observed_at = str(payload.get("generated_utc") or utc_now())
    for (rule, horizon), evidence_rows in sorted(grouped.items()):
        selected = any(bool(row.get("discovery_candidate")) for row in evidence_rows)
        hypothesis_id = (
            f"{source_family}.{rule}.h{horizon}d.discovery."
            f"{data_snapshot_hash[:16]}"
        )
        definition_contract = {
            "source_family": source_family,
            "rule": rule,
            "horizon_trading_days": horizon,
            "availability_contract": payload.get("availability_contract"),
            "evidence_class": payload.get("evidence_class"),
            "proof_eligible": False,
            "execution_eligible": False,
        }
        definition_sha = stable_hash(definition_contract)
        definition = {
            "hypothesis_id": hypothesis_id,
            "parent_hypothesis_id": None,
            "experiment_kind": "external_source_historical_discovery",
            "research_generation": "orthogonal_source_discovery_20260808",
            "idea_origin": source_family,
            "pre_registered": 1,
            "data_sources_json": canonical_json(
                [source_family, "OANDA_practice_GET_only_daily_bid_ask"]
            ),
            "feature_contract_json": canonical_json(
                {"rule": rule, "availability_contract": payload.get("availability_contract")}
            ),
            "label_contract_json": canonical_json(
                {"horizon_trading_days": horizon, "target": "executable_after_cost_return"}
            ),
            "model_contract_json": canonical_json({"transparent_rule": rule}),
            "cost_contract_json": canonical_json(
                {"entry": "executable_bid_or_ask", "exit": "executable_bid_or_ask"}
            ),
            "allocator_contract_json": "{}",
            "training_period_json": "{}",
            "selection_period_json": canonical_json(payload.get("date_range") or {}),
            "confirmation_period_json": canonical_json(
                {"required": "new_immutable_prospective_source_cohort"}
            ),
            "all_parameters_tried_json": canonical_json(
                sorted({
                    (str(row.get("rule")), int(row.get("horizon_trading_days")))
                    for row in summaries
                    if isinstance(row, dict) and row.get("rule") and row.get("horizon_trading_days")
                })
            ),
            "selection_rule": "split, multiplicity, concentration, and cross-period stability gates",
            "holdouts_touched_json": canonical_json(["retrospective_holdout"]),
            "source_code_hash": "recorded_by_dated_discovery_artifact",
            "data_snapshot_hash": data_snapshot_hash,
            "definition_sha256": definition_sha,
            "created_at": observed_at,
            "definition_json": canonical_json(definition_contract),
        }
        definitions += int(insert_experiment(target, definition))
        result = (
            "historical_discovery_candidate_requires_prospective_cohort"
            if selected
            else "historical_discovery_rejected"
        )
        observations += int(observe(
            target,
            hypothesis_id=hypothesis_id,
            observed_at=observed_at,
            source_system=source.name,
            result=result,
            retirement_reason=(
                None if selected else "failed_split_multiplicity_concentration_or_cross_period_gate"
            ),
            retired_at=(None if selected else observed_at),
            evidence={
                "report_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                "evidence_rows": evidence_rows,
                "limitations": payload.get("limitations") or [],
                "supported_action": payload.get("supported_action"),
            },
        ))
    return definitions, observations


def import_internal_discovery_artifact(
    target: sqlite3.Connection,
    source: Path,
    *,
    research_id_override: str | None = None,
) -> tuple[int, int]:
    """Register a bounded internal archive discovery without implying proof."""
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    research_id = str(payload.get("research_id") or research_id_override or "")
    if not research_id:
        return 0, 0
    snapshot_hash = stable_hash({
        "config_sha256": payload.get("config_sha256") or (payload.get("contracts") or {}).get("config_sha256"),
        "source_manifest_sha256": payload.get("source_manifest_sha256") or (payload.get("contracts") or {}).get("price_manifest_sha256"),
        "source_event_highwater_utc": payload.get("source_event_highwater_utc"),
        "contract_id": payload.get("contract_id"),
    })
    hypothesis_id = f"{research_id}.archive_discovery.{snapshot_hash[:16]}"
    definition_contract = {
        "research_id": research_id,
        "evidence_class": payload.get("evidence_class"),
        "research_only": True,
        "execution_eligible": False,
        "source": source.name,
    }
    observed_at = str(payload.get("generated_utc") or utc_now())
    definition = {
        "hypothesis_id": hypothesis_id,
        "parent_hypothesis_id": None,
        "experiment_kind": "internal_archive_discovery",
        "research_generation": "proof_first_internal_discovery_20260809",
        "idea_origin": research_id,
        "pre_registered": 1,
        "data_sources_json": canonical_json(["OANDA_practice_BAM_bid_ask", "point_in_time_source_governance"]),
        "feature_contract_json": canonical_json({
            "price_features": payload.get("features") or payload.get("price_features") or [],
            "source_features": payload.get("source_features") or [],
        }),
        "label_contract_json": canonical_json({"target": "executable_after_cost_cost_clearance_or_movement_episode"}),
        "model_contract_json": canonical_json({"research_id": research_id}),
        "cost_contract_json": canonical_json({"entry_exit": "recorded_bid_ask", "proof_eligible": False}),
        "allocator_contract_json": canonical_json({"can_place_orders": False}),
        "training_period_json": canonical_json({"archive_discovery": True}),
        "selection_period_json": canonical_json({"archive_already_inspected": True}),
        "confirmation_period_json": canonical_json({"required": "new_untouched_cohort"}),
        "all_parameters_tried_json": "{}",
        "selection_rule": "predeclared archive diagnostics with candidate lock fail closed",
        "holdouts_touched_json": canonical_json(["retrospective_holdout"]),
        "source_code_hash": str(payload.get("source_code_sha256") or "recorded_in_artifact_contract"),
        "data_snapshot_hash": snapshot_hash,
        "definition_sha256": stable_hash(definition_contract),
        "created_at": observed_at,
        "definition_json": canonical_json(definition_contract),
    }
    definitions = int(insert_experiment(target, definition))
    result = "archive_discovery_recorded_no_proof_candidate"
    observations = int(observe(
        target,
        hypothesis_id=hypothesis_id,
        observed_at=observed_at,
        source_system=source.name,
        result=result,
        retirement_reason=None,
        retired_at=None,
        evidence={
            "proof_eligible": False,
            "can_place_orders": False,
            "results": payload.get("results") or payload.get("horizons") or [],
            "matched_control_comparison": payload.get("matched_control_comparison") or [],
            "technical_confirmation_by_horizon": payload.get("technical_confirmation_by_horizon") or {},
            "by_horizon": payload.get("by_horizon") or {},
        },
    ))
    return definitions, observations


def import_macro_point_in_time_validation(
    target: sqlite3.Connection,
    source: Path,
) -> tuple[int, int]:
    """Register historical initial-release checks without mutating live cohorts."""
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    observed_at = str(payload.get("generated_utc") or utc_now())
    definitions = observations = 0
    for raw in payload.get("candidates") or []:
        if not isinstance(raw, dict):
            continue
        candidate = raw.get("candidate") or {}
        candidate_id = str(candidate.get("candidate_id") or "")
        if not candidate_id:
            continue
        hypothesis_id = f"macro_point_in_time.{candidate_id}.robustness.20260816"
        contract = {
            "candidate": candidate,
            "availability_contract": "ALFRED_output_type_4_initial_release_next_day_clock",
            "historical_results_can_confirm": False,
            "execution_eligible": False,
        }
        evidence = {
            "robustness_state": raw.get("robustness_state"),
            "current_view_counterfactual": raw.get("current_view_counterfactual") or {},
            "initial_release_point_in_time": raw.get("initial_release_point_in_time") or {},
            "point_in_time_technical_aligned": raw.get("point_in_time_technical_aligned") or {},
            "prospective_cohorts_remain_unchanged": payload.get("prospective_cohorts_remain_unchanged"),
            "conclusion": payload.get("conclusion"),
        }
        definition = {
            "hypothesis_id": hypothesis_id,
            "parent_hypothesis_id": None,
            "experiment_kind": "historical_point_in_time_robustness",
            "research_generation": "macro_initial_release_validation_20260816",
            "idea_origin": str(candidate.get("signal_rule") or candidate_id),
            "pre_registered": 1,
            "data_sources_json": canonical_json([
                "FRED_ALFRED_initial_release_output_type_4",
                "OANDA_practice_GET_only_completed_bid_ask",
            ]),
            "feature_contract_json": canonical_json(candidate),
            "label_contract_json": canonical_json({
                "target": "executable_after_cost_return",
                "horizon_hours": candidate.get("horizon_hours"),
            }),
            "model_contract_json": canonical_json({"transparent_rule": candidate.get("signal_rule")}),
            "cost_contract_json": canonical_json({"entry_exit": "executable_bid_ask"}),
            "allocator_contract_json": canonical_json({"execution_eligible": False}),
            "training_period_json": "{}",
            "selection_period_json": canonical_json({"fixed_before_point_in_time_replay": True}),
            "confirmation_period_json": canonical_json({"required": "later_untouched_prospective_releases"}),
            "all_parameters_tried_json": canonical_json({
                "threshold": candidate.get("threshold"),
                "liquidity_bucket": candidate.get("liquidity_bucket"),
            }),
            "selection_rule": "fixed current-view candidate replayed on initial-release vintages",
            "holdouts_touched_json": canonical_json(["historical_initial_release_replay"]),
            "source_code_hash": "recorded_by_macro_point_in_time_validation_artifact",
            "data_snapshot_hash": stable_hash(evidence),
            "definition_sha256": stable_hash(contract),
            "created_at": observed_at,
            "definition_json": canonical_json(contract),
        }
        definitions += int(insert_experiment(target, definition))
        result = str(raw.get("robustness_state") or "unavailable")
        observations += int(observe(
            target,
            hypothesis_id=hypothesis_id,
            observed_at=observed_at,
            source_system=source.name,
            result=result,
            retirement_reason=(
                "historical_current_view_candidate_failed_initial_release_replay"
                if result.startswith("failed_") else None
            ),
            retired_at=(observed_at if result.startswith("failed_") else None),
            evidence=evidence,
        ))
    return definitions, observations


def import_zero_output_adapter_retirement(target: sqlite3.Connection) -> tuple[int, int]:
    """Formally retire the disabled intrasecond adapter without statistical claims."""
    hypothesis_id = "intrasecond_ridge.zero_valid_outputs.engineering_retired.20260809"
    contract = {
        "family": "intrasecond_ridge",
        "adapter": "second_ridge",
        "expected_runtime_output": False,
        "account_eligible": False,
        "reason": "zero valid outputs; disabled timing-only adapter",
        "reconsideration": "new validated input and output contract requires a new hypothesis ID",
    }
    definition = {
        "hypothesis_id": hypothesis_id,
        "parent_hypothesis_id": None,
        "experiment_kind": "adapter_retirement",
        "research_generation": "source_audit_closure_20260809",
        "idea_origin": "intrasecond_ridge",
        "pre_registered": 0,
        "data_sources_json": canonical_json(["quote_intensity"]),
        "feature_contract_json": "{}",
        "label_contract_json": "{}",
        "model_contract_json": canonical_json(contract),
        "cost_contract_json": "{}",
        "allocator_contract_json": canonical_json({"account_eligible": False}),
        "training_period_json": "{}",
        "selection_period_json": "{}",
        "confirmation_period_json": "{}",
        "all_parameters_tried_json": "{}",
        "selection_rule": "engineering retirement after zero valid outputs",
        "holdouts_touched_json": "[]",
        "source_code_hash": "registered_runtime_inventory",
        "data_snapshot_hash": stable_hash(contract),
        "definition_sha256": stable_hash(contract),
        "created_at": "2026-08-09T00:00:00+00:00",
        "definition_json": canonical_json(contract),
    }
    definitions = int(insert_experiment(target, definition))
    observations = int(observe(
        target,
        hypothesis_id=hypothesis_id,
        observed_at="2026-08-09T00:00:00+00:00",
        source_system="source_audit_closure",
        result="engineering_retired_zero_valid_outputs",
        retirement_reason="zero valid outputs and no active structural-entry role",
        retired_at="2026-08-09T00:00:00+00:00",
        evidence=contract,
    ))
    return definitions, observations


def import_gdelt_mapping_report(
    target: sqlite3.Connection,
    source: Path,
) -> tuple[int, int]:
    """Register the dated GDELT direction nulls and magnitude discovery.

    Direction and magnitude are deliberately separate hypotheses.  A large
    post-news move does not imply that generic article tone predicts its side.
    The dated replay can only retire/nominate discovery hypotheses; it cannot
    confirm or authorize anything.
    """
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    report_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    data_snapshot_hash = stable_hash({
        "report_sha256": report_sha,
        "candle_hashes": payload.get("candle_hashes") or {},
        "source_ids": payload.get("source_ids") or [],
    })
    observed_at = str(payload.get("generated_utc") or utc_now())
    definitions = observations = 0
    rows: list[tuple[str, str, int, dict[str, Any], bool]] = []
    for raw in payload.get("arm_summaries") or []:
        if isinstance(raw, dict) and raw.get("arm") and raw.get("horizon_minutes"):
            rows.append((
                "direction", str(raw["arm"]), int(raw["horizon_minutes"]),
                dict(raw), bool(raw.get("confirmation_eligible")),
            ))
    for raw in payload.get("attention_magnitude_summaries") or []:
        if isinstance(raw, dict) and raw.get("horizon_minutes"):
            # This was the one transparent association strong enough to earn
            # a prospective cohort, but it abstains from predicting direction.
            rows.append((
                "magnitude", "high_attention_absolute_move",
                int(raw["horizon_minutes"]), dict(raw),
                bool(raw.get("confirmation_eligible")),
            ))
    for target_kind, rule, horizon, evidence_row, eligible in rows:
        hypothesis_id = (
            f"gdelt_attention.{target_kind}.{rule}.h{horizon}m.discovery."
            f"{data_snapshot_hash[:16]}"
        )
        contract = {
            "source_family": "gdelt_attention",
            "target_kind": target_kind,
            "rule": rule,
            "horizon_minutes": horizon,
            "story_independence": "source_event_lineage_x_currency_hour",
            "proof_eligible": False,
            "execution_eligible": False,
        }
        definition = {
            "hypothesis_id": hypothesis_id,
            "parent_hypothesis_id": None,
            "experiment_kind": "external_source_historical_discovery",
            "research_generation": "orthogonal_source_discovery_20260808",
            "idea_origin": "gdelt_attention",
            "pre_registered": 1,
            "data_sources_json": canonical_json(
                ["GDELT_discovery", "OANDA_practice_GET_only_M15_bid_ask"]
            ),
            "feature_contract_json": canonical_json(
                {"rule": rule, "target_kind": target_kind, "minimum_story_count": 3}
            ),
            "label_contract_json": canonical_json({
                "horizon_minutes": horizon,
                "target": (
                    "absolute_executable_move" if target_kind == "magnitude"
                    else "executable_after_cost_direction_return"
                ),
            }),
            "model_contract_json": canonical_json({"transparent_rule": rule}),
            "cost_contract_json": canonical_json(
                {"entry": "executable_bid_or_ask", "exit": "executable_bid_or_ask"}
            ),
            "allocator_contract_json": "{}",
            "training_period_json": "{}",
            "selection_period_json": canonical_json(
                {"market_days": payload.get("market_days")}
            ),
            "confirmation_period_json": canonical_json(
                {"required": "new_immutable_prospective_source_cohort"}
            ),
            "all_parameters_tried_json": canonical_json({
                "direction_arms": sorted({
                    str(item.get("arm")) for item in payload.get("arm_summaries") or []
                    if isinstance(item, dict) and item.get("arm")
                }),
                "horizons_minutes": sorted({
                    int(item.get("horizon_minutes"))
                    for item in (payload.get("arm_summaries") or [])
                    if isinstance(item, dict) and item.get("horizon_minutes")
                }),
            }),
            "selection_rule": "day independence, BH multiplicity, and best-day exclusion",
            "holdouts_touched_json": canonical_json(["retained_historical_window"]),
            "source_code_hash": "recorded_by_dated_discovery_artifact",
            "data_snapshot_hash": data_snapshot_hash,
            "definition_sha256": stable_hash(contract),
            "created_at": observed_at,
            "definition_json": canonical_json(contract),
        }
        definitions += int(insert_experiment(target, definition))
        if target_kind == "magnitude" and not eligible:
            result = "historical_magnitude_discovery_requires_prospective_cohort"
            retirement_reason = retired_at = None
        elif eligible:
            result = "historical_discovery_candidate_requires_prospective_cohort"
            retirement_reason = retired_at = None
        else:
            result = "historical_discovery_rejected"
            retirement_reason = "failed_multiplicity_or_concentration_gate"
            retired_at = observed_at
        observations += int(observe(
            target,
            hypothesis_id=hypothesis_id,
            observed_at=observed_at,
            source_system=source.name,
            result=result,
            retirement_reason=retirement_reason,
            retired_at=retired_at,
            evidence={
                "report_sha256": report_sha,
                "evidence_row": evidence_row,
                "mapping_quality": payload.get("mapping_quality") or {},
                "limitations": payload.get("limitations") or [],
            },
        ))
    return definitions, observations


def import_prospective_source_state(
    target: sqlite3.Connection,
    source: Path,
) -> tuple[int, int]:
    """Register a source-specific prospective collector without promoting it."""
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    cohort = payload.get("cohort") or {}
    cohort_id = str(cohort.get("cohort_id") or "")
    if not cohort_id:
        return 0, 0
    contract = {
        "cohort_id": cohort_id,
        "config_sha256": cohort.get("config_sha256"),
        "collector_sha256": cohort.get("collector_sha256"),
        "direction_policy": cohort.get("direction_policy"),
        "forecasts_written_before_outcomes": cohort.get("forecasts_written_before_outcomes"),
        "late_backfill_refused": cohort.get("late_backfill_refused"),
        "execution_eligible": False,
        "supersedes_cohort_id": cohort.get("supersedes_cohort_id"),
    }
    supersedes = str(cohort.get("supersedes_cohort_id") or "")
    definition = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": supersedes or None,
        "experiment_kind": "external_source_prospective_collection",
        "research_generation": "orthogonal_source_prospective_20260808",
        "idea_origin": "gdelt_attention_magnitude",
        "pre_registered": 1,
        "data_sources_json": canonical_json(
            ["GDELT_point_in_time_story_lineage", "OANDA_executable_bid_ask"]
        ),
        "feature_contract_json": canonical_json(
            {"config_sha256": cohort.get("config_sha256"), "direction_policy": "abstain"}
        ),
        "label_contract_json": canonical_json(
            {"target": "absolute_move_and_cost_clearance", "exact_horizon": True}
        ),
        "model_contract_json": canonical_json(
            {"collector_sha256": cohort.get("collector_sha256")}
        ),
        "cost_contract_json": canonical_json(
            {"entry_spread_recorded": True, "best_direction_is_diagnostic_only": True}
        ),
        "allocator_contract_json": canonical_json({"direction_policy": "abstain"}),
        "training_period_json": "{}",
        "selection_period_json": "{}",
        "confirmation_period_json": canonical_json({"untouched_prospective": True}),
        "all_parameters_tried_json": canonical_json({
            "frozen_by_config_sha256": True,
            "frozen_by_collector_sha256": True,
        }),
        "selection_rule": "collect only timely completed currency-hours with >=3 independent stories",
        "holdouts_touched_json": canonical_json([]),
        "source_code_hash": str(cohort.get("collector_sha256") or ""),
        "data_snapshot_hash": str(cohort.get("config_sha256") or ""),
        "definition_sha256": stable_hash(contract),
        "created_at": str(payload.get("generated_utc") or utc_now()),
        "definition_json": canonical_json(contract),
    }
    definitions = int(insert_experiment(target, definition))
    observations = 0
    if (
        supersedes
        and supersedes != cohort_id
        and int((payload.get("totals") or {}).get("forecasts") or 0) == 0
        and target.execute(
            "SELECT 1 FROM experiments WHERE hypothesis_id=?", (supersedes,)
        ).fetchone() is not None
    ):
        observations += int(observe(
            target,
            hypothesis_id=supersedes,
            observed_at=str(payload.get("generated_utc") or utc_now()),
            source_system=source.name,
            result="engineering_superseded",
            retirement_reason=(
                "material collector/observation-time contract changed before "
                "any prospective forecast"
            ),
            retired_at=str(payload.get("generated_utc") or utc_now()),
            evidence={
                "superseded_by": cohort_id,
                "prospective_forecasts_before_supersession": 0,
            },
        ))
    evidence = {
        "status": payload.get("status"),
        "latest_quote_utc": payload.get("latest_quote_utc"),
        "decision_utc": payload.get("decision_utc"),
        "totals": payload.get("totals") or {},
        "cycle": payload.get("cycle") or {},
        "supported_execution_decision": payload.get("supported_execution_decision"),
    }
    observations += int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(payload.get("generated_utc") or utc_now()),
        source_system=source.name,
        result="continue_collecting",
        evidence=evidence,
    ))
    return definitions, observations


def import_treasury_source_state(
    target: sqlite3.Connection,
    source: Path,
) -> tuple[int, int]:
    """Register the source-only Treasury cohort separately from rate rules."""
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    cohort = payload.get("cohort") or {}
    cohort_id = str(cohort.get("cohort_id") or "")
    if not cohort_id:
        return 0, 0
    contract = {
        "cohort_id": cohort_id,
        "config_sha256": cohort.get("config_sha256"),
        "collector_sha256": cohort.get("collector_sha256"),
        "source": cohort.get("source"),
        "first_seen_contract": cohort.get("first_seen_contract"),
        "bootstrap_current_view_proof_eligible": False,
        "historical_revisions_direction_eligible": False,
        "direction_policy": "abstain",
        "execution_eligible": False,
        "supersedes_cohort_id": cohort.get("supersedes_cohort_id"),
    }
    supersedes = str(cohort.get("supersedes_cohort_id") or "")
    definition = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": supersedes or None,
        "experiment_kind": "external_source_prospective_collection",
        "research_generation": "orthogonal_source_prospective_20260808",
        "idea_origin": "us_treasury_daily_yield_curve",
        "pre_registered": 1,
        "data_sources_json": canonical_json(["official_us_treasury_daily_par_yield_curve"]),
        "feature_contract_json": canonical_json({
            "levels": ["2Y", "10Y", "2s10s"],
            "direction_policy": "abstain",
            "config_sha256": cohort.get("config_sha256"),
        }),
        "label_contract_json": canonical_json({"target": "source_integrity_only"}),
        "model_contract_json": canonical_json({"model": "none_source_collection_only"}),
        "cost_contract_json": "{}",
        "allocator_contract_json": "{}",
        "training_period_json": "{}",
        "selection_period_json": "{}",
        "confirmation_period_json": canonical_json({
            "bootstrap_excluded": True,
            "new_yield_dates_only": True,
        }),
        "all_parameters_tried_json": canonical_json({"none": True}),
        "selection_rule": "first locally observed value for each future yield date",
        "holdouts_touched_json": canonical_json([]),
        "source_code_hash": str(cohort.get("collector_sha256") or ""),
        "data_snapshot_hash": str(cohort.get("config_sha256") or ""),
        "definition_sha256": stable_hash(contract),
        "created_at": str(payload.get("generated_utc") or utc_now()),
        "definition_json": canonical_json(contract),
    }
    definitions = int(insert_experiment(target, definition))
    observations = 0
    if (
        supersedes
        and supersedes != cohort_id
        and int((payload.get("totals") or {}).get("prospective_eligible_rows") or 0) == 0
        and target.execute(
            "SELECT 1 FROM experiments WHERE hypothesis_id=?", (supersedes,)
        ).fetchone() is not None
    ):
        observations += int(observe(
            target,
            hypothesis_id=supersedes,
            observed_at=str(payload.get("generated_utc") or utc_now()),
            source_system=source.name,
            result="engineering_superseded",
            retirement_reason=(
                "material collector contract changed before any prospective-eligible row"
            ),
            retired_at=str(payload.get("generated_utc") or utc_now()),
            evidence={
                "superseded_by": cohort_id,
                "prospective_eligible_rows_before_supersession": 0,
            },
        ))
    observations += int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(payload.get("generated_utc") or utc_now()),
        source_system=source.name,
        result="continue_collecting",
        evidence={
            "status": payload.get("status"),
            "totals": payload.get("totals") or {},
            "cycle": payload.get("cycle") or {},
            "database_integrity": payload.get("database_integrity"),
            "observation_clock": payload.get("observation_clock") or {},
            "limitations": payload.get("limitations") or [],
        },
    ))
    return definitions, observations


def import_alfred_source_state(
    target: sqlite3.Connection,
    source: Path,
) -> tuple[int, int]:
    """Register the blocked or collecting point-in-time ALFRED source cohort."""
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    cohort = payload.get("cohort") or {}
    cohort_id = str(cohort.get("cohort_id") or "")
    if not cohort_id:
        return 0, 0
    supersedes = str(cohort.get("supersedes_cohort_id") or "")
    contract = {
        "cohort_id": cohort_id,
        "supersedes_cohort_id": supersedes or None,
        "config_sha256": cohort.get("config_sha256"),
        "collector_sha256": cohort.get("collector_sha256"),
        "source_contract_id": cohort.get("source_contract_id"),
        "first_seen_contract": cohort.get("first_seen_contract"),
        "initial_current_view_proof_eligible": False,
        "same_day_intrahour_replay_eligible": False,
        "direction_policy": "abstain",
        "execution_eligible": False,
    }
    definition = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": supersedes or None,
        "experiment_kind": "external_source_prospective_collection",
        "research_generation": "orthogonal_source_prospective_20260808",
        "idea_origin": "fred_alfred_point_in_time_macro_vintages",
        "pre_registered": 1,
        "data_sources_json": canonical_json(["official_fred_alfred_api"]),
        "feature_contract_json": canonical_json({
            "config_sha256": cohort.get("config_sha256"),
            "direction_policy": "abstain",
        }),
        "label_contract_json": canonical_json({"target": "source_integrity_only"}),
        "model_contract_json": canonical_json({"model": "none_source_collection_only"}),
        "cost_contract_json": "{}",
        "allocator_contract_json": "{}",
        "training_period_json": "{}",
        "selection_period_json": "{}",
        "confirmation_period_json": canonical_json({
            "bootstrap_excluded": True,
            "first_seen_releases_and_revisions_only": True,
            "date_only_vintages_barred_from_same_day_intrahour_replay": True,
        }),
        "all_parameters_tried_json": canonical_json({"none": True}),
        "selection_rule": "first locally observed point-in-time release or revision",
        "holdouts_touched_json": canonical_json([]),
        "source_code_hash": str(cohort.get("collector_sha256") or ""),
        "data_snapshot_hash": str(cohort.get("config_sha256") or ""),
        "definition_sha256": stable_hash(contract),
        "created_at": str(payload.get("generated_utc") or utc_now()),
        "definition_json": canonical_json(contract),
    }
    definitions = int(insert_experiment(target, definition))
    observations = int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(payload.get("generated_utc") or utc_now()),
        source_system=source.name,
        result=(
            "blocked_external_credential"
            if payload.get("status") == "blocked_missing_fred_api_key"
            else "continue_collecting"
        ),
        evidence={
            "status": payload.get("status"),
            "credential_environment": payload.get("credential_environment"),
            "credential_present": payload.get("credential_present"),
            "configured_series": payload.get("configured_series"),
            "totals": payload.get("totals") or {},
            "cycle": payload.get("cycle") or {},
            "database_integrity": payload.get("database_integrity"),
            "observation_clock": payload.get("observation_clock") or {},
        },
    ))
    return definitions, observations


def import_shadow_runtime_state(
    target: sqlite3.Connection,
    source: Path,
    *,
    idea_origin: str,
    data_sources: list[str],
    label_target: str,
    selection_rule: str,
) -> tuple[int, int]:
    """Register a research-only runtime cohort that cannot promote or execute."""
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    collection = payload.get("collection_cohort") or {}
    cohort_id = str(payload.get("cohort_id") or collection.get("cohort_id") or "")
    if not cohort_id or not bool(payload.get("research_only")):
        return 0, 0
    parent = str(
        payload.get("model_cohort_id")
        or collection.get("model_cohort_id")
        or collection.get("supersedes_cohort_id")
        or ""
    )
    collector_sha = str(collection.get("collector_sha256") or "")
    contract = {
        "cohort_id": cohort_id,
        "parent_cohort_id": parent or None,
        "collector_sha256": collector_sha or None,
        "runtime_contract": payload.get("contract") or payload.get("policy") or {},
        "research_only": True,
        "execution_eligible": False,
        "can_promote": False,
        "can_place_orders": False,
    }
    definition = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": parent or None,
        "experiment_kind": "prospective_shadow_runtime",
        "research_generation": "proof_first_runtime_20260808",
        "idea_origin": idea_origin,
        "pre_registered": 1,
        "data_sources_json": canonical_json(data_sources),
        "feature_contract_json": canonical_json(payload.get("contract") or payload.get("policy") or {}),
        "label_contract_json": canonical_json({"target": label_target}),
        "model_contract_json": canonical_json({
            "model_cohort_id": payload.get("model_cohort_id"),
            "collector_sha256": collector_sha or None,
        }),
        "cost_contract_json": canonical_json({"executable_bid_ask_required": True}),
        "allocator_contract_json": canonical_json({"execution_eligible": False}),
        "training_period_json": "{}",
        "selection_period_json": "{}",
        "confirmation_period_json": canonical_json({"untouched_prospective": True}),
        "all_parameters_tried_json": canonical_json({"frozen_runtime_contract": True}),
        "selection_rule": selection_rule,
        "holdouts_touched_json": canonical_json([]),
        "source_code_hash": collector_sha,
        "data_snapshot_hash": stable_hash(data_sources),
        "definition_sha256": stable_hash(contract),
        "created_at": str(payload.get("generated_utc") or utc_now()),
        "definition_json": canonical_json(contract),
    }
    definitions = int(insert_experiment(target, definition))
    status = str(payload.get("status") or "unknown")
    result = (
        "closed_market_collecting"
        if status in {"market_or_source_stale", "closed_or_stale", "market_or_quote_stale"}
        else "continue_collecting"
    )
    observations = int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(payload.get("generated_utc") or utc_now()),
        source_system=source.name,
        result=result,
        evidence={
            "status": status,
            "totals": payload.get("totals") or payload.get("summary") or {},
            "cycle": payload.get("cycle") or {},
            "inserted_entries": payload.get("inserted_entries"),
            "matured_entries": payload.get("matured_entries"),
            "matured_targets": payload.get("matured_targets"),
            "observation_clock": payload.get("observation_clock") or {},
            "supported_execution_decision": payload.get("supported_execution_decision", "no_trade"),
        },
    ))
    additional = payload.get("additional_shadow_cohorts") or []
    if isinstance(additional, list):
        for raw in additional:
            if not isinstance(raw, dict):
                continue
            child_id = str(raw.get("cohort_id") or "")
            if not child_id:
                continue
            child_parent = str(raw.get("parent_cohort_id") or cohort_id)
            child_contract = {
                "cohort_id": child_id,
                "parent_cohort_id": child_parent or None,
                "feature_contract": raw.get("feature_contract") or {},
                "label_contract": raw.get("label_contract") or {},
                "selection_rule": str(raw.get("selection_rule") or ""),
                "research_only": True,
                "execution_eligible": False,
                "can_promote": False,
                "can_place_orders": False,
            }
            child_definition = {
                "hypothesis_id": child_id,
                "parent_hypothesis_id": child_parent or None,
                "experiment_kind": "prospective_shadow_runtime",
                "research_generation": "proof_first_runtime_20260814",
                "idea_origin": str(raw.get("idea_origin") or idea_origin),
                "pre_registered": 1,
                "data_sources_json": canonical_json(raw.get("data_sources") or data_sources),
                "feature_contract_json": canonical_json(raw.get("feature_contract") or {}),
                "label_contract_json": canonical_json(raw.get("label_contract") or {}),
                "model_contract_json": canonical_json({"parent_runtime_cohort": cohort_id}),
                "cost_contract_json": canonical_json({"executable_bid_ask_required": True}),
                "allocator_contract_json": canonical_json({"execution_eligible": False}),
                "training_period_json": "{}",
                "selection_period_json": canonical_json({"starts": raw.get("created_at")}),
                "confirmation_period_json": canonical_json({"untouched_prospective": True}),
                "all_parameters_tried_json": canonical_json({"frozen_runtime_contract": True}),
                "selection_rule": str(raw.get("selection_rule") or "frozen"),
                "holdouts_touched_json": canonical_json([]),
                "source_code_hash": str(raw.get("source_code_hash") or collector_sha),
                "data_snapshot_hash": stable_hash(raw.get("data_sources") or data_sources),
                "definition_sha256": stable_hash(child_contract),
                "created_at": str(raw.get("created_at") or payload.get("generated_utc") or utc_now()),
                "definition_json": canonical_json(child_contract),
            }
            definitions += int(insert_experiment(target, child_definition))
            observations += int(observe(
                target,
                hypothesis_id=child_id,
                observed_at=str(payload.get("generated_utc") or utc_now()),
                source_system=source.name,
                result=(
                    "closed_market_collecting"
                    if status in {"market_or_source_stale", "closed_or_stale", "market_or_quote_stale"}
                    else "continue_collecting"
                ),
                evidence={
                    "status": status,
                    "inserted_entries": payload.get("inserted_entries"),
                    "matured_entries": payload.get("matured_entries"),
                    "diagnostics": payload.get("diagnostics") or {},
                    "observation_clock": payload.get("observation_clock") or {},
                },
            ))
    return definitions, observations


def import_model_artifact_manifest(
    target: sqlite3.Connection,
    source: Path,
    *,
    idea_origin: str,
) -> tuple[int, int]:
    """Register a frozen research model artifact before its collector child."""
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    cohort_id = str(payload.get("cohort_id") or "")
    if not cohort_id or not bool(payload.get("research_only")):
        return 0, 0
    artifact_path = source.parent / "models.joblib"
    artifact_sha = (
        hashlib.sha256(artifact_path.read_bytes()).hexdigest()
        if artifact_path.is_file() else ""
    )
    contract = {
        "manifest": payload,
        "artifact_sha256": artifact_sha,
        "execution_eligible": False,
    }
    definition = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": None,
        "experiment_kind": "frozen_model_artifact",
        "research_generation": "proof_first_runtime_20260808",
        "idea_origin": idea_origin,
        "pre_registered": 1,
        "data_sources_json": canonical_json(["OANDA_executable_bid_ask", "quote_intensity"]),
        "feature_contract_json": canonical_json(payload.get("feature_contract") or []),
        "label_contract_json": canonical_json({"target": "after_cost_executable_opportunity"}),
        "model_contract_json": canonical_json({"artifact_sha256": artifact_sha}),
        "cost_contract_json": canonical_json({"config_sha256": payload.get("config_sha256")}),
        "allocator_contract_json": canonical_json({"execution_eligible": False}),
        "training_period_json": canonical_json({"source_highwater_utc": payload.get("source_highwater_utc")}),
        "selection_period_json": "{}",
        "confirmation_period_json": canonical_json({"prospective_start_utc": payload.get("prospective_start_utc")}),
        "all_parameters_tried_json": canonical_json({"manifest_frozen": True}),
        "selection_rule": "frozen artifact; proof requires later prospective collection",
        "holdouts_touched_json": canonical_json([]),
        "source_code_hash": str(payload.get("source_code_sha256") or ""),
        "data_snapshot_hash": str(payload.get("config_sha256") or ""),
        "definition_sha256": stable_hash(contract),
        "created_at": str(payload.get("created_utc") or utc_now()),
        "definition_json": canonical_json(contract),
    }
    definitions = int(insert_experiment(target, definition))
    observations = int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(payload.get("created_utc") or utc_now()),
        source_system=source.name,
        result="frozen_awaiting_prospective_evidence",
        evidence={
            "artifact_sha256": artifact_sha,
            "prospective_start_utc": payload.get("prospective_start_utc"),
            "source_instrument_count": payload.get("source_instrument_count"),
        },
    ))
    return definitions, observations


def observe_after_cost_definition_conflict(
    target: sqlite3.Connection,
    *,
    source: Path,
    payload: dict[str, Any],
    cohort_id: str,
    definition: dict[str, Any],
    frozen_manifest: dict[str, Any],
) -> tuple[int, int] | None:
    """Hard-block a reused after-cost cohort ID without crash-looping.

    The immutable registered definition remains authoritative. A regenerated
    state with different bytes is recorded as an incident and cannot merge,
    overwrite, promote, or authorize anything.
    """
    existing = target.execute(
        "SELECT definition_sha256,source_code_hash FROM experiments "
        "WHERE hypothesis_id=?",
        (cohort_id,),
    ).fetchone()
    if existing is None or str(existing[0]) == str(definition["definition_sha256"]):
        return None
    summary = payload.get("summary") or {}
    evidence_counts = {
        field: int(summary.get(field) or 0)
        for field in (
            "admissible_canonical_record_count",
            "economics_admissible_count",
            "ranked_count",
            "selectable_count",
            "hold_switch_count",
        )
    }
    zero_evidence = not any(evidence_counts.values())
    incident = {
        "incident": "same_hypothesis_id_definition_conflict",
        "hard_blocked": True,
        "registered_definition_sha256": str(existing[0]),
        "registered_source_code_hash": str(existing[1] or ""),
        "proposed_definition_sha256": str(definition["definition_sha256"]),
        "proposed_source_code_hash": str(definition["source_code_hash"]),
        "proposed_state_artifact_sha256": hashlib.sha256(
            source.read_bytes()
        ).hexdigest(),
        "proposed_snapshot_id": payload.get("snapshot_id"),
        "proposed_manifest_id": frozen_manifest.get("manifest_id"),
        "proposed_manifest_sha256": frozen_manifest.get("manifest_sha256"),
        "evidence_counts": evidence_counts,
        "zero_admissible_evidence": zero_evidence,
        "definition_merged_or_overwritten": False,
        "execution_eligible": False,
        "supported_execution_decision": "no_trade",
        "required_action": "open_a_new_cohort_id_for_material_changes",
    }
    result = (
        "engineering_quarantined_artifact_drift_zero_evidence"
        if zero_evidence
        else "artifact_definition_conflict_hard_blocked_nonzero_evidence"
    )
    return 0, int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(payload.get("generated_utc") or utc_now()),
        source_system=source.name,
        result=result,
        evidence=incident,
        retirement_reason="same_cohort_id_reused_with_different_definition",
    ))


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _atomic_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(value)
    _replace_with_retry(temporary, path)


def _registry_database_metadata(path: Path) -> dict[str, Any]:
    """Read and validate the immutable portion of a genealogy registry."""
    if path.is_symlink() or not path.is_file():
        raise ValueError("genealogy registry must be an ordinary file")
    connection = ro(path)
    try:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity.lower() != "ok":
            raise ValueError("genealogy registry integrity check failed")
        experiment_columns = tuple(
            str(row[1]) for row in connection.execute("PRAGMA table_info(experiments)")
        )
        observation_columns = tuple(
            str(row[1])
            for row in connection.execute("PRAGMA table_info(experiment_observations)")
        )
        if experiment_columns != EXPERIMENT_COLUMNS:
            raise ValueError("unsupported predecessor experiment schema")
        if observation_columns != OBSERVATION_COLUMNS:
            raise ValueError("unsupported predecessor observation schema")
        return {
            "sha256": _sha256_file(path),
            "bytes": path.stat().st_size,
            "experiment_count": int(connection.execute(
                "SELECT COUNT(*) FROM experiments"
            ).fetchone()[0]),
            "observation_count": int(connection.execute(
                "SELECT COUNT(*) FROM experiment_observations"
            ).fetchone()[0]),
            "integrity_check": "ok",
        }
    finally:
        connection.close()


def _registry_rows(
    connection: sqlite3.Connection,
) -> tuple[dict[str, tuple[Any, ...]], dict[str, tuple[Any, ...]]]:
    experiments = {
        str(row[0]): tuple(row)
        for row in connection.execute(
            f"SELECT {','.join(EXPERIMENT_COLUMNS)} FROM experiments"
        )
    }
    observations = {
        str(row[0]): tuple(row)
        for row in connection.execute(
            f"SELECT {','.join(OBSERVATION_COLUMNS)} FROM experiment_observations"
        )
    }
    return experiments, observations


def _validate_legacy_experiment(row: tuple[Any, ...]) -> None:
    if len(row) != len(EXPERIMENT_COLUMNS) or not row[0]:
        raise ValueError("invalid predecessor experiment row")
    for index, column in enumerate(EXPERIMENT_COLUMNS):
        if column != "parent_hypothesis_id" and row[index] is None:
            raise ValueError("null field in predecessor experiment")
        if column.endswith("_json"):
            try:
                json.loads(str(row[index]))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError("invalid JSON in predecessor experiment") from exc


def _validate_legacy_observation(
    row: tuple[Any, ...], experiments: dict[str, tuple[Any, ...]],
) -> None:
    if len(row) != len(OBSERVATION_COLUMNS) or not row[0] or str(row[1]) not in experiments:
        raise ValueError("invalid predecessor observation row")
    try:
        evidence = json.loads(str(row[8]))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("invalid predecessor observation evidence") from exc
    if stable_hash(evidence) != str(row[7]):
        raise ValueError("predecessor observation evidence hash mismatch")


def _id_root(values: list[str]) -> str:
    return stable_hash(sorted(values))


def _counter_rows(counter: Counter[str]) -> list[dict[str, Any]]:
    return [
        {"value": value, "count": int(count)}
        for value, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def _genealogy_reconciliation_census(
    predecessor: sqlite3.Connection, baseline: sqlite3.Connection,
) -> dict[str, Any]:
    """Classify every immutable predecessor row before any merge is attempted."""
    old_experiments, old_observations = _registry_rows(predecessor)
    new_experiments, new_observations = _registry_rows(baseline)
    for row in old_experiments.values():
        _validate_legacy_experiment(row)
    for row in old_observations.values():
        _validate_legacy_observation(row, old_experiments)

    old_ids, new_ids = set(old_experiments), set(new_experiments)
    common_ids = old_ids & new_ids
    exact_ids = sorted(
        hypothesis_id for hypothesis_id in common_ids
        if old_experiments[hypothesis_id] == new_experiments[hypothesis_id]
    )
    conflict_ids = sorted(common_ids - set(exact_ids))
    old_only_ids = sorted(old_ids - new_ids)
    new_only_ids = sorted(new_ids - old_ids)
    conflicts = []
    for hypothesis_id in conflict_ids:
        old_row, new_row = old_experiments[hypothesis_id], new_experiments[hypothesis_id]
        conflicts.append({
            "hypothesis_id": hypothesis_id,
            "changed_columns": [
                column for column, old_value, new_value
                in zip(EXPERIMENT_COLUMNS, old_row, new_row) if old_value != new_value
            ],
            "predecessor_definition_row_sha256": stable_hash(dict(zip(EXPERIMENT_COLUMNS, old_row))),
            "baseline_definition_row_sha256": stable_hash(dict(zip(EXPERIMENT_COLUMNS, new_row))),
            "predecessor_observation_count": sum(
                1 for row in old_observations.values() if str(row[1]) == hypothesis_id
            ),
        })

    same_observation_conflicts = sorted(
        observation_id for observation_id in set(old_observations) & set(new_observations)
        if old_observations[observation_id] != new_observations[observation_id]
    )
    old_only_observation_ids = sorted(set(old_observations) - set(new_observations))
    eligible_hypotheses = set(old_only_ids) | set(exact_ids)
    eligible_observation_ids = [
        observation_id for observation_id in old_only_observation_ids
        if str(old_observations[observation_id][1]) in eligible_hypotheses
    ]
    quarantined_observation_ids = [
        observation_id for observation_id in old_only_observation_ids
        if str(old_observations[observation_id][1]) in set(conflict_ids)
    ]
    unexplained = set(old_only_observation_ids) - set(eligible_observation_ids) - set(
        quarantined_observation_ids
    )
    if unexplained:
        raise ValueError("predecessor observations could not be reconciled")
    old_only_observation_id_set = set(old_only_observation_ids)
    semantic_keys = {
        canonical_json((row[1], *row[3:]))
        for observation_id, row in old_observations.items()
        if observation_id in old_only_observation_id_set
    }
    return {
        "schema_version": 1,
        "predecessor_experiment_count": len(old_experiments),
        "predecessor_observation_count": len(old_observations),
        "baseline_experiment_count": len(new_experiments),
        "baseline_observation_count": len(new_observations),
        "predecessor_only_definition_count": len(old_only_ids),
        "predecessor_only_definition_id_root": _id_root(old_only_ids),
        "predecessor_only_definition_kind_counts": _counter_rows(Counter(
            str(old_experiments[hypothesis_id][2]) for hypothesis_id in old_only_ids
        )),
        "baseline_only_definition_count": len(new_only_ids),
        "baseline_only_definition_id_root": _id_root(new_only_ids),
        "common_exact_definition_count": len(exact_ids),
        "common_exact_definition_id_root": _id_root(exact_ids),
        "definition_conflict_count": len(conflict_ids),
        "definition_conflict_id_root": _id_root(conflict_ids),
        "definition_conflicts": conflicts,
        "same_observation_id_conflict_count": len(same_observation_conflicts),
        "same_observation_id_conflict_root": _id_root(same_observation_conflicts),
        "merge_eligible_observation_count": len(eligible_observation_ids),
        "merge_eligible_observation_id_root": _id_root(eligible_observation_ids),
        "merge_eligible_observation_result_counts": _counter_rows(Counter(
            str(old_observations[value][4]) for value in eligible_observation_ids
        )),
        "merge_eligible_observation_source_counts": _counter_rows(Counter(
            str(old_observations[value][3]) for value in eligible_observation_ids
        )),
        "quarantined_observation_count": len(quarantined_observation_ids),
        "quarantined_observation_id_root": _id_root(quarantined_observation_ids),
        "quarantined_observation_result_counts": _counter_rows(Counter(
            str(old_observations[value][4]) for value in quarantined_observation_ids
        )),
        "quarantined_observation_source_counts": _counter_rows(Counter(
            str(old_observations[value][3]) for value in quarantined_observation_ids
        )),
        "predecessor_only_observation_count": len(old_only_observation_ids),
        "predecessor_only_observation_id_root": _id_root(old_only_observation_ids),
        "predecessor_only_observation_semantic_unique_count": len(semantic_keys),
        "predecessor_only_observation_semantic_repeat_count": (
            len(old_only_observation_ids) - len(semantic_keys)
        ),
        "semantic_repeat_contract": (
            "same hypothesis/source/result/retirement/evidence after excluding "
            "observation identity and observation timestamp"
        ),
        "predecessor_only_observation_result_counts": _counter_rows(Counter(
            str(old_observations[value][4]) for value in old_only_observation_ids
        )),
        "predecessor_only_observation_source_counts": _counter_rows(Counter(
            str(old_observations[value][3]) for value in old_only_observation_ids
        )),
    }


def stage_genealogy_predecessor_snapshot(
    ledger_root: Path, *, predecessor_database: Path,
    predecessor_state: Path, predecessor_report: Path,
    baseline_database: Path, captured_at: str | None = None,
) -> dict[str, Any]:
    """Preserve old bytes and freeze a lossless reconciliation contract.

    The caller must stop the writer and checkpoint the old WAL first.  This
    helper deliberately refuses a non-empty WAL instead of taking an ambiguous
    copy of a live registry.
    """
    for value in (predecessor_database, predecessor_state, predecessor_report, baseline_database):
        if value.is_symlink() or not value.is_file():
            raise ValueError("predecessor staging inputs must be ordinary files")
    wal = predecessor_database.with_name(predecessor_database.name + "-wal")
    if wal.exists() and wal.stat().st_size:
        raise ValueError("checkpoint predecessor WAL before staging")
    predecessor_metadata = _registry_database_metadata(predecessor_database)
    baseline_metadata = _registry_database_metadata(baseline_database)
    old_connection, new_connection = ro(predecessor_database), ro(baseline_database)
    try:
        census = _genealogy_reconciliation_census(old_connection, new_connection)
    finally:
        old_connection.close(); new_connection.close()
    if census["same_observation_id_conflict_count"]:
        raise ValueError("observation ID conflict blocks predecessor staging")

    source_files: list[tuple[str, Path]] = [
        ("database", predecessor_database),
        ("state", predecessor_state),
        ("report", predecessor_report),
    ]
    for role, path in (
        ("database_wal", wal),
        ("database_shm", predecessor_database.with_name(predecessor_database.name + "-shm")),
    ):
        if path.exists():
            if path.is_symlink() or not path.is_file():
                raise ValueError("predecessor sidecar must be an ordinary file")
            source_files.append((role, path))
    descriptors = {
        role: {
            "role": role,
            "filename": path.name,
            "bytes": path.stat().st_size,
            "sha256": _sha256_file(path),
        }
        for role, path in source_files
    }
    material = {
        "schema_version": 1,
        "contract_id": "research_genealogy_predecessor_snapshot_v1",
        "safety": dict(FULL_RESEARCH_ONLY_SAFETY),
        "predecessor_database": predecessor_metadata,
        "baseline_database": baseline_metadata,
        "files": descriptors,
        "reconciliation": census,
    }
    material_sha = stable_hash(material)
    snapshot_id = "research_genealogy_predecessor_v1." + material_sha[:20]
    snapshot_root = ledger_root / "snapshots" / snapshot_id
    immutable_manifest = snapshot_root / "manifest.json"
    current_manifest = ledger_root / "research_genealogy_predecessor_v1.json"
    if immutable_manifest.is_file():
        existing = json.loads(immutable_manifest.read_text(encoding="utf-8"))
        if existing.get("material_contract_sha256") != material_sha:
            raise ValueError("immutable predecessor snapshot conflict")
        for descriptor in existing.get("files", {}).values():
            archived = _safe_child_file(snapshot_root, snapshot_root / descriptor["filename"])
            if archived is None or _sha256_file(archived) != descriptor["sha256"]:
                raise ValueError("immutable predecessor snapshot bytes changed")
        _atomic_bytes(current_manifest, immutable_manifest.read_bytes())
        return existing

    staging = ledger_root / "snapshots" / f".{snapshot_id}.{os.getpid()}.stage"
    if staging.exists():
        raise ValueError("predecessor staging path already exists")
    staging.mkdir(parents=True)
    try:
        for role, source in source_files:
            shutil.copy2(source, staging / descriptors[role]["filename"])
            copied = staging / descriptors[role]["filename"]
            if (
                copied.stat().st_size != descriptors[role]["bytes"]
                or _sha256_file(copied) != descriptors[role]["sha256"]
            ):
                raise ValueError("predecessor staging copy verification failed")
        manifest = {
            "schema_version": 1,
            "snapshot_id": snapshot_id,
            "captured_at": captured_at or utc_now(),
            "material_contract_sha256": material_sha,
            "material_contract": material,
            "files": descriptors,
        }
        atomic_json(staging / "manifest.json", manifest)
        snapshot_root.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staging, snapshot_root)
        _atomic_bytes(current_manifest, immutable_manifest.read_bytes())
        return manifest
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise


def _validated_predecessor_manifest(ledger_root: Path) -> tuple[dict[str, Any], Path]:
    current = ledger_root / "research_genealogy_predecessor_v1.json"
    if current.is_symlink() or not current.is_file():
        raise ValueError("missing predecessor snapshot manifest")
    manifest = json.loads(current.read_text(encoding="utf-8"))
    material = manifest.get("material_contract")
    if not isinstance(material, dict) or stable_hash(material) != manifest.get(
        "material_contract_sha256"
    ):
        raise ValueError("predecessor material contract mismatch")
    if not _full_research_only_policy_is_closed(material.get("safety") or {}):
        raise ValueError("predecessor snapshot safety is not fail closed")
    snapshot_id = str(manifest.get("snapshot_id") or "")
    if snapshot_id != "research_genealogy_predecessor_v1." + str(
        manifest.get("material_contract_sha256")
    )[:20]:
        raise ValueError("predecessor snapshot ID mismatch")
    snapshot_root = ledger_root / "snapshots" / snapshot_id
    immutable = _safe_child_file(snapshot_root, snapshot_root / "manifest.json")
    if immutable is None or current.read_bytes() != immutable.read_bytes():
        raise ValueError("current predecessor manifest is not immutable manifest")
    descriptors = manifest.get("files")
    if not isinstance(descriptors, dict) or descriptors != material.get("files"):
        raise ValueError("predecessor file manifest mismatch")
    for role, descriptor in descriptors.items():
        if not isinstance(descriptor, dict) or descriptor.get("role") != role:
            raise ValueError("invalid predecessor file descriptor")
        expected = snapshot_root / str(descriptor.get("filename") or "")
        archived = _safe_child_file(snapshot_root, expected)
        if archived is None or archived.name != descriptor.get("filename"):
            raise ValueError("unsafe predecessor archive path")
        if archived.stat().st_size != descriptor.get("bytes") or _sha256_file(
            archived
        ) != descriptor.get("sha256"):
            raise ValueError("predecessor archive bytes mismatch")
    database = _safe_child_file(
        snapshot_root, snapshot_root / descriptors["database"]["filename"]
    )
    if database is None or _registry_database_metadata(database) != material.get(
        "predecessor_database"
    ):
        raise ValueError("predecessor database metadata mismatch")
    return manifest, database


def _predecessor_snapshot_definition(manifest: dict[str, Any]) -> dict[str, Any]:
    material = manifest["material_contract"]
    snapshot_id = str(manifest["snapshot_id"])
    definition_core = {
        "snapshot_id": snapshot_id,
        "contract_id": material["contract_id"],
        "material_contract_sha256": manifest["material_contract_sha256"],
        "predecessor_database_sha256": material["predecessor_database"]["sha256"],
        "predecessor_experiment_count": material["predecessor_database"]["experiment_count"],
        "predecessor_observation_count": material["predecessor_database"]["observation_count"],
        "safety": material["safety"],
    }
    return {
        "hypothesis_id": snapshot_id,
        "parent_hypothesis_id": None,
        "experiment_kind": "archived_genealogy_snapshot",
        "research_generation": "pre_hardening_genealogy_preservation_v1",
        "idea_origin": "lossless_predecessor_registry_preservation",
        "pre_registered": 1,
        "data_sources_json": canonical_json(["archived_research_genealogy_registry"]),
        "feature_contract_json": canonical_json({"none": "archival_contract_only"}),
        "label_contract_json": canonical_json({"none": "archival_contract_only"}),
        "model_contract_json": canonical_json({"none": "archival_contract_only"}),
        "cost_contract_json": canonical_json({"none": "archival_contract_only"}),
        "allocator_contract_json": canonical_json(material["safety"]),
        "training_period_json": canonical_json({}),
        "selection_period_json": canonical_json({}),
        "confirmation_period_json": canonical_json({}),
        "all_parameters_tried_json": canonical_json([]),
        "selection_rule": "preserve exact predecessor bytes; merge only byte-compatible immutable history",
        "holdouts_touched_json": canonical_json([]),
        "source_code_hash": stable_hash("research_genealogy_predecessor_snapshot_v1"),
        "data_snapshot_hash": material["predecessor_database"]["sha256"],
        "definition_sha256": manifest["material_contract_sha256"],
        "created_at": str(manifest["captured_at"]),
        "definition_json": canonical_json(definition_core),
    }


def import_genealogy_predecessor_snapshot(
    target: sqlite3.Connection, ledger_root: Path,
) -> tuple[int, int]:
    """Merge compatible predecessor rows and explicitly register the quarantine."""
    current = ledger_root / "research_genealogy_predecessor_v1.json"
    if not current.exists():
        return 0, 0
    manifest, database = _validated_predecessor_manifest(ledger_root)
    predecessor = ro(database)
    try:
        old_experiments, old_observations = _registry_rows(predecessor)
        for row in old_experiments.values():
            _validate_legacy_experiment(row)
        for row in old_observations.values():
            _validate_legacy_observation(row, old_experiments)
        frozen = manifest["material_contract"]["reconciliation"]
        snapshot_id = str(manifest["snapshot_id"])
        already_registered = target.execute(
            "SELECT 1 FROM experiments WHERE hypothesis_id=?", (snapshot_id,)
        ).fetchone() is not None
        if not already_registered:
            current_census = _genealogy_reconciliation_census(predecessor, target)
            if current_census != frozen:
                raise ValueError("predecessor reconciliation census changed")
        if frozen.get("same_observation_id_conflict_count"):
            raise ValueError("predecessor observation conflicts cannot be merged")
        conflict_ids = {
            str(row["hypothesis_id"]) for row in frozen.get("definition_conflicts", [])
        }
        definitions_to_insert: list[tuple[Any, ...]] = []
        for hypothesis_id, old_row in old_experiments.items():
            existing = target.execute(
                f"SELECT {','.join(EXPERIMENT_COLUMNS)} FROM experiments WHERE hypothesis_id=?",
                (hypothesis_id,),
            ).fetchone()
            if existing is None:
                definitions_to_insert.append(old_row)
            elif tuple(existing) != old_row and hypothesis_id not in conflict_ids:
                raise ValueError("unexpected predecessor definition conflict")
        observations_to_insert: list[tuple[Any, ...]] = []
        for observation_id, old_row in old_observations.items():
            if str(old_row[1]) in conflict_ids:
                continue
            registered_definition = target.execute(
                f"SELECT {','.join(EXPERIMENT_COLUMNS)} FROM experiments WHERE hypothesis_id=?",
                (str(old_row[1]),),
            ).fetchone()
            if registered_definition is not None and tuple(registered_definition) != old_experiments[str(old_row[1])]:
                raise ValueError("observation would attach to incompatible definition")
            existing = target.execute(
                f"SELECT {','.join(OBSERVATION_COLUMNS)} FROM experiment_observations WHERE observation_id=?",
                (observation_id,),
            ).fetchone()
            if existing is None:
                observations_to_insert.append(old_row)
            elif tuple(existing) != old_row:
                raise ValueError("predecessor observation immutable conflict")

        target.execute("SAVEPOINT predecessor_snapshot_import")
        try:
            before = target.total_changes
            for row in definitions_to_insert:
                target.execute(
                    f"INSERT INTO experiments ({','.join(EXPERIMENT_COLUMNS)}) VALUES ({','.join('?' for _ in EXPERIMENT_COLUMNS)})",
                    row,
                )
            merged_definitions = target.total_changes - before
            before = target.total_changes
            for row in observations_to_insert:
                target.execute(
                    f"INSERT INTO experiment_observations ({','.join(OBSERVATION_COLUMNS)}) VALUES ({','.join('?' for _ in OBSERVATION_COLUMNS)})",
                    row,
                )
            merged_observations = target.total_changes - before
            definition = _predecessor_snapshot_definition(manifest)
            archive_definition_added = int(_insert_experiment_exact(target, definition))
            archive_root = ledger_root / "snapshots" / snapshot_id
            evidence = {
                "schema_version": 1,
                "snapshot_id": snapshot_id,
                "archive_relative_path": archive_root.relative_to(ledger_root).as_posix(),
                "material_contract_sha256": manifest["material_contract_sha256"],
                "files": manifest["files"],
                "predecessor_database": manifest["material_contract"]["predecessor_database"],
                "baseline_database": manifest["material_contract"]["baseline_database"],
                "reconciliation": frozen,
                "merge_eligible_definition_count": frozen["predecessor_only_definition_count"],
                "merge_eligible_observation_count": frozen["merge_eligible_observation_count"],
                "quarantined_definition_count": frozen["definition_conflict_count"],
                "quarantined_observation_count": frozen["quarantined_observation_count"],
                **dict(FULL_RESEARCH_ONLY_SAFETY),
            }
            archive_observation_added = int(observe(
                target, hypothesis_id=snapshot_id,
                observed_at=str(manifest["captured_at"]),
                source_system="research_genealogy_predecessor_snapshot_v1",
                result="verified_predecessor_archived_compatible_history_merged_conflicts_quarantined",
                evidence=evidence,
            ))
            target.execute("RELEASE SAVEPOINT predecessor_snapshot_import")
        except Exception:
            target.execute("ROLLBACK TO SAVEPOINT predecessor_snapshot_import")
            target.execute("RELEASE SAVEPOINT predecessor_snapshot_import")
            raise
        return (
            int(merged_definitions) + archive_definition_added,
            int(merged_observations) + archive_observation_added,
        )
    finally:
        predecessor.close()


def _registered_predecessor_snapshots(
    connection: sqlite3.Connection,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT e.hypothesis_id,o.evidence_json FROM experiments e "
        "JOIN experiment_observations o ON o.hypothesis_id=e.hypothesis_id "
        "WHERE e.experiment_kind='archived_genealogy_snapshot' "
        "ORDER BY o.observed_at,o.observation_id"
    )
    output = []
    for hypothesis_id, evidence_json in rows:
        evidence = json.loads(str(evidence_json))
        output.append({
            "snapshot_id": str(hypothesis_id),
            "archive_relative_path": evidence.get("archive_relative_path"),
            "material_contract_sha256": evidence.get("material_contract_sha256"),
            "files": evidence.get("files"),
            "predecessor_database": evidence.get("predecessor_database"),
            "reconciliation": evidence.get("reconciliation"),
            "merge_eligible_definition_count": evidence.get("merge_eligible_definition_count"),
            "merge_eligible_observation_count": evidence.get("merge_eligible_observation_count"),
            "quarantined_definition_count": evidence.get("quarantined_definition_count"),
            "quarantined_observation_count": evidence.get("quarantined_observation_count"),
        })
    return output


def _after_cost_v4_external_trust_anchor(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Independently attest the complete frozen v4 artifact closure.

    Neither the state-embedded manifest nor its claimed hashes are trust
    anchors.  The canonical manifest is read from the registrar's ROOT, its
    semantic hash is reconstructed, and every exact declared artifact path is
    resolved beneath ROOT and byte-hashed before an anchor is returned.
    """
    manifest_path = ROOT / AFTER_COST_V4_MANIFEST_RELATIVE_PATH
    config_path = ROOT / AFTER_COST_V4_EXPECTED_ARTIFACT_PATHS[
        "counterfactual_v4_config"
    ]
    try:
        manifest_bytes = manifest_path.read_bytes()
        manifest = json.loads(manifest_bytes.decode("utf-8"))
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(manifest, dict) or not isinstance(config, dict):
        return None
    cohort_id = str(payload.get("counterfactual_cohort_id") or "")
    contract_id = str(payload.get("counterfactual_contract_id") or "")
    if (
        manifest.get("manifest_id") != config.get("required_frozen_manifest_id")
        or manifest.get("contract_id") != contract_id
        or manifest.get("contract_id") != config.get("contract_id")
        or manifest.get("cohort_id") != cohort_id
        or manifest.get("cohort_id") != config.get("counterfactual_cohort_id")
        or set(config.get("required_manifest_artifacts") or [])
        != set(AFTER_COST_V4_EXPECTED_ARTIFACT_PATHS)
    ):
        return None
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or set(artifacts) != set(
        AFTER_COST_V4_EXPECTED_ARTIFACT_PATHS
    ):
        return None
    root_resolved = ROOT.resolve()
    normalized_artifacts: dict[str, dict[str, str]] = {}
    for label in sorted(AFTER_COST_V4_EXPECTED_ARTIFACT_PATHS):
        row = artifacts.get(label)
        if not isinstance(row, dict):
            return None
        relative_path = str(row.get("relative_path") or "").replace("\\", "/")
        declared_sha256 = str(row.get("sha256") or "").lower()
        if (
            relative_path != AFTER_COST_V4_EXPECTED_ARTIFACT_PATHS[label]
            or len(declared_sha256) != 64
            or any(character not in "0123456789abcdef" for character in declared_sha256)
        ):
            return None
        artifact_path = (ROOT / relative_path).resolve()
        try:
            artifact_path.relative_to(root_resolved)
        except ValueError:
            return None
        try:
            if not artifact_path.is_file() or _sha256_file(artifact_path) != declared_sha256:
                return None
        except OSError:
            return None
        normalized_artifacts[label] = {
            "relative_path": relative_path,
            "sha256": declared_sha256,
        }
    normalized_manifest = {
        "manifest_id": manifest["manifest_id"],
        "contract_id": manifest["contract_id"],
        "cohort_id": manifest["cohort_id"],
        "artifacts": normalized_artifacts,
    }
    semantic_manifest_sha256 = stable_hash(normalized_manifest)
    normalized_manifest["manifest_sha256"] = semantic_manifest_sha256
    manifest_file_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    if (
        manifest_file_sha256 != AFTER_COST_V4_FROZEN_MANIFEST_FILE_SHA256
        or semantic_manifest_sha256
        != AFTER_COST_V4_FROZEN_MANIFEST_SEMANTIC_SHA256
        or normalized_artifacts["counterfactual_v4_module"]["sha256"]
        != AFTER_COST_V4_FROZEN_MODULE_SHA256
        or normalized_artifacts["counterfactual_v4_config"]["sha256"]
        != AFTER_COST_V4_FROZEN_CONFIG_SHA256
        or normalized_artifacts["counterfactual_v4_cli"]["sha256"]
        != AFTER_COST_V4_FROZEN_CLI_SHA256
    ):
        return None
    state_manifest = payload.get("frozen_manifest")
    if state_manifest != normalized_manifest:
        return None
    expected_anchor = {
        "anchor_schema": "currency_state_after_cost_v4_external_trust_anchor_v1",
        "manifest_relative_path": AFTER_COST_V4_MANIFEST_RELATIVE_PATH,
        "manifest_file_sha256": manifest_file_sha256,
        "manifest_id": normalized_manifest["manifest_id"],
        "manifest_sha256": semantic_manifest_sha256,
        "artifacts": normalized_artifacts,
        "counterfactual_v4_module_sha256": normalized_artifacts[
            "counterfactual_v4_module"
        ]["sha256"],
        "counterfactual_v4_config_sha256": normalized_artifacts[
            "counterfactual_v4_config"
        ]["sha256"],
        "counterfactual_v4_cli_sha256": normalized_artifacts[
            "counterfactual_v4_cli"
        ]["sha256"],
    }
    if payload.get("expected_external_trust_anchor") != expected_anchor:
        return None
    return normalized_manifest, expected_anchor


def _after_cost_v4_is_zero_evidence_engineering_state(
    payload: dict[str, Any],
) -> bool:
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return False
    required_zero_fields = (
        "submitted_envelope_count",
        "submitted_record_count",
        "submitted_canonical_record_count",
        "submitted_account_state_record_count",
        "admissible_canonical_record_count",
        "admissible_account_state_record_count",
        "admissible_quote_count",
        "admissible_verifier_count",
        "venue_quote_input_count",
        "verifier_input_count",
        "economics_input_count",
        "economics_admissible_count",
        "ranked_count",
        "selectable_count",
        "hold_switch_count",
    )
    try:
        zero_counts = all(int(summary.get(field, -1)) == 0 for field in required_zero_fields)
    except (TypeError, ValueError):
        return False
    allocations = payload.get("allocations")
    if not isinstance(allocations, dict):
        return False
    try:
        for horizons in allocations.values():
            if not isinstance(horizons, dict):
                return False
            for allocation in horizons.values():
                if (
                    not isinstance(allocation, dict)
                    or int(allocation.get("admissible_ranked_count", -1)) != 0
                    or int(allocation.get("selectable_count", -1)) != 0
                    or allocation.get("top_one") is not None
                    or (allocation.get("disjoint_basket") or [])
                    or allocation.get("execution_eligible") is not False
                    or allocation.get("can_place_orders") is not False
                    or allocation.get("supported_execution_decision") != "no_trade"
                ):
                    return False
    except (TypeError, ValueError):
        return False
    return bool(
        zero_counts
        and payload.get("registration_status") == "engineering_blocked_zero_evidence"
        and payload.get("producer_integration_state") == "producer_integration_missing"
        and payload.get("research_only") is True
        and payload.get("execution_eligible") is False
        and payload.get("can_place_orders") is False
        and payload.get("supported_execution_decision") == "no_trade"
        and not (payload.get("hold_switch_counterfactuals") or [])
    )


def import_currency_state_after_cost_state(
    target: sqlite3.Connection,
    source: Path,
    *,
    registration_status: str = "frozen",
) -> tuple[int, int]:
    """Register the frozen CurrencyState after-cost policy cohort.

    This records the counterfactual as one policy-level research hypothesis. It
    does not convert its pair/horizon rows into promotion candidates and it
    cannot authorize execution.
    """
    if not source.is_file():
        return 0, 0
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0
    if registration_status not in {"frozen", "engineering_blocked"}:
        raise ValueError("unsupported after-cost registration status")
    cohort_id = str(payload.get("counterfactual_cohort_id") or "")
    contract_id = str(payload.get("counterfactual_contract_id") or "")
    is_v3 = (
        str(payload.get("snapshot_schema") or "")
        == "currency_state_after_cost_counterfactual_snapshot_v3"
    )
    is_v4 = (
        str(payload.get("snapshot_schema") or "")
        == "currency_state_after_cost_counterfactual_snapshot_v4"
    )
    frozen_manifest = payload.get("frozen_manifest") or {}
    is_manifest_governed = bool(frozen_manifest)
    v4_external_trust_anchor: dict[str, Any] | None = None
    if is_v4:
        # V4 is a byte-attested zero-input engineering artifact, not a frozen
        # evidence cohort.  It must never fall through to the historical,
        # state-manifest-trusting path.
        if (
            registration_status != "engineering_blocked"
            or not _after_cost_v4_is_zero_evidence_engineering_state(payload)
        ):
            return 0, 0
        v4_attestation = _after_cost_v4_external_trust_anchor(payload)
        if v4_attestation is None:
            return 0, 0
        frozen_manifest, v4_external_trust_anchor = v4_attestation
        artifacts = frozen_manifest["artifacts"]
        fingerprints = {
            "counterfactual_module_sha256": artifacts[
                "counterfactual_v4_module"
            ]["sha256"],
            "counterfactual_config_file_sha256": artifacts[
                "counterfactual_v4_config"
            ]["sha256"],
            "counterfactual_cli_sha256": artifacts[
                "counterfactual_v4_cli"
            ]["sha256"],
            "independent_verifier_producer_sha256": artifacts[
                "independent_verifier_producer"
            ]["sha256"],
        }
    elif is_manifest_governed:
        if (
            frozen_manifest.get("cohort_id") != cohort_id
            or frozen_manifest.get("contract_id") != contract_id
            or not str(frozen_manifest.get("manifest_id") or "")
            or len(str(frozen_manifest.get("manifest_sha256") or "")) != 64
            or not isinstance(frozen_manifest.get("artifacts"), dict)
        ):
            return 0, 0
        artifacts = frozen_manifest["artifacts"]
        fingerprints = {
            "counterfactual_module_sha256": (
                artifacts.get("counterfactual_v3_module")
                or artifacts.get("counterfactual_module") or {}
            ).get("sha256"),
            "counterfactual_config_file_sha256": (
                artifacts.get("counterfactual_v3_config")
                or artifacts.get("counterfactual_config") or {}
            ).get("sha256"),
            "counterfactual_cli_sha256": (
                artifacts.get("counterfactual_v3_cli")
                or artifacts.get("counterfactual_cli") or {}
            ).get("sha256"),
            "independent_verifier_producer_sha256": (
                artifacts.get("independent_verifier_producer") or {}
            ).get("sha256"),
        }
    else:
        fingerprints = payload.get("artifact_fingerprints") or {}
    if (
        not cohort_id
        or not contract_id
        or payload.get("research_only") is not True
        or payload.get("execution_eligible") is not False
        or payload.get("can_place_orders") is not False
    ):
        return 0, 0
    contract = {
        "cohort_id": cohort_id,
        "contract_id": contract_id,
        "contract_sha256": payload.get("counterfactual_contract_sha256"),
        "artifact_fingerprints": fingerprints,
        "response_arm_contract_id": payload.get("response_arm_contract_id"),
        "response_snapshot_id": payload.get("response_snapshot_id"),
        "response_snapshot_sha256": payload.get("response_snapshot_sha256"),
        "currency_state_snapshot_id": payload.get("currency_state_snapshot_id"),
        "horizons_sec": payload.get("horizons_sec") or [],
        "arm_ids": payload.get("arm_ids") or [],
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "supported_execution_decision": "no_trade",
    }
    if is_v4:
        contract.update({
            "v4_external_trust_anchor": v4_external_trust_anchor,
            "v4_registration_status": payload.get("registration_status"),
            "v4_producer_integration_state": payload.get(
                "producer_integration_state"
            ),
            "v4_zero_evidence_engineering_artifact": True,
        })
    if is_manifest_governed:
        contract.update({
            "snapshot_schema": payload.get("snapshot_schema"),
            "supersedes_contract_id": payload.get("supersedes_contract_id"),
            "supersedes_cohort_id": payload.get("supersedes_cohort_id"),
            "frozen_manifest": frozen_manifest,
            "quote_envelope_present": payload.get("quote_envelope") is not None,
            "verifier_envelope_present": payload.get("verifier_envelope") is not None,
            "economics_envelope_present": payload.get("economics_envelope") is not None,
            "hold_switch_envelope_present": payload.get("hold_switch_envelope") is not None,
        })
        if is_v3:
            contract["account_envelope_present"] = bool(
                ((payload.get("canonical_input_hashes") or {}).get("envelopes") or {}).get("account_state")
            )
    data_sources = [
        "CurrencyState_v2",
        "OfficialFactAdapter",
        "versioned_response_timing_arms",
        "OANDA_executable_bid_ask",
        "locked_calibration_and_cost_envelopes_when_available",
    ]
    if is_manifest_governed:
        data_sources.extend([
            "fresh_immutable_Practice_007_venue_quote_envelope",
            "independent_verifier_evidence_envelope",
            "explicit_entry_exit_slippage_latency_rotation_cost_envelope",
            "exact_account_position_hold_switch_envelope",
        ])
    definition = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": payload.get("supersedes_cohort_id"),
        "experiment_kind": (
            "frozen_policy_level_counterfactual"
            if registration_status == "frozen"
            else "engineering_policy_level_counterfactual"
        ),
        "research_generation": "currency_state_branch_20260817",
        "idea_origin": "currency_state_after_cost_top_one_basket_hold_switch",
        "pre_registered": 1,
        "data_sources_json": canonical_json(data_sources),
        "feature_contract_json": canonical_json({
            "response_arm_contract_id": payload.get("response_arm_contract_id"),
            "horizons_sec": payload.get("horizons_sec") or [],
            "arm_ids": payload.get("arm_ids") or [],
        }),
        "label_contract_json": canonical_json({
            "targets": [
                "after_cost_ev",
                "cost_clearance",
                "top_one",
                "disjoint_basket",
                "hold_switch",
            ],
            "observed_price_is_never_forward_evidence": True,
        }),
        "model_contract_json": canonical_json(
            {
                "contract_id": contract_id,
                "contract_sha256": fingerprints.get(
                    "counterfactual_config_file_sha256"
                ),
                "module_sha256": fingerprints.get("counterfactual_module_sha256"),
                "frozen_manifest_id": frozen_manifest.get("manifest_id"),
                "frozen_manifest_sha256": frozen_manifest.get("manifest_sha256"),
            }
            if is_manifest_governed
            else {
                "contract_id": contract_id,
                "contract_sha256": payload.get("counterfactual_contract_sha256"),
                "module_sha256": fingerprints.get("counterfactual_module_sha256"),
            }
        ),
        "cost_contract_json": canonical_json({
            "missing_cost_policy": "unavailable_not_zero",
            "executable_bid_ask_required": True,
            "config_sha256": fingerprints.get("counterfactual_config_file_sha256"),
        }),
        "allocator_contract_json": canonical_json({
            "top_one_and_disjoint_basket": True,
            "hold_switch": True,
            "execution_eligible": False,
            "supported_execution_decision": "no_trade",
        }),
        "training_period_json": "{}",
        "selection_period_json": canonical_json({
            "decision_cutoff_utc": payload.get("decision_cutoff_utc")
        }),
        "confirmation_period_json": canonical_json({
            "untouched_prospective_required": True
        }),
        "all_parameters_tried_json": canonical_json(
            {
                "frozen_by_contract_and_byte_hash_manifest": True,
                "manifest_governed": True,
            }
            if is_manifest_governed
            else {"frozen_by_contract_and_byte_hash_manifest": True}
        ),
        "selection_rule": (
            "rank only causally grounded rows with locked probability, magnitude, "
            "executable spread, slippage, latency, and rotation economics"
        ),
        "holdouts_touched_json": canonical_json([]),
        "source_code_hash": str(fingerprints.get("counterfactual_module_sha256") or ""),
        "data_snapshot_hash": str(payload.get("response_snapshot_sha256") or ""),
        "definition_sha256": stable_hash(contract),
        "created_at": str(payload.get("generated_utc") or utc_now()),
        "definition_json": canonical_json(contract),
    }
    conflict = observe_after_cost_definition_conflict(
        target,
        source=source,
        payload=payload,
        cohort_id=cohort_id,
        definition=definition,
        frozen_manifest=frozen_manifest,
    )
    if conflict is not None:
        return conflict
    definitions = int(insert_experiment(target, definition))
    summary = payload.get("summary") or {}
    if registration_status == "engineering_blocked":
        result = "engineering_blocked_zero_evidence"
    else:
        result = (
            "frozen_awaiting_admissible_inputs"
            if int(summary.get("economics_admissible_count") or 0) == 0
            else "continue_collecting"
        )
    observation_evidence = {
        "snapshot_id": payload.get("snapshot_id"),
        "decision_cutoff_utc": payload.get("decision_cutoff_utc"),
        "summary": summary,
        "input_rejection_count": len(payload.get("input_rejections") or []),
        "supported_execution_decision": payload.get(
            "supported_execution_decision", "no_trade"
        ),
        "artifact_fingerprints": fingerprints,
    }
    if is_manifest_governed:
        observation_evidence.update({
            "frozen_manifest_id": frozen_manifest.get("manifest_id"),
            "frozen_manifest_sha256": frozen_manifest.get("manifest_sha256"),
            "supersedes_contract_id": payload.get("supersedes_contract_id"),
            "supersedes_cohort_id": payload.get("supersedes_cohort_id"),
        })
    if is_v4:
        observation_evidence["v4_external_trust_anchor"] = (
            v4_external_trust_anchor
        )
    observations = int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(payload.get("generated_utc") or utc_now()),
        source_system=source.name,
        result=result,
        evidence=observation_evidence,
    ))
    return definitions, observations


def observe_currency_state_after_cost_supersession(
    target: sqlite3.Connection, prior_source: Path, replacement_source: Path,
) -> int:
    """Append a non-statistical supersession only for a zero-evidence cohort."""
    try:
        prior = json.loads(prior_source.read_text(encoding="utf-8"))
        replacement = json.loads(replacement_source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return 0
    prior_id = str(prior.get("counterfactual_cohort_id") or "")
    replacement_id = str(replacement.get("counterfactual_cohort_id") or "")
    if (
        not prior_id
        or not replacement_id
        or replacement.get("supersedes_cohort_id") != prior_id
        or prior.get("research_only") is not True
        or replacement.get("research_only") is not True
        or prior.get("execution_eligible") is not False
        or replacement.get("execution_eligible") is not False
    ):
        return 0
    prior_summary = prior.get("summary") or {}
    evidence_counts = [
        int(prior_summary.get(field) or 0)
        for field in (
            "economics_admissible_count", "ranked_count", "selectable_count",
            "hold_switch_count",
        )
    ]
    if any(evidence_counts):
        return 0
    input_counts = [
        int(prior_summary.get(field) or 0)
        for field in (
            "venue_quote_input_count", "verifier_input_count",
            "economics_input_count", "submitted_canonical_record_count",
        )
    ]
    if any(input_counts):
        return 0
    if any(prior.get(field) is not None for field in (
        "quote_envelope", "verifier_envelope", "economics_envelope",
        "hold_switch_envelope",
    )):
        return 0
    replacement_manifest = replacement.get("frozen_manifest") or {}
    observed_at = str(replacement.get("generated_utc") or utc_now())
    existing = target.execute(
        "SELECT 1 FROM experiment_observations "
        "WHERE hypothesis_id=? AND observed_at=? AND source_system=? AND result=? "
        "LIMIT 1",
        (
            prior_id,
            observed_at,
            replacement_source.name,
            "engineering_superseded_zero_evidence",
        ),
    ).fetchone()
    if existing is not None:
        return 0
    return int(observe(
        target,
        hypothesis_id=prior_id,
        observed_at=observed_at,
        source_system=replacement_source.name,
        result="engineering_superseded_zero_evidence",
        evidence={
            "prior_snapshot_id": prior.get("snapshot_id"),
            "replacement_snapshot_id": replacement.get("snapshot_id"),
            "replacement_contract_id": replacement.get("counterfactual_contract_id"),
            "replacement_cohort_id": replacement_id,
            "replacement_manifest_id": replacement_manifest.get("manifest_id"),
            "replacement_manifest_sha256": replacement_manifest.get("manifest_sha256"),
            "prior_evidence_counts": dict(zip(
                (
                    "economics_admissible_count", "ranked_count",
                    "selectable_count", "hold_switch_count",
                ),
                evidence_counts,
            )),
            "prior_input_counts": dict(zip(
                (
                    "venue_quote_input_count", "verifier_input_count",
                    "economics_input_count", "submitted_canonical_record_count",
                ),
                input_counts,
            )),
            "reason": "material contract and evidence-lineage hardening before admissible evidence",
            "statistical_futility_retirement": False,
            "execution_eligible": False,
            "supported_execution_decision": "no_trade",
        },
    ))


def import_counterfactual_sim_gym(
    target: sqlite3.Connection, source: Path
) -> tuple[int, int]:
    """Register immutable SIM-gym cohorts without treating replay as proof.

    The gym owns its append-only cohort lineage.  This importer is deliberately
    read-only and records each material contract as one historical diagnostic
    hypothesis.  A later cohort supersedes an earlier engineering contract; it
    never rewrites, confirms, promotes, or statistically combines the earlier
    replay.
    """
    if not source.is_file():
        return 0, 0
    connection = ro(source)
    definitions = observations = 0
    try:
        required = {"sim_cohorts", "sim_cohort_transitions"}
        present = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if not required.issubset(present):
            return 0, 0
        parents = cohort_parent_map(connection, "sim_cohort_transitions")
        cohort_rows = list(
            connection.execute("SELECT * FROM sim_cohorts ORDER BY created_utc,cohort_id")
        )
        if not cohort_rows:
            return 0, 0
        current_cohort_id = str(cohort_rows[-1]["cohort_id"])
        for row in cohort_rows:
            cohort_id = str(row["cohort_id"])
            contract = json.loads(str(row["material_contract_json"] or "{}"))
            effective = contract.get("effective_config") or {}
            source_contract = effective.get("source") or {}
            sampling = effective.get("sampling") or {}
            partitions = effective.get("historical_partitions") or {}
            signal_rules = effective.get("signal_rules") or []
            execution_grid = effective.get("execution_grid") or {}
            source_manifest = contract.get("source_manifest") or []
            definition = {
                "hypothesis_id": cohort_id,
                "parent_hypothesis_id": parents.get(cohort_id),
                "experiment_kind": "historical_counterfactual_sim",
                "research_generation": str(
                    contract.get("contract_id") or row["experiment_key"]
                ),
                "idea_origin": "high_volume_matched_virtual_order_replay",
                "pre_registered": 1,
                "data_sources_json": canonical_json(
                    [source_contract.get("kind") or "OANDA_completed_M1_bid_ask"]
                ),
                "feature_contract_json": canonical_json({
                    "knowledge_time": source_contract.get("knowledge_time"),
                    "signal_rules": signal_rules,
                    "cadence_min": sampling.get("cadence_min"),
                }),
                "label_contract_json": canonical_json({
                    "entry_quote": source_contract.get("entry_quote"),
                    "path_quote": source_contract.get("path_quote"),
                    "execution_grid": execution_grid,
                    "partitions": partitions,
                    "historical_replay_can_confirm": False,
                }),
                "model_contract_json": canonical_json({"signal_rules": signal_rules}),
                "cost_contract_json": canonical_json(effective.get("costs") or {}),
                "allocator_contract_json": canonical_json(
                    effective.get("comparators") or {}
                ),
                "training_period_json": canonical_json({
                    "training": False,
                    "historical_replay": contract.get("window") or {},
                }),
                "selection_period_json": canonical_json({
                    "diagnostic_partitions": partitions.get("labels") or [],
                    "purged": bool(partitions.get("purge_overlapping_outcomes")),
                }),
                "confirmation_period_json": canonical_json({
                    "untouched_prospective_required": True,
                    "historical_replay_can_confirm": False,
                }),
                "all_parameters_tried_json": canonical_json({
                    "signal_rules": signal_rules,
                    "execution_grid": execution_grid,
                    "comparators": effective.get("comparators") or {},
                }),
                "selection_rule": (
                    "matched as-signaled, flipped, deterministic-random, and "
                    "no-trade historical diagnostics; no historical promotion"
                ),
                "holdouts_touched_json": canonical_json(
                    partitions.get("labels") or []
                ),
                "source_code_hash": stable_hash({
                    "runner": row["runner_sha256"],
                    "core": row["core_sha256"],
                }),
                "data_snapshot_hash": stable_hash(source_manifest),
                "definition_sha256": str(row["material_contract_sha256"]),
                "created_at": str(row["created_utc"]),
                "definition_json": canonical_json(contract),
            }
            definitions += int(insert_experiment(target, definition))
            result = (
                "historical_diagnostic_current"
                if cohort_id == current_cohort_id
                else "engineering_superseded_material_contract"
            )
            evidence = {
                "cohort_id": cohort_id,
                "contract_id": contract.get("contract_id"),
                "parent_cohort_id": parents.get(cohort_id),
                "material_contract_sha256": row["material_contract_sha256"],
                "source_manifest_count": len(source_manifest),
                "historical_replay_can_confirm": False,
                "execution_eligible": False,
                "supported_decision": "no_trade",
            }
            observations += int(observe(
                target,
                hypothesis_id=cohort_id,
                observed_at=str(row["created_utc"]),
                source_system=source.name,
                result=result,
                evidence=evidence,
                retirement_reason=(
                    "material engineering contract superseded; evidence preserved"
                    if cohort_id != current_cohort_id
                    else None
                ),
                retired_at=(
                    str(cohort_rows[-1]["created_utc"])
                    if cohort_id != current_cohort_id
                    else None
                ),
            ))
    finally:
        connection.close()
    return definitions, observations


def import_sequential_deliberate_replay(
    target: sqlite3.Connection, source: Path
) -> tuple[int, int]:
    """Register the training-only deliberate-practice projection.

    The source SIM cohort remains the parent.  Practice cases and repeated
    attempts cannot become proof, so this importer never confirms or promotes.
    """
    if not source.is_file():
        return 0, 0
    connection = ro(source)
    definitions = observations = 0
    try:
        present = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if not {"replay_cohorts", "replay_snapshots"}.issubset(present):
            return 0, 0
        for row in connection.execute(
            "SELECT * FROM replay_cohorts ORDER BY created_utc,cohort_id"
        ):
            cohort_id = str(row["cohort_id"])
            parent_id = str(row["source_cohort_id"])
            contract = json.loads(str(row["contract_json"] or "{}"))
            snapshot = connection.execute(
                "SELECT statistics_json FROM replay_snapshots "
                "WHERE cohort_id=? ORDER BY generated_utc DESC LIMIT 1",
                (cohort_id,),
            ).fetchone()
            statistics = json.loads(str(snapshot[0])) if snapshot else {}
            census = statistics.get("repetition_census") or {}
            definition = {
                "hypothesis_id": cohort_id,
                "parent_hypothesis_id": parent_id,
                "experiment_kind": "historical_deliberate_practice",
                "research_generation": "sequential_deliberate_replay_v1",
                "idea_origin": "high_volume_deliberate_market_replay_with_honest_repetition_counts",
                "pre_registered": 1,
                "data_sources_json": canonical_json([
                    "independently_verified_counterfactual_sim_gym",
                ]),
                "feature_contract_json": canonical_json({
                    "global_portfolio_clock": True,
                    "future_free_situation_fingerprint": True,
                    "blind_case_presentation": True,
                    "source_cohort_id": parent_id,
                }),
                "label_contract_json": canonical_json({
                    "historical_training_discovery_only": True,
                    "variants_are_nested": True,
                    "repeat_attempts_are_not_new_market_repetitions": True,
                    "management_exit_rotation": "not_measured_in_endpoint_only_source",
                }),
                "model_contract_json": canonical_json({
                    "model": "none_case_bank_and_precommitment_journal",
                }),
                "cost_contract_json": "{}",
                "allocator_contract_json": canonical_json({
                    "one_primary_action_per_session_clock": True,
                    "actions": ["wait", "enter", "hold", "exit", "rotate"],
                    "maximum_open_positions": 1,
                }),
                "training_period_json": canonical_json({
                    "source": "all already-inspected SIM historical partitions",
                    "portfolio_decision_clocks": census.get("portfolio_decision_clocks"),
                }),
                "selection_period_json": canonical_json({
                    "training_discovery_only": True,
                }),
                "confirmation_period_json": canonical_json({
                    "none": True,
                    "later_untouched_prospective_cohort_required": True,
                }),
                "all_parameters_tried_json": canonical_json({
                    "source_virtual_variants": census.get("source_virtual_variants"),
                    "source_two_sided_outcome_parts": census.get("source_two_sided_outcome_parts"),
                }),
                "selection_rule": (
                    "one primary portfolio action per global clock; all variants and "
                    "repeat attempts remain nested training diagnostics"
                ),
                "holdouts_touched_json": canonical_json([
                    "diagnostic_early", "diagnostic_middle", "diagnostic_late",
                ]),
                "source_code_hash": stable_hash({
                    "runner": row["runner_sha256"],
                    "core": row["core_sha256"],
                }),
                "data_snapshot_hash": str(row["source_database_sha256"]),
                "definition_sha256": str(row["contract_sha256"]),
                "created_at": str(row["created_utc"]),
                "definition_json": canonical_json(contract),
            }
            definitions += int(insert_experiment(target, definition))
            observations += int(observe(
                target,
                hypothesis_id=cohort_id,
                observed_at=str(row["created_utc"]),
                source_system=source.name,
                result="historical_practice_curriculum",
                evidence={
                    "source_cohort_id": parent_id,
                    "repetition_census": census,
                    "historical_replay_can_confirm": False,
                    "execution_eligible": False,
                    "supported_decision": "no_trade",
                },
            ))
    finally:
        connection.close()
    return definitions, observations


def import_sequential_portfolio_replay(
    target: sqlite3.Connection, source_root: Path
) -> tuple[int, int]:
    """Register the verified bar-by-bar portfolio-training sidecar.

    The latest state resolves its content-addressed cohort database. Every
    session is historical training/discovery, permanently proof-ineligible.
    """
    state_path = source_root / "sequential_portfolio_replay_v1.json"
    verifier_path = source_root / "sequential_portfolio_replay_verifier_v1.json"
    if not state_path.is_file() or not verifier_path.is_file():
        return 0, 0
    state = json.loads(state_path.read_text(encoding="utf-8"))
    verifier = json.loads(verifier_path.read_text(encoding="utf-8"))
    if verifier.get("verified") is not True or verifier.get("failures"):
        return 0, 0
    source = Path(str(state.get("database") or ""))
    if not source.is_file():
        return 0, 0
    connection = ro(source)
    definitions = observations = 0
    try:
        cohort = connection.execute(
            "SELECT * FROM spr_cohorts WHERE cohort_id=?",
            (str(state["cohort_id"]),),
        ).fetchone()
        session = connection.execute(
            "SELECT * FROM spr_sessions WHERE session_id=?",
            (str(state["session_id"]),),
        ).fetchone()
        if cohort is None or session is None:
            return 0, 0
        contract = json.loads(str(cohort["contract_json"]))
        statistics = {
            "global_clock_count": state.get("global_clock_count"),
            "pair_context_count": state.get("pair_context_count"),
            "action_counts": state.get("action_counts"),
            "execution_leg_count": state.get("execution_leg_count"),
            "counterfactual_count": state.get("counterfactual_count"),
            "terminal_realized_pips": state.get("terminal_realized_pips"),
            "terminal_flat": state.get("terminal_flat"),
            "structural_component_count": state.get("structural_component_count"),
            "independent_regime_count": state.get("independent_regime_count"),
            "verifier_failures": verifier.get("failures"),
        }
        code = contract.get("code") or {}
        source_contract = contract.get("source") or {}
        definition = {
            "hypothesis_id": str(cohort["cohort_id"]),
            "parent_hypothesis_id": str(cohort["source_cohort_id"]),
            "experiment_kind": "historical_sequential_portfolio_training",
            "research_generation": "sequential_portfolio_replay_v1",
            "idea_origin": "high_volume_deliberate_practice_with_real_portfolio_state",
            "pre_registered": 1,
            "data_sources_json": canonical_json([
                "independently_verified_frozen_oanda_m1_bid_ask_archives",
            ]),
            "feature_contract_json": canonical_json({
                "completed_m1_only": True,
                "global_decision_cadence_min": 5,
                "future_free_source_slices": True,
                "four_pair_opportunity_set": True,
            }),
            "label_contract_json": canonical_json({
                "historical_training_discovery_only": True,
                "one_primary_action_per_clock": True,
                "counterfactuals_count_as_repetitions": False,
                "independent_regimes": "unknown_one_inspected_window",
            }),
            "model_contract_json": canonical_json({
                "policy_id": contract.get("config", {}).get("frozen_policy", {}).get("policy_id"),
                "policy_frozen": True,
                "training_mechanics_baseline_only": True,
            }),
            "cost_contract_json": canonical_json(
                contract.get("config", {}).get("costs") or {}
            ),
            "allocator_contract_json": canonical_json({
                "actions": ["wait", "enter", "hold", "exit", "rotate"],
                "maximum_open_positions": 1,
                "one_minute_execution_delay": True,
                "rotation_has_two_execution_legs": True,
            }),
            "training_period_json": canonical_json({
                "start_epoch": session["start_epoch"],
                "end_epoch": session["end_epoch"],
                "already_inspected": True,
            }),
            "selection_period_json": canonical_json({
                "metadata_selected_friday_overlap_block": True,
                "outcome_selected": False,
            }),
            "confirmation_period_json": canonical_json({
                "none": True,
                "later_untouched_prospective_cohort_required": True,
            }),
            "all_parameters_tried_json": canonical_json({
                "one_frozen_policy": True,
                "depth_one_counterfactuals": True,
            }),
            "selection_rule": (
                "one frozen action policy over a predeclared four-hour training "
                "session; no result from this inspected session can promote"
            ),
            "holdouts_touched_json": canonical_json(["historical_training_discovery"]),
            "source_code_hash": stable_hash({
                key: value.get("sha256") for key, value in sorted(code.items())
            }),
            "data_snapshot_hash": stable_hash(source_contract),
            "definition_sha256": str(cohort["contract_sha256"]),
            "created_at": str(cohort["created_utc"]),
            "definition_json": canonical_json(contract),
        }
        definitions += int(insert_experiment(target, definition))
        observations += int(observe(
            target,
            hypothesis_id=str(cohort["cohort_id"]),
            observed_at=str(state.get("generated_utc") or cohort["created_utc"]),
            source_system=source.name,
            result="historical_sequential_portfolio_training",
            evidence={
                **statistics,
                "source_cohort_id": cohort["source_cohort_id"],
                "proof_eligible": False,
                "execution_eligible": False,
                "supported_decision": "no_trade",
            },
        ))
    finally:
        connection.close()
    return definitions, observations


def _genealogy_has_hypothesis(
    target: sqlite3.Connection, hypothesis_id: str | None, *,
    experiment_kind: str | None = None,
) -> bool:
    if not hypothesis_id:
        return False
    row = target.execute(
        "SELECT experiment_kind FROM experiments WHERE hypothesis_id=?",
        (str(hypothesis_id),),
    ).fetchone()
    return bool(
        row is not None
        and (experiment_kind is None or str(row[0]) == experiment_kind)
    )


def _research_only_policy_is_closed(value: dict[str, Any]) -> bool:
    """Return true only for the explicit nonexecuting/nonpromotion policy."""
    return bool(
        value.get("research_only") is True
        and value.get("execution_eligible") is False
        and value.get("can_place_orders") is False
        and value.get("can_promote") is False
        and value.get("supported_decision") == "no_trade"
        and value.get("can_authorize") is False
    )


def _full_research_only_policy_is_closed(value: dict[str, Any]) -> bool:
    """Require every safety field explicitly; missing is never equivalent to false."""
    return bool(
        isinstance(value, dict)
        and all(value.get(key) == expected for key, expected in FULL_RESEARCH_ONLY_SAFETY.items())
        and all(key in value for key in FULL_RESEARCH_ONLY_SAFETY)
    )


def _variadic_hash(*parts: Any) -> str:
    """Hash using the producer convention: canonical JSON of the argument list."""
    return hashlib.sha256(canonical_json(list(parts)).encode("utf-8")).hexdigest()


def _is_sha256(value: Any) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _safe_child_file(root: Path, value: str | Path) -> Path | None:
    """Resolve an ordinary file beneath root; reject links and path escapes."""
    try:
        root_resolved = root.resolve(strict=True)
        path = Path(value)
        if path.is_symlink():
            return None
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError):
        return None
    if (
        not resolved.is_file()
        or resolved.is_symlink()
        or root_resolved not in resolved.parents
    ):
        return None
    return resolved


def _row_self_hash(row: sqlite3.Row) -> str:
    return _variadic_hash({
        key: row[key] for key in row.keys() if key != "row_sha256"
    })


def _sealed_table_root(
    connection: sqlite3.Connection, table: str, cohort_id: str,
) -> dict[str, Any] | None:
    try:
        rows = list(connection.execute(
            f"SELECT * FROM {table} WHERE cohort_id=?", (cohort_id,)
        ))
    except sqlite3.Error:
        return None
    if any(
        "row_sha256" not in row.keys()
        or str(row["row_sha256"]) != _row_self_hash(row)
        for row in rows
    ):
        return None
    hashes = sorted(str(row["row_sha256"]) for row in rows)
    return {"count": len(hashes), "set_sha256": _variadic_hash(hashes)}


def _stored_hash_table_root(
    connection: sqlite3.Connection, table: str, cohort_id: str,
) -> dict[str, Any] | None:
    """Rebuild a replay table root from its immutable stored identity hashes.

    Replay rows seal table-specific identity subsets rather than every storage
    column.  The receipt separately pins the independently verified database
    bytes, so genealogy reproduces that declared root without substituting a
    different generic full-row hashing contract.
    """
    try:
        rows = list(connection.execute(
            f"SELECT row_sha256 FROM {table} WHERE cohort_id=?", (cohort_id,)
        ))
    except sqlite3.Error:
        return None
    hashes = sorted(str(row[0]) for row in rows)
    if not all(_is_sha256(value) for value in hashes):
        return None
    return {"count": len(hashes), "set_sha256": _variadic_hash(hashes)}


def _insert_experiment_exact(
    connection: sqlite3.Connection, value: dict[str, Any],
) -> bool:
    """Scoped immutable insert: an existing ID must match every registered field."""
    existing = connection.execute(
        f"SELECT {','.join(EXPERIMENT_COLUMNS)} FROM experiments WHERE hypothesis_id=?",
        (value.get("hypothesis_id"),),
    ).fetchone()
    if existing is not None:
        proposed = tuple(value.get(column) for column in EXPERIMENT_COLUMNS)
        if tuple(existing) != proposed:
            raise ValueError("hypothesis_id immutable registration conflict")
        return False
    return insert_experiment(connection, value)


def _dataset_manifest_binding(
    cohort_root: Path, datasets: Any,
) -> dict[str, dict[str, Any]] | None:
    """Bind every all-68 dataset descriptor to its exact gzip bytes and row roots."""
    expected_files = {
        "clocks": "global_clocks.jsonl.gz",
        "pair_contexts": "pair_contexts.jsonl.gz",
        "decisions": "portfolio_decisions.jsonl.gz",
        "feedback": "feedback.jsonl.gz",
        "counterfactuals": "counterfactuals.jsonl.gz",
        "terminals": "session_terminals.jsonl.gz",
    }
    if not isinstance(datasets, dict) or set(datasets) != set(expected_files):
        return None
    verified: dict[str, dict[str, Any]] = {}
    for name, expected_name in expected_files.items():
        descriptor = datasets.get(name)
        if not isinstance(descriptor, dict):
            return None
        if str(descriptor.get("relative_path") or "") != expected_name:
            return None
        for key in ("gzip_sha256", "row_set_sha256", "ordered_row_sha256"):
            if not _is_sha256(descriptor.get(key)):
                return None
        path = _safe_child_file(cohort_root, cohort_root / expected_name)
        if path is None:
            return None
        payload = path.read_bytes()
        if (
            hashlib.sha256(payload).hexdigest() != descriptor["gzip_sha256"]
            or len(payload) != int(descriptor.get("gzip_bytes") or -1)
        ):
            return None
        try:
            raw = gzip.decompress(payload)
            rows = [
                json.loads(line) for line in raw.decode("utf-8").splitlines() if line
            ]
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        if len(rows) != int(descriptor.get("row_count") or -1):
            return None
        row_hashes: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                return None
            claimed = str(row.get("row_sha256") or "")
            expected = _variadic_hash({
                key: value for key, value in row.items() if key != "row_sha256"
            })
            if claimed != expected:
                return None
            row_hashes.append(claimed)
        if (
            _variadic_hash(sorted(row_hashes)) != descriptor["row_set_sha256"]
            or _variadic_hash(row_hashes) != descriptor["ordered_row_sha256"]
        ):
            return None
        verified[name] = {
            key: descriptor[key]
            for key in (
                "relative_path", "gzip_bytes", "gzip_sha256", "row_count",
                "row_set_sha256", "ordered_row_sha256",
            )
        }
    return verified


def _all68_dataset_summary(
    cohort_root: Path, cohort_id: str,
    datasets: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """Independently derive the state summary from the six sealed datasets."""
    rows: dict[str, list[dict[str, Any]]] = {}
    market_repetition_contract = {
        "clocks": 1,
        "pair_contexts": 0,
        "decisions": 1,
        "feedback": 0,
        "counterfactuals": 0,
        "terminals": 0,
    }
    try:
        for name, descriptor in datasets.items():
            path = _safe_child_file(
                cohort_root, cohort_root / str(descriptor["relative_path"]),
            )
            if path is None:
                return None
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                values = [json.loads(line) for line in handle if line.strip()]
            if (
                len(values) != int(descriptor["row_count"])
                or any(
                    not isinstance(row, dict)
                    or str(row.get("cohort_id") or "") != cohort_id
                    or row.get("proof_eligible") is not False
                    or row.get("counts_as_regime_repetition") != 0
                    or row.get("counts_as_market_repetition")
                    != market_repetition_contract[name]
                    for row in values
                )
            ):
                return None
            rows[name] = values
    except (OSError, EOFError, gzip.BadGzipFile, json.JSONDecodeError, KeyError,
            TypeError, ValueError):
        return None
    clocks = rows["clocks"]
    contexts = rows["pair_contexts"]
    decisions = rows["decisions"]
    terminals = rows["terminals"]
    availability: Counter[str] = Counter()
    for row in contexts:
        if row.get("fully_ready") is True:
            availability["fully_ready"] += 1
        missing = row.get("missing_reasons")
        if not isinstance(missing, list):
            return None
        for reason in missing:
            availability[f"missing_{reason}"] += 1
    try:
        action_counts = Counter(
            str((row.get("decision") or {})["action"]) for row in decisions
        )
        execution_leg_count = sum(
            len((row.get("execution") or {})["legs"]) for row in decisions
        )
        latest_terminal = max(terminals, key=lambda row: int(row["terminal_epoch"]))
        terminal_flat = all(
            row.get("terminal_flat") is True
            and (row.get("state_after") or {}).get("position") is None
            for row in terminals
        )
        realized_pips = float(
            (latest_terminal.get("state_after") or {})["realized_pips"]
        )
    except (KeyError, TypeError, ValueError):
        return None
    return {
        "session_count": len(terminals),
        "global_clock_count": len(clocks),
        "pair_context_count": len(contexts),
        "causal_ready_context_count": sum(
            row.get("causal_ready") is True for row in contexts
        ),
        "candidate_context_count": sum(
            row.get("candidate") is not None for row in contexts
        ),
        "execution_leg_count": execution_leg_count,
        "counterfactual_count": len(rows["counterfactuals"]),
        "realized_pips": realized_pips,
        "terminal_flat": terminal_flat,
        "market_repetition_count": sum(
            int(row.get("counts_as_market_repetition") or 0) for row in clocks
        ),
        "action_counts": dict(action_counts),
        "context_availability_counts": dict(availability),
    }


def _source_window_end(source_manifest: dict[str, Any]) -> str:
    """Return a deterministic cohort timestamp from the immutable source window."""
    sessions = source_manifest.get("sessions")
    if not isinstance(sessions, list):
        return ""
    return max(
        (
            str(row.get("end_utc_exclusive") or "")
            for row in sessions if isinstance(row, dict)
        ),
        default="",
    )


def import_sequential_portfolio_curriculum(
    target: sqlite3.Connection, source_root: Path,
) -> tuple[int, int]:
    """Register one independently verified deliberate-practice curriculum.

    This is learner-training evidence, not market proof. The source replay is
    the immutable parent and must already exist in the genealogy.
    """
    state_path = source_root / "sequential_portfolio_curriculum_v1.json"
    verifier_path = source_root / "sequential_portfolio_curriculum_verifier_v1.json"
    if not state_path.is_file() or not verifier_path.is_file():
        return 0, 0
    state = json.loads(state_path.read_text(encoding="utf-8"))
    verifier = json.loads(verifier_path.read_text(encoding="utf-8"))
    cohort_id = str(state.get("cohort_id") or "")
    parent_id = str(state.get("source_cohort_id") or "")
    contract_sha = str(state.get("contract_sha256") or "")
    database_path = _safe_child_file(
        source_root, Path(str(state.get("database") or "")),
    )
    if not (
        cohort_id == "sequential_portfolio_curriculum_v1." + contract_sha[:20]
        and _is_sha256(contract_sha)
        and verifier.get("verified") is True
        and verifier.get("failures") == []
        and str(verifier.get("cohort_id") or "") == cohort_id
        and _full_research_only_policy_is_closed(state)
        and _full_research_only_policy_is_closed(verifier)
        and state.get("evidence_role") == "historical_training_discovery"
        and verifier.get("evidence_role") == "historical_training_discovery"
        and state.get("counterfactual_options_count_as_repetitions") is False
        and state.get("repeat_attempts_count_as_repetitions") is False
        and state.get("independent_regime_count") is None
        and _genealogy_has_hypothesis(
            target, parent_id,
            experiment_kind="historical_sequential_portfolio_training",
        )
        and database_path is not None
        and database_path.parent.name == cohort_id
    ):
        return 0, 0
    assert database_path is not None
    connection = ro(database_path)
    definitions = observations = 0
    try:
        cohort = connection.execute(
            "SELECT * FROM spc_cohorts WHERE cohort_id=?", (cohort_id,)
        ).fetchone()
        if cohort is None:
            return 0, 0
        contract = json.loads(str(cohort["contract_json"]))
        policy = contract.get("policy") or {}
        statistics = {
            key: state.get(key) for key in (
                "practice_session_count", "attempt_count", "distinct_case_count",
                "repeated_attempt_count", "distinct_market_repetition_count",
                "structural_episode_count", "independent_regime_count",
                "independent_regime_count_state", "correct_attempt_count",
                "mean_regret_pips", "mistake_counts", "roots",
            )
        }
        root_tables = {
            "cases": "spc_cases",
            "source_outcomes": "spc_source_outcomes",
            "sessions": "spc_sessions",
            "assignments": "spc_assignments",
            "attempts": "spc_attempts",
            "feedback": "spc_feedback",
            "memory_transitions": "spc_memory_transitions",
        }
        rebuilt_roots = {
            name: _sealed_table_root(connection, table, cohort_id)
            for name, table in root_tables.items()
        }
        checks = verifier.get("checks") or {}
        receipt_check_map = {
            "precommitted_session_count": "practice_session_count",
            "attempt_count": "attempt_count",
            "distinct_case_count": "distinct_case_count",
            "repeated_attempt_count": "repeated_attempt_count",
            "distinct_market_repetition_count": "distinct_market_repetition_count",
            "structural_episode_count": "structural_episode_count",
            "correct_attempt_count": "correct_attempt_count",
            "mean_regret_pips": "mean_regret_pips",
            "mistake_counts": "mistake_counts",
        }
        seal = connection.execute(
            "SELECT * FROM spc_session_seals WHERE seal_id=? AND cohort_id=?",
            (str(state.get("session_seal_id") or ""), cohort_id),
        ).fetchone()
        snapshots = list(connection.execute(
            "SELECT * FROM spc_snapshots WHERE cohort_id=?", (cohort_id,)
        ))
        expected_seal_payload = {
            "cohort_id": cohort_id,
            "contract_sha256": contract_sha,
            **statistics,
        }
        timestamp = str((contract.get("timestamp_contract") or {}).get("value") or "")
        if not (
            str(cohort["source_cohort_id"]) == parent_id
            and str(cohort["source_session_id"]) == str(state.get("source_session_id"))
            and str(cohort["contract_sha256"]) == contract_sha
            and _variadic_hash(contract) == contract_sha
            and str(contract.get("source_cohort_id") or "") == parent_id
            and str(contract.get("source_session_id") or "")
            == str(state.get("source_session_id") or "")
            and str(contract.get("source_session_seal_id") or "")
            == str(state.get("source_session_seal_id") or "")
            and _full_research_only_policy_is_closed(policy)
            and rebuilt_roots == state.get("roots")
            and all(value is not None for value in rebuilt_roots.values())
            and all(
                checks.get(receipt_key) == state.get(state_key)
                for receipt_key, state_key in receipt_check_map.items()
            )
            and checks.get("verifier_forbidden_imports") == []
            and seal is not None
            and json.loads(str(seal["seal_json"])) == expected_seal_payload
            and str(seal["seal_sha256"]) == _variadic_hash(expected_seal_payload)
            and str(seal["seal_id"])
            == "spcseal_" + _variadic_hash(expected_seal_payload)[:28]
            and len(snapshots) == 1
            and json.loads(str(snapshots[0]["statistics_json"])) == statistics
            and str(snapshots[0]["statistics_sha256"]) == _variadic_hash(statistics)
            and str(snapshots[0]["snapshot_id"])
            == "spcsnapshot_" + _variadic_hash(cohort_id, statistics)[:28]
            and timestamp
            and str(cohort["created_utc"]) == timestamp
            and str(state.get("generated_utc") or "") == timestamp
        ):
            return 0, 0
        definition = {
            "hypothesis_id": cohort_id,
            "parent_hypothesis_id": parent_id,
            "experiment_kind": "historical_sequential_portfolio_curriculum",
            "research_generation": "sequential_portfolio_curriculum_v1",
            "idea_origin": "novelty_weighted_spaced_deliberate_practice",
            "pre_registered": 1,
            "data_sources_json": canonical_json([
                "independently_verified_sequential_portfolio_replay_v1",
            ]),
            "feature_contract_json": canonical_json({
                "immutable_case_identity": True,
                "session_precommit_before_feedback": True,
                "within_session_adaptation_forbidden": True,
            }),
            "label_contract_json": canonical_json({
                "local_depth_one_regret": True,
                "historical_training_only": True,
                "repeat_attempts_are_not_market_repetitions": True,
                "independent_regime_count": None,
            }),
            "model_contract_json": canonical_json({
                "learner_id": (contract.get("curriculum") or {}).get("learner_id"),
                "scheduler_id": (contract.get("curriculum") or {}).get("scheduler_id"),
                "mechanics_fixture_not_edge_claim": True,
            }),
            "cost_contract_json": canonical_json({
                "inherited_from_source_replay": True,
            }),
            "allocator_contract_json": canonical_json({
                "action_set": ["wait", "enter", "hold", "exit", "rotate"],
                "nonexecuting": True,
            }),
            "training_period_json": canonical_json({
                "source_session_id": state.get("source_session_id"),
                "practice_session_count": state.get("practice_session_count"),
                "attempt_count": state.get("attempt_count"),
            }),
            "selection_period_json": canonical_json({
                "historical_training_discovery": True,
            }),
            "confirmation_period_json": canonical_json({
                "none": True,
                "later_untouched_prospective_cohort_required": True,
            }),
            "all_parameters_tried_json": canonical_json(
                contract.get("curriculum") or {}
            ),
            "selection_rule": (
                "precommit complete practice sessions before feedback; reviews "
                "remain nested beneath their original historical cases"
            ),
            "holdouts_touched_json": canonical_json([
                "already_inspected_historical_training_window",
            ]),
            "source_code_hash": stable_hash({
                "core": contract.get("core_code_sha256"),
                "producer": contract.get("producer_code_sha256"),
            }),
            "data_snapshot_hash": stable_hash({
                "source_seal": state.get("source_session_seal_id"),
                "curriculum_seal_id": state.get("session_seal_id"),
                "curriculum_seal_sha256": seal["seal_sha256"],
                "roots": state.get("roots"),
                "verifier_semantic_sha256": stable_hash({
                    key: value for key, value in verifier.items()
                    if key != "generated_utc"
                }),
            }),
            "definition_sha256": contract_sha,
            "created_at": str(cohort["created_utc"]),
            "definition_json": canonical_json(contract),
        }
        definitions += int(_insert_experiment_exact(target, definition))
        observations += int(observe(
            target,
            hypothesis_id=cohort_id,
            observed_at=str(state.get("generated_utc") or cohort["created_utc"]),
            source_system=state_path.name,
            result="verified_historical_training_curriculum",
            evidence={
                "practice_session_count": state.get("practice_session_count"),
                "attempt_count": state.get("attempt_count"),
                "distinct_case_count": state.get("distinct_case_count"),
                "repeated_attempt_count": state.get("repeated_attempt_count"),
                "distinct_market_repetition_count": state.get(
                    "distinct_market_repetition_count"
                ),
                "structural_episode_count": state.get("structural_episode_count"),
                "independent_regime_count": None,
                "proof_eligible": False,
                "execution_eligible": False,
                "supported_decision": "no_trade",
                "verifier_failures": [],
            },
        ))
    finally:
        connection.close()
    return definitions, observations


def import_sequential_portfolio_mistake_curriculum(
    target: sqlite3.Connection, replay_root: Path,
) -> tuple[int, int]:
    """Register only the independently reconstructed, sealed mistake cohort."""
    state_path = replay_root / "sequential_portfolio_replay_v1.json"
    replay_verifier_path = replay_root / "sequential_portfolio_replay_verifier_v1.json"
    mistake_root = replay_root.parent / "sequential_portfolio_mistake_curriculum_v1"
    # A short-root alias is supported for Windows test/restore locations where
    # the immutable content-addressed filename would otherwise exceed MAX_PATH.
    if not mistake_root.exists():
        mistake_root = replay_root.parent / ".mistake_v1"
    current_report_path = mistake_root / "sequential_portfolio_mistake_curriculum_v1.json"
    current_receipt_path = (
        mistake_root / "sequential_portfolio_mistake_curriculum_verifier_v1.json"
    )
    if not all(path.is_file() for path in (
        state_path, replay_verifier_path, current_report_path, current_receipt_path,
    )):
        return 0, 0
    state = json.loads(state_path.read_text(encoding="utf-8"))
    replay_verifier = json.loads(replay_verifier_path.read_text(encoding="utf-8"))
    report = json.loads(current_report_path.read_text(encoding="utf-8"))
    receipt = json.loads(current_receipt_path.read_text(encoding="utf-8"))
    parent_id = str(state.get("cohort_id") or "")
    cohort_id = str(report.get("cohort_id") or "")
    cohort_root = mistake_root / "cohorts" / cohort_id
    report_path = cohort_root / "SEQUENTIAL_PORTFOLIO_MISTAKE_CURRICULUM_V1.json"
    receipt_path = cohort_root / "sequential_portfolio_mistake_curriculum_verifier_v1.json"
    material_path = cohort_root / "material_contract_v1.json"
    if not all(path.is_file() and not path.is_symlink() for path in (
        report_path, receipt_path, material_path,
    )):
        return 0, 0
    if (
        current_report_path.read_bytes() != report_path.read_bytes()
        or current_receipt_path.read_bytes() != receipt_path.read_bytes()
    ):
        return 0, 0
    material = report.get("material_contract") or {}
    archived_material = json.loads(material_path.read_text(encoding="utf-8"))
    # The mistake-curriculum producer seals the canonical argument tuple.
    material_sha = _variadic_hash(material)
    report_semantic = dict(report)
    report_id = str(report_semantic.pop("report_id", ""))
    claimed_report_sha = str(report_semantic.pop("report_sha256", ""))
    computed_report_sha = _variadic_hash(report_semantic)
    binding = report.get("source_binding") or {}
    classification = report.get("classification_contract") or {}
    checks = receipt.get("checks") or {}
    database_path = _safe_child_file(
        replay_root, Path(str(state.get("database") or "")),
    )
    if database_path is None:
        return 0, 0
    if not (
        cohort_id == "sequential_portfolio_mistake_curriculum_v1." + material_sha[:20]
        and str(report.get("material_contract_sha256") or "") == material_sha
        and archived_material == material
        and receipt.get("verified") is True
        and receipt.get("failures") == []
        and str(receipt.get("cohort_id") or "") == cohort_id
        and str(receipt.get("report_id") or "") == report_id
        and str(receipt.get("material_contract_sha256") or "") == material_sha
        and str(receipt.get("report_content_sha256") or "") == computed_report_sha
        and str(receipt.get("report_sha256") or "") == _sha256_file(report_path)
        and report_id == "sprmistakecurriculum_" + computed_report_sha[:28]
        and claimed_report_sha == computed_report_sha
        and _full_research_only_policy_is_closed(report)
        and _full_research_only_policy_is_closed(receipt)
        and _full_research_only_policy_is_closed(material.get("policy") or {})
        and report.get("evidence_role") == "historical_training_curriculum"
        and receipt.get("evidence_role") == "historical_training_curriculum"
        and (material.get("policy") or {}).get("evidence_role")
        == "historical_training_curriculum"
        and report.get("lifecycle_write") is False
        and report.get("signal_feed_write") is False
        and receipt.get("lifecycle_write") is False
        and receipt.get("signal_feed_write") is False
        and replay_verifier.get("verified") is True
        and replay_verifier.get("failures") == []
        and str(replay_verifier.get("cohort_id") or "") == parent_id
        and _research_only_policy_is_closed(state)
        and state.get("proof_eligible") is False
        and _sha256_file(state_path) == str(binding.get("source_state_sha256") or "")
        and _sha256_file(replay_verifier_path)
        == str(binding.get("source_verifier_receipt_sha256") or "")
        and _sha256_file(database_path)
        == str(binding.get("source_database_sha256") or "")
    ):
        return 0, 0
    if not (
        str(binding.get("cohort_id") or "") == parent_id
        and str(binding.get("session_id") or "") == str(state.get("session_id") or "")
        and str(binding.get("session_seal_id") or "") == str(
            state.get("session_seal_id") or ""
        )
        and binding.get("verified_roots") == state.get("roots")
        and checks.get("source_cohort_id") == parent_id
        and checks.get("source_session_id") == state.get("session_id")
        and checks.get("source_session_seal_id") == state.get("session_seal_id")
        and checks.get("source_session_seal_sha256")
        == binding.get("session_seal_sha256")
        and checks.get("source_database_sha256")
        == binding.get("source_database_sha256")
        and checks.get("source_state_sha256") == binding.get("source_state_sha256")
        and checks.get("source_verifier_receipt_sha256")
        == binding.get("source_verifier_receipt_sha256")
        and checks.get("source_roots") == binding.get("verified_roots")
        and checks.get("raw_observation_count")
        == (report.get("summary") or {}).get("raw_observation_count")
        and checks.get("structural_cluster_count")
        == (report.get("summary") or {}).get("structural_cluster_count")
        and checks.get("verifier_forbidden_imports") == []
        and (report.get("summary") or {}).get("independent_regime_count") is None
        and classification.get("raw_observations_are_not_independent") is True
        and classification.get("structural_clusters_are_not_independent_regimes") is True
        and _genealogy_has_hypothesis(
            target, parent_id,
            experiment_kind="historical_sequential_portfolio_training",
        )
    ):
        return 0, 0
    connection = ro(database_path)
    try:
        root_tables = {
            "administrative_events": "spr_administrative_events",
            "clocks": "spr_clocks",
            "counterfactuals": "spr_counterfactuals",
            "decisions": "spr_decisions",
            "execution_legs": "spr_execution_legs",
            "execution_outcomes": "spr_execution_outcomes",
            "feedback": "spr_feedback",
            "pair_contexts": "spr_pair_contexts",
        }
        rebuilt_roots = {
            name: _stored_hash_table_root(connection, table, parent_id)
            for name, table in root_tables.items()
        }
        seal = connection.execute(
            "SELECT * FROM spr_session_seals WHERE seal_id=? AND cohort_id=? "
            "AND session_id=?",
            (state.get("session_seal_id"), parent_id, state.get("session_id")),
        ).fetchone()
        if not (
            all(value is not None for value in rebuilt_roots.values())
            and rebuilt_roots == state.get("roots")
            and seal is not None
            and str(seal["seal_sha256"]) == binding.get("session_seal_sha256")
            and _variadic_hash(json.loads(str(seal["seal_json"])))
            == str(seal["seal_sha256"])
            and (json.loads(str(seal["seal_json"]))).get("roots") == rebuilt_roots
        ):
            return 0, 0
    finally:
        connection.close()
    contract_definition = {
        "material_contract": material,
        "report_id": report_id,
        "report_content_sha256": computed_report_sha,
        "classification_contract": classification,
        "source_binding": binding,
        "limitations": report.get("limitations"),
        "policy": material.get("policy"),
    }
    definition = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": parent_id,
        "experiment_kind": "historical_sequential_portfolio_mistake_curriculum",
        "research_generation": "sequential_portfolio_mistake_curriculum_v1",
        "idea_origin": "structurally_deduplicated_local_decision_mistake_taxonomy",
        "pre_registered": 1,
        "data_sources_json": canonical_json([
            "independently_verified_sequential_portfolio_replay_v1",
            "sealed_depth_one_counterfactual_feedback",
        ]),
        "feature_contract_json": canonical_json(classification),
        "label_contract_json": canonical_json({
            "categories_nonexclusive": True,
            "local_not_globally_optimal": True,
            "structural_clusters_not_independent_regimes": True,
        }),
        "model_contract_json": canonical_json({
            "model": "none_frozen_rule_taxonomy",
        }),
        "cost_contract_json": canonical_json({
            "inherited_from_source_replay": True,
        }),
        "allocator_contract_json": canonical_json({
            "nonexecuting_diagnostic": True,
        }),
        "training_period_json": canonical_json({
            "source_session_id": binding.get("session_id"),
            "as_of_seal_utc": binding.get("as_of_seal_utc"),
        }),
        "selection_period_json": canonical_json({
            "already_inspected_historical_training": True,
        }),
        "confirmation_period_json": canonical_json({
            "none": True,
            "cannot_confirm_or_promote": True,
        }),
        "all_parameters_tried_json": canonical_json({
            "classification_contract": classification,
            "categories": sorted((report.get("categories") or {}).keys()),
        }),
        "selection_rule": (
            "classify material depth-one regret, then structurally cluster by "
            "category, episode, currency resource, and position thesis"
        ),
        "holdouts_touched_json": canonical_json([
            "source_replay_historical_training_window",
        ]),
        "source_code_hash": stable_hash({
            "core": material.get("core_code_sha256"),
            "producer": material.get("producer_code_sha256"),
            "verifier": material.get("verifier_code_sha256"),
        }),
        "data_snapshot_hash": stable_hash({
            "source_binding": binding,
            "source_roots": rebuilt_roots,
            "report_content_sha256": computed_report_sha,
            "verifier_semantic_sha256": stable_hash({
                key: value for key, value in receipt.items()
                if key != "generated_utc"
            }),
        }),
        "definition_sha256": material_sha,
        "created_at": str(binding.get("as_of_seal_utc") or ""),
        "definition_json": canonical_json(contract_definition),
    }
    definitions = int(_insert_experiment_exact(target, definition))
    observations = int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(binding.get("as_of_seal_utc") or ""),
        source_system=report_path.name,
        result="verified_historical_mistake_curriculum",
        evidence={
            "source_counts": report.get("source_counts"),
            "summary": report.get("summary"),
            "curriculum_priorities": report.get("curriculum_priorities"),
            "report_content_address_verified": True,
            "report_id": report_id,
            "report_content_sha256": computed_report_sha,
            "source_database_seal_verified": True,
            "proof_eligible": False,
            "execution_eligible": False,
            "supported_decision": "no_trade",
        },
    ))
    return definitions, observations


def import_sequential_replay_source_pack(
    target: sqlite3.Connection, source_root: Path, *,
    parent_hypothesis_id: str | None,
) -> tuple[int, int]:
    """Register verified all-universe replay data as infrastructure only."""
    state_path = source_root / "sequential_replay_source_pack_v1.json"
    verifier_path = source_root / "sequential_replay_source_pack_verifier_v1.json"
    if not state_path.is_file() or not verifier_path.is_file():
        return 0, 0
    state = json.loads(state_path.read_text(encoding="utf-8"))
    verifier = json.loads(verifier_path.read_text(encoding="utf-8"))
    material = state.get("material_contract") or {}
    # The source-pack producer passes one value to stable_hash and therefore
    # hashes that value directly (unlike replay/curriculum variadic hashing).
    material_sha = stable_hash(material)
    pack_id = str(state.get("pack_id") or "")
    immutable_manifest_path = source_root / "packs" / pack_id / "manifest.json"
    sessions = list(state.get("sessions") or [])
    observed_at = max(
        (str(row.get("end_utc_exclusive") or "") for row in sessions),
        default="",
    )
    if not (
        pack_id == "sequential_replay_source_pack_v1." + material_sha[:20]
        and str(state.get("material_sha256") or "") == material_sha
        and verifier.get("verified") is True
        and verifier.get("failures") == []
        and int(verifier.get("failure_count") or 0) == 0
        and str(verifier.get("pack_id") or "") == pack_id
        and str(verifier.get("material_sha256") or "") == material_sha
        and str(verifier.get("source_manifest_sha256") or "")
        == str(state.get("source_manifest_sha256") or "")
        and int(verifier.get("reconstructed_slice_count") or -1)
        == int(state.get("slice_count") or 0)
        and int(verifier.get("reconstructed_instrument_count") or -1)
        == int(state.get("instrument_count") or 0)
        and int(verifier.get("reconstructed_global_clock_count") or -1)
        == int(state.get("scheduled_global_clock_count") or 0)
        and int(verifier.get("reconstructed_pair_context_count") or -1)
        == int(state.get("scheduled_pair_context_count") or 0)
        and _full_research_only_policy_is_closed(state)
        and _full_research_only_policy_is_closed(material.get("safety") or {})
        and state.get("evidence_role") == "historical_training_discovery"
        and state.get("proof_eligible") is False
        and state.get("independent_regime_count") is None
        and state.get("independent_regime_state")
        == "unknown_structural_weekday_blocks_only"
        and _genealogy_has_hypothesis(
            target, parent_hypothesis_id,
            experiment_kind="historical_counterfactual_sim",
        )
        and observed_at
        and isinstance(state.get("source_manifest"), list)
        and stable_hash(state.get("source_manifest"))
        == str(state.get("source_manifest_sha256") or "")
        and immutable_manifest_path.is_file()
        and not immutable_manifest_path.is_symlink()
        and immutable_manifest_path.read_bytes() == state_path.read_bytes()
    ):
        return 0, 0
    code_hashes = {
        key: value.get("sha256")
        for key, value in sorted((state.get("contracts") or {}).items())
    }
    definition = {
        "hypothesis_id": pack_id,
        "parent_hypothesis_id": str(parent_hypothesis_id),
        "experiment_kind": "historical_replay_source_infrastructure",
        "research_generation": "sequential_replay_source_pack_v1",
        "idea_origin": "all68_exact_window_oanda_m1_bid_ask_source_pack",
        "pre_registered": 1,
        "data_sources_json": canonical_json([
            "retained_OANDA_practice_M1_bid_ask_archives",
        ]),
        "feature_contract_json": canonical_json({
            "instrument_count": state.get("instrument_count"),
            "slice_count": state.get("slice_count"),
            "exact_quotes_only": True,
            "missing_quotes_fail_closed": True,
        }),
        "label_contract_json": canonical_json({
            "infrastructure_only": True,
            "one_market_repetition_per_global_clock": True,
            "pair_contexts_not_independent_repetitions": True,
            "independent_regime_count": None,
        }),
        "model_contract_json": canonical_json({"model": "none_source_pack"}),
        "cost_contract_json": "{}",
        "allocator_contract_json": canonical_json({
            "no_allocator_decisions": True,
        }),
        "training_period_json": canonical_json({"sessions": sessions}),
        "selection_period_json": canonical_json({
            "rule": state.get("selection_rule"),
            "selected_by_outcome": False,
        }),
        "confirmation_period_json": canonical_json({
            "none": True,
            "historical_training_infrastructure_only": True,
        }),
        "all_parameters_tried_json": canonical_json({
            "material_contract": material,
        }),
        "selection_rule": str(state.get("selection_rule") or ""),
        "holdouts_touched_json": canonical_json([
            row.get("session_key") for row in sessions
        ]),
        "source_code_hash": stable_hash(code_hashes),
        "data_snapshot_hash": str(state.get("source_manifest_sha256") or ""),
        "definition_sha256": material_sha,
        "created_at": observed_at,
        "definition_json": canonical_json(material),
    }
    definitions = int(_insert_experiment_exact(target, definition))
    observations = int(observe(
        target,
        hypothesis_id=pack_id,
        observed_at=observed_at,
        source_system=state_path.name,
        result="verified_historical_replay_source_infrastructure",
        evidence={
            "instrument_count": state.get("instrument_count"),
            "session_count": state.get("session_count"),
            "slice_count": state.get("slice_count"),
            "scheduled_global_clock_count": state.get("scheduled_global_clock_count"),
            "scheduled_pair_context_count": state.get("scheduled_pair_context_count"),
            "market_repetition_count": state.get("market_repetition_count"),
            "independent_regime_count": None,
            "coverage": state.get("coverage"),
            "proof_eligible": False,
            "execution_eligible": False,
            "supported_decision": "no_trade",
            "verifier_failures": [],
            "verifier_semantic_sha256": stable_hash({
                key: value for key, value in verifier.items()
                if key != "generated_utc"
            }),
        },
    ))
    # Preserve the byte-pinned parents of reviewed historical diagnostics in a
    # fresh genealogy.  They are intentionally not treated as passing the
    # current explicit safety schema.
    packs_root = source_root / "packs"
    for legacy_id, legacy_spec in sorted(SEQUENTIAL_LEGACY_SOURCE_PACK_SPECS.items()):
        legacy_path = packs_root / legacy_id / "manifest.json"
        if not legacy_path.is_file() or legacy_path.is_symlink():
            continue
        if _sha256_file(legacy_path) != legacy_spec["manifest_sha256"]:
            continue
        legacy = json.loads(legacy_path.read_text(encoding="utf-8"))
        legacy_material = legacy.get("material_contract") or {}
        legacy_material_sha = stable_hash(legacy_material)
        legacy_sessions = list(legacy.get("sessions") or [])
        legacy_observed_at = max(
            (str(row.get("end_utc_exclusive") or "") for row in legacy_sessions),
            default="",
        )
        if not (
            str(legacy.get("pack_id") or "") == legacy_id
            and legacy_id == "sequential_replay_source_pack_v1." + legacy_material_sha[:20]
            and str(legacy.get("material_sha256") or "") == legacy_material_sha
            and legacy.get("research_only") is True
            and legacy.get("execution_eligible") is False
            and legacy.get("can_place_orders") is False
            and legacy.get("can_promote") is False
            and legacy.get("can_authorize") is False
            and legacy.get("supported_decision") == "no_trade"
            and legacy.get("independent_regime_count") is None
            and legacy_observed_at
        ):
            continue
        legacy_definition = {
            **definition,
            "hypothesis_id": legacy_id,
            "feature_contract_json": canonical_json({
                "instrument_count": legacy.get("instrument_count"),
                "slice_count": legacy.get("slice_count"),
                "legacy_explicit_safety_schema_incomplete": True,
                "quarantined_lineage_only": True,
            }),
            "training_period_json": canonical_json({"sessions": legacy_sessions}),
            "selection_period_json": canonical_json({
                "rule": legacy.get("selection_rule"),
                "selected_by_outcome": False,
            }),
            "all_parameters_tried_json": canonical_json({
                "material_contract": legacy_material,
                "lineage_state": legacy_spec["lineage_state"],
            }),
            "holdouts_touched_json": canonical_json([
                row.get("session_key") for row in legacy_sessions
            ]),
            "source_code_hash": stable_hash({
                key: value.get("sha256")
                for key, value in sorted((legacy.get("contracts") or {}).items())
            }),
            "data_snapshot_hash": legacy_spec["manifest_sha256"],
            "definition_sha256": legacy_material_sha,
            "created_at": legacy_observed_at,
            "definition_json": canonical_json(legacy_material),
        }
        definitions += int(_insert_experiment_exact(target, legacy_definition))
        observations += int(observe(
            target,
            hypothesis_id=legacy_id,
            observed_at=legacy_observed_at,
            source_system=legacy_path.name,
            result="quarantined_historical_source_pack_lineage",
            evidence={
                "lineage_state": legacy_spec["lineage_state"],
                "manifest_sha256": legacy_spec["manifest_sha256"],
                "legacy_explicit_safety_schema_incomplete": True,
                "proof_eligible": False,
                "execution_eligible": False,
                "supported_decision": "no_trade",
            },
        ))
    return definitions, observations


def import_sequential_all68_portfolio_batch_replays(
    target: sqlite3.Connection, source_root: Path, *,
    cohort_specs: dict[str, dict[str, str]] | None = None,
) -> tuple[int, int]:
    """Register only reviewed, immutable all-universe replay cohorts.

    The original nonhomogeneous-pack diagnostic remains a first-class
    historical row. The corrected replay is a sibling under its corrected
    source-pack parent; neither is statistical proof or execution evidence.
    """
    specifications = cohort_specs or SEQUENTIAL_ALL68_GENEALOGY_COHORT_SPECS
    definitions = observations = 0
    for cohort_id, specification in sorted(specifications.items()):
        cohort_root = source_root / "cohorts" / cohort_id
        state_path = cohort_root / "state.json"
        verifier_path = cohort_root / "verifier_receipt.json"
        source_receipt_path = cohort_root / "source_pack_clean_verifier_receipt.json"
        source_manifest_path = cohort_root / "source_pack_manifest.json"
        if not all(path.is_file() for path in (
            state_path, verifier_path, source_receipt_path, source_manifest_path,
        )):
            continue
        state = json.loads(state_path.read_text(encoding="utf-8"))
        verifier = json.loads(verifier_path.read_text(encoding="utf-8"))
        source_receipt = json.loads(source_receipt_path.read_text(encoding="utf-8"))
        source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
        material = state.get("material_contract") or {}
        # The frozen all-68 producer hashes its variadic argument tuple, unlike
        # this module's single-value stable_hash helper.
        material_sha = hashlib.sha256(
            canonical_json([material]).encode("utf-8")
        ).hexdigest()
        binding = state.get("source_binding") or {}
        parent_id = str(binding.get("pack_id") or "")
        independence = material.get("independence") or {}
        safety = material.get("safety") or {}
        acceptance_schema = str(specification.get("acceptance_schema") or "")
        verified_datasets = _dataset_manifest_binding(
            cohort_root, state.get("datasets"),
        )
        derived_summary = (
            _all68_dataset_summary(cohort_root, cohort_id, verified_datasets)
            if verified_datasets is not None else None
        )
        state_semantic = {
            key: value for key, value in state.items() if key != "generated_utc"
        }
        verifier_semantic = {
            key: value for key, value in verifier.items()
            if key != "generated_utc"
        }
        state_semantic_sha = _variadic_hash(state_semantic)
        verifier_semantic_sha = _variadic_hash(verifier_semantic)
        dataset_roots_sha = (
            _variadic_hash(verified_datasets)
            if verified_datasets is not None else ""
        )
        reviewed_artifact_binding = bool(
            state_semantic_sha
            == str(specification.get("state_semantic_sha256") or "")
            and verifier_semantic_sha
            == str(specification.get("verifier_semantic_sha256") or "")
            and dataset_roots_sha
            == str(specification.get("dataset_roots_sha256") or "")
            and (
                acceptance_schema != "strict_v3"
                or (
                    _sha256_file(state_path)
                    == str(specification.get("state_sha256") or "")
                    and _sha256_file(verifier_path)
                    == str(specification.get("verifier_sha256") or "")
                )
            )
        )
        summary_keys = (
            "session_count", "global_clock_count", "pair_context_count",
            "causal_ready_context_count", "candidate_context_count",
            "execution_leg_count", "counterfactual_count", "terminal_flat",
            "market_repetition_count", "action_counts",
            "context_availability_counts",
        )
        state_summary_matches = bool(
            derived_summary is not None
            and all(state.get(key) == derived_summary.get(key) for key in summary_keys)
            and abs(
                float(state.get("realized_pips") or 0.0)
                - float(derived_summary.get("realized_pips") or 0.0)
            ) <= 1e-9
        )
        parent = target.execute(
            "SELECT definition_sha256 FROM experiments WHERE hypothesis_id=? "
            "AND experiment_kind='historical_replay_source_infrastructure'",
            (parent_id,),
        ).fetchone()
        source_manifest_material = source_manifest.get("material_contract") or {}
        source_manifest_material_sha = stable_hash(source_manifest_material)
        immutable_observed_at = _source_window_end(source_manifest)
        source_receipt_strict = bool(
            source_receipt.get("verified") is True
            and source_receipt.get("failures") == []
            and str(source_receipt.get("pack_id") or "") == parent_id
            and (
                acceptance_schema == "legacy_pre_hardening_v1"
                or (
                    str(source_receipt.get("material_sha256") or "")
                    == source_manifest_material_sha
                    and str(source_receipt.get("source_manifest_sha256") or "")
                    == str(source_manifest.get("source_manifest_sha256") or "")
                    and _full_research_only_policy_is_closed(source_manifest)
                    and _full_research_only_policy_is_closed(
                        source_manifest_material.get("safety") or {}
                    )
                )
            )
        )
        if not (
            str(state.get("cohort_id") or "") == cohort_id
            and cohort_id
            == "sequential_all68_portfolio_batch_replay_v1." + material_sha[:20]
            and str(state.get("material_sha256") or "") == material_sha
            and acceptance_schema in {
                "legacy_pre_hardening_v1", "strict_v2", "strict_v3",
            }
            and parent_id == str(specification.get("source_pack_id") or "")
            and _genealogy_has_hypothesis(
                target, parent_id,
                experiment_kind="historical_replay_source_infrastructure",
            )
            and verifier.get("verified") is True
            and verifier.get("failures") == []
            and str(verifier.get("cohort_id") or "") == cohort_id
            and verifier.get("independent_regime_count") is None
            and int(verifier.get("reconstructed_global_clocks") or -1)
            == int(state.get("global_clock_count") or 0)
            and int(verifier.get("reconstructed_pair_contexts") or -1)
            == int(state.get("pair_context_count") or 0)
            and _full_research_only_policy_is_closed(state)
            and _full_research_only_policy_is_closed(safety)
            and state.get("independent_regime_count") is None
            and state.get("independent_regime_state")
            == "unknown_structural_sessions_only"
            and independence.get("independent_regime_count") is None
            and independence.get("independent_regime_state")
            == "unknown_structural_sessions_only"
            and independence.get("pair_contexts_count_as_market_repetitions")
            is False
            and independence.get("counterfactuals_count_as_market_repetitions")
            is False
            and independence.get("replays_count_as_market_repetitions") is False
            and material.get("source_binding") == binding
            and state.get("terminal_flat") is True
            and int(state.get("market_repetition_count") or -1)
            == int(state.get("global_clock_count") or 0)
            and int(state.get("pair_context_count") or -1)
            == int(state.get("instrument_count") or 0)
            * int(state.get("global_clock_count") or 0)
            and verified_datasets is not None
            and state_summary_matches
            and reviewed_artifact_binding
            and _sha256_file(source_receipt_path)
            == str(binding.get("clean_verifier_receipt_sha256") or "")
            and _sha256_file(source_manifest_path)
            == str(binding.get("pack_manifest_sha256") or "")
            and source_receipt_strict
            and str(source_manifest.get("pack_id") or "") == parent_id
            and str(source_manifest.get("material_sha256") or "")
            == source_manifest_material_sha
            and immutable_observed_at
            and parent is not None
            and str(parent[0]) == source_manifest_material_sha
        ):
            continue
        datasets = verified_datasets
        assert datasets is not None
        definition = {
            "hypothesis_id": cohort_id,
            "parent_hypothesis_id": parent_id,
            "experiment_kind": "historical_all68_portfolio_batch_replay",
            "research_generation": "sequential_all68_portfolio_batch_replay_v1",
            "idea_origin": "all68_single_portfolio_sequential_training_replay",
            "pre_registered": 1,
            "data_sources_json": canonical_json([
                parent_id,
                "exact_content_addressed_OANDA_M1_bid_ask_slices",
            ]),
            "feature_contract_json": canonical_json({
                "causal_prefix_only": True,
                "instrument_count": state.get("instrument_count"),
                "pair_context_count": state.get("pair_context_count"),
                "missing_context_retained": True,
            }),
            "label_contract_json": canonical_json({
                "historical_training_discovery_only": True,
                "market_repetition_unit": "global_clock",
                "pair_contexts_are_not_independent_repetitions": True,
                "counterfactuals_are_not_independent_repetitions": True,
                "independent_regime_count": None,
            }),
            "model_contract_json": canonical_json({
                "frozen_policy": material.get("frozen_policy"),
                "no_interim_retuning": True,
            }),
            "cost_contract_json": canonical_json(material.get("costs") or {}),
            "allocator_contract_json": canonical_json({
                "one_global_portfolio": True,
                "maximum_open_positions": 1,
                "actions": ["wait", "enter", "hold", "exit", "rotate"],
            }),
            "training_period_json": canonical_json({
                "session_count": state.get("session_count"),
                "global_clock_count": state.get("global_clock_count"),
            }),
            "selection_period_json": canonical_json({
                "predeclared_source_pack": parent_id,
                "historical_training_only": True,
            }),
            "confirmation_period_json": canonical_json({
                "none": True,
                "later_untouched_prospective_cohort_required": True,
            }),
            "all_parameters_tried_json": canonical_json({
                "material_contract": material,
                "source_pack_state": specification.get("source_pack_state"),
            }),
            "selection_rule": (
                "one frozen policy over all available pair contexts at each "
                "predeclared global clock; unavailable contexts remain recorded"
            ),
            "holdouts_touched_json": canonical_json([
                "already_inspected_historical_training_sessions",
            ]),
            "source_code_hash": stable_hash({
                "core": material.get("core_sha256"),
                "runner": material.get("runner_sha256"),
                "verifier": material.get("verifier_sha256"),
            }),
            "data_snapshot_hash": stable_hash({
                "source_binding": binding,
                "datasets": datasets,
                "verifier_semantic_sha256": stable_hash(verifier_semantic),
            }),
            "definition_sha256": material_sha,
            "created_at": immutable_observed_at,
            "definition_json": canonical_json(material),
        }
        definitions += int(_insert_experiment_exact(target, definition))
        observations += int(observe(
            target,
            hypothesis_id=cohort_id,
            observed_at=immutable_observed_at,
            source_system=state_path.name,
            result=str(specification.get("result") or "historical_diagnostic"),
            evidence={
                "source_pack_id": parent_id,
                "source_pack_state": specification.get("source_pack_state"),
                "global_clock_count": state.get("global_clock_count"),
                "pair_context_count": state.get("pair_context_count"),
                "causal_ready_context_count": state.get("causal_ready_context_count"),
                "candidate_context_count": state.get("candidate_context_count"),
                "action_counts": state.get("action_counts"),
                "execution_leg_count": state.get("execution_leg_count"),
                "counterfactual_count": state.get("counterfactual_count"),
                "realized_pips": state.get("realized_pips"),
                "terminal_flat": state.get("terminal_flat"),
                "context_availability_counts": state.get(
                    "context_availability_counts"
                ),
                "market_repetition_count": state.get("market_repetition_count"),
                "independent_regime_count": None,
                "proof_eligible": False,
                "execution_eligible": False,
                "supported_decision": "no_trade",
                "verifier_failures": [],
                "dataset_manifest_sha256": stable_hash(datasets),
                "verifier_semantic_sha256": stable_hash(verifier_semantic),
            },
        ))
    return definitions, observations


def _read_verified_dataset_rows(
    cohort_root: Path, datasets: dict[str, dict[str, Any]],
) -> dict[str, list[dict[str, Any]]] | None:
    rows: dict[str, list[dict[str, Any]]] = {}
    try:
        for name, descriptor in datasets.items():
            path = _safe_child_file(
                cohort_root, cohort_root / str(descriptor["relative_path"]),
            )
            if path is None:
                return None
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                values = [json.loads(line) for line in handle if line.strip()]
            if len(values) != int(descriptor["row_count"]):
                return None
            rows[name] = values
    except (OSError, EOFError, gzip.BadGzipFile, json.JSONDecodeError, KeyError,
            TypeError, ValueError):
        return None
    return rows


def _self_sealed_report_rows(value: Any) -> bool:
    return bool(
        isinstance(value, list)
        and all(
            isinstance(row, dict)
            and _is_sha256(row.get("row_sha256"))
            and str(row["row_sha256"])
            == _variadic_hash({
                key: item for key, item in row.items() if key != "row_sha256"
            })
            for row in value
        )
    )


def _import_one_sequential_all68_mistake_curriculum(
    target: sqlite3.Connection, source_root: Path, *, cohort_id: str,
    specification: dict[str, Any],
) -> tuple[int, int]:
    """Register one reviewed, sealed all-68 curriculum under its exact replay."""
    report_current = source_root / "sequential_all68_mistake_curriculum_v1.json"
    receipt_current = (
        source_root / "sequential_all68_mistake_curriculum_verifier_v1.json"
    )
    cohort_root = source_root / "cohorts" / cohort_id
    report_path = cohort_root / "SEQUENTIAL_ALL68_MISTAKE_CURRICULUM_V1.json"
    receipt_path = cohort_root / "verifier_receipt.json"
    material_path = cohort_root / "material_contract_v1.json"
    if not all(path.is_file() and not path.is_symlink() for path in (
        report_path, receipt_path, material_path,
    )):
        return 0, 0
    if specification.get("require_current"):
        if not report_current.is_file() or not receipt_current.is_file():
            return 0, 0
        if (
            report_current.read_bytes() != report_path.read_bytes()
            or receipt_current.read_bytes() != receipt_path.read_bytes()
        ):
            return 0, 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    material = report.get("material_contract") or {}
    archived_material = json.loads(material_path.read_text(encoding="utf-8"))
    material_sha = _variadic_hash(material)
    semantic_report = dict(report)
    report_id = str(semantic_report.pop("report_id", ""))
    claimed_report_sha = str(semantic_report.pop("report_sha256", ""))
    computed_report_sha = _variadic_hash(semantic_report)
    binding = report.get("source_binding") or {}
    checks = receipt.get("checks") or {}
    parent_id = str(binding.get("cohort_id") or "")
    parent = target.execute(
        "SELECT parent_hypothesis_id,definition_sha256,created_at FROM experiments "
        "WHERE hypothesis_id=? "
        "AND experiment_kind='historical_all68_portfolio_batch_replay'",
        (parent_id,),
    ).fetchone()
    if not (
        cohort_id
        == "sequential_all68_mistake_curriculum_v1." + material_sha[:20]
        and str(report.get("material_contract_sha256") or "") == material_sha
        and archived_material == material
        and parent_id == str(specification.get("parent_cohort_id") or "")
        and report_id == str(specification.get("report_id") or "")
        and computed_report_sha
        == str(specification.get("report_content_sha256") or "")
        and _sha256_file(report_path)
        == str(specification.get("report_file_sha256") or "")
        and receipt.get("verified") is True
        and receipt.get("failures") == []
        and str(report.get("cohort_id") or "") == cohort_id
        and str(receipt.get("cohort_id") or "") == cohort_id
        and str(receipt.get("report_id") or "") == report_id
        and str(receipt.get("material_contract_sha256") or "") == material_sha
        and str(receipt.get("report_content_sha256") or "")
        == computed_report_sha
        and str(receipt.get("report_file_sha256") or "")
        == _sha256_file(report_path)
        and report_id == "a68mistakecurriculum_" + computed_report_sha[:28]
        and claimed_report_sha == computed_report_sha
        and _full_research_only_policy_is_closed(report)
        and _full_research_only_policy_is_closed(receipt)
        and _full_research_only_policy_is_closed(material.get("safety") or {})
        and report.get("evidence_role") == "historical_training_curriculum"
        and receipt.get("evidence_role") == "historical_training_curriculum"
        and (material.get("safety") or {}).get("evidence_role")
        == "historical_training_curriculum"
        and report.get("lifecycle_write") is False
        and report.get("signal_feed_write") is False
        and receipt.get("lifecycle_write") is False
        and receipt.get("signal_feed_write") is False
        and checks.get("verifier_forbidden_imports") == []
        and parent is not None
        and str(parent[1]) == str(binding.get("source_material_sha256") or "")
        and str(material.get("source_cohort_id") or "") == parent_id
    ):
        return 0, 0
    assert parent is not None
    all68_root = (
        source_root.parent / "sequential_all68_portfolio_batch_replay_v1"
        / "cohorts" / parent_id
    )
    state_path = all68_root / "state.json"
    verifier_path = all68_root / "verifier_receipt.json"
    pack_receipt_path = all68_root / "source_pack_clean_verifier_receipt.json"
    pack_manifest_path = all68_root / "source_pack_manifest.json"
    if not all(path.is_file() and not path.is_symlink() for path in (
        state_path, verifier_path, pack_receipt_path, pack_manifest_path,
    )):
        return 0, 0
    state = json.loads(state_path.read_text(encoding="utf-8"))
    verifier = json.loads(verifier_path.read_text(encoding="utf-8"))
    pack_receipt = json.loads(pack_receipt_path.read_text(encoding="utf-8"))
    pack_manifest = json.loads(pack_manifest_path.read_text(encoding="utf-8"))
    datasets = _dataset_manifest_binding(all68_root, state.get("datasets"))
    dataset_rows = (
        _read_verified_dataset_rows(all68_root, datasets)
        if datasets is not None else None
    )
    source_state_semantic = _variadic_hash({
        key: value for key, value in state.items() if key != "generated_utc"
    })
    source_verifier_semantic = _variadic_hash({
        key: value for key, value in verifier.items() if key != "generated_utc"
    })
    source_state_file_sha = _sha256_file(state_path)
    source_verifier_file_sha = _sha256_file(verifier_path)
    acceptance_schema = str(specification.get("acceptance_schema") or "")
    raw_source_binding_required = acceptance_schema == "strict_raw_parent_binding_v3"
    if acceptance_schema not in {
        "semantic_parent_binding_v1",
        "semantic_parent_binding_v2",
        "strict_raw_parent_binding_v3",
    }:
        return 0, 0
    source_pack_id = str(binding.get("source_pack_id") or "")
    source_pack = target.execute(
        "SELECT definition_sha256 FROM experiments WHERE hypothesis_id=? "
        "AND experiment_kind='historical_replay_source_infrastructure'",
        (source_pack_id,),
    ).fetchone()
    pack_material = pack_manifest.get("material_contract") or {}
    if not (
        str(state.get("cohort_id") or "") == parent_id
        and str(state.get("material_sha256") or "") == str(parent[1])
        and verifier.get("verified") is True
        and verifier.get("failures") == []
        and str(verifier.get("cohort_id") or "") == parent_id
        and _full_research_only_policy_is_closed(state)
        and datasets is not None
        and dataset_rows is not None
        and state.get("datasets") == datasets
        and binding.get("source_dataset_specs") == datasets
        and material.get("source_dataset_specs") == datasets
        and _variadic_hash(datasets)
        == str(binding.get("source_dataset_roots_sha256") or "")
        and str(material.get("source_dataset_roots_sha256") or "")
        == str(binding.get("source_dataset_roots_sha256") or "")
        and source_state_semantic
        == str(binding.get("source_state_semantic_sha256") or "")
        and source_verifier_semantic
        == str(binding.get("source_verifier_semantic_sha256") or "")
        and str(material.get("source_state_semantic_sha256") or "")
        == source_state_semantic
        and str(material.get("source_verifier_semantic_sha256") or "")
        == source_verifier_semantic
        and (
            not raw_source_binding_required
            or (
                str(binding.get("source_state_sha256") or "")
                == source_state_file_sha
                and str(material.get("source_state_sha256") or "")
                == source_state_file_sha
                and str(binding.get("source_verifier_sha256") or "")
                == source_verifier_file_sha
                and str(material.get("source_verifier_sha256") or "")
                == source_verifier_file_sha
            )
        )
        and _sha256_file(pack_receipt_path)
        == str(binding.get("source_pack_verifier_receipt_sha256") or "")
        and _sha256_file(pack_manifest_path)
        == str(binding.get("source_pack_manifest_sha256") or "")
        and str(material.get("source_pack_verifier_receipt_sha256") or "")
        == str(binding.get("source_pack_verifier_receipt_sha256") or "")
        and str(material.get("source_pack_manifest_sha256") or "")
        == str(binding.get("source_pack_manifest_sha256") or "")
        and str(parent[0]) == source_pack_id
        and str(material.get("source_pack_id") or "") == source_pack_id
        and str((state.get("source_binding") or {}).get("pack_id") or "")
        == source_pack_id
        and pack_receipt.get("verified") is True
        and pack_receipt.get("failures") == []
        and str(pack_receipt.get("pack_id") or "") == source_pack_id
        and str(pack_manifest.get("pack_id") or "") == source_pack_id
        and source_pack is not None
        and str(source_pack[0]) == str(pack_manifest.get("material_sha256") or "")
        and str(source_pack[0]) == stable_hash(pack_material)
        and _full_research_only_policy_is_closed(pack_manifest)
        and _full_research_only_policy_is_closed(pack_material.get("safety") or {})
    ):
        return 0, 0
    curriculum_rows = report.get("curriculum_rows")
    resource_components = report.get("feedback_resource_components")
    structural_clusters = report.get("structural_clusters")
    summary = report.get("summary") or {}
    classification = report.get("classification_contract") or {}
    if not all(_self_sealed_report_rows(value) for value in (
        curriculum_rows, resource_components, structural_clusters,
    )):
        return 0, 0
    assert isinstance(curriculum_rows, list)
    assert isinstance(resource_components, list)
    assert isinstance(structural_clusters, list)
    assert dataset_rows is not None
    source_maps = {
        name: {
            str(row[id_key]): str(row["row_sha256"])
            for row in dataset_rows[name]
        }
        for name, id_key in (
            ("clocks", "clock_id"),
            ("decisions", "decision_id"),
            ("feedback", "feedback_id"),
            ("counterfactuals", "counterfactual_id"),
        )
    }
    curriculum_ids = [str(row.get("curriculum_row_id") or "") for row in curriculum_rows]
    resource_cluster_ids = {
        str(row.get("cluster_id") or "") for row in resource_components
    }
    references_valid = all(
        row.get("proof_eligible") is False
        and row.get("execution_eligible") is False
        and row.get("counts_as_regime_repetition") == 0
        and str(row.get("source_cohort_id") or "") == parent_id
        and source_maps["clocks"].get(str(row.get("clock_id") or ""))
        == str(row.get("source_clock_row_sha256") or "")
        and source_maps["decisions"].get(str(row.get("decision_id") or ""))
        == str(row.get("source_decision_row_sha256") or "")
        and source_maps["feedback"].get(str(row.get("feedback_id") or ""))
        == str(row.get("source_feedback_row_sha256") or "")
        and (
            (
                row.get("counterfactual_id") is None
                and row.get("source_counterfactual_row_sha256") is None
            )
            or source_maps["counterfactuals"].get(
                str(row.get("counterfactual_id") or "")
            ) == str(row.get("source_counterfactual_row_sha256") or "")
        )
        and str(row.get("structural_cluster_id") or "") in resource_cluster_ids
        for row in curriculum_rows
    )
    primary_rows = [
        row for row in curriculum_rows if row.get("row_role") == "primary_feedback"
    ]
    review_rows = [
        row for row in curriculum_rows if row.get("row_role") == "depth_one_review"
    ]
    if not (
        references_valid
        and len(set(curriculum_ids)) == len(curriculum_ids)
        and all(curriculum_ids)
        and all(
            row.get("proof_eligible") is False
            and row.get("counts_as_regime_repetition") == 0
            for row in resource_components + structural_clusters
        )
        and len(resource_cluster_ids) == len(resource_components)
        and all(resource_cluster_ids)
        and all(
            set(row.get("curriculum_row_ids") or []).issubset(set(curriculum_ids))
            for row in structural_clusters
        )
        and classification.get("raw_rows_are_not_independent") is True
        and classification.get("structural_clusters_are_not_independent_regimes")
        is True
        and summary.get("independent_regime_count") is None
        and int(summary.get("curriculum_row_count", -1)) == len(curriculum_rows)
        and int(summary.get("feedback_resource_component_count", -1))
        == len(resource_components)
        and int(summary.get("structural_cluster_count", -1))
        == len(structural_clusters)
        and int(summary.get("source_global_clock_count", -1))
        == int(datasets["clocks"]["row_count"])
        and int(summary.get("source_primary_feedback_count", -1))
        == int(datasets["feedback"]["row_count"])
        and int(summary.get("source_depth_one_review_count", -1))
        == int(datasets["counterfactuals"]["row_count"])
        and len(primary_rows) == int(datasets["feedback"]["row_count"])
        and len(review_rows) == int(datasets["counterfactuals"]["row_count"])
        and sum(int(row.get("curriculum_weight") or 0) for row in primary_rows)
        == int(summary.get("primary_weight_sum", -1))
        and sum(int(row.get("curriculum_weight") or 0) for row in review_rows)
        == int(summary.get("review_weight_sum", -1))
        and int(summary.get("deduplicated_primary_weight", -1))
        == sum(int(row.get("counts_as_market_repetition") or 0) for row in primary_rows)
        and checks.get("source_cohort_id") == parent_id
        and checks.get("source_pack_id") == source_pack_id
        and checks.get("source_state_semantic_sha256") == source_state_semantic
        and checks.get("source_verifier_semantic_sha256")
        == source_verifier_semantic
        and (
            not raw_source_binding_required
            or (
                checks.get("source_state_sha256") == source_state_file_sha
                and checks.get("source_verifier_sha256")
                == source_verifier_file_sha
            )
        )
        and checks.get("source_dataset_roots_sha256")
        == binding.get("source_dataset_roots_sha256")
        and checks.get("curriculum_row_count") == len(curriculum_rows)
        and checks.get("structural_cluster_count") == len(structural_clusters)
    ):
        return 0, 0
    definition = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": parent_id,
        "experiment_kind": "historical_all68_mistake_curriculum",
        "research_generation": "sequential_all68_mistake_curriculum_v1",
        "idea_origin": "all68_cost_direction_entry_exit_rotation_mistake_curriculum",
        "pre_registered": 1,
        "data_sources_json": canonical_json([parent_id, source_pack_id]),
        "feature_contract_json": canonical_json({
            "exact_source_row_hashes": True,
            "signed_currency_resource_components": True,
            "depth_one_reviews_weight_zero": True,
        }),
        "label_contract_json": canonical_json({
            "categories_are_nonexclusive": True,
            "structural_clusters_are_not_independent_regimes": True,
            "independent_regime_count": None,
        }),
        "model_contract_json": canonical_json({
            "model": "none_historical_mistake_curriculum",
            "classification": classification,
        }),
        "cost_contract_json": canonical_json({
            "inherited_from_parent_replay": True,
        }),
        "allocator_contract_json": canonical_json({
            "nonexecuting": True,
            "no_allocator_decisions": True,
        }),
        "training_period_json": canonical_json({
            "source_cohort_id": parent_id,
            "source_global_clock_count": summary.get("source_global_clock_count"),
            "curriculum_row_count": len(curriculum_rows),
        }),
        "selection_period_json": canonical_json({
            "already_inspected_historical_training_window": True,
        }),
        "confirmation_period_json": canonical_json({
            "none": True,
            "later_untouched_prospective_cohort_required": True,
        }),
        "all_parameters_tried_json": canonical_json({
            "classification": classification,
        }),
        "selection_rule": (
            "independently reconstructed nonexclusive mistake labels nested "
            "beneath exact all-68 clocks and zero-weight depth-one reviews"
        ),
        "holdouts_touched_json": canonical_json([
            "already_inspected_historical_training_sessions",
        ]),
        "source_code_hash": stable_hash({
            "core": material.get("core_code_sha256"),
            "producer": material.get("producer_code_sha256"),
            "verifier": material.get("verifier_code_sha256"),
        }),
        "data_snapshot_hash": stable_hash({
            "source_cohort_id": parent_id,
            "source_dataset_roots_sha256": binding.get(
                "source_dataset_roots_sha256"
            ),
            "source_state_semantic_sha256": source_state_semantic,
            "source_verifier_semantic_sha256": source_verifier_semantic,
            "source_state_sha256": (
                source_state_file_sha if raw_source_binding_required else None
            ),
            "source_verifier_sha256": (
                source_verifier_file_sha if raw_source_binding_required else None
            ),
            "report_content_sha256": computed_report_sha,
        }),
        "definition_sha256": material_sha,
        "created_at": str(parent[2]),
        "definition_json": canonical_json(material),
    }
    definitions = int(_insert_experiment_exact(target, definition))
    observations = int(observe(
        target,
        hypothesis_id=cohort_id,
        observed_at=str(parent[2]),
        source_system=report_path.name,
        result=str(specification.get("result") or "historical_diagnostic"),
        evidence={
            "source_cohort_id": parent_id,
            "source_pack_id": source_pack_id,
            "summary": summary,
            "categories": report.get("categories"),
            "report_id": report_id,
            "report_content_sha256": computed_report_sha,
            "source_dataset_roots_sha256": binding.get(
                "source_dataset_roots_sha256"
            ),
            "source_state_sha256": (
                source_state_file_sha if raw_source_binding_required else None
            ),
            "source_verifier_sha256": (
                source_verifier_file_sha if raw_source_binding_required else None
            ),
            "acceptance_schema": acceptance_schema,
            "independent_regime_count": None,
            "proof_eligible": False,
            "execution_eligible": False,
            "supported_decision": "no_trade",
        },
    ))
    return definitions, observations


def import_sequential_all68_mistake_curriculum(
    target: sqlite3.Connection, source_root: Path, *,
    cohort_specs: dict[str, dict[str, Any]] | None = None,
) -> tuple[int, int]:
    """Register only reviewed immutable all-68 mistake-curriculum lineage.

    Superseded cohorts remain first-class historical lineage.  Only the newest
    reviewed cohort must equal the current convenience files, and only its
    strengthened schema can satisfy the raw parent-state/receipt binding.
    """
    definitions = observations = 0
    specifications = (
        SEQUENTIAL_ALL68_MISTAKE_GENEALOGY_COHORT_SPECS
        if cohort_specs is None else cohort_specs
    )
    for cohort_id, specification in specifications.items():
        imported_definitions, imported_observations = (
            _import_one_sequential_all68_mistake_curriculum(
                target, source_root, cohort_id=cohort_id,
                specification=specification,
            )
        )
        definitions += imported_definitions
        observations += imported_observations
    return definitions, observations


def _policy_challenger_safety_is_closed(value: Any) -> bool:
    return bool(
        isinstance(value, dict)
        and _full_research_only_policy_is_closed(value)
        and value.get("signal_feed_write") is False
        and value.get("lifecycle_write") is False
        and "signal_feed_write" in value
        and "lifecycle_write" in value
    )


def _bounded_policy_dataset_binding(
    cohort_root: Path, datasets: Any, limits: dict[str, Any] | None,
) -> dict[str, dict[str, Any]] | None:
    expected = {
        "clocks": "global_clocks.jsonl.gz",
        "arm_decisions": "arm_decisions.jsonl.gz",
        "terminals": "arm_session_terminals.jsonl.gz",
        "oof_training": "oof_training_observations.jsonl.gz",
        "oof_calibrations": "oof_calibrations.jsonl.gz",
    }
    safe_limits = limits or {
        "maximum_gzip_bytes": 4 * 1024 * 1024,
        "maximum_raw_bytes": 64 * 1024 * 1024,
        "maximum_row_count": 20_000,
    }
    try:
        maximum_gzip = int(safe_limits["maximum_gzip_bytes"])
        maximum_raw = int(safe_limits["maximum_raw_bytes"])
        maximum_rows = int(safe_limits["maximum_row_count"])
    except (KeyError, TypeError, ValueError):
        return None
    if not (0 < maximum_gzip <= maximum_raw <= 64 * 1024 * 1024 and 0 < maximum_rows <= 30_000):
        return None
    if not isinstance(datasets, dict) or set(datasets) != set(expected):
        return None
    verified: dict[str, dict[str, Any]] = {}
    for name, filename in expected.items():
        descriptor = datasets.get(name)
        if not isinstance(descriptor, dict) or descriptor.get("relative_path") != filename:
            return None
        try:
            gzip_bytes = int(descriptor["gzip_bytes"])
            row_count = int(descriptor["row_count"])
        except (KeyError, TypeError, ValueError):
            return None
        if not (0 < gzip_bytes <= maximum_gzip and 0 <= row_count <= maximum_rows):
            return None
        if any(not _is_sha256(descriptor.get(key)) for key in (
            "gzip_sha256", "row_set_sha256", "ordered_row_sha256"
        )):
            return None
        path = _safe_child_file(cohort_root, cohort_root / filename)
        if path is None or path.stat().st_size != gzip_bytes or _sha256_file(path) != descriptor["gzip_sha256"]:
            return None
        try:
            with path.open("rb") as raw_handle, gzip.GzipFile(fileobj=raw_handle, mode="rb") as stream:
                raw = stream.read(maximum_raw + 1)
            if len(raw) > maximum_raw:
                return None
            rows = [json.loads(line) for line in raw.decode("utf-8").splitlines() if line]
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        if len(rows) != row_count:
            return None
        row_hashes = []
        for row in rows:
            if not isinstance(row, dict):
                return None
            claimed = str(row.get("row_sha256") or "")
            if claimed != _variadic_hash({
                key: value for key, value in row.items() if key != "row_sha256"
            }):
                return None
            row_hashes.append(claimed)
        if (
            _variadic_hash(sorted(row_hashes)) != descriptor["row_set_sha256"]
            or _variadic_hash(row_hashes) != descriptor["ordered_row_sha256"]
        ):
            return None
        verified[name] = dict(descriptor)
    return verified


def _import_one_sequential_all68_policy_challenger(
    target: sqlite3.Connection, source_root: Path, *, cohort_id: str,
    specification: dict[str, Any],
) -> tuple[int, int]:
    cohort_root = source_root / "cohorts" / cohort_id
    state_path = _safe_child_file(cohort_root, cohort_root / "state.json")
    receipt_path = _safe_child_file(cohort_root, cohort_root / "verifier_receipt.json")
    report_path = _safe_child_file(cohort_root, cohort_root / "report.md")
    material_path = _safe_child_file(cohort_root, cohort_root / "material_contract.json")
    if any(path is None for path in (state_path, receipt_path, report_path, material_path)):
        return 0, 0
    assert state_path is not None and receipt_path is not None
    assert report_path is not None and material_path is not None
    if (
        _sha256_file(state_path) != specification["state_sha256"]
        or _sha256_file(receipt_path) != specification["verifier_sha256"]
        or _sha256_file(report_path) != specification["report_sha256"]
        or _sha256_file(material_path) != specification["material_file_sha256"]
    ):
        return 0, 0
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        material_file = json.loads(material_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return 0, 0
    material = state.get("material_contract")
    if (
        not isinstance(state, dict) or not isinstance(receipt, dict)
        or not isinstance(material, dict) or material_file != material
        or state.get("cohort_id") != cohort_id
        or state.get("material_sha256") != specification["material_sha256"]
        or _variadic_hash(material) != specification["material_sha256"]
        or cohort_id != "sequential_all68_policy_challenger_v1." + specification["material_sha256"][:20]
        or not _policy_challenger_safety_is_closed(state)
        or not _policy_challenger_safety_is_closed(material.get("safety"))
        or not _policy_challenger_safety_is_closed(receipt)
        or receipt.get("verified") is not True
        or receipt.get("failures") != []
        or state.get("terminal_flat_all_arms") is not True
        or state.get("independent_regime_count") is not None
    ):
        return 0, 0
    if specification["bounded_datasets_required"]:
        limits = material.get("dataset_limits")
        if limits != {
            "maximum_gzip_bytes": 4 * 1024 * 1024,
            "maximum_raw_bytes": 64 * 1024 * 1024,
            "maximum_row_count": 20_000,
        }:
            return 0, 0
    else:
        limits = None
    datasets = _bounded_policy_dataset_binding(cohort_root, state.get("datasets"), limits)
    if datasets is None or _variadic_hash(datasets) != specification["dataset_manifest_sha256"]:
        return 0, 0

    source_binding = state.get("source_binding")
    mistake_binding = state.get("mistake_curriculum_binding")
    expected_source_id = str(specification.get("source_cohort_id") or "sequential_all68_portfolio_batch_replay_v1.9d6e7e0f27ad289c4c83")
    expected_source_sha = str(specification.get("source_material_sha256") or "9d6e7e0f27ad289c4c8368679bdc350058e61750387ccb94d9a98dcb26ecbb04")
    expected_pack_id = str(specification.get("source_pack_id") or "sequential_replay_source_pack_v1.ee6d6fd4d38744ecc1da")
    expected_mistake_id = str(specification.get("mistake_cohort_id") or "sequential_all68_mistake_curriculum_v1.98755c2c2f9bd26b014a")
    expected_mistake_sha = str(specification.get("mistake_material_sha256") or "98755c2c2f9bd26b014aad140754c771482de69d0c418bceb1003e8313d7b83c")
    source_parent = target.execute(
        "SELECT definition_sha256 FROM experiments WHERE hypothesis_id=? "
        "AND experiment_kind='historical_all68_portfolio_batch_replay'",
        (expected_source_id,),
    ).fetchone()
    mistake_parent = target.execute(
        "SELECT definition_sha256 FROM experiments WHERE hypothesis_id=? "
        "AND experiment_kind='historical_all68_mistake_curriculum'",
        (expected_mistake_id,),
    ).fetchone()
    if (
        not isinstance(source_binding, dict) or source_binding != material.get("source_binding")
        or not isinstance(mistake_binding, dict) or mistake_binding != material.get("mistake_curriculum_binding")
        or source_binding.get("cohort_id") != expected_source_id
        or source_binding.get("source_pack_id") != expected_pack_id
        or mistake_binding.get("cohort_id") != expected_mistake_id
        or source_parent is None
        or str(source_parent[0]) != expected_source_sha
        or mistake_parent is None
        or str(mistake_parent[0]) != expected_mistake_sha
    ):
        return 0, 0
    bound_payloads: dict[str, dict[str, Any]] = {}
    expected_bound_files = specification.get("bound_files") or SEQUENTIAL_ALL68_POLICY_BOUND_FILE_SHA256
    for filename, expected_sha in expected_bound_files.items():
        path = _safe_child_file(cohort_root, cohort_root / filename)
        if path is None or _sha256_file(path) != expected_sha:
            return 0, 0
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return 0, 0
        if not isinstance(value, dict):
            return 0, 0
        bound_payloads[filename] = value
    if (
        source_binding.get("state_sha256") != expected_bound_files["bound_state.json"]
        or source_binding.get("verifier_sha256") != expected_bound_files["bound_verifier_receipt.json"]
        or source_binding.get("source_pack_manifest_sha256") != expected_bound_files["bound_source_pack_manifest.json"]
        or source_binding.get("source_pack_verifier_sha256") != expected_bound_files["bound_source_pack_clean_verifier_receipt.json"]
        or bound_payloads["bound_state.json"].get("cohort_id") != source_binding.get("cohort_id")
        or bound_payloads["bound_verifier_receipt.json"].get("verified") is not True
        or bound_payloads["bound_verifier_receipt.json"].get("failures") != []
        or bound_payloads["bound_source_pack_manifest.json"].get("pack_id") != source_binding.get("source_pack_id")
        or bound_payloads["bound_source_pack_clean_verifier_receipt.json"].get("verified") is not True
        or bound_payloads["bound_source_pack_clean_verifier_receipt.json"].get("failures") != []
    ):
        return 0, 0
    checks = receipt.get("checks")
    if (
        not isinstance(checks, dict)
        or checks.get("cohort_id") != cohort_id
        or checks.get("source_cohort_id") != source_binding.get("cohort_id")
        or checks.get("source_pack_id") != source_binding.get("source_pack_id")
        or int(checks.get("global_clock_count") or -1) != int(state.get("global_clock_count") or -2)
        or int(checks.get("arm_decision_count") or -1) != int(state.get("arm_decision_count") or -2)
        or int(checks.get("training_observation_count") or -1) != int(state.get("training_observation_count") or -2)
        or int(checks.get("calibration_cell_count") or -1) != int(state.get("calibration_cell_count") or -2)
        or checks.get("arm_summaries") != state.get("arm_summaries")
    ):
        return 0, 0
    if specification.get("require_current"):
        current_state = _safe_child_file(source_root, source_root / "sequential_all68_policy_challenger_v1.json")
        current_receipt = _safe_child_file(source_root, source_root / "sequential_all68_policy_challenger_verifier_v1.json")
        if (
            current_state is None or current_receipt is None
            or current_state.read_bytes() != state_path.read_bytes()
            or current_receipt.read_bytes() != receipt_path.read_bytes()
        ):
            return 0, 0

    definition_core = {
        "cohort_id": cohort_id,
        "material_sha256": specification["material_sha256"],
        "state_sha256": specification["state_sha256"],
        "verifier_sha256": specification["verifier_sha256"],
        "report_sha256": specification["report_sha256"],
        "material_file_sha256": specification["material_file_sha256"],
        "dataset_manifest_sha256": specification["dataset_manifest_sha256"],
        "source_binding": source_binding,
        "mistake_curriculum_binding": mistake_binding,
        "datasets": datasets,
        "safety": material["safety"],
    }
    definition = {
        "hypothesis_id": cohort_id,
        "parent_hypothesis_id": str(source_binding["cohort_id"]),
        "experiment_kind": "historical_sequential_all68_policy_challenger",
        "research_generation": "historical_training_policy_challenger_v1",
        "idea_origin": "all68_frozen_matched_policy_challenger",
        "pre_registered": 1,
        "data_sources_json": canonical_json([
            str(source_binding["source_pack_id"]), str(source_binding["cohort_id"]),
            str(mistake_binding["cohort_id"]),
        ]),
        "feature_contract_json": canonical_json({
            "dataset_manifest_sha256": specification["dataset_manifest_sha256"],
            "datasets": datasets,
        }),
        "label_contract_json": canonical_json({
            "historical_training_only": True,
            "market_repetition_count": state.get("market_repetition_count"),
            "independent_regime_count": None,
        }),
        "model_contract_json": canonical_json({
            "arms": state.get("arms"), "policy": material.get("policy"),
            "oof_calibration": material.get("oof_calibration"),
            "mistake_curriculum_binding": mistake_binding,
        }),
        "cost_contract_json": canonical_json(material.get("costs") or {}),
        "allocator_contract_json": canonical_json(material["safety"]),
        "training_period_json": canonical_json({"historical_replay": True}),
        "selection_period_json": canonical_json({"none": "matched_frozen_arms"}),
        "confirmation_period_json": canonical_json({"none": "not_proof_evidence"}),
        "all_parameters_tried_json": canonical_json(state.get("arms") or []),
        "selection_rule": "matched frozen arms including no-trade; no arm may authorize or promote",
        "holdouts_touched_json": canonical_json(["historical_all68_replay_sessions"]),
        "source_code_hash": _variadic_hash(
            material.get("runner_sha256"), material.get("core_sha256"),
            material.get("foundation_core_sha256"), material.get("verifier_sha256"),
        ),
        "data_snapshot_hash": _variadic_hash(source_binding, mistake_binding, datasets),
        "definition_sha256": specification["material_sha256"],
        "created_at": str(state.get("generated_utc")),
        "definition_json": canonical_json(definition_core),
    }
    definitions = int(_insert_experiment_exact(target, definition))
    observations = int(observe(
        target, hypothesis_id=cohort_id,
        observed_at=str(state.get("generated_utc")),
        source_system="sequential_all68_policy_challenger_v1",
        result=str(specification["result"]),
        evidence={
            **definition_core,
            "arm_summaries": state.get("arm_summaries"),
            "paired_pips_vs_no_trade": state.get("paired_pips_vs_no_trade"),
            "paired_pips_vs_v1": state.get("paired_pips_vs_v1"),
            "global_clock_count": state.get("global_clock_count"),
            "market_repetition_count": state.get("market_repetition_count"),
            "independent_regime_count": None,
            **dict(FULL_RESEARCH_ONLY_SAFETY),
        },
    ))
    return definitions, observations


def import_sequential_all68_policy_challengers(
    target: sqlite3.Connection, source_root: Path, *,
    cohort_specs: dict[str, dict[str, Any]] | None = None,
) -> tuple[int, int]:
    definitions = observations = 0
    specifications = (
        SEQUENTIAL_ALL68_POLICY_CHALLENGER_GENEALOGY_COHORT_SPECS
        if cohort_specs is None else cohort_specs
    )
    for cohort_id, specification in specifications.items():
        new_definitions, new_observations = _import_one_sequential_all68_policy_challenger(
            target, source_root, cohort_id=cohort_id, specification=specification,
        )
        definitions += new_definitions; observations += new_observations
    return definitions, observations


def import_sequential_all68_policy_expansion(
    target: sqlite3.Connection, source_root: Path, *,
    specification: dict[str, Any] | None = None,
) -> tuple[int, int]:
    """Register the reviewed seven-Wednesday policy expansion, fail closed.

    The policy implementation reuses challenger code, but its sealed contract
    names the seven-Wednesday replay (and that replay's source pack) as its
    evidence parent.  Code reuse is deliberately not represented as evidence
    ancestry.
    """
    if specification is None:
        definitions = observations = 0
        for reviewed in SEQUENTIAL_ALL68_POLICY_EXPANSION_SPECS.values():
            new_definitions, new_observations = import_sequential_all68_policy_expansion(
                target, source_root, specification=reviewed,
            )
            definitions += new_definitions; observations += new_observations
        return definitions, observations
    spec = specification
    cohort_id = str(spec["cohort_id"])
    cohort_root = source_root / "cohorts" / cohort_id
    paths = {
        name: _safe_child_file(cohort_root, cohort_root / name)
        for name in ("state.json", "verifier_receipt.json", "report.md", "material_contract.json")
    }
    if any(path is None for path in paths.values()):
        return 0, 0
    expected = {
        "state.json": spec["state_sha256"],
        "verifier_receipt.json": spec["verifier_sha256"],
        "report.md": spec["report_sha256"],
        "material_contract.json": spec["material_file_sha256"],
    }
    if any(_sha256_file(paths[name]) != digest for name, digest in expected.items()):
        return 0, 0
    try:
        state = json.loads(paths["state.json"].read_text(encoding="utf-8"))
        receipt = json.loads(paths["verifier_receipt.json"].read_text(encoding="utf-8"))
        material_file = json.loads(paths["material_contract.json"].read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return 0, 0
    material = state.get("material_contract")
    limits = material.get("dataset_limits") if isinstance(material, dict) else None
    if (
        not isinstance(state, dict) or not isinstance(receipt, dict)
        or not isinstance(material, dict) or material_file != material
        or state.get("cohort_id") != cohort_id
        or state.get("material_sha256") != spec["material_sha256"]
        or _variadic_hash(material) != spec["material_sha256"]
        or cohort_id != "sequential_all68_policy_expansion_v1." + spec["material_sha256"][:20]
        or state.get("evidence_role") != "historical_training_policy_expansion"
        or not _policy_challenger_safety_is_closed(state)
        or not _policy_challenger_safety_is_closed(material.get("safety"))
        or not _policy_challenger_safety_is_closed(receipt)
        or receipt.get("verified") is not True or receipt.get("failures") != []
        or state.get("terminal_flat_all_arms") is not True
        or state.get("independent_regime_count") is not None
        or limits != {"maximum_gzip_bytes": 4 * 1024 * 1024,
                      "maximum_raw_bytes": 64 * 1024 * 1024,
                      "maximum_row_count": 30_000}
    ):
        return 0, 0
    datasets = _bounded_policy_dataset_binding(cohort_root, state.get("datasets"), limits)
    if datasets is None or _variadic_hash(datasets) != spec["dataset_manifest_sha256"]:
        return 0, 0

    bound: dict[str, dict[str, Any]] = {}
    for name, digest in spec["bound_files"].items():
        path = _safe_child_file(cohort_root, cohort_root / name)
        if path is None or _sha256_file(path) != digest:
            return 0, 0
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return 0, 0
        if not isinstance(value, dict):
            return 0, 0
        bound[name] = value
    source_state = bound["bound_state.json"]
    source_receipt = bound["bound_verifier_receipt.json"]
    pack_manifest = bound["bound_source_pack_manifest.json"]
    pack_receipt = bound["bound_source_pack_clean_verifier_receipt.json"]
    source_binding = state.get("source_binding")
    if (
        not isinstance(source_binding, dict) or source_binding != material.get("source_binding")
        or source_binding.get("cohort_id") != spec["source_cohort_id"]
        or source_binding.get("source_pack_id") != spec["source_pack_id"]
        or source_binding.get("state_sha256") != spec["bound_files"]["bound_state.json"]
        or source_binding.get("verifier_sha256") != spec["bound_files"]["bound_verifier_receipt.json"]
        or source_binding.get("source_pack_manifest_sha256") != spec["bound_files"]["bound_source_pack_manifest.json"]
        or source_binding.get("source_pack_verifier_sha256") != spec["bound_files"]["bound_source_pack_clean_verifier_receipt.json"]
        or source_state.get("cohort_id") != spec["source_cohort_id"]
        or source_state.get("material_sha256") != spec["source_material_sha256"]
        or _variadic_hash(source_state.get("material_contract")) != spec["source_material_sha256"]
        or not _full_research_only_policy_is_closed(source_state)
        or source_receipt.get("verified") is not True or source_receipt.get("failures") != []
        or pack_manifest.get("pack_id") != spec["source_pack_id"]
        or pack_manifest.get("material_sha256") != spec["source_pack_material_sha256"]
        or not _full_research_only_policy_is_closed(pack_manifest)
        or pack_receipt.get("verified") is not True or pack_receipt.get("failures") != []
        or pack_receipt.get("pack_id") != spec["source_pack_id"]
        or pack_receipt.get("material_sha256") != spec["source_pack_material_sha256"]
    ):
        return 0, 0
    checks = receipt.get("checks")
    if (
        not isinstance(checks, dict) or checks.get("cohort_id") != cohort_id
        or checks.get("source_cohort_id") != spec["source_cohort_id"]
        or checks.get("source_pack_id") != spec["source_pack_id"]
        or int(checks.get("global_clock_count", -1)) != 336
        or int(checks.get("arm_decision_count", -1)) != 2016
        or int(checks.get("training_observation_count", -1)) != 928
        or int(checks.get("calibration_cell_count", -1)) != 21
        or checks.get("arm_summaries") != state.get("arm_summaries")
    ):
        return 0, 0
    if spec.get("require_current"):
        current_state = _safe_child_file(source_root, source_root / "sequential_all68_policy_expansion_v1.json")
        current_receipt = _safe_child_file(source_root, source_root / "sequential_all68_policy_expansion_verifier_v1.json")
        if (current_state is None or current_receipt is None
                or current_state.read_bytes() != paths["state.json"].read_bytes()
                or current_receipt.read_bytes() != paths["verifier_receipt.json"].read_bytes()):
            return 0, 0

    # Register the exact sealed data ancestry before its child.  These are
    # research-only definitions, not operational promotion records.
    pack_definition = {
        "hypothesis_id": spec["source_pack_id"], "parent_hypothesis_id": None,
        "experiment_kind": "historical_sequential_replay_source_pack",
        "research_generation": "sequential_replay_source_pack_v1",
        "idea_origin": "seven_wednesday_historical_source_pack", "pre_registered": 1,
        "data_sources_json": canonical_json(pack_manifest.get("source_manifest") or []),
        "feature_contract_json": canonical_json(pack_manifest.get("contracts") or {}),
        "label_contract_json": canonical_json({"independent_regime_count": None}),
        "model_contract_json": canonical_json({"model": "none_source_pack"}),
        "cost_contract_json": canonical_json({}),
        "allocator_contract_json": canonical_json(FULL_RESEARCH_ONLY_SAFETY),
        "training_period_json": canonical_json(pack_manifest.get("sessions") or []),
        "selection_period_json": canonical_json({"historical": True}),
        "confirmation_period_json": canonical_json({"none": True}),
        "all_parameters_tried_json": canonical_json({}),
        "selection_rule": "sealed seven-Wednesday historical training source pack",
        "holdouts_touched_json": canonical_json(["historical_training_sessions"]),
        "source_code_hash": str(pack_manifest.get("source_manifest_sha256") or ""),
        "data_snapshot_hash": str(pack_manifest.get("material_sha256")),
        "definition_sha256": spec["source_pack_material_sha256"],
        "created_at": str(state.get("generated_utc")),
        "definition_json": canonical_json(pack_manifest.get("material_contract")),
    }
    definitions = int(_insert_experiment_exact(target, pack_definition))
    observations = int(observe(target, hypothesis_id=spec["source_pack_id"],
        observed_at=str(state.get("generated_utc")), source_system="bound_source_pack_manifest.json",
        result="verified_historical_training_source_pack", evidence={
            "manifest_sha256": spec["bound_files"]["bound_source_pack_manifest.json"],
            "receipt_sha256": spec["bound_files"]["bound_source_pack_clean_verifier_receipt.json"],
            **dict(FULL_RESEARCH_ONLY_SAFETY)}))
    replay_definition = {
        **pack_definition, "hypothesis_id": spec["source_cohort_id"],
        "parent_hypothesis_id": spec["source_pack_id"],
        "experiment_kind": "historical_sequential_all68_expansion_replay",
        "research_generation": "seq_a68_wed_exp_v1",
        "idea_origin": "seven_wednesday_all68_historical_replay",
        "data_sources_json": canonical_json([spec["source_pack_id"]]),
        "feature_contract_json": canonical_json(source_state.get("datasets") or {}),
        "definition_sha256": spec["source_material_sha256"],
        "definition_json": canonical_json(source_state.get("material_contract")),
        "data_snapshot_hash": str(source_binding.get("dataset_roots_sha256")),
    }
    definitions += int(_insert_experiment_exact(target, replay_definition))
    observations += int(observe(target, hypothesis_id=spec["source_cohort_id"],
        observed_at=str(state.get("generated_utc")), source_system="bound_state.json",
        result="verified_historical_seven_wednesday_all68_replay", evidence={
            "state_sha256": spec["bound_files"]["bound_state.json"],
            "verifier_sha256": spec["bound_files"]["bound_verifier_receipt.json"],
            "global_clock_count": 336, "market_repetition_count": 336,
            "independent_regime_count": None, **dict(FULL_RESEARCH_ONLY_SAFETY)}))

    definition_core = {
        "cohort_id": cohort_id, "material_sha256": spec["material_sha256"],
        "file_sha256": expected, "bound_file_sha256": spec["bound_files"],
        "source_binding": source_binding, "datasets": datasets,
        "safety": material["safety"],
    }
    definition = {
        **replay_definition, "hypothesis_id": cohort_id,
        "parent_hypothesis_id": spec["source_cohort_id"],
        "experiment_kind": "historical_sequential_all68_policy_expansion",
        "research_generation": "sequential_all68_policy_expansion_v1",
        "idea_origin": "seven_wednesday_oof_policy_expansion",
        "data_sources_json": canonical_json([spec["source_pack_id"], spec["source_cohort_id"]]),
        "feature_contract_json": canonical_json({"datasets": datasets}),
        "model_contract_json": canonical_json({"arms": state.get("arms"), "policy": material.get("policy"),
                                                "oof_calibration": material.get("oof_calibration")}),
        "cost_contract_json": canonical_json(material.get("costs") or {}),
        "allocator_contract_json": canonical_json(material["safety"]),
        "definition_sha256": spec["material_sha256"],
        "definition_json": canonical_json(definition_core),
        "data_snapshot_hash": _variadic_hash(source_binding, datasets),
        "source_code_hash": _variadic_hash(material.get("runner_sha256"),
                                             material.get("expansion_core_sha256"),
                                             material.get("policy_core_sha256"),
                                             material.get("verifier_sha256")),
        "selection_rule": "frozen seven-Wednesday OOF research arms; no arm may authorize or promote",
    }
    definitions += int(_insert_experiment_exact(target, definition))
    observations += int(observe(target, hypothesis_id=cohort_id,
        observed_at=str(state.get("generated_utc")), source_system="sequential_all68_policy_expansion_v1",
        result="verified_historical_training_policy_expansion", evidence={
            **definition_core, "arm_summaries": state.get("arm_summaries"),
            "paired_pips_vs_no_trade": state.get("paired_pips_vs_no_trade"),
            "paired_pips_vs_v1": state.get("paired_pips_vs_v1"),
            "global_clock_count": 336, "market_repetition_count": 336,
            "independent_regime_count": None, **dict(FULL_RESEARCH_ONLY_SAFETY)}))
    return definitions, observations


def run(
    *, database: Path = DEFAULT_DATABASE, state: Path = DEFAULT_STATE,
    report: Path = DEFAULT_REPORT, root_state: Path = STATE,
) -> dict[str, Any]:
    observed = utc_now()
    target = connect(database)
    definition_count = observation_count = 0
    sources = [
        (root_state / "proof_cohort_registry_v1.sqlite", "proof_model"),
        (root_state / "candidate_cohort_registry_v1.sqlite", "strategy_candidate"),
    ]
    try:
        for source, kind in sources:
            definitions, observations = import_proof_registry(
                target, source, kind=kind, imported_at=observed
            )
            definition_count += definitions; observation_count += observations
        definitions, observations = import_allocator(
            target, root_state / "allocator_proof_v1.sqlite", imported_at=observed
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_lifecycle(
            target, root_state / "evidence_lifecycle_v1.sqlite", imported_at=observed
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_counterfactual_sim_gym(
            target,
            root_state.parent / "research_ledgers" /
            "counterfactual_sim_gym_v1" / "counterfactual_sim_gym_v1.sqlite",
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_sequential_deliberate_replay(
            target,
            root_state.parent / "research_ledgers" /
            "sequential_deliberate_replay_v1" / "sequential_deliberate_replay_v1.sqlite",
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_sequential_portfolio_replay(
            target,
            root_state.parent / "research_ledgers" /
            "sequential_portfolio_replay_v1",
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_sequential_portfolio_curriculum(
            target,
            root_state.parent / "research_ledgers" /
            "sequential_portfolio_curriculum_v1",
        )
        definition_count += definitions; observation_count += observations
        replay_root = (
            root_state.parent / "research_ledgers" /
            "sequential_portfolio_replay_v1"
        )
        definitions, observations = import_sequential_portfolio_mistake_curriculum(
            target, replay_root,
        )
        definition_count += definitions; observation_count += observations
        replay_state_path = replay_root / "sequential_portfolio_replay_v1.json"
        historical_source_parent_id = None
        if replay_state_path.is_file():
            replay_state = json.loads(replay_state_path.read_text(encoding="utf-8"))
            registered_replay = target.execute(
                "SELECT parent_hypothesis_id FROM experiments WHERE hypothesis_id=? "
                "AND experiment_kind='historical_sequential_portfolio_training'",
                (str(replay_state.get("cohort_id") or ""),),
            ).fetchone()
            if registered_replay is not None:
                historical_source_parent_id = registered_replay[0]
        definitions, observations = import_sequential_replay_source_pack(
            target,
            root_state.parent / "research_ledgers" /
            "sequential_replay_source_pack_v1",
            parent_hypothesis_id=(
                str(historical_source_parent_id)
                if historical_source_parent_id else None
            ),
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_sequential_all68_portfolio_batch_replays(
            target,
            root_state.parent / "research_ledgers" /
            "sequential_all68_portfolio_batch_replay_v1",
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_sequential_all68_mistake_curriculum(
            target,
            root_state.parent / "research_ledgers" /
            "sequential_all68_mistake_curriculum_v1",
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_sequential_all68_policy_challengers(
            target,
            root_state.parent / "research_ledgers" /
            "sequential_all68_policy_challenger_v1",
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_sequential_all68_policy_expansion(
            target,
            root_state.parent / "research_ledgers" /
            "sequential_all68_policy_expansion_v1",
        )
        definition_count += definitions; observation_count += observations
        discovery_reports = [
            (
                root_state.parent / "reports" / "cftc_positioning" / "CFTC_POSITIONING_DISCOVERY_20260808.json",
                "cftc_tff_positioning",
            ),
            (
                root_state.parent / "reports" / "rates" / "US_TREASURY_YIELD_DISCOVERY_20260808.json",
                "us_treasury_daily_yield_curve",
            ),
        ]
        for source, family in discovery_reports:
            definitions, observations = import_external_discovery_report(
                target, source, source_family=family
            )
            definition_count += definitions; observation_count += observations
        internal_discovery_reports = [
            root_state.parent / "reports" / "cost_clearance_cross_sectional" / "COST_CLEARANCE_CROSS_SECTIONAL_20260809.json",
            root_state.parent / "reports" / "movement_news_episode_research" / "MOVEMENT_NEWS_EPISODE_RESEARCH_20260809.json",
            root_state.parent / "reports" / "news_price_incremental_value" / "NEWS_PRICE_INCREMENTAL_VALUE_20260809.json",
        ]
        for source in internal_discovery_reports:
            definitions, observations = import_internal_discovery_artifact(target, source)
            definition_count += definitions; observation_count += observations
        direct_source_discovery_reports = [
            (
                root_state.parent / "reports" / "direct_source_response" /
                "DIRECT_SOURCE_HISTORICAL_REPLAY_20260816.json",
                "direct_source_all68_historical_response_20260816",
            ),
            (
                root_state.parent / "reports" / "direct_source_response" /
                "DIRECT_SOURCE_SIMPLE_RULES_20260816.json",
                "direct_source_simple_rules_20260816",
            ),
        ]
        for source, research_id in direct_source_discovery_reports:
            definitions, observations = import_internal_discovery_artifact(
                target,
                source,
                research_id_override=research_id,
            )
            definition_count += definitions; observation_count += observations
        definitions, observations = import_macro_point_in_time_validation(
            target,
            root_state.parent / "reports" / "macro_relative_strength" /
            "MACRO_POINT_IN_TIME_VALIDATION_20260816.json",
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_zero_output_adapter_retirement(target)
        definition_count += definitions; observation_count += observations
        definitions, observations = import_gdelt_mapping_report(
            target,
            root_state.parent / "reports" / "news_mapping" / "GDELT_ATTENTION_MAPPING_AUDIT_20260808.json",
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_prospective_source_state(
            target, root_state / "gdelt_attention_magnitude_prospective_v1.json"
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_treasury_source_state(
            target, root_state / "us_treasury_yield_prospective_v1.json"
        )
        definition_count += definitions; observation_count += observations
        definitions, observations = import_alfred_source_state(
            target, root_state / "alfred_vintage_prospective_v1.json"
        )
        definition_count += definitions; observation_count += observations
        runtime_states = [
            (
                root_state / "executable_opportunity_prospective_v1.json",
                "executable_opportunity_ranking",
                ["OANDA_executable_bid_ask", "quote_intensity", "frozen_model_artifact"],
                "after_cost_direction_magnitude_and_cost_clearance",
                "apply the frozen executable-opportunity gate before the exact outcome horizon",
            ),
            (
                root_state / "direct_source_response_v1.json",
                "direct_numeric_source_response",
                ["official_macro_numeric_values", "OANDA_executable_bid_ask"],
                "source_native_currency_response_and_cost_clearance",
                "abstain unless a frozen causal source rule is available",
            ),
            (
                root_state / "news_technical_watchlist_v1.json",
                "news_technical_four_arm_watchlist",
                ["point_in_time_news", "technical_signal_snapshot", "OANDA_executable_bid_ask"],
                "four_arm_after_cost_market_response",
                "keep news-only, technical-only, aligned, and conflicted arms separate",
            ),
            (
                root_state / "pure_change_strategy_space_v1.json",
                "pure_change_strategy_space",
                [
                    "OANDA_practice_BAM_candles",
                    "OANDA_quote_intensity",
                    "official_macro_and_policy_publishers",
                    "CFTC_TFF_positioning",
                    "US_Treasury_daily_curve",
                    "point_in_time_news_blurbs",
                ],
                "after_cost_5m_10m_15m_opportunity_and_rule_space",
                "measure opportunity first; select signal-unique rules on development and validation only; require untouched confirmation",
            ),
        ]
        definitions, observations = import_model_artifact_manifest(
            target,
            ROOT / "data" / "oanda_training_manager" / "model_space" /
            "proof_cohorts" / "executable_opportunity_ranking_v1" / "manifest.json",
            idea_origin="executable_opportunity_ranking",
        )
        definition_count += definitions; observation_count += observations
        after_cost_v1 = root_state / "currency_state_after_cost_counterfactual_v1.json"
        after_cost_v2 = root_state / "currency_state_after_cost_counterfactual_v2.json"
        after_cost_v3 = root_state / "currency_state_after_cost_counterfactual_v3.json"
        for after_cost_source, registration_status in (
            (after_cost_v1, "frozen"),
            (after_cost_v2, "engineering_blocked"),
            (after_cost_v3, "engineering_blocked"),
        ):
            definitions, observations = import_currency_state_after_cost_state(
                target, after_cost_source,
                registration_status=registration_status,
            )
            definition_count += definitions; observation_count += observations
        observation_count += observe_currency_state_after_cost_supersession(
            target, after_cost_v1, after_cost_v2,
        )
        observation_count += observe_currency_state_after_cost_supersession(
            target, after_cost_v2, after_cost_v3,
        )
        for source_path, origin, data_sources, target_name, rule in runtime_states:
            definitions, observations = import_shadow_runtime_state(
                target, source_path, idea_origin=origin, data_sources=data_sources,
                label_target=target_name, selection_rule=rule,
            )
            definition_count += definitions; observation_count += observations
        definitions, observations = import_genealogy_predecessor_snapshot(
            target,
            root_state.parent / "research_ledgers" /
            "research_genealogy_predecessor_v1",
        )
        definition_count += definitions; observation_count += observations
        predecessor_snapshots = _registered_predecessor_snapshots(target)
        totals = {
            "experiments": int(target.execute("SELECT COUNT(*) FROM experiments").fetchone()[0]),
            "observations": int(target.execute("SELECT COUNT(*) FROM experiment_observations").fetchone()[0]),
            "retired": int(target.execute("SELECT COUNT(DISTINCT hypothesis_id) FROM experiment_observations WHERE result='futility_rejected'").fetchone()[0]),
            "confirmed": int(target.execute("SELECT COUNT(DISTINCT hypothesis_id) FROM experiment_observations WHERE result='confirmed_candidate'").fetchone()[0]),
        }
        kinds = [
            {"kind": str(kind), "count": int(count)}
            for kind, count in target.execute(
                "SELECT experiment_kind,COUNT(*) FROM experiments GROUP BY experiment_kind ORDER BY experiment_kind"
            )
        ]
        fingerprint = stable_hash({"totals": totals, "kinds": kinds})
        target.execute(
            "INSERT OR IGNORE INTO genealogy_imports VALUES (?,?,?,?,?,?)",
            ("genealogy_import_" + fingerprint[:24], observed, str(root_state.resolve()), fingerprint, definition_count, observation_count),
        )
        target.commit()
    finally:
        target.close()
    payload = {
        "schema_version": 1, "generated_utc": observed, "status": "ok",
        "research_only": True, "can_place_orders": False, "can_promote": False,
        "registry": str(database.resolve()), "new_definitions": definition_count,
        "new_observations": observation_count, "totals": totals, "kinds": kinds,
        "predecessor_snapshots": predecessor_snapshots,
        "coverage_note": (
            "All current lifecycle hypotheses plus proof, candidate, allocator, and dated external-source discovery cohorts are registered. "
            "Older undocumented parameter searches are explicitly marked not_reconstructed rather than invented."
        ),
    }
    lines = [
        "# FX research genealogy", "", f"Generated: `{observed}`", "",
        "Append-only and research-only; this registry cannot promote or execute.", "",
        f"- Experiments: **{totals['experiments']:,}**",
        f"- Observations: **{totals['observations']:,}**",
        f"- Retired/confirmed: **{totals['retired']:,} / {totals['confirmed']:,}**", "",
        "| Kind | Count |", "|---|---:|",
    ]
    lines.extend(f"| {row['kind']} | {row['count']:,} |" for row in kinds)
    if predecessor_snapshots:
        lines.extend([
            "", "## Archived predecessor registries", "",
            "| Snapshot | Archived DB SHA-256 | Experiments | Observations | Compatible observations | Quarantined observations |",
            "|---|---|---:|---:|---:|---:|",
        ])
        for row in predecessor_snapshots:
            predecessor = row.get("predecessor_database") or {}
            lines.append(
                f"| `{row['snapshot_id']}` | `{predecessor.get('sha256') or ''}` "
                f"| {int(predecessor.get('experiment_count') or 0):,} "
                f"| {int(predecessor.get('observation_count') or 0):,} "
                f"| {int(row.get('merge_eligible_observation_count') or 0):,} "
                f"| {int(row.get('quarantined_observation_count') or 0):,} |"
            )
    lines.extend(["", payload["coverage_note"], ""])
    atomic_json(state, payload); atomic_text(report, "\n".join(lines))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--root-state", type=Path, default=STATE)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()
    started = time.time()
    while True:
        run(database=args.database, state=args.state, report=args.report, root_state=args.root_state)
        if args.interval_sec <= 0.0 or (
            args.duration_sec > 0.0 and time.time() - started >= args.duration_sec
        ):
            break
        time.sleep(max(60.0, args.interval_sec))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "connect",
    "import_external_discovery_report",
    "import_gdelt_mapping_report",
    "import_prospective_source_state",
    "import_treasury_source_state",
    "import_alfred_source_state",
    "import_shadow_runtime_state",
    "import_model_artifact_manifest",
    "import_macro_point_in_time_validation",
    "import_counterfactual_sim_gym",
    "import_sequential_deliberate_replay",
    "import_sequential_portfolio_replay",
    "import_sequential_portfolio_curriculum",
    "import_sequential_portfolio_mistake_curriculum",
    "import_sequential_replay_source_pack",
    "import_sequential_all68_portfolio_batch_replays",
    "import_sequential_all68_mistake_curriculum",
    "import_sequential_all68_policy_challengers",
    "import_sequential_all68_policy_expansion",
    "stage_genealogy_predecessor_snapshot",
    "import_genealogy_predecessor_snapshot",
    "insert_experiment",
    "observe",
    "run",
]
