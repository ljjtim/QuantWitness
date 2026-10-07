"""模型组合金融事实必须绑定独立复核的仿真、账本与TCA。"""
import hashlib
import json
from copy import deepcopy

import pyarrow.parquet as pq
import pytest
from research_pipeline.evidence import model_validity
from research_pipeline.evidence.errors import EvidenceContractError
from research_pipeline.evidence.financial_oracle.bar_tca import verify_tca
from research_pipeline.evidence.financial_oracle.canonical import (
    verify_canonical_tables,
)
from research_pipeline.evidence.financial_oracle.daily_etf import (
    verify_daily_etf_financial_context,
)
from research_pipeline.platform import typed_canonical_hash
from test_daily_cash_artifact import _case, _run

PREDICTION_ONLY = {
    "applicability": "not_applicable",
    "reason": "prediction_diagnostics_has_no_trading_simulation",
}


@pytest.fixture(scope="module")
def financial_case(tmp_path_factory):
    root = tmp_path_factory.mktemp("model-financial")
    import sqlite3

    import duckdb

    def reject(*args, **kwargs):
        pytest.fail("模型金融绑定验收不访问数据库")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(duckdb, "connect", reject)
        patch.setattr(sqlite3, "connect", reject)
        payload = _run(root, _case(root))
        simulation = root / "output/simulation"
        canonical = {name: pq.read_table(simulation / "result-contract" / name).to_pylist()
                     for name in ("orders", "fills", "positions", "cash", "costs", "valuations")}
        tca_root = simulation / "tca"
        tables = {name: pq.read_table(tca_root / name)
                  for name in ("orders", "fills", "daily", "research")}
        def read(path):
            return json.loads(path.read_text(encoding="utf-8"))

        simulation_manifest = read(simulation / "result-contract/manifest.json")
        tca_manifest = read(tca_root / "manifest.json")
        oracle_input = read(tca_root / "oracle-input.json")
        verify_canonical_tables(canonical, frequency=simulation_manifest["semantics"]["frequency"])
        verify_daily_etf_financial_context(
            context=read(simulation / "daily-context.json"), canonical=canonical,
            simulation_manifest=simulation_manifest, oracle_input=oracle_input,
        )
        expectations = verify_tca(
            canonical=canonical, tca_tables={name: table.to_pylist() for name, table in tables.items()},
            simulation_manifest=simulation_manifest, tca_manifest=tca_manifest,
            oracle_input=oracle_input,
            expected_tca_files={path: hashlib.sha256((tca_root / path).read_bytes()).hexdigest()
                                for path in tca_manifest["files"]},
            expected_tca_schema_hashes={name: typed_canonical_hash(str(table.schema))
                                        for name, table in tables.items()},
            expected_tca_table_hashes={}, minute_context_tables=None,
        )
    financial = {
        "applicability": "applicable", "mode": "daily_cash_simulation_v1",
        "simulation_result_hash": payload["simulation_result_hash"],
        "source_ledger_hash": payload["source_ledger_hash"],
        "bar_tca": {**{key: value for key, value in payload.items()
                      if key.startswith("tca_") and key not in {
                          "tca_claim_ceiling", "tca_reconciliation_delta_units",
                          "tca_liquidity_attribution_status", "tca_table_rows"}},
                    "claim_ceiling": payload["tca_claim_ceiling"],
                    "reconciliation_delta_units": payload["tca_reconciliation_delta_units"],
                    "liquidity_attribution_status": payload["tca_liquidity_attribution_status"]},
    }
    return financial, expectations


def test_prediction_only_without_simulation_remains_valid():
    model_validity._financial_tradability(PREDICTION_ONLY, None)


def test_portfolio_binds_recomputed_canonical_costs_and_tca(financial_case):
    financial, oracle = financial_case
    model_validity._financial_tradability(financial, oracle)
    assert oracle["reconciliation_delta_units"] == 0
    assert oracle["claim_ceiling"] == "analysis_only"


@pytest.mark.parametrize("field", ["simulation_result_hash", "source_ledger_hash", "mode", "applicability"])
def test_portfolio_rejects_changed_simulation_identity(financial_case, field):
    financial, oracle = deepcopy(financial_case)
    financial[field] = "f" * 64
    with pytest.raises(EvidenceContractError):
        model_validity._financial_tradability(financial, oracle)


@pytest.mark.parametrize("field", [
    "tca_result_hash", "tca_source_simulation_hash", "tca_source_ledger_hash",
    "tca_source_fill_manifest_hash", "claim_ceiling", "reconciliation_delta_units",
    "liquidity_attribution_status",
])
def test_portfolio_rejects_changed_tca_fact(financial_case, field):
    financial, oracle = deepcopy(financial_case)
    financial["bar_tca"][field] = "changed"
    with pytest.raises(EvidenceContractError, match="独立重算"):
        model_validity._financial_tradability(financial, oracle)


def test_portfolio_cannot_pass_with_only_self_reported_facts(financial_case):
    financial, _ = financial_case
    with pytest.raises(EvidenceContractError, match="oracle"):
        model_validity._financial_tradability(financial, None)


def test_portfolio_cannot_hide_simulation_as_prediction_only(financial_case):
    _, oracle = financial_case
    with pytest.raises(EvidenceContractError, match="不得声明无交易"):
        model_validity._financial_tradability(PREDICTION_ONLY, oracle)


def test_portfolio_requires_complete_tca_expectations(financial_case):
    financial, oracle = deepcopy(financial_case)
    oracle.pop("tca_source_fill_manifest_hash")
    financial["bar_tca"] = oracle
    with pytest.raises(EvidenceContractError, match="事实不完整"):
        model_validity._financial_tradability(financial, oracle)


def test_model_financial_gate_consumes_independent_oracle(financial_case, monkeypatch):
    financial, oracle = financial_case
    monkeypatch.setattr(model_validity, "_split", lambda *_: ({}, {}, {}))
    monkeypatch.setattr(model_validity, "_selection", lambda *_: {})
    monkeypatch.setattr(model_validity, "_fit_configs", lambda *_: None)
    monkeypatch.setattr(model_validity, "_holdout", lambda *_: (0.0, 1))
    monkeypatch.setattr(model_validity, "_statistics", lambda *_: None)
    mode = model_validity.MODEL_DIAGNOSTICS_MODE
    facts = {"model_diagnostics": {"mode": mode, "design": {}, "tables": {}}, "label_split": {"mode": mode},
             "search_holdout": {"mode": mode}, "statistics": {},
             "financial_tradability": financial}
    assert not model_validity.recompute_model_validity_issues(
        facts, bar_tca_expectations=oracle,
    )["financial.tradability"]
    assert model_validity.recompute_model_validity_issues(facts)["financial.tradability"] == {
        "financial.bar_tca_invalid",
    }


@pytest.mark.parametrize("applicability,issues,expected", [
    ("applicable", set(), "pass"),
    ("not_applicable", set(), "not_applicable"),
    ("not_applicable", {"financial.bar_tca_invalid"}, "fail"),
])
def test_model_financial_gate_status_matches_applicability(
    monkeypatch, applicability, issues, expected,
):
    from research_pipeline.evidence import validity_recompute

    issue_map = {gate: set() for gate in validity_recompute.VALIDITY_GATE_IDS}
    issue_map["financial.tradability"] = issues
    monkeypatch.setattr(validity_recompute, "_recompute_issue_map", lambda *a, **k: issue_map)
    facts = {
        "model_diagnostics": {"mode": model_validity.MODEL_DIAGNOSTICS_MODE},
        "financial_tradability": {"applicability": applicability},
    }
    gates = validity_recompute.recompute_gate_results(facts, input_hashes=("a" * 64,))
    financial = next(gate for gate in gates if gate.gate_id == "financial.tradability")
    assert financial.status == expected
