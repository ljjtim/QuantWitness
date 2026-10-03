"""正式包循环的冻结边界、开发反馈与窗口样本合同。"""
import copy
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest

from quantwitness_rdagent.package_campaign import PackageCampaign, validate_package_campaign, project_development_metric

ROOT = Path(__file__).resolve().parents[1]


def _prepare():
    spec = importlib.util.spec_from_file_location("package_campaign_prepare", ROOT / "examples/package_campaign/prepare.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def prepared(tmp_path_factory):
    root = tmp_path_factory.mktemp("package-campaign") / "fixture"
    spec = importlib.util.spec_from_file_location("volume_campaign_input", ROOT / "examples/volume_concentration/prepare.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    import sys
    module.build(root / "input", ROOT.parents[1], sys.executable, "/source", "/output")
    template = json.loads((root / "input/request-template.json").read_text(encoding="utf-8"))
    return _prepare().make_request(root, template, [])


def test_prepare_and_freeze_have_distinct_formal_variants(prepared, monkeypatch):
    import duckdb
    import sqlite3
    def forbidden(*args, **kwargs):
        raise AssertionError("研究准备不得访问数据库")
    monkeypatch.setattr(duckdb, "connect", forbidden)
    monkeypatch.setattr(sqlite3, "connect", forbidden)
    campaign = PackageCampaign(prepared)
    assert len(campaign.data["packages"]) == 2
    assert len({item["package_hash"] for item in campaign.data["packages"].values()}) == 2
    assert PackageCampaign(prepared).data == campaign.data
    variant = Path(campaign.data["packages"]["window_2"]["path"]) / "spec/research.yaml"
    original = variant.read_text(encoding="utf-8")
    try:
        variant.write_text(original.replace("window_sessions: 2", "window_sessions: 4"), encoding="utf-8")
        with pytest.raises(ValueError, match="冻结变体包"):
            campaign.evaluate_metrics(prepared["candidates"][1])
    finally:
        variant.write_text(original, encoding="utf-8")


@pytest.mark.parametrize("fault", ["holdout_table", "source_db", "late_range", "unknown_budget", "duplicate_candidate", "live_without_budget"])
def test_invalid_request_rejected(prepared, fault):
    request = copy.deepcopy(prepared)
    if fault == "holdout_table":
        request["objective"]["table_id"] = "holdout_predictions"
    elif fault == "source_db":
        request["source"]["runtime_options"]["source_db"] = "private.duckdb"
    elif fault == "late_range":
        request["development"]["end"] = "2026-01-01"
    elif fault == "unknown_budget":
        request["budget"]["unlimited"] = True
    elif fault == "duplicate_candidate":
        request["candidates"][1]["id"] = request["candidates"][0]["id"]
    else:
        request["proposer"] = {"mode": "live", "model": "example", "base_url": "https://example.invalid/v1"}
    with pytest.raises(ValueError):
        validate_package_campaign(request)


def _projection(monkeypatch, prepared, **updates):
    import research_pipeline.evidence
    payload = copy.deepcopy(prepared)
    row = {"session": "2025-01-08", "available_at": "2025-01-08T15:00:00+08:00", "rolling_value": 0.1,
           "rolling_status": "computed", "holdout_metric": 999.0, **updates}
    class Snapshot:
        bundle = SimpleNamespace(result_id="result1", tables=[SimpleNamespace(table_id="daily", schema_id=payload["objective"]["schema_id"])])
        def table_schema(self, schema):
            return pa.Table.from_pylist([row]).schema
        def iter_table_batches(self, schema, *, columns, batch_size):
            assert "holdout_metric" not in columns
            yield pa.Table.from_pylist([row]).select(columns).to_batches()[0]
    context = SimpleNamespace(snapshot=Snapshot(), verification=SimpleNamespace(status="pass"))
    monkeypatch.setattr(research_pipeline.evidence, "load_verified_result_context", lambda *a, **kw: context)
    result = {"verification_ref": "verification", "execution_ref": "execution", "result_ref": "result"}
    return payload, result, context


def test_only_whitelisted_development_metric_is_projected(prepared, monkeypatch):
    payload, result, context = _projection(monkeypatch, prepared)
    projected = project_development_metric(result, payload)
    assert projected["value"] == 0.1
    assert projected["rows"] == 1
    assert "holdout_metric" not in projected
    campaign = object.__new__(PackageCampaign)
    campaign.payload = payload
    campaign.history = lambda: [{"round": 0, "status": "evaluated", "candidate_id": "window_3", "metrics": projected,
                                 "diagnostic": "private secret"}]
    prompt = campaign.prompt()
    assert "private secret" not in prompt and "verification_ref" not in prompt and '"result_ref"' not in prompt
    assert campaign.loss(projected) == 0.1
    payload["objective"]["direction"] = "maximize"
    assert campaign.loss(projected) == -0.1


@pytest.mark.parametrize("fault", ["future_label", "future_day", "failed_verification", "nonfinite", "stage_test", "wrong_schema"])
def test_invalid_development_result_never_reaches_feedback(prepared, monkeypatch, fault):
    updates = {}
    if fault == "future_label":
        updates["available_at"] = "2026-01-01T15:00:00+08:00"
    elif fault == "future_day":
        updates["session"] = "2025-01-14"
    elif fault == "nonfinite":
        updates["rolling_value"] = float("nan")
    elif fault == "stage_test":
        updates["stage"] = "test"
    payload, result, context = _projection(monkeypatch, prepared, **updates)
    if fault == "failed_verification":
        context.verification.status = "fail"
    elif fault == "wrong_schema":
        payload["objective"]["schema_id"] = "wrong"
    elif fault == "stage_test":
        payload["objective"]["stage_column"] = "stage"
    with pytest.raises(ValueError):
        project_development_metric(result, payload)


def test_interrupted_evaluation_keeps_reservation_and_can_resume(prepared, tmp_path):
    campaign = object.__new__(PackageCampaign)
    campaign.payload = copy.deepcopy(prepared)
    campaign.root = tmp_path / "interrupted"
    proposal = {"action": "evaluate", "candidate_id": "window_3", "parent_id": None, "reason": "baseline"}
    def interrupted(candidate):
        raise RuntimeError("execution interrupted")
    campaign.evaluate_metrics = interrupted
    with pytest.raises(RuntimeError, match="execution interrupted"):
        campaign.evaluate(0, proposal)
    path = campaign.root / "rounds/0000/evaluation.json"
    reserved = json.loads(path.read_text(encoding="utf-8"))
    assert reserved["evaluation_reserved"] and reserved["status"] == "reserved"
    campaign.evaluate_metrics = lambda candidate: {"value": 0.1, "result_ref": "result", "verification_ref": "verification"}
    result = campaign.evaluate(0, proposal)
    assert result["status"] == "evaluated"
    assert len(list(campaign.root.glob("rounds/*/evaluation.json"))) == 1
    def forbidden(candidate):
        raise AssertionError("已评价轮次不再计算")
    campaign.evaluate_metrics = forbidden
    assert campaign.evaluate(0, proposal) == result


def test_interrupted_variant_publication_is_rebuilt_in_same_session(prepared, tmp_path):
    payload = copy.deepcopy(prepared)
    payload["session_root"] = str(tmp_path / "new-session")
    pending = Path(payload["session_root"]) / ".packages.tmp"
    pending.mkdir(parents=True)
    (pending / "partial.txt").write_text("partial", encoding="utf-8")
    campaign = PackageCampaign(payload)
    assert set(campaign.data["packages"]) == {"window_2", "window_3"}
    assert not pending.exists()
    assert (campaign.root / "interrupted-variants/attempt-0/partial.txt").read_text(encoding="utf-8") == "partial"


@pytest.mark.parametrize("operator,scope,version,allowed", [
    ("research.model.split-manifest", "development", "2.0.0", True),
    ("research.model.split-manifest", "final", "2.0.0", False),
    ("research.model.split-manifest", None, "2.0.0", False),
    ("research.model.selection", "development", "2.0.0", False),
    ("research.model.locked-holdout", "development", "2.0.0", False),
    ("research.model.fit", "development", "2.0.0", True),
    ("research.model.fit", "development", "3.0.0", False),
])
def test_development_model_nodes_require_explicit_supported_scope(operator, scope, version, allowed):
    from quantwitness_rdagent.package_campaign import _check_development_package
    package = SimpleNamespace(spec_payload={
        "as_of": "2024-04-20T00:00:00+08:00", "fixed_clock": "2024-04-20T00:00:00+08:00",
        "requests": [], "graph": {"nodes": [{"operator_id": operator, "operator_version": version,
            "parameters": {} if scope is None else {"evaluation_scope": scope}}]}})
    scope = {"start": "2024-01-01", "end": "2024-04-19", "as_of": "2024-04-20T00:00:00+08:00"}
    if allowed:
        _check_development_package(package, scope)
    else:
        with pytest.raises(ValueError):
            _check_development_package(package, scope)
