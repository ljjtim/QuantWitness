"""仅使用内存 Arrow 与临时 HTML，禁止数据库依赖的模型报告验收。"""
from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pytest

from research_pipeline.evidence.qlib_report import (
    _model_figures, read_prediction_frame, render_qlib_report, validate_report_request,
)
from research_pipeline.cli.report_output import prepare_qlib_request, write_qlib_report


def _request():
    return {
        "contract_version": "qlib-research-report-v1", "result_id": "result.fixture",
        "table_id": "predictions", "selection": {
            "candidate_id": "ridge", "stage": "validation", "fold_id": "fold-1", "horizon_sessions": 1,
        }, "window": {"start": "2020-01-01", "end": "2020-03-01"},
        "columns": {"instrument": "entity_id", "datetime": "observation_session", "score": "prediction", "label": "actual"},
        "method": {"graphs": ["group_return", "pred_ic", "pred_autocorr"], "groups": 5, "ic_methods": ["IC", "Rank IC"], "lag": 1, "reverse": False},
        "budget": {"max_rows": 1000, "memory_bytes": 64 * 1024 * 1024},
    }


def _table(*, constant=False):
    return pa.Table.from_pylist([
        {"entity_id": f"S{j}", "observation_session": date(2020, 1, 1) + timedelta(days=i),
         "prediction": 1.0 if constant else float(np.cos(i / 4 + j) + j / 10),
         "actual": float(np.sin(i + j) / 10 + j / 100),
         "candidate_id": "ridge", "fold_id": "fold-1", "stage": "validation", "horizon_sessions": 1, "score_semantics": "raw_return_prediction"}
        for i in range(10) for j in range(10)
    ])


def _context(table):
    class Snapshot:
        bundle = SimpleNamespace(result_id="result.fixture", tables=[SimpleNamespace(table_id="predictions", schema_id="prediction.v2")])
        projected = None

        def table_schema(self, schema_id):
            return table.schema

        def iter_table_batches(self, schema_id, *, columns, batch_size):
            self.projected = columns
            yield from table.select(columns).to_batches(max_chunksize=batch_size)

    return SimpleNamespace(snapshot=Snapshot(), verification=SimpleNamespace(verification_hash="verification.fixture", status="fail"))


def test_projection_selection_and_original_label():
    table = _table()
    other = table.set_column(table.schema.get_field_index("candidate_id"), "candidate_id", pa.array(["other"] * table.num_rows))
    context = _context(pa.concat_tables([table, other]))
    request = _request()
    request["window"] = {"start": "2020-01-02", "end": "2020-01-03"}
    frame = read_prediction_frame(context, request)
    assert len(frame) == 20
    assert frame.index.names == ["instrument", "datetime"]
    assert frame.loc[("S0", "2020-01-02"), "label"] == pytest.approx(np.sin(1) / 10)
    assert set(context.snapshot.projected) == {"entity_id", "observation_session", "prediction", "actual", "candidate_id", "stage", "fold_id", "horizon_sessions", "score_semantics"}


def test_duplicate_sources_and_budgets_are_not_sampled():
    table = _table()
    with pytest.raises(ValueError, match="重复"):
        read_prediction_frame(_context(pa.concat_tables([table, table.slice(0, 1)])), _request())
    request = _request()
    request["budget"]["max_rows"] = 9
    with pytest.raises(ValueError, match="max_rows"):
        read_prediction_frame(_context(table), request)
    request = _request()
    request["budget"]["memory_bytes"] = 20
    with pytest.raises(ValueError, match="memory_bytes"):
        read_prediction_frame(_context(table), request)


def test_constant_scores_have_undefined_statistics():
    pytest.importorskip("qlib")
    pytest.importorskip("plotly")
    request = _request()
    frame = read_prediction_frame(_context(_table(constant=True)), request)
    figures, diagnostics, notes = _model_figures(frame, request["method"])
    assert figures == []
    assert diagnostics["ic_mean"] is None
    assert diagnostics["ic_sessions"] == 0
    assert "未定义" in " ".join(notes)


def test_qlib_figures_match_direct_call_and_html_is_self_contained(tmp_path):
    pytest.importorskip("qlib")
    pytest.importorskip("plotly")
    from qlib.contrib.report.analysis_model.analysis_model_performance import model_performance_graph

    request = _request()
    context = _context(_table())
    frame = read_prediction_frame(context, request)
    figures, diagnostics, notes = _model_figures(frame, request["method"])
    direct = model_performance_graph(frame.copy(), graph_names=["pred_ic"], methods=("IC", "Rank IC"), show_notebook=False)
    assert diagnostics["group_sessions"] == 10
    assert diagnostics["group_remainder_rows"] == 0
    for actual, expected in zip(figures[2].data, direct[0].data):
        np.testing.assert_allclose(np.asarray(actual.y), np.asarray(expected.y), equal_nan=True)
    output = tmp_path / "report.html"
    write_qlib_report(output, context=context, request=request, markdown="验证状态：fail；范围：测试")
    text = output.read_text(encoding="utf-8")
    assert text.count("plotly.js v") == 1
    assert "<script src=" not in text
    assert "verification.fixture" in text and "验证状态：fail" in text
    assert "非资金净值" in text
    original = output.read_bytes()
    with pytest.raises(FileExistsError):
        write_qlib_report(output, context=context, request=request, markdown="改变")
    assert output.read_bytes() == original


def test_request_rejects_ranked_label_and_ambiguous_fold():
    request = _request()
    request["columns"]["label"] = "ranked_label"
    with pytest.raises(ValueError, match="原始收益"):
        validate_report_request(request)
    request = _request()
    request["selection"]["fold_id"] = "*"
    with pytest.raises(ValueError, match="跨 fold"):
        validate_report_request(request)
    request["selection"]["stage"] = "test"
    assert validate_report_request(request)["selection"]["fold_id"] == "*"


def test_html_request_parameters_and_parser():
    from research_pipeline.cli.parser import build_parser

    for prefix in (["report"], ["package", "report", "--package", "fixture"]):
        args = build_parser().parse_args([*prefix, "--verification-result", "verification", "--result-store", "store", "--format", "html"])
        with pytest.raises(ValueError, match="--output.*--request"):
            prepare_qlib_request(args)


def test_report_command_verifies_requested_table_and_preserves_failure(tmp_path, monkeypatch):
    import json
    from research_pipeline.cli.commands import evidence_lifecycle
    from research_pipeline.cli.parser import build_parser

    request_path = tmp_path / "request.json"
    request_path.write_text(json.dumps(_request()), encoding="utf-8")
    context = _context(_table())
    context.verification.validity_status = "invalid"
    context.verification.claim_level = "none"
    observed = {}

    def load(path, *, result_store, additional_table_ids):
        observed["tables"] = additional_table_ids
        return context

    monkeypatch.setattr(evidence_lifecycle, "load_verified_result_context", load)
    monkeypatch.setattr(evidence_lifecycle, "render_verification_report", lambda context: "验证失败")
    monkeypatch.setattr(evidence_lifecycle, "write_qlib_report", lambda output, **kwargs: output)
    args = build_parser().parse_args([
        "report", "--verification-result", str(tmp_path / "verification.json"),
        "--result-store", str(tmp_path / "store"), "--format", "html",
        "--request", str(request_path), "--output", str(tmp_path / "report.html"),
    ])
    result = evidence_lifecycle._execute(args)
    assert observed["tables"] == ("predictions",)
    assert result["verification_status"] == "fail"
    assert result["validity_status"] == "invalid"


def test_rolling_folds_merge_unique_dates_and_reject_overlap():
    request = _request()
    request["selection"].update(stage="test", fold_id="*")
    request = validate_report_request(request)
    rows = _table().to_pylist()
    for row in rows:
        row["stage"] = "test"
        row["fold_id"] = "fold-1" if row["observation_session"].day <= 5 else "fold-2"
    table = pa.Table.from_pylist(rows)
    frame = read_prediction_frame(_context(table), request)
    assert len(frame) == len(rows)
    assert frame.index.is_unique
    overlap = dict(rows[0], fold_id="fold-2")
    with pytest.raises(ValueError, match="重叠来源.*fold-1.*fold-2"):
        read_prediction_frame(_context(pa.Table.from_pylist([*rows, overlap])), request)


def test_real_model_prediction_result_to_report(tmp_path):
    """真实 Qlib 训练与预测，经 Result 封存和验证后生成报告；全程无数据库。"""
    import json
    import pyarrow.parquet as pq
    from research_pipeline.research.modeling.qlib import fit_bundle, predict_bundle
    from research_pipeline.runtime.walk_forward_model_execution import _prediction_frame
    from research_pipeline.runtime.external_artifact import ExternalArtifactStore
    from research_pipeline.results import ResultAssembler, ResultSpec, ResultTableSpec
    from research_pipeline.evidence import verify_result, load_verified_result_context
    from research_pipeline.platform import canonical_json
    from test_qlib_model_integration import synthetic_samples, candidate
    from test_result_bundle import (
        HASH_A, HASH_B, HASH_C, HASH_D, VALIDITY_FACTS_PRODUCER_HASH,
        _data_reference, _proof, _runtime_fixture, _spec, policy_id_for_claim,
    )

    samples, _ = synthetic_samples()
    train, valid, infer = samples.iloc[:390].copy(), samples.iloc[400:490].copy(), samples.iloc[500:600].copy()
    spec = candidate()
    model_root = tmp_path / "model"
    row = fit_bundle(train, valid, candidate=spec, feature_columns=("x1", "x2"),
                     output_root=model_root, bundle_path="bundles/0", root_seed=7, fit_scope_ref="train-only")
    values = predict_bundle(model_root, row, infer)
    predicted = _prediction_frame(infer, values, spec["candidate_id"], "fold-1", "test",
                                  row["model_path"], row["config_path"])
    table = pa.Table.from_pandas(predicted, preserve_index=False)
    assert pa.types.is_date32(table.schema.field("observation_session").type)
    assert set(table["score_semantics"].to_pylist()) == {"raw_return_prediction"}
    np.testing.assert_array_equal(table["actual"].to_numpy(), infer.target.to_numpy())
    run_root, result_root, _ = _runtime_fixture(tmp_path / "sealed")
    external = ExternalArtifactStore(run_root / "external-artifacts")
    staging = external.prepare()
    (staging / "predictions").mkdir()
    pq.write_table(table, staging / "predictions/part-0.parquet")
    committed = external.commit(staging, artifact_name="predictions", artifact_type="research.model-validation-predictions.v2")
    record_path = run_root / "operator-dag-run.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["outputs"]["statistics"]["predictions"] = committed.artifact_ref.to_dict()
    record_path.write_text(canonical_json(record), encoding="utf-8")
    result_spec = ResultSpec.build((*_spec().tables, ResultTableSpec(
        "predictions", "diagnostic", "statistics", "predictions",
        "research.model-validation-predictions.v2", "research.model-predictions.v2", "predictions",
    )))
    bundle, directory, _ = ResultAssembler(result_root).finalize(
        run_root=run_root, project_id="research_package_test", package_hash=HASH_B,
        plan_hash=HASH_C, result_spec=result_spec, catalog_hashes={"prices": HASH_D},
        data_references={"prices": _data_reference()}, metric_proofs=(_proof(),),
        implementation_manifest_hash=HASH_A,
        verification_policy_id=policy_id_for_claim("research_observation"),
        validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH,
    )
    verification_path = tmp_path / "verification.json"
    verified = verify_result(directory, result_store=result_root, output=verification_path)
    context = load_verified_result_context(verification_path, result_store=result_root,
                                          additional_table_ids=("predictions",))
    request = _request()
    request.update(result_id=bundle.result_id)
    request["selection"].update(candidate_id=spec["candidate_id"], stage="test")
    request["window"] = {"start": str(infer.observation_session.min()), "end": str(infer.observation_session.max())}
    output = tmp_path / "model-result-report.html"
    write_qlib_report(output, context=context, request=request, markdown=f"验证状态：{verified.verification.status}")
    document = output.read_text(encoding="utf-8")
    assert bundle.result_id in document
    assert verified.verification.verification_hash in document
    assert "plotly.js v" in document
    assert not list(tmp_path.rglob("*.db")) and not list(tmp_path.rglob("*.sqlite"))
