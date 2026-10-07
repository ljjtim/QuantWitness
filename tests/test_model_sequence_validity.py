"""真实序列模型工件的研究窗口、训练子集与Result绑定独立验收。"""
from __future__ import annotations

from copy import deepcopy
import importlib.util
import json
from statistics import fmean
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pytest

from research_pipeline.evidence.model_validity import recompute_model_validity_issues, verify_model_result_binding
from research_pipeline.evidence.model_sequence_validity import verify_sequence_research_facts
from research_pipeline.evidence.errors import EvidenceContractError
from research_pipeline.platform import canonical_json, typed_canonical_hash
import research_pipeline.runtime.walk_forward_model_execution as runtime
from test_qlib_sequence_runtime import source, split, parameters, BUDGET
from test_qlib_model_integration import _read

pytestmark = pytest.mark.skipif(importlib.util.find_spec("torch") is None, reason="需要ml-sequence可选依赖")


def records(frame):
    return list(runtime._records(frame))


@pytest.fixture(scope="module", params=[
    "development", "final", "lstm-development", "lstm-final",
    "transformer-development", "transformer-final",
])
def sequence_result(request, tmp_path_factory):
    root = tmp_path_factory.mktemp("sequence-result-" + request.param)
    _, _, p = source(root, late=True)
    scope = request.param.removeprefix("lstm-").removeprefix("transformer-")
    p["evaluation_scope"] = scope
    metadata = split(root, p)
    params = parameters()
    params["candidate_jsons"] = params["candidate_jsons"][:1]
    candidate_spec = json.loads(params["candidate_jsons"][0])
    if request.param.startswith("lstm-"):
        candidate_spec["model"]["class"] = "LSTM"
        candidate_spec["model"]["module_path"] = "qlib.contrib.model.pytorch_lstm_ts"
        candidate_spec["candidate_id"] = "lstm-contract"
        params["candidate_jsons"] = [canonical_json(candidate_spec)]
    elif request.param.startswith("transformer-"):
        from test_qlib_transformer_contract import transformer_candidate
        candidate_spec = transformer_candidate()
        candidate_spec["processors"] = json.loads(params["candidate_jsons"][0])["processors"]
        params["candidate_jsons"] = [canonical_json(candidate_spec)]
    cid = "candidate_" + typed_canonical_hash(candidate_spec)[:16]
    design = {**p, "split_calendar_sessions":p["calendar_sessions"], "root_seed":7,
        "candidate_ids":[cid], "candidate_parameters_json":canonical_json({cid:candidate_spec}), "sequence":metadata["sequence"]}
    params["research_identity_hash"] = typed_canonical_hash(design)
    fit_metadata = runtime.execute_model_fit_artifact(split_root=root/"split", parameters=params,
        output_root=root/"fit", root_seed=7, max_memory_bytes=BUDGET)
    runtime.execute_model_predict_artifact(split_root=root/"split", model_root=root/"fit",
        output_root=root/"predict", max_memory_bytes=BUDGET)
    tables = {name: records(_read(root/"split", name)) for name in ("samples", "split_audit", "holdout_index", "sequence_context", "sequence_targets", "sequence_members", "sequence_exclusions")}
    tables.update({name: records(_read(root/name, name)) for name in ("features", "labels")})
    tables["fit_audit"] = records(_read(root/"fit", "fit_audit"))
    tables["validation_predictions"] = records(_read(root/"predict", "validation_predictions"))
    mode = "walk_forward_development_v1" if scope == "development" else "walk_forward_prediction_v1"
    ledger = {}
    ports = ["fit"]
    if scope == "final":
        runtime.execute_model_fold_metrics_artifact(prediction_root=root/"predict", model_root=root/"fit",
            parameters=params, output_root=root/"metrics", max_memory_bytes=BUDGET)
        runtime.execute_model_selection_artifact(metrics_root=root/"metrics", split_root=root/"split", model_root=root/"fit",
            parameters=params, output_root=root/"selection", max_memory_bytes=BUDGET)
        params.update(package_hash="4"*64, implementation_hash="5"*64, holdout_actor="test", holdout_reason="序列证据验收",
            holdout_unlock_at="2024-06-01T00:00:00Z", fixed_clock="2024-06-02T00:00:00Z")
        runtime.execute_model_locked_holdout_artifact(split_root=root/"split", selection_root=root/"selection",
            feature_root=root/"features", label_root=root/"labels", parameters=params, output_root=root/"holdout",
            ledger_root=root/"ledger", root_seed=7, max_memory_bytes=BUDGET)
        for name in ("selection", "test_predictions", "fold_selections"):
            tables[name] = records(_read(root/"selection", name))
        for name in ("holdout_predictions", "holdout_receipt"):
            tables[name] = records(_read(root/"holdout", name))
        ledger = {name:json.loads((root/"holdout/holdout-ledger"/(name+".json")).read_text(encoding="utf-8")) for name in ("plan", "prepared", "opened", "terminal")}
        ports.append("holdout")
    configs, windows, models, support, content = {}, {}, [], [], {}
    def add(path, data, artifact="summary"):
        relative = "support/" + artifact + "/" + path
        support.append(SimpleNamespace(artifact_key=artifact, source_path=path, relative_path=relative))
        content[relative] = data
    for port in ports:
        for row in records(_read(root/port, "models")):
            config = json.loads((root/port/row["config_path"]).read_text(encoding="utf-8"))
            config_path = port + "/" + row["config_path"]
            files = config["sequence"]["files"]
            windows[config_path] = {name: records(pd.read_parquet(root/port/path)) for name, path in files.items()}
            for name, path in files.items(): add(port+"/"+path, (root/port/path).read_bytes())
            config["sequence"]["files"] = {name:port+"/"+path for name, path in files.items()}
            config["weights_path"] = port+"/"+config["weights_path"]
            config["effective_fit_kwargs"]["save_path"] = config["weights_path"]
            config["model_path"] = port+"/"+config["model_path"]
            config["processor_files"] = {name:[port+"/"+path for path in paths] for name, paths in config["processor_files"].items()}
            add(config_path, canonical_json(config).encode())
            add(config["weights_path"], (root/config["weights_path"]).read_bytes())
            configs[config_path] = config
            models.append({**row, "config_path":config_path, "model_path":config["model_path"]})
    for name, data in ledger.items(): add("holdout-ledger/"+name+".json", canonical_json(data).encode(), "holdout")
    tables["models"] = pa.Table.from_pylist(models).to_pylist()
    candidates = {item["candidate_id"]:item["parameters"] for item in fit_metadata["search_manifest"]["candidates"]}
    assert candidates == {cid:candidate_spec}
    tables["study_design"] = [{"design_json":canonical_json(design)}]
    pred = tables["validation_predictions"] if scope == "development" else tables["holdout_predictions"]
    mse = fmean((row["prediction"]-row["actual"])**2 for row in pred)
    tables["metrics"] = [{"metric_ref":"test.mse@1.0.0", "value":mse, "unit":"squared_decimal_price_change", "status":"computed",
        "sample_size":len(pred), "sample_start":min(row["observation_session"] for row in pred), "sample_end":max(row["observation_session"] for row in pred)}]
    model = {"mode":mode, "design":design, "tables":tables, "table_bindings":{name:name for name in tables},
        "model_configs":configs, "model_window_facts":windows, "holdout_ledger":ledger}
    facts = {"model_diagnostics":model, "label_split":{"mode":mode}, "search_holdout":{"mode":mode},
        "statistics":{"method":"validation_mse" if scope=="development" else "prediction_mse", "sample_count":len(pred), "mse":mse, "metric_ref":"test.mse@1.0.0"},
        "financial_tradability":{"applicability":"not_applicable", "reason":"prediction_diagnostics_has_no_trading_simulation"}}
    snapshot = SimpleNamespace(read_table=lambda schema:pa.Table.from_pylist(tables[schema]),
        table_manifest=lambda schema:SimpleNamespace(artifact_key="holdout" if schema=="holdout_receipt" else "summary"),
        bundle=SimpleNamespace(support_files=support), support_bytes=content, artifact_root=root)
    return facts, snapshot


def test_real_gru_window_and_model_facts_pass_without_production_sampling(sequence_result, monkeypatch):
    facts, snapshot = sequence_result
    import research_pipeline.research.modeling.sequence as production
    monkeypatch.setattr(production, "build_sequence_windows", lambda *a, **k:pytest.fail("独立验证不得调用生产构建器"))
    verify_model_result_binding(snapshot, facts)
    verify_sequence_research_facts(facts["model_diagnostics"])
    from research_pipeline.evidence.model_validity import _split
    samples, folds, _ = _split(facts["model_diagnostics"])
    if facts["model_diagnostics"]["mode"] == "walk_forward_prediction_v1":
        from research_pipeline.evidence.model_validity import _selection, _fit_configs, _holdout
        selected = _selection(facts["model_diagnostics"], samples, folds, set(facts["model_diagnostics"]["design"]["candidate_ids"]))
        _fit_configs(facts["model_diagnostics"], samples, folds, selected)
        _holdout(facts["model_diagnostics"], selected)
    issues = recompute_model_validity_issues(facts)
    assert not any(issues.values()), issues


@pytest.mark.parametrize("mutation", ["missing_target", "member_future", "wrong_exclusion", "context_value", "sample_value", "bundle_member", "bundle_extra_context", "window_length", "torch_version", "curve_sign", "weights_path", "sample_decision", "holdout_decision", "label_value"])
def test_sequence_facts_reject_changed_evidence(sequence_result, mutation):
    facts = deepcopy(sequence_result[0])
    model = facts["model_diagnostics"]
    path = next(iter(model["model_configs"]))
    config = model["model_configs"][path]
    if mutation == "missing_target": model["tables"]["sequence_targets"].pop()
    elif mutation == "member_future": model["tables"]["sequence_members"][0]["feature_available_time"] = "2050-01-01T00:00:00Z"
    elif mutation == "wrong_exclusion": model["tables"]["sequence_exclusions"][0]["reason_code"] = "missing_feature"
    elif mutation == "context_value": model["tables"]["sequence_context"][0]["x1__w1"] += 1
    elif mutation == "sample_value": model["tables"]["samples"][0]["x1__w1"] += 1
    elif mutation == "bundle_member": model["model_window_facts"][path]["members"].pop()
    elif mutation == "bundle_extra_context": model["model_window_facts"][path]["context"].append(model["tables"]["sequence_context"][-1])
    elif mutation == "window_length": config["sequence"]["step_len"] += 1
    elif mutation == "torch_version": config["versions"]["torch"] = "2.6.0"
    elif mutation == "curve_sign": config["training_curve"][0]["value"] = 1
    elif mutation == "weights_path": config["weights_path"] = ""
    elif mutation == "sample_decision":
        row = model["tables"]["samples"][0]
        row["decision_time"] = (pd.Timestamp(row["decision_time"]) - pd.Timedelta(seconds=1)).isoformat()
    elif mutation == "holdout_decision":
        name = "holdout_predictions" if "holdout_predictions" in model["tables"] else "samples"
        row = model["tables"][name][0]
        row["decision_time"] = (pd.Timestamp(row["decision_time"]) + pd.Timedelta(seconds=1)).isoformat()
    elif mutation == "label_value": model["tables"]["samples"][0]["target"] += 1
    issues = recompute_model_validity_issues(facts)
    assert issues["statistics"], issues


def test_window_file_fact_must_match_result_snapshot(sequence_result):
    facts = deepcopy(sequence_result[0])
    path = next(iter(facts["model_diagnostics"]["model_window_facts"]))
    facts["model_diagnostics"]["model_window_facts"][path]["context"][0]["x1__w1"] += 1
    with pytest.raises(EvidenceContractError, match="封存文件"):
        verify_model_result_binding(sequence_result[1], facts)


@pytest.mark.parametrize("collector", ["example", "project"])
def test_sequence_fact_collectors_bind_all_window_tables(sequence_result, collector):
    import importlib.util
    from pathlib import Path
    facts, snapshot = sequence_result
    model = facts["model_diagnostics"]
    development = model["mode"] == "walk_forward_development_v1"
    source_path = (Path(__file__).resolve().parents[1] / ("examples/qlib_portfolio/extension/validity.py"
                   if collector == "example" else "project_extensions/qlib_prediction_validity/operator.py"))
    spec = importlib.util.spec_from_file_location("sequence_collector_" + collector, source_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    class Input:
        def __init__(self, port):
            self.port, self.files = port, {}
            for name, rows in model["tables"].items():
                mapped = "splits" if name.startswith("sequence_") else module.TABLE_PORTS.get(name)
                if mapped == port:
                    self.files[name+"/part-0.parquet"] = self.parquet(rows)
            if port == "summary":
                self.files["raw_prices/part-0.parquet"] = self.parquet([{"session":"2024-01-02", "entity_id":"SYN_00", "close":1.0}])
                for item in snapshot.bundle.support_files:
                    if item.artifact_key == "summary": self.files[item.source_path] = snapshot.support_bytes[item.relative_path]
            if port == "holdout":
                for name, value in model["holdout_ledger"].items(): self.files["holdout-ledger/"+name+".json"] = canonical_json(value).encode()
            self.file_paths = tuple(self.files)
        def parquet(self, rows):
            import pyarrow.parquet as pq
            out = pa.BufferOutputStream()
            pq.write_table(pa.Table.from_pylist(rows), out)
            return out.getvalue().to_pybytes()
        def read_bytes(self, name): return self.files[name]
        def read_json(self, name): return json.loads(self.files[name])
        def admission(self, name):
            return {"input_claim_ceiling":"research_observation", "as_of_cutoff":"2024-06-01T00:00:00+00:00", "source_revision_hash":"1"*64, "availability_policy_hash":"2"*64}
    ports = set(module.TABLE_PORTS.values()) | {"data"}
    if development: ports -= {"holdout", "selection"}
    bindings = {name:name for name in model["tables"]}
    bindings["raw_prices"] = "raw_prices"
    context = SimpleNamespace(parameters={"table_bindings":bindings, "data_request_ids":["prices"], "metric_ref":"test.mse@1.0.0"}, fixed_clock="2024-06-02T00:00:00+00:00")
    if collector == "project" and development:
        with pytest.raises(ValueError, match="绑定表集合"):
            module.collect_facts(context, [Input(port) for port in ports])
        return
    collected = module.collect_facts(context, [Input(port) for port in ports])
    assert collected["model_diagnostics"]["model_window_facts"] == model["model_window_facts"]
    assert not any(recompute_model_validity_issues(collected).values())


def test_real_gru_result_store_keeps_window_proofs_and_model_recovery(sequence_result, tmp_path):
    from pathlib import Path
    import shutil
    import pyarrow.parquet as pq
    from research_pipeline.results import ResultAssembler, ResultSpec, ResultTableSpec, ResultStore, QLIB_MODEL_INVENTORY_SCHEMA_ID
    from research_pipeline.runtime.external_artifact import ExternalArtifactStore
    from research_pipeline.evidence.model_result_binding import model_verification_support_paths
    from research_pipeline.research.modeling.qlib import predict_bundle
    from research_pipeline.runtime.model_sequence import load_runtime_windows
    from research_pipeline.research.dataframe_budget import PandasFrameBudget
    from test_result_bundle import (_runtime_fixture, _spec, _proof, _data_reference, HASH_A, HASH_B, HASH_C, HASH_D,
                                    VALIDITY_FACTS_PRODUCER_HASH, policy_id_for_claim)
    facts, original_snapshot = sequence_result
    model = facts["model_diagnostics"]
    from validity_facts_support import default_passing_validity_facts
    payload = default_passing_validity_facts()
    payload.update(deepcopy(facts))
    payload["model_diagnostics"]["table_bindings"] = {name: QLIB_MODEL_INVENTORY_SCHEMA_ID if name == "models" else "project.sequence."+name+".v1" for name in model["tables"]}
    run_root, result_root, _ = _runtime_fixture(tmp_path, validity_payload=payload)
    record_path = run_root/"operator-dag-run.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    external = ExternalArtifactStore(run_root/"external-artifacts")
    specifications, bindings = [], {}
    for port in ("summary", "holdout"):
        names = [name for name in model["tables"] if (name == "holdout_receipt") == (port == "holdout")]
        if not names: continue
        staging = external.prepare()
        for name in names:
            directory = staging/name
            directory.mkdir()
            pq.write_table(pa.Table.from_pylist(model["tables"][name]), directory/"part-0.parquet")
        for item in original_snapshot.bundle.support_files:
            if item.artifact_key != port: continue
            path = staging/item.source_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(original_snapshot.support_bytes[item.relative_path])
        if port == "summary":
            for config in model["model_configs"].values():
                for source in [config["model_path"], *[path for paths in config["processor_files"].values() for path in paths]]:
                    target = staging/source
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes((original_snapshot.artifact_root/source).read_bytes())
        artifact_type = "project.sequence.summary.v1" if port == "summary" else "research.model-locked-holdout.v2"
        committed = external.commit(staging, artifact_name=port, artifact_type=artifact_type)
        record["outputs"]["statistics"][port] = committed.artifact_ref.to_dict()
        for name in names:
            schema = QLIB_MODEL_INVENTORY_SCHEMA_ID if name == "models" else "project.sequence."+name+".v1"
            bindings[name] = schema
            specifications.append(ResultTableSpec("sequence_"+name, "diagnostic", "statistics", port, artifact_type, schema, name))
    record_path.write_text(canonical_json(record), encoding="utf-8")
    bundle, directory, _ = ResultAssembler(result_root).finalize(run_root=run_root, project_id="research_package_test",
        package_hash=HASH_B, plan_hash=HASH_C, result_spec=ResultSpec.build((*_spec().tables, *specifications)),
        catalog_hashes={"prices":HASH_D}, data_references={"prices":_data_reference()}, metric_proofs=(_proof(),),
        implementation_manifest_hash=HASH_A, verification_policy_id=policy_id_for_claim("research_observation"),
        validity_producer_hash=VALIDITY_FACTS_PRODUCER_HASH)
    assert run_root.resolve().is_relative_to(tmp_path.resolve())
    shutil.rmtree(run_root)
    copied = deepcopy(facts)
    copied["model_diagnostics"]["table_bindings"] = bindings
    store = ResultStore(result_root)
    snapshot = store.open_snapshot(directory, schema_ids=tuple(bindings.values()), support_paths=tuple(model_verification_support_paths(bundle)))
    verify_model_result_binding(snapshot, copied)
    assert not any(recompute_model_validity_issues(copied).values())
    model_key = snapshot.table_manifest(QLIB_MODEL_INVENTORY_SCHEMA_ID).artifact_key
    restored = tmp_path/"restored"
    for item in bundle.support_files:
        if item.artifact_key == model_key:
            target = restored/item.source_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(store.read_support_bytes(bundle, artifact_key=model_key, source_path=item.source_path))
    row = model["tables"]["models"][0]
    config = model["model_configs"][row["config_path"]]
    samples = _read(original_snapshot.artifact_root/"split", "samples")
    valid = samples.loc[samples.sample_id.isin(config["valid_ids"])]
    metadata = json.loads((original_snapshot.artifact_root/"split/artifact-metadata.json").read_text(encoding="utf-8"))
    windows = load_runtime_windows(original_snapshot.artifact_root/"split", metadata, PandasFrameBudget(BUDGET), runtime._load_table)
    actual = predict_bundle(restored, row, valid, sequence_context=windows)
    expected = {r["sample_id"]:r["prediction"] for r in model["tables"]["validation_predictions"]
                if r["fold_id"] == row["fold_id"] and r["candidate_id"] == row["candidate_id"]}
    assert actual.tolist() == [expected[sid] for sid in valid.sample_id]
