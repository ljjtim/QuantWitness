"""生成教学分钟归档、研究包与待审请求，不连接数据库。"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import shutil
import sys

EXAMPLE_ROOT = Path(__file__).resolve().parent
ENTITIES = ("000001.XSHE", "600000.XSHG")
CLOCK = "2025-01-13T16:00:00+08:00"
MATERIAL_LINES = [
    "Volume concentration: synthetic teaching definition",
    "For each complete session, C = sum(v_i^2) / sum(v_i)^2.",
    "Each session has 240 completed one-minute volume observations.",
    "Missing bars give missing_bars; they are not filled with zero.",
    "Negative or non-finite volume gives invalid_volume.",
    "A complete session with total volume zero gives zero_volume.",
    "R is the arithmetic mean of C for the latest three sessions.",
    "The first two sessions give warmup. Keep every calendar position.",
    "Any invalid C in the three-session window gives invalid_window.",
    "C and R become available only at the last completed bar of that session.",
    "Scope: two synthetic securities, 2025-01-06 through 2025-01-10.",
    "No return labels, strategy, investment claim, or empirical paper result.",
]

SYNTHETIC_CATALOG = r'''
bundle_version: catalog-bundle-v1
coverage_slots:
- slot_id: synthetic_minute
  decision: approved
  target_ids:
  - cn_equity.minute_bar
  evidence_refs:
  - volume-concentration-teaching-definition
policies:
- policy_id: available.minute.completed_bar.v1
  policy_type: availability
  rules:
    market_visible_after: completed_bar_end
    source_delivery: historical_batch
    same_day_realtime: false
    timezone: Asia/Shanghai
- policy_id: scope.minute.catalog.v4
  policy_version: 5
  policy_type: reference_scope
  validator_id: catalog.minute.capability-manifest.v1
  applicability:
    market: cn
    frequency: minute
  rules:
    consumer_id: catalog.minute.contracts
- policy_id: drift.strict.v1
  policy_type: schema_drift
  rules:
    schema_changed: reject
    unknown: reject
- policy_id: revision.none.v1
  policy_type: revision
  rules:
    mode: none
datasets:
- dataset_id: cn_equity.minute_bar
  dataset_version: 4
  binding_version: 4
  market: cn_stock
  instrument_type: equity
  frequency: minute
  object_name: min1_stock
  expected_schema_revision: 081cbdfc9da7bc9510a11b426c4f7f2ddfc36d9b782f425e2fc9a285f37e57d6
  primary_key:
  - fld_equity_minute_dt
  - fld_equity_minute_code
  event_time_field: fld_equity_minute_dt
  available_time_policy: available.minute.completed_bar.v1
  evidence_refs:
  - volume-concentration-teaching-definition
  minute_semantics:
    asset_class: cn_stock
    instrument_role: tradable
    bar_interval: 1m
    timezone: Asia/Shanghai
    timestamp_storage: naive_local_wall_clock
    timestamp_role: completed_bar_end
    session_policy_ref: session.cn_stock.minute.v1
    availability_policy_ref: available.minute.completed_bar.v1
    scope_policy_ref: scope.minute.catalog.v4
    source_delivery: historical_batch
    quality_policy_refs:
    - quality.minute.common_fields.v1
    - quality.minute.stock.v1
    semantics_version: minute-dataset-semantics-v1
  minute_source_semantics:
    adjustment_mode: raw
    adjustment_usage: pit_allowed
    source_kind: collected
    adjustment_anchor: none
    factor_snapshot_policy: not_applicable
    evidence_refs:
    - volume-concentration-teaching-definition
    semantics_version: minute-source-semantics-v1
  fields:
  - field_id: fld_equity_minute_dt
    field_version: 4
    logical_name: market.equity.minute.raw.end_time
    physical_column: dt
    data_type: timestamp[us]
    semantic_type: datetime
    unit: minute
    nullable: false
    availability_policy: available.minute.completed_bar.v1
  - field_id: fld_equity_minute_code
    field_version: 4
    logical_name: instrument.minute_equity.raw.id
    physical_column: code
    data_type: string
    semantic_type: identifier
    unit: dimensionless
    nullable: false
    availability_policy: available.minute.completed_bar.v1
  - field_id: fld_equity_minute_open
    field_version: 4
    logical_name: market.equity.minute.raw.open
    physical_column: open
    data_type: float64
    semantic_type: price
    unit: CNY
    nullable: true
    availability_policy: available.minute.completed_bar.v1
    adjustment_allowed:
    - raw
  - field_id: fld_equity_minute_close
    field_version: 4
    logical_name: market.equity.minute.raw.close
    physical_column: close
    data_type: float64
    semantic_type: price
    unit: CNY
    nullable: true
    availability_policy: available.minute.completed_bar.v1
    adjustment_allowed:
    - raw
  - field_id: fld_equity_minute_high
    field_version: 4
    logical_name: market.equity.minute.raw.high
    physical_column: high
    data_type: float64
    semantic_type: price
    unit: CNY
    nullable: true
    availability_policy: available.minute.completed_bar.v1
    adjustment_allowed:
    - raw
  - field_id: fld_equity_minute_low
    field_version: 4
    logical_name: market.equity.minute.raw.low
    physical_column: low
    data_type: float64
    semantic_type: price
    unit: CNY
    nullable: true
    availability_policy: available.minute.completed_bar.v1
    adjustment_allowed:
    - raw
  - field_id: fld_equity_minute_volume
    field_version: 4
    logical_name: market.equity.minute.raw.volume
    physical_column: volume
    data_type: float64
    semantic_type: volume
    unit: source_native_volume
    nullable: true
    availability_policy: available.minute.completed_bar.v1
    adjustment_allowed:
    - raw
  - field_id: fld_equity_minute_money
    field_version: 4
    logical_name: market.equity.minute.raw.money
    physical_column: money
    data_type: float64
    semantic_type: turnover
    unit: CNY
    nullable: true
    availability_policy: available.minute.completed_bar.v1
    adjustment_allowed:
    - raw
  - field_id: fld_equity_minute_avg
    field_version: 4
    logical_name: market.equity.minute.raw.avg
    physical_column: avg
    data_type: float64
    semantic_type: price
    unit: CNY
    nullable: true
    availability_policy: available.minute.completed_bar.v1
    adjustment_allowed:
    - raw
  source_profile: synthetic_volume_concentration
  environment: synthetic
'''


def write_json(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_pdf(path):
    """用标准PDF文本对象保存单页教学定义，供材料定位复核。"""
    commands = ["BT /F1 10 Tf 40 800 Td 16 TL"]
    for index, line in enumerate(MATERIAL_LINES):
        text = line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        commands.append(("T* " if index else "") + "(" + text + ") Tj")
    stream = ("\n".join(commands) + "\nET\n").encode("ascii")
    objects = [b"<< /Type /Catalog /Pages 2 0 R >>", b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
               b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
               b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
               b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"endstream"]
    content, offsets = bytearray(b"%PDF-1.4\n"), [0]
    for index, body in enumerate(objects, 1):
        offsets.append(len(content))
        content.extend(str(index).encode() + b" 0 obj\n" + body + b"\nendobj\n")
    xref = len(content)
    content.extend(b"xref\n0 6\n0000000000 65535 f \n")
    for offset in offsets[1:]:
        content.extend(f"{offset:010d} 00000 n \n".encode())
    content.extend(f"trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode())
    path.write_bytes(content)


def calendar_rows():
    start = datetime.fromisoformat("2025-01-06T00:00:00+08:00")
    days = []
    for index in range(5):
        day = start + timedelta(days=index)
        bars = [day.replace(hour=9, minute=31) + timedelta(minutes=i) for i in range(120)]
        bars += [day.replace(hour=13, minute=1) + timedelta(minutes=i) for i in range(120)]
        days.append({"session": day.date().isoformat(), "expected_bars": [value.isoformat() for value in bars]})
    return days


def build(root, repo, windows_python, linux_repo, linux_output):
    """输出根必须不存在；所有个人路径只写入该次本地输出。"""
    import pyarrow as pa
    import pyarrow.parquet as pq
    import yaml
    from research_pipeline.catalog import (ApprovalDecision, DriftAttestation,
        PhysicalBindingContract, PolicyContract, compile_catalog, load_declarative_catalog)
    from research_pipeline.data_plane import (InstantRangeV2, QueryBudget, QueryIR, QueryPurpose,
        SortKey, UniverseSelection, admit_query)
    from research_pipeline.data_plane.archived_inputs import ArchivedInputInspector
    from research_pipeline.data_plane.minute_scan import build_minute_scan_plan, build_minute_partitioned_dataset
    from research_pipeline.extensions import compile_project_verifier_bundle
    from research_pipeline.packages.source_provenance import ingest_source_snapshot
    from research_pipeline.platform import load_minute_capability_manifest

    root, repo = Path(root).resolve(), Path(repo).resolve()
    rp = repo if (repo / "src/research_pipeline").is_dir() else repo / "research_pipeline"
    root.mkdir(parents=True, exist_ok=False)
    calendar = calendar_rows()
    minute_root = root / "minute"
    target = minute_root / "stock/year=2025/month=01/data.parquet"
    target.parent.mkdir(parents=True)
    rows = []
    for entity_index, entity in enumerate(ENTITIES):
        for day_index, day in enumerate(calendar):
            for bar_index, stamp in enumerate(day["expected_bars"]):
                volume = float(10 + ((bar_index * (entity_index + 2) + day_index * 7) % (23 + day_index)))
                rows.append({"code": entity, "dt": datetime.fromisoformat(stamp).replace(tzinfo=None),
                             "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0,
                             "volume": volume, "money": volume * 10, "avg": 10.0})
    pq.write_table(pa.Table.from_pylist(rows), target)
    declaration_path = root / "synthetic-catalog.yaml"
    catalog_payload = yaml.safe_load(SYNTHETIC_CATALOG)
    identity = load_minute_capability_manifest().downstream_identity("catalog.minute.contracts")
    scope = next(policy for policy in catalog_payload["policies"]
                 if policy["policy_id"] == "scope.minute.catalog.v4")
    scope["rules"].update(identity)
    catalog_payload["datasets"][0]["minute_source_semantics"]["scope_binding_hash"] = identity["binding_hash"]
    declaration_path.write_text(yaml.safe_dump(catalog_payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    declaration = load_declarative_catalog((declaration_path,), allow_generated_approvals=True)
    catalog = compile_catalog(baseline=declaration.baseline, expected_baseline_hash=declaration.baseline.content_hash,
        manifest=declaration.manifest, contracts=declaration.contracts, decisions=declaration.decisions,
        release_root=root / "baseline-catalog")
    dataset = catalog.datasets["cn_equity.minute_bar"]
    query = QueryIR(dataset_id="cn_equity.minute_bar", dataset_version=int(dataset["dataset_version"]),
        field_ids=tuple(dataset["fields"]), purpose=QueryPurpose.FEATURE,
        time_range=InstantRangeV2(datetime.fromisoformat(calendar[0]["expected_bars"][0]),
                                 datetime.fromisoformat(calendar[-1]["expected_bars"][-1]) + timedelta(minutes=1)),
        universe=UniverseSelection(ENTITIES, None), filters=(), sort=tuple(SortKey(value) for value in dataset["primary_key"]),
        budget=QueryBudget(2400, 16 * 1024 * 1024, 1024), adjustment="raw", as_of=CLOCK, ir_version="query-ir-v2")
    original_binding = next(value for value in catalog.bindings.values() if value["dataset_id"] == query.dataset_id)
    policy = PolicyContract(**dict(catalog.policies[original_binding["drift_policy_id"]]))
    attestation = DriftAttestation(catalog.catalog_hash, original_binding["binding_id"], original_binding["source_profile"],
        original_binding["environment"], original_binding["expected_schema_revision"], original_binding["expected_schema_revision"], policy.content_hash, True)
    original = admit_query(query, catalog=catalog, binding=original_binding, attestation=attestation)
    scan = build_minute_scan_plan(original, source=minute_root / "stock", allowed_root=minute_root)
    reference = build_minute_partitioned_dataset(scan)
    entry = {"kind": "minute", "original_plan": original.to_dict(), "root": str(minute_root),
             "reference": reference.to_dict(), "binding_id": "archive.volume_concentration.minute"}
    inspector = ArchivedInputInspector(entry, source_profile="archive_volume_concentration", environment=original.environment,
                                       object_name=original.object_name)
    inventory = inspector.observe_current_schema(original.object_name)
    binding = PhysicalBindingContract(**{**dict(original_binding), "binding_id": entry["binding_id"],
        "source_profile": "archive_volume_concentration", "expected_schema_revision": inventory.schema_revision})
    catalog_lock = root / "catalog"
    compile_catalog(baseline=declaration.baseline, expected_baseline_hash=declaration.baseline.content_hash,
        manifest=declaration.manifest, contracts=(*declaration.contracts, binding),
        decisions=(*declaration.decisions, ApprovalDecision("physical_binding", binding.binding_id, binding.content_hash,
            "approved", "公开教学合成Parquet输入，不涉及真实数据库", (), "synthetic-example", "2026-10-03T00:00:00+00:00")), release_root=catalog_lock)
    manifest_path = root / "input-snapshot-manifest.json"
    write_json(manifest_path, {"contract_version": "archived-input-manifest-v1", "requests": {"raw_stock_minute": entry}})
    source = root / "candidate"
    shutil.copytree(EXAMPLE_ROOT / "candidate", source, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    shutil.copy2(EXAMPLE_ROOT / "operator.yaml", root / "operator.yaml")
    verifier = yaml.safe_load((EXAMPLE_ROOT / "verifier.yaml").read_text(encoding="utf-8"))
    verifier_bundle = compile_project_verifier_bundle(source_root=EXAMPLE_ROOT / "verifier", output_root=root / "verifier-bundles",
        project_id=verifier["project_id"], verifier_id=verifier["verifier_id"], verifier_version=verifier["verifier_version"],
        entry_module=verifier["entry"]["module"], entry_function=verifier["entry"]["function"],
        authorized_schema_ids=tuple(verifier["authorized_schema_ids"]), metric_definitions=(), dependency_lock=verifier["dependency_lock"])
    package = root / "package"
    (package / "spec").mkdir(parents=True)
    (package / "sources").mkdir()
    def yaml_file(path, value):
        path.write_text(yaml.safe_dump(value, allow_unicode=True, sort_keys=False), encoding="utf-8")
    yaml_file(package / "package.yaml", {"package_slug": "synthetic_volume_concentration", "display_name": "合成成交量集中度教学案例",
        "package_version": "1.0.0", "builder_id": "operator_graph_plan_v1",
        "metric_contract": {"contract_id": "synthetic_minute_observation", "version": "v1", "metrics": ["minute.row_count@1.0.0"],
                            "semantics": {"minute.row_count@1.0.0": "合成分钟输入观察行数，公式结论由独立Verifier给出"}},
        "claim_contract": {"allowed_claim_levels": ["research_observation"], "max_claim_level": "research_observation"}})
    def edge(port, node, output):
        return {"input_port": port, "source_node_id": node, "source_output_port": output}
    def node(name, operator, parameters, inputs=()):
        return {"node_id": name, "operator_id": operator, "operator_version": "1.0.0", "inputs": list(inputs), "parameters": parameters, "strategies": []}
    nodes = [node("scan", "data.minute.scan", {"request_ids": ["raw_stock_minute"], "scope_binding_hash": scan.minute_capability_manifest_hash,
                 "max_source_bytes": 16 * 1024 * 1024, "max_returned_rows": 2400, "max_batch_bytes": 1024 * 1024,
                 "availability_policy_ref": "available.minute.completed_bar.v1"}),
             node("bars_1m", "research.bars.minute_resample", {"interval_minutes": 1, "session_policy_ref": "session.cn_stock.minute.v1",
                 "quality_policy_refs": ["quality.minute.common_fields.v1", "quality.minute.stock.v1"], "adjustment_mode": "none", "asset_class": "cn_stock",
                 "availability_policy_ref": "available.minute.completed_bar.v1"}, [edge("minute_1m", "scan", "minute_1m")]),
             node("formula", "project.volume-concentration.formula", {"calendar_json": json.dumps(calendar), "entities_json": json.dumps(ENTITIES),
                 "decision_time": CLOCK, "window_sessions": 3}, [edge("bars", "bars_1m", "bars")]),
             node("observe", "research.observation.minute-bars", {"request_id": "raw_stock_minute"}, [edge("bars", "bars_1m", "bars")]),
             node("validity", "research.validity.minute-observation", {"request_id": "raw_stock_minute"},
                 [edge("minute_1m", "scan", "minute_1m"), edge("decision_minute_1m", "scan", "minute_1m"), edge("observation", "observe", "observation")])]
    tables = [{"table_id": name, "role": "diagnostic", "source_node_id": "formula", "source_port": name,
               "artifact_type": "project.volume-concentration.evidence.v1", "schema_id": "project.volume-concentration." + name + ".v1", "path_prefix": name}
              for name in ("bars", "calendar", "coverage", "daily")]
    tables.append({"table_id": "minute_metrics", "role": "primary", "source_node_id": "observe", "source_port": "observation",
                   "artifact_type": "research.minute-observation.v1", "schema_id": "research.minute-observation.metrics.v1", "path_prefix": "observation"})
    request = query.to_dict()
    request.pop("ir_version")
    request.pop("as_of")
    request["request_id"] = "raw_stock_minute"
    yaml_file(package / "spec/research.yaml", {"contract_version": "research-operator-graph-package-v2", "research_id": "synthetic_volume_concentration",
        "as_of": CLOCK, "root_seed": 17, "fixed_clock": CLOCK, "requests": [request],
        "result": {"contract_version": "research-result-spec-v1", "tables": tables},
        "graph": {"contract_version": "research-operator-graph-recipe-v1", "graph_id": "synthetic_volume_concentration", "nodes": nodes}})
    material_root = root / "material"
    material_root.mkdir()
    pdf = material_root / "formula.pdf"
    write_pdf(pdf)
    yaml_file(package / "sources/sources.yaml", {"sources": [{"source_id": "teaching_definition", "source_type": "paper",
        "title": "成交量集中度教学定义（非实证论文）", "url": "https://example.invalid/volume-concentration-teaching-definition",
        "accessed_at": "2026-10-03", "content_hash": hashlib.sha256(pdf.read_bytes()).hexdigest(), "license_id": "project-example",
        "status": "available", "limitation": "自编教学定义和明确虚构行情，不声称真实金融效果",
        "provenance": {"mode": "citation_only", "content_digest": None, "media_type": None, "snapshot_artifact_id": None,
                       "snapshot_manifest_hash": None, "importer_id": None, "imported_at": None, "contract_version": "research-source-provenance-v1"}}]})
    yaml_file(package / "localization.yaml", {"decisions": [{"decision_id": "synthetic_scope", "original_assumption": "公式用于数学演示",
        "local_market": "cn_stock", "local_adaptation": "仅使用两个证券代码格式的虚构分钟数据，不推断市场效果",
        "evidence_source_ids": ["teaching_definition"], "status": "proxy", "claim_effect": "downgrade_to_research_observation"}]})
    archive = root / "source-archive"
    archive.mkdir()
    ingest_source_snapshot(package_root=package, source_id="teaching_definition", input_root=material_root,
        input_file=pdf, archive_root=archive, media_type="application/pdf", importer_id="volume-concentration-teaching-example",
        imported_at="2026-10-03T00:00:00+00:00")
    from quantwitness_rdagent.source_materials import extract_materials
    materials = extract_materials(package, archive, "teaching_definition", [1])
    write_json(root / "materials.json", materials)
    lines = materials["pages"][0]["lines"]
    draft = {"schema_version": "paper-formula-draft-v1", "title": "合成成交量集中度：待审教学规格", "rules": [
        {"rule_id": "teaching_definition", "origin": "project_decision", "statement": "教学计算规则以附带材料逐行定义为准。",
         "evidence_refs": [{"pdf_page": 1, "line_start": 1, "line_end": len(lines), "quote": "\n".join(lines)}]}],
        "ambiguities": [], "limitations": ["教学数学定义与合成行情，不是论文复现或投资效果证明。", "prepare仅生成待审草稿，不代表用户确认。"]}
    write_json(root / "draft.json", draft)
    write_json(root / "decisions-template.json", {"accepted_rule_ids": [], "resolutions": [],
        "interface": "daily_value(volumes, expected_count)与rolling_value(history)均返回{value,status}；volumes按分钟位置索引，history保留session/value/status。",
        "review_notes": ""})
    correct = (source / "compute.py").read_text(encoding="utf-8")
    wrong = correct.replace(" / (total * total)", " / (total * total) + 0.01")
    if wrong == correct:
        raise ValueError("错误候选必须与正确公式不同")
    payload = {"request_id": "volume-concentration-synthetic", "mode": "formula_reproduction", "base_package": str(package),
        "source_archive_root": str(archive), "input_snapshot_manifest": str(manifest_path), "editable_source_root": str(source),
        "reference_bundle": str(verifier_bundle), "development_scope": {"role": "development", "start": "2025-01-06", "end": "2025-01-10", "synthetic": True},
        "formula_evaluation": {"verifier_id": verifier["verifier_id"], "verifier_version": verifier["verifier_version"],
                               "coverage_schema_id": "project.volume-concentration.coverage.v1"},
        "budget": {"outer_loops": 1, "coder_attempts": 3, "parallel": 1, "live_llm_calls": 0},
        "runtime_binding": {"windows_python": str(Path(windows_python).resolve()), "windows_repo": str(repo), "linux_repo": linux_repo,
            "windows_session_root": str(root / "session"), "linux_session_root": linux_output.rstrip("/") + "/session",
            "operator_spec": str(root / "operator.yaml"), "catalog_lock": str(catalog_lock), "extension_bundles": [],
            "fixed_responses": [wrong, correct], "runtime_options": {"minute_data_root": str(minute_root), "workers": 1}}}
    write_json(root / "request-template.json", payload)
    return {"status": "prepared_pending_review", "output": str(root), "request_template": str(root / "request-template.json"),
            "draft": str(root / "draft.json"), "materials": str(root / "materials.json"), "raw_rows": len(rows), "entity_sessions": 10}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--windows-python", required=True)
    parser.add_argument("--windows-repo", required=True, type=Path)
    parser.add_argument("--linux-repo", required=True)
    parser.add_argument("--linux-output", required=True)
    args = parser.parse_args()
    repo = args.windows_repo.resolve()
    rp = repo if (repo / "src/research_pipeline").is_dir() else repo / "research_pipeline"
    sys.path[:0] = [str(rp / "src"), str(rp / "integrations/rdagent/src")]
    print(json.dumps(build(args.output, repo, args.windows_python, args.linux_repo, args.linux_output), ensure_ascii=False))


if __name__ == "__main__":
    main()
