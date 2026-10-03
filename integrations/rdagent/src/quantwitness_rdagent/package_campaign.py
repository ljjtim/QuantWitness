"""有限参数变体的正式研究循环，提案只消费已验证的开发指标。"""
from datetime import date, datetime, timedelta
import json
import math
from pathlib import Path
import re
from statistics import fmean

from .campaign import Campaign
from .contracts import write_json


def _time(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("开发时点必须含时区")
    return result


def validate_package_campaign(value):
    fields = {"research_kind", "campaign_id", "session_root", "source", "development", "objective",
              "candidates", "baseline_id", "budget", "stop", "proposer"}
    if not isinstance(value, dict) or set(value) != fields or value["research_kind"] != "package":
        raise ValueError("package研究请求字段不完整")
    request = json.loads(json.dumps(value, allow_nan=False))
    for name in ("campaign_id", "session_root"):
        if not isinstance(request[name], str) or not request[name].strip():
            raise ValueError(name + "必须非空")
    source = request["source"]
    if set(source) != {"kind", "path", "source_archive_root", "input_snapshot_manifest", "catalog_lock",
                       "verifier_bundle", "extension_bundles", "runtime_options", "verification_process_slots"} or source["kind"] != "research_package":
        raise ValueError("来源必须绑定完整研究包、归档、Catalog和Verifier")
    for key in ("path", "source_archive_root", "input_snapshot_manifest", "catalog_lock", "verifier_bundle"):
        if not isinstance(source[key], str) or not source[key]:
            raise ValueError("来源路径必须明确:" + key)
    if (not isinstance(source["extension_bundles"], list) or
            any(not isinstance(x, str) or not x for x in source["extension_bundles"]) or
            type(source["verification_process_slots"]) is not int or source["verification_process_slots"] < 1):
        raise ValueError("扩展或Verifier资源声明无效")
    if not isinstance(source["runtime_options"], dict) or set(source["runtime_options"]) & {
        "data_db", "source_db", "input_snapshot_manifest", "reuse_run_root", "reuse_failed_run_root", "require_reused_node"}:
        raise ValueError("研究循环只消费冻结归档，不跨候选复用运行")
    development = request["development"]
    if set(development) != {"start", "end", "as_of"}:
        raise ValueError("开发范围必须声明start/end/as_of")
    if not date.fromisoformat(development["start"]) <= date.fromisoformat(development["end"]) <= _time(development["as_of"]).date():
        raise ValueError("开发范围晚于研究时点")
    objective = request["objective"]
    if set(objective) != {"table_id", "schema_id", "value_column", "date_column", "availability_column", "stage_column", "filters", "reduction", "direction"}:
        raise ValueError("开发目标必须明确表、列、时间、筛选与聚合")
    for key in ("table_id", "schema_id", "value_column", "date_column", "availability_column"):
        if not isinstance(objective[key], str) or not objective[key]:
            raise ValueError("开发目标字段必须非空:" + key)
    if any(word in objective["table_id"].lower() for word in ("holdout", "test")):
        raise ValueError("开发目标不能读取test或holdout表")
    if objective["stage_column"] is not None and (not isinstance(objective["stage_column"], str) or not objective["stage_column"]):
        raise ValueError("stage列声明无效")
    if not isinstance(objective["filters"], dict) or any(not isinstance(k, str) or not k or not isinstance(v, (str, int, float, bool, type(None))) for k, v in objective["filters"].items()):
        raise ValueError("开发筛选仅支持固定列的标量等值")
    if objective["reduction"] not in {"mean", "single"} or objective["direction"] not in {"minimize", "maximize"}:
        raise ValueError("开发目标聚合或方向无效")
    candidates = request["candidates"]
    if not isinstance(candidates, list) or not candidates:
        raise ValueError("候选菜单必须非空")
    ids = []
    for candidate in candidates:
        if (set(candidate) != {"id", "parameter_overrides"} or not isinstance(candidate["id"], str)
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", candidate["id"])):
            raise ValueError("候选只能声明ID与参数覆盖")
        if not isinstance(candidate["parameter_overrides"], list) or not candidate["parameter_overrides"]:
            raise ValueError("候选必须声明非空参数覆盖")
        ids.append(candidate["id"])
    if len(set(ids)) != len(ids) or request["baseline_id"] not in ids:
        raise ValueError("候选ID必须唯一且包含基准")
    budget = request["budget"]
    if set(budget) != {"rounds", "evaluations", "model_calls", "output_tokens", "max_output_tokens_per_call", "max_rows", "memory_bytes"}:
        raise ValueError("预算声明不完整")
    for key, number in budget.items():
        if type(number) is not int or number < (1 if key in {"rounds", "evaluations", "max_rows", "memory_bytes"} else 0):
            raise ValueError("预算必须为允许的整数:" + key)
    stop = request["stop"]
    if set(stop) != {"target_value", "min_improvement", "patience"} or type(stop["patience"]) is not int or stop["patience"] < 1:
        raise ValueError("停止条件声明不完整")
    for key in ("target_value", "min_improvement"):
        number = stop[key]
        if key == "target_value" and number is None:
            continue
        if type(number) not in (int, float) or not math.isfinite(number) or (key == "min_improvement" and number < 0):
            raise ValueError("停止目标必须为有限数值")
    proposer = request["proposer"]
    if proposer == {"mode": "fixed_policy"}:
        if any(budget[k] for k in ("model_calls", "output_tokens", "max_output_tokens_per_call")):
            raise ValueError("固定提案器必须使用零模型预算")
    elif set(proposer) == {"mode", "model", "base_url"} and proposer["mode"] == "live":
        from urllib.parse import urlsplit
        endpoint = urlsplit(proposer["base_url"])
        if (not proposer["model"] or endpoint.scheme != "https" or not endpoint.netloc or
                endpoint.username or endpoint.password or endpoint.query or endpoint.fragment or
                budget["model_calls"] < 1 or not 1 <= budget["max_output_tokens_per_call"] <= min(8192, budget["output_tokens"])):
            raise ValueError("live身份或预算无效")
    else:
        raise ValueError("提案器只支持固定策略或live")
    return request


def _check_development_package(package, development):
    from research_pipeline.data_plane import resolve_as_of_cutoff
    spec = package.spec_payload
    cutoff = _time(development["as_of"])
    if resolve_as_of_cutoff(spec["as_of"], reference_clock=cutoff, field="spec.as_of") > cutoff or _time(spec["fixed_clock"]) > cutoff:
        raise ValueError("研究包时点晚于开发截止")
    for node in spec["graph"]["nodes"]:
        operator = node["operator_id"].lower()
        allowed_models = {"research.model.split-manifest", "research.model.fit",
                          "research.model.predict", "research.model.fold-metrics"}
        if ("holdout" in operator or "locked-test" in operator or
                (operator.startswith("research.model.") and operator not in allowed_models)):
            raise ValueError("开发候选不能执行test/holdout模型节点:" + node["operator_id"])
        if operator in allowed_models and node["operator_version"] != "2.0.0":
            raise ValueError("开发模型节点版本未经确认")
        if operator == "research.model.split-manifest" and node["parameters"].get("evaluation_scope") != "development":
            raise ValueError("开发模型必须显式声明evaluation_scope=development")
    for query in spec["requests"]:
        _check_range(query["time_range"], development)


def _check_range(span, development):
    end = date.fromisoformat(development["end"])
    cutoff = _time(development["as_of"])
    if "end_at" in span:
        finish = _time(span["end_at"])
        if finish > cutoff or (finish - timedelta(microseconds=1)).date() > end:
            raise ValueError("归档或查询超出开发截止")
    else:
        finish = span.get("end") or span.get("end_date")
        if finish is None or date.fromisoformat(finish) > end:
            raise ValueError("归档或查询日期范围无效")


class PackageCampaign(Campaign):
    def validate_payload(self, payload):
        return validate_package_campaign(payload)

    def prepare_data(self, data_loader=None):
        from research_pipeline.packages import load_research_package
        from research_pipeline.packages.store import PACKAGE_FILES
        from research_pipeline.packages.variants import expand_research_package_variants, VARIANT_SET_VERSION
        from research_pipeline.data_plane.archived_inputs import load_archived_input_manifest
        from research_pipeline.data_plane import PathRolePolicy, resolve_as_of_cutoff
        source = self.payload["source"]
        package = load_research_package(source["path"])
        _check_development_package(package, self.payload["development"])
        archived = load_archived_input_manifest(source["input_snapshot_manifest"])
        inputs = {key: source[key] for key in ("path", "source_archive_root", "input_snapshot_manifest", "catalog_lock", "verifier_bundle")}
        inputs.update({"bundle_" + str(i): path for i, path in enumerate(source["extension_bundles"])})
        inputs.update({"archive_" + key: entry["root"] for key, entry in archived["requests"].items()})
        PathRolePolicy().validate({"campaign_output": str(self.root), **inputs}, read_only_roles=tuple(inputs))
        for entry in archived["requests"].values():
            query = entry["original_plan"]["query"]
            _check_range(query["time_range"], self.payload["development"])
            if resolve_as_of_cutoff(query["as_of"], reference_clock=_time(self.payload["development"]["as_of"]), field="archive.as_of") > _time(self.payload["development"]["as_of"]):
                raise ValueError("输入归档可见时点晚于开发截止")
        files = [Path(source["path"]) / name for name in PACKAGE_FILES]
        files.append(Path(source["input_snapshot_manifest"]))
        lock = Path(source["catalog_lock"])
        current = lock / "CURRENT"
        files.append(current)
        active = lock / current.read_text(encoding="utf-8").strip()
        if not active.is_dir():
            active = lock / "versions" / current.read_text(encoding="utf-8").strip()
        files.extend(path for path in active.rglob("*") if path.is_file())
        for bundle in [source["verifier_bundle"], *source["extension_bundles"]]:
            files.extend(path for path in Path(bundle).rglob("*") if path.is_file() and path.suffix in {".json", ".py", ".yaml"})
        facts = {str(path): path.read_text(encoding="utf-8") for path in files}
        identity = {"files": facts, "archived_inputs": archived, "payload": self.payload}
        receipt = self.root / "package-inputs.json"
        if receipt.exists() and json.loads(receipt.read_text(encoding="utf-8")) != identity:
            raise ValueError("冻结研究包或输入合同已改变")
        if not receipt.exists():
            write_json(receipt, identity)
        variants = [{"variant_id": c["id"], "package_slug": "candidate_" + c["id"],
                     "display_name": package.display_name + " / " + c["id"], "package_version": package.package_version,
                     "graph_id": "candidate_" + c["id"], "parameter_overrides": c["parameter_overrides"]}
                    for c in self.payload["candidates"]]
        manifest = {"contract_version": VARIANT_SET_VERSION, "base_package_hash": package.package_hash, "variants": variants}
        manifest_path = self.root / "variants.json"
        if not manifest_path.exists():
            write_json(manifest_path, manifest)
        elif json.loads(manifest_path.read_text(encoding="utf-8")) != manifest:
            raise ValueError("冻结变体清单已改变")
        generated = self.root / "packages"
        if not generated.exists():
            pending = generated.with_name("." + generated.name + ".tmp")
            if pending.exists():
                if not pending.resolve().is_relative_to(self.root.resolve()):
                    raise ValueError("中断的变体目录不属于当前会话")
                retained = self.root / "interrupted-variants"
                retained.mkdir(exist_ok=True)
                pending.rename(retained / ("attempt-" + str(len(tuple(retained.iterdir())))))
            expand_research_package_variants(base_root=source["path"], manifest_path=manifest_path, output_root=generated)
        packages = {}
        for c in self.payload["candidates"]:
            root = generated / ("candidate_" + c["id"])
            item = load_research_package(root)
            _check_development_package(item, self.payload["development"])
            packages[c["id"]] = {"path": str(root), "package_hash": item.package_hash}
        return {"provenance": {"kind": "research_package", "base_package_hash": package.package_hash}, "packages": packages}

    def fixed_proposal(self, history):
        successes = [row for row in history if row["status"] == "evaluated"]
        best = min(successes, key=lambda row: self.loss(row["metrics"])) if successes else None
        tried = {row.get("candidate_id") for row in history}
        remaining = [c for c in self.payload["candidates"] if c["id"] not in tried]
        if not remaining:
            return {"action": "stop", "reason": "允许候选已用尽"}
        chosen = next(c for c in remaining if c["id"] == self.payload["baseline_id"]) if not history else remaining[0]
        return {"action": "evaluate", "candidate_id": chosen["id"], "parent_id": best["candidate_id"] if best else None,
                "reason": "评价冻结基准" if not history else "以当前开发目标最优候选为父节点检验下一参数变体"}

    def prompt(self):
        facts = {"question": "比较已声明研究包参数变体的独立验证开发结果", "objective": self.payload["objective"],
                 "development": self.payload["development"], "candidates": self.payload["candidates"],
                 "baseline_id": self.payload["baseline_id"], "history": []}
        for row in self.history():
            item = {key: row[key] for key in ("round", "candidate_id", "status", "parent_id", "improvement") if key in row}
            if row["status"] == "evaluated":
                item["value"] = row["metrics"]["value"]
            facts["history"].append(item)
        return json.dumps(facts, ensure_ascii=False, allow_nan=False)

    def evaluate_metrics(self, candidate):
        from .package_execution import execute_package
        if self.prepare_data() != self.data:
            raise ValueError("冻结变体包发生变化")
        source = self.payload["source"]
        result = execute_package(root=self.root / "candidates" / candidate["id"],
            workspace_id=self.payload["campaign_id"] + "-" + candidate["id"], allocation_label=candidate["id"],
            base_package=self.data["packages"][candidate["id"]]["path"], source_archive_root=source["source_archive_root"],
            input_snapshot_manifest=source["input_snapshot_manifest"], verifier_bundle=source["verifier_bundle"], binding=source)
        return project_development_metric(result, self.payload)

    def loss(self, metrics):
        return metrics["value"] * (1 if self.payload["objective"]["direction"] == "minimize" else -1)

    def target_reached(self, successes):
        target = self.payload["stop"]["target_value"]
        return bool(successes and target is not None and min(self.loss(row["metrics"]) for row in successes) <= self.loss({"value": target}))

    def outcome_fields(self, best):
        return {"scope": "development_package_research", "new_formal_result": bool(best),
                "selected_development_value": best["metrics"]["value"] if best else None,
                "selected_result_ref": best["metrics"]["result_ref"] if best else None,
                "selected_verification_ref": best["metrics"]["verification_ref"] if best else None}


def project_development_metric(result, payload):
    from research_pipeline.evidence import load_verified_result_context
    objective, scope, budget = payload["objective"], payload["development"], payload["budget"]
    context = load_verified_result_context(result["verification_ref"], result_store=Path(result["execution_ref"]) / "results",
                                           additional_table_ids=(objective["table_id"],))
    if context.verification.status != "pass":
        raise ValueError("候选未通过独立验证")
    tables = [table for table in context.snapshot.bundle.tables if table.table_id == objective["table_id"]]
    if len(tables) != 1 or tables[0].schema_id != objective["schema_id"]:
        raise ValueError("开发目标与正式结果表绑定不符")
    columns = {objective[key] for key in ("value_column", "date_column", "availability_column")}
    columns.update(objective["filters"])
    if objective["stage_column"]:
        columns.add(objective["stage_column"])
    if not columns <= set(context.snapshot.table_schema(objective["schema_id"]).names):
        raise ValueError("开发目标列不存在")
    values, row_count = [], 0
    for batch in context.snapshot.iter_table_batches(objective["schema_id"], columns=tuple(sorted(columns)), batch_size=min(1024, budget["max_rows"] + 1)):
        if batch.nbytes * 8 > budget["memory_bytes"]:
            raise ValueError("开发目标读取超过内存预算")
        for row in batch.to_pylist():
            row_count += 1
            if row_count > budget["max_rows"]:
                raise ValueError("开发目标读取超过行数预算")
            if objective["stage_column"] and row[objective["stage_column"]] != "validation":
                raise ValueError("开发目标包含非validation记录")
            day = str(row[objective["date_column"]])
            date.fromisoformat(day)
            available = row[objective["availability_column"]]
            when = available if isinstance(available, datetime) else _time(available)
            if when.tzinfo is None or when > _time(scope["as_of"]) or day > scope["end"]:
                raise ValueError("开发目标含未来记录")
            if day < scope["start"] or any(row[key] != value for key, value in objective["filters"].items()):
                continue
            number = row[objective["value_column"]]
            if type(number) not in (int, float) or not math.isfinite(number):
                raise ValueError("开发目标必须为有限数值")
            if (len(values) + 1) * 32 + batch.nbytes * 8 > budget["memory_bytes"]:
                raise ValueError("开发目标汇总超过内存预算")
            values.append(float(number))
    if not values or (objective["reduction"] == "single" and len(values) != 1):
        raise ValueError("开发目标样本数量不符合声明")
    return {"value": fmean(values), "rows": len(values), "result_id": context.snapshot.bundle.result_id,
            "result_ref": result["result_ref"], "verification_ref": result["verification_ref"], "verification_status": "pass"}
