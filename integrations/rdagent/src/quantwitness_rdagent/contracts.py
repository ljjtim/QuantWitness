"""冻结请求、离线预算和两侧路径映射。"""
from dataclasses import dataclass
import json
import os
from pathlib import Path


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    temporary.replace(path)


@dataclass(frozen=True)
class FrozenRequest:
    payload: dict

    @classmethod
    def load(cls, path):
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))

    @classmethod
    def from_dict(cls, payload):
        fields = {"request_id", "mode", "base_package", "source_archive_root", "input_snapshot_manifest",
                  "editable_source_root", "reference_bundle", "development_scope", "runtime_binding", "budget"}
        optional = {"code_generation", "confirmed_formula", "formula_evaluation", "coding_knowledge"}
        if not isinstance(payload, dict) or not fields <= set(payload) or set(payload) - fields - optional:
            raise ValueError("冻结请求字段必须完整且无额外字段")
        if payload["mode"] != "formula_reproduction":
            raise ValueError("首批只支持 formula_reproduction")
        rid = payload["request_id"]
        if not isinstance(rid, str) or not rid or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in rid):
            raise ValueError("request_id 必须是简单稳定标识")
        confirmed = payload.get("confirmed_formula")
        if "confirmed_formula" in payload:
            if (not isinstance(confirmed, dict) or set(confirmed) != {"formula", "interface"}
                    or any(not isinstance(value, str) or not value.strip() for value in confirmed.values())):
                raise ValueError("confirmed_formula必须包含完整公式与接口")
        knowledge = payload.get("coding_knowledge")
        if "coding_knowledge" in payload:
            if (not isinstance(knowledge, dict)
                    or not {"max_records", "max_source_chars"} <= set(knowledge)
                    or set(knowledge) - {"source_session", "retrieval_scope", "max_records", "max_source_chars"}
                    or confirmed is None):
                raise ValueError("coding_knowledge需要确认公式、记录数与代码长度上限")
            if (type(knowledge["max_records"]) is not int or not 1 <= knowledge["max_records"] <= 6
                    or type(knowledge["max_source_chars"]) is not int
                    or not 1 <= knowledge["max_source_chars"] <= 60000):
                raise ValueError("编码知识预算无效")
            if (not isinstance(knowledge.get("retrieval_scope", "exact_formula"), str)
                    or knowledge.get("retrieval_scope", "exact_formula") not in {"exact_formula", "technical_transfer"}):
                raise ValueError("编码知识检索范围无效")
            if "source_session" in knowledge and (not isinstance(knowledge["source_session"], str)
                    or not knowledge["source_session"].strip()):
                raise ValueError("编码知识来源会话路径无效")
        evaluation = payload.get("formula_evaluation")
        if "formula_evaluation" in payload:
            if (not isinstance(evaluation, dict)
                    or set(evaluation) != {"verifier_id", "verifier_version", "coverage_schema_id"}
                    or any(not isinstance(value, str) or not value.strip() for value in evaluation.values())):
                raise ValueError("formula_evaluation必须绑定验证器身份和覆盖表schema")
        live = "code_generation" in payload
        budget = payload["budget"]
        base_budget = {"outer_loops": 1, "coder_attempts": 3, "parallel": 1}
        if not isinstance(budget, dict) or any(type(budget.get(k)) is not int or budget[k] != v for k, v in base_budget.items()):
            raise ValueError("预算固定为单循环、三次编码、串行")
        if live:
            spec = payload["code_generation"]
            keys = {"mode", "model", "base_url", "interface", "formula", "initial_stub", "max_output_tokens_per_call"}
            if not isinstance(spec, dict) or set(spec) not in (keys, keys | {"initial_source"}) or spec["mode"] != "live":
                raise ValueError("code_generation 必须声明live模式、固定模型与完整接口")
            if any(not isinstance(spec[key], str) or not spec[key].strip() for key in ("model", "base_url", "interface", "formula", "initial_stub")):
                raise ValueError("代码接口、公式与初始stub必须非空")
            if confirmed is not None and any(spec[key] != confirmed[key] for key in ("formula", "interface")):
                raise ValueError("生成公式与已确认冻结文本不一致")
            from urllib.parse import urlsplit
            endpoint = urlsplit(spec["base_url"])
            if endpoint.scheme != "https" or not endpoint.netloc or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment:
                raise ValueError("base_url必须为不含凭据的HTTPS接口地址")
            if set(budget) != {*base_budget, "live_llm_calls", "max_output_tokens"}:
                raise ValueError("live预算必须包含调用数和总输出token预约额度")
            if type(budget["live_llm_calls"]) is not int or not 1 <= budget["live_llm_calls"] <= 3:
                raise ValueError("live模型调用必须为1至3次")
            if (type(budget["max_output_tokens"]) is not int or type(spec["max_output_tokens_per_call"]) is not int
                    or not 1 <= spec["max_output_tokens_per_call"] <= min(8192, budget["max_output_tokens"])):
                raise ValueError("输出token预算必须为正整数且单次不超过8192与总额")
            from .generation import validate_initial_stub
            validate_initial_stub(spec["initial_stub"])
            if "initial_source" in spec:
                from .generation import validate_generated_source
                if not isinstance(spec["initial_source"], str) or not spec["initial_source"].strip():
                    raise ValueError("initial_source必须为用户提供的待修复源码")
                validate_generated_source(spec["initial_source"])
        elif budget != {**base_budget, "live_llm_calls": 0}:
            raise ValueError("固定响应预算必须为零实时调用")
        binding = payload["runtime_binding"]
        required = {"windows_python", "windows_repo", "linux_repo", "windows_session_root", "linux_session_root",
                    "operator_spec", "catalog_lock", "extension_bundles", "fixed_responses", "runtime_options"}
        if not isinstance(binding, dict) or set(binding) != required:
            raise ValueError("runtime_binding 字段不完整")
        responses = binding["fixed_responses"]
        if live:
            if responses != []:
                raise ValueError("live模式不接收预设答案")
        elif not isinstance(responses, list) or not 1 <= len(responses) <= 3 or any(not isinstance(x, str) for x in responses):
            raise ValueError("fixed_responses 必须声明一至三份 compute.py 文本")
        scope = payload["development_scope"]
        if not isinstance(scope, dict) or not scope or scope.get("role") != "development":
            raise ValueError("只允许冻结 development 评价范围")
        return cls(json.loads(json.dumps(payload)))

    @property
    def session_root(self):
        return Path(self.payload["runtime_binding"]["linux_session_root"])

    def _local_path(self, value):
        if os.name != "nt" and len(value) > 2 and value[1] == ":":
            return Path("/mnt/" + value[0].lower() + value[2:].replace("\\", "/"))
        return Path(value.replace("\\", "/"))

    def _materials(self):
        binding = self.payload["runtime_binding"]
        selected = {}
        package = self._local_path(self.payload["base_package"])
        for relative in ("package.yaml", "localization.yaml", "sources/sources.yaml", "spec/research.yaml"):
            selected["package/" + relative] = (package / relative).read_text(encoding="utf-8")
        source = self._local_path(self.payload["editable_source_root"])
        fixed = sorted(path for path in source.rglob("*.py") if path.relative_to(source).as_posix() != "compute.py")
        if not fixed:
            raise ValueError("可编辑源码目录缺少固定 adapter")
        for path in fixed:
            selected["source/" + path.relative_to(source).as_posix()] = path.read_text(encoding="utf-8")
        for name, value in (("operator_spec", binding["operator_spec"]),
                            ("input_snapshot_manifest", self.payload["input_snapshot_manifest"])):
            selected[name] = self._local_path(value).read_text(encoding="utf-8")
        catalog = self._local_path(binding["catalog_lock"])
        current = (catalog / "CURRENT").read_text(encoding="utf-8")
        selected["catalog/CURRENT"] = current
        for relative in ("catalog.lock.json", "catalog.source-manifest.json", "catalog.audit.json", "catalog.schema.json"):
            selected["catalog/" + relative] = (catalog / current.strip() / relative).read_text(encoding="utf-8")
        manifest_path = self._local_path(self.payload["input_snapshot_manifest"])
        manifest = json.loads(selected["input_snapshot_manifest"])
        for request_id, entry in sorted(manifest.get("requests", {}).items()):
            original = entry.get("original_plan")
            if isinstance(original, str):
                original_path = self._local_path(original)
                if not original_path.is_absolute():
                    original_path = manifest_path.parent / original_path
                selected["original_plans/" + request_id] = original_path.read_text(encoding="utf-8")
        for index, value in enumerate([self.payload["reference_bundle"], *binding["extension_bundles"]]):
            root = self._local_path(value)
            for relative in ("manifest.json", "COMMITTED"):
                selected[f"bundles/{index}/{relative}"] = (root / relative).read_text(encoding="utf-8")
        return selected

    def freeze(self):
        output = self.session_root.resolve()
        manifest_path = self._local_path(self.payload["input_snapshot_manifest"])
        archive = json.loads(manifest_path.read_text(encoding="utf-8"))
        source_roots = [self._local_path(self.payload[key]).resolve() for key in
                        ("base_package", "source_archive_root", "editable_source_root", "reference_bundle")]
        for entry in archive.get("requests", {}).values():
            root = self._local_path(entry["root"])
            source_roots.append((root if root.is_absolute() else manifest_path.parent / root).resolve())
        if any(output.is_relative_to(root) or root.is_relative_to(output) for root in source_roots):
            raise ValueError("会话输出不能与只读来源目录重叠")
        source_session = self.payload.get("coding_knowledge", {}).get("source_session")
        if source_session:
            knowledge_input = self._local_path(source_session).resolve()
            if output.is_relative_to(knowledge_input) or knowledge_input.is_relative_to(output):
                raise ValueError("编码知识来源与当前会话不能重叠")
        materials = self._materials()
        material_path = self.session_root / "frozen-inputs.json"
        if material_path.exists():
            if json.loads(material_path.read_text(encoding="utf-8")) != materials:
                raise ValueError("冻结研究包、固定源码或工件身份已改变；须新会话")
        else:
            write_json(material_path, materials)
        path = self.session_root / "request.json"
        if path.exists():
            if json.loads(path.read_text(encoding="utf-8")) != self.payload:
                raise ValueError("现有会话请求已冻结；改变研究事实须新会话")
        else:
            write_json(path, self.payload)
        return path
