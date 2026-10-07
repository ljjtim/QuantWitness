"""模型实现的跨会话技术经验；正式来源由模型主链接口核验。"""
import json
from pathlib import Path
import re

from .contracts import write_json
from .coding_transfer import formal_refs, model_needs_repair, prompt_record, select_records, technical_feedback


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _task(value):
    if (not isinstance(value, dict) or not {"interface", "model_family"} <= set(value) or set(value) - {"interface", "model_family", "definition"}
            or any(not isinstance(value[key], str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.@-]{0,159}", value[key])
                   for key in ("interface", "model_family"))):
        raise ValueError("模型知识任务必须声明接口合同与模型族标识")
    definition = value.get("definition")
    if definition is not None:
        if (not isinstance(definition, dict) or set(definition) != {"nodes"}
                or not isinstance(definition["nodes"], list)
                or any(not isinstance(node, dict) or set(node) != {"inputs", "width", "activation"}
                       or not isinstance(node["inputs"], list) or any(type(item) is not int for item in node["inputs"])
                       or type(node["width"]) is not int or node["activation"] not in {"identity", "relu", "tanh", "gelu"}
                       for node in definition["nodes"])):
            raise ValueError("模型知识仅接受nodes的输入、宽度与激活定义")
    return json.loads(json.dumps({"kind": "model", **value}, allow_nan=False))


class ModelCodingKnowledge:
    """冻结来源与每轮提示，不在经验模块中执行模型或读取金融表。"""
    def __init__(self, session_root, *, task, source_sessions=(), max_records=4, max_source_chars=30000,
                 validate_source=None, source_template=""):
        self.task = _task(task)
        if (type(max_records) is not int or not 1 <= max_records <= 6
                or type(max_source_chars) is not int or not 1 <= max_source_chars <= 60000):
            raise ValueError("模型编码知识预算无效")
        self.session_root = Path(session_root).resolve()
        self.session_id = (self.session_root.parent.name + "/session" if self.session_root.name == "session" else self.session_root.name)
        self.root = self.session_root / "model-coding-knowledge"
        self.sources = [Path(source).resolve() for source in source_sessions]
        if any(self.session_root.is_relative_to(source) or source.is_relative_to(self.session_root) for source in self.sources):
            raise ValueError("模型知识来源与当前会话不能重叠")
        if self.sources and validate_source is None:
            raise ValueError("模型知识来源必须由正式模型验证接口核验")
        self.validate_source = validate_source
        self.max_records, self.max_source_chars = max_records, max_source_chars
        self.source_template = source_template
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "records").mkdir(exist_ok=True)
        self.input_path = self.root / "input.json"
        declaration = {"contract_version": "model-coding-knowledge-v1", "task": self.task,
                       "source_sessions": [str(source) for source in self.sources],
                       "max_records": max_records, "max_source_chars": max_source_chars,
                       "source_template": source_template}
        if self.input_path.exists():
            frozen = _read(self.input_path)
            if frozen["declaration"] != declaration:
                raise ValueError("冻结模型编码知识合同改变；须新会话")
            self.records = frozen["records"]
        else:
            self.records = []
            for source in self.sources:
                source_root = source / "model-coding-knowledge"
                records = [_read(path) for path in sorted((source_root / "records").glob("*.json"))]
                records.sort(key=lambda record: record.get("sequence", 0))
                successes = [record for record in records if record["success"]]
                if not successes:
                    raise ValueError("模型知识来源缺少正式验证通过的修复实现")
                source_declaration = _read(source_root / "input.json")["declaration"]
                for record in records:
                    if any(record["task"].get(key) != source_declaration["task"].get(key) for key in ("interface", "model_family")):
                        raise ValueError("模型知识来源候选与冻结任务不一致")
                    self.validate_source(source, record)
                self.records.extend(records)
            write_json(self.input_path, {"declaration": declaration, "records": self.records})

    def record(self, *, candidate_id, source, evidence, task=None):
        if not isinstance(candidate_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", candidate_id):
            raise ValueError("模型知识候选标识无效")
        success = all(evidence.get(key) == status for key, status in (
            ("command_status", "succeeded"), ("execution_status", "succeeded"),
            ("verification_status", "pass"), ("model_status", "pass")))
        if not success and not model_needs_repair(evidence):
            return None
        refs = formal_refs(evidence)
        if success and set(refs) != {"execution_ref", "result_ref", "verification_ref", "bundle_ref"}:
            raise ValueError("成功模型编码知识必须保存完整正式来源引用")
        record_task = self.task if task is None else _task(task)
        if record_task["interface"] != self.task["interface"]:
            raise ValueError("候选模型接口与会话声明不一致")
        path = self.root / "records" / (candidate_id + ".json")
        sequence = _read(path)["sequence"] if path.exists() else len(tuple((self.root / "records").glob("*.json")))
        record = {"record_id": self.session_id + "/" + candidate_id, "task": record_task, "sequence": sequence,
                  "source": source, "technical_feedback": technical_feedback(evidence),
                  "success": success, "formal_refs": refs}
        if candidate_id.startswith("model_"):
            record["repair_group"] = self.session_id + "/" + candidate_id.split("_attempt_", 1)[0]
        if path.exists():
            if _read(path) != record:
                raise ValueError("模型编码知识候选内容改变；须使用新候选标识")
        else:
            write_json(path, record)
        return record

    def query(self, attempt, *, task=None, source_template=None):
        if type(attempt) is not int or attempt < 0:
            raise ValueError("模型编码尝试序号无效")
        target = self.task if task is None else _task(task)
        source = self.source_template if source_template is None else source_template
        if target["interface"] != self.task["interface"]:
            raise ValueError("查询模型接口与会话声明不一致")
        path = self.root / ("query-%04d.json" % attempt)
        if path.exists():
            frozen = _read(path)
            if frozen["task"] != target or frozen["source_template"] != source:
                raise ValueError("该轮模型知识查询目标改变；须使用新尝试序号")
            return frozen["records"]
        current = [_read(item) for item in sorted((self.root / "records").glob("*.json"))]
        current.sort(key=lambda record: record.get("sequence", 0))
        pending, repaired = [], set()
        for record in reversed(current):
            group = record.get("repair_group", json.dumps(record["task"], sort_keys=True))
            if record["success"]:
                repaired.add(group)
            elif group not in repaired:
                view = prompt_record(record, target, source)
                if view is not None:
                    view["match_reason"]["kind"] = "current_failure"
                    pending.append(view)
        used, records = 0, []
        for view in pending:
            if len(records) < self.max_records and used + len(view["source"]) <= self.max_source_chars:
                records.append(view)
                used += len(view["source"])
        if len(records) < self.max_records and used < self.max_source_chars:
            records.extend(select_records(self.records + current, target, source,
                max_records=self.max_records - len(records), max_source_chars=self.max_source_chars - used))
        write_json(path, {"task": target, "source_template": source, "records": records})
        return records


def validate_generated_model_source(source_session, record):
    """把来源节点定义与源码绑定到开发 Result 和独立验证。"""
    source_session = Path(source_session)
    candidate_id = record["record_id"].rsplit("/", 1)[1]
    persisted = _read(source_session / "model-coding-knowledge" / "records" / (candidate_id + ".json"))
    if persisted != record:
        raise ValueError("模型知识与实际来源候选记录不一致")
    if not record["success"]:
        if not record["technical_feedback"].get("diagnostic_codes"):
            raise ValueError("失败模型知识缺少实际技术诊断")
        return
    if record["task"]["model_family"] != "GeneratedModel" or "definition" not in record["task"]:
        raise ValueError("正式生成模型知识必须绑定节点定义")
    refs = record["formal_refs"]
    source_file = Path(refs["bundle_ref"])
    if source_file.read_text(encoding="utf-8") != record["source"]:
        raise ValueError("模型知识源码与实际候选源码不一致")
    from .model_research_evidence import _formal_network
    source, result_id = _formal_network(refs["execution_ref"], refs, record["task"]["definition"])
    if source != record["source"]:
        raise ValueError("模型知识源码与正式封存网络不一致")
    return {"result_id": result_id}
