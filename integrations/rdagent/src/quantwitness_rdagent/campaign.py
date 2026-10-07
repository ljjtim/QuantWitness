"""已验证开发预测上的有限校准研究；过程收据不替代正式Result。"""
from __future__ import annotations

from datetime import date, datetime
import json
import math
from pathlib import Path
from statistics import fmean

from .contracts import write_json


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _positive(value):
    return type(value) is int and value > 0


def validate_campaign(value):
    fields = {"campaign_id", "session_root", "source", "development", "candidates", "baseline_id", "budget", "stop", "proposer"}
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError("研究会话字段必须完整且无额外字段")
    request = json.loads(json.dumps(value, allow_nan=False))
    if not isinstance(request["campaign_id"], str) or not request["campaign_id"].strip():
        raise ValueError("campaign_id必须非空")
    if not isinstance(request["session_root"], str) or not request["session_root"]:
        raise ValueError("session_root必须显式提供")
    development = request["development"]
    if set(development) != {"start", "end", "as_of", "fold_ids", "horizon_sessions"}:
        raise ValueError("开发范围必须明确日期、研究时点、fold与标签周期")
    if date.fromisoformat(development["start"]) > date.fromisoformat(development["end"]):
        raise ValueError("开发日期顺序错误")
    if datetime.fromisoformat(development["as_of"]).tzinfo is None:
        raise ValueError("研究时点必须带时区")
    folds = development["fold_ids"]
    if (not isinstance(folds, list) or not folds or any(not isinstance(x, str) or not x or x == "*" for x in folds)
            or len(set(folds)) != len(folds) or not _positive(development["horizon_sessions"])):
        raise ValueError("fold必须明确唯一，标签周期必须为正整数")
    candidates = request["candidates"]
    if not isinstance(candidates, list) or not candidates:
        raise ValueError("研究必须预先声明有限候选")
    ids = []
    for item in candidates:
        if (not isinstance(item, dict) or set(item) != {"id", "model_candidate_id", "shrinkage"}
                or any(not isinstance(item[k], str) or not item[k] for k in ("id", "model_candidate_id"))
                or type(item["shrinkage"]) not in (int, float) or not 0 <= item["shrinkage"] <= 1):
            raise ValueError("候选只允许声明模型ID和零至一的收缩系数")
        ids.append(item["id"])
    if (len(ids) != len(set(ids)) or request["baseline_id"] not in ids
            or len({(x["model_candidate_id"], x["shrinkage"]) for x in candidates}) != len(ids)):
        raise ValueError("候选须唯一且包含baseline")
    budget = request["budget"]
    if set(budget) != {"rounds", "evaluations", "model_calls", "output_tokens", "max_output_tokens_per_call", "max_rows", "memory_bytes"}:
        raise ValueError("跨轮预算字段不完整")
    if any(not _positive(budget[k]) for k in ("rounds", "evaluations", "max_rows", "memory_bytes")):
        raise ValueError("轮数、评价与读取预算必须为正整数")
    if any(type(budget[k]) is not int or budget[k] < 0 for k in ("model_calls", "output_tokens", "max_output_tokens_per_call")):
        raise ValueError("模型预算必须为非负整数")
    stop = request["stop"]
    if set(stop) != {"target_mse", "min_improvement", "patience"} or not _positive(stop["patience"]):
        raise ValueError("停止条件须包含目标误差、最小改进和耐心轮数")
    for name in ("target_mse", "min_improvement"):
        if name == "target_mse" and stop[name] is None:
            continue
        if type(stop[name]) not in (int, float) or not math.isfinite(stop[name]) or stop[name] < 0:
            raise ValueError("停止阈值必须为非负有限数")
    proposer = request["proposer"]
    if proposer == {"mode": "fixed_policy"}:
        if any(budget[k] for k in ("model_calls", "output_tokens", "max_output_tokens_per_call")):
            raise ValueError("固定反馈策略必须使用零模型预算")
    elif (set(proposer) == {"mode", "model", "base_url"} and proposer["mode"] == "live"):
        from urllib.parse import urlsplit
        endpoint = urlsplit(proposer["base_url"])
        if (not proposer["model"] or endpoint.scheme != "https" or not endpoint.netloc
                or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment):
            raise ValueError("live模型身份或HTTPS地址无效")
        if (not _positive(budget["model_calls"]) or not 1 <= budget["max_output_tokens_per_call"] <= min(8192, budget["output_tokens"])):
            raise ValueError("live调用及输出预约预算无效")
    else:
        raise ValueError("proposer只支持fixed_policy或live")
    return request


def evaluate_candidate(rows, candidate, folds):
    """各fold分别计算MSE后等权平均；系数只改变预测，不改变原标签。"""
    selected = [r for r in rows if r["candidate_id"] == candidate["model_candidate_id"]]
    metrics = []
    for fold in folds:
        part = [r for r in selected if r["fold_id"] == fold]
        if not part:
            raise ValueError("候选缺少冻结fold的预测")
        losses = [(candidate["shrinkage"] * r["prediction"] - r["actual"]) ** 2 for r in part]
        metric = fmean(losses)
        if not math.isfinite(metric):
            raise ValueError("开发误差不可计算")
        metrics.append({"fold_id": fold, "mse": metric, "sample_count": len(part)})
    return {"mse": fmean(x["mse"] for x in metrics), "sample_count": len(selected), "folds": metrics}


def _aligned(rows, candidates, folds):
    baseline = None
    for model in sorted({c["model_candidate_id"] for c in candidates}):
        selected = [r for r in rows if r["candidate_id"] == model]
        indexed = {(r["fold_id"], r["sample_id"]): (r["entity_id"], r["observation_session"], r["actual"]) for r in selected}
        if not indexed or len(indexed) != len(selected) or {r["fold_id"] for r in selected} != set(folds):
            raise ValueError("候选开发样本缺失、重复或fold不完整")
        if baseline is not None and baseline != indexed:
            raise ValueError("候选必须使用完全相同的开发样本与标签")
        baseline = indexed


def fixed_proposal(candidates, history, baseline_id):
    """反馈改善时沿当前方向探索；未改善时切换剩余候选方向。"""
    if not history:
        return {"action": "evaluate", "candidate_id": baseline_id, "parent_id": None, "reason": "评估预先声明的基准"}
    tried = {r.get("candidate_id") for r in history}
    remaining = [c for c in candidates if c["id"] not in tried]
    if not remaining:
        return {"action": "stop", "reason": "允许候选已用尽"}
    succeeded = [r for r in history if r["status"] == "evaluated"]
    best = min(succeeded, key=lambda x: x["metrics"]["mse"]) if succeeded else None
    ordered = sorted(remaining, key=lambda x: (x["shrinkage"], x["id"]))
    improved = bool(best and (len(succeeded) == 1 or succeeded[-1].get("improvement", 0) > 0))
    chosen = ordered[0] if improved else ordered[-1]
    return {"action": "evaluate", "candidate_id": chosen["id"], "parent_id": best["candidate_id"] if best else None,
            "reason": "根据开发误差改进继续检验较强收缩" if improved else "开发误差未改善，切换剩余候选方向"}


class Campaign:
    def __init__(self, payload, *, data_loader=None):
        self.payload = self.validate_payload(payload)
        self.root = Path(self.payload["session_root"])
        source = self.payload["source"]
        for key in ("path", "result_store", "verification_result"):
            if source.get(key):
                original = Path(source[key]).resolve()
                output = self.root.resolve()
                if original == output or original.is_relative_to(output) or output.is_relative_to(original):
                    raise ValueError("研究输出不能与来源重叠")
        frozen = self.root / "request.json"
        if frozen.exists() and _read(frozen) != self.payload:
            raise ValueError("研究请求已冻结，恢复不能改变预算或研究范围")
        self.data = self.prepare_data(data_loader)
        inputs = self.root / "development.json"
        if inputs.exists() and _read(inputs) != self.data:
            raise ValueError("冻结开发输入发生变化")
        if not frozen.exists():
            write_json(frozen, self.payload)
        if not inputs.exists():
            write_json(inputs, self.data)

    def validate_payload(self, payload):
        return validate_campaign(payload)

    def prepare_data(self, data_loader=None):
        if data_loader is None:
            from .campaign_data import load_development
            data_loader = load_development
        data = data_loader(self.payload)
        _aligned(data["rows"], self.payload["candidates"], self.payload["development"]["fold_ids"])
        return data

    def fixed_proposal(self, history):
        return fixed_proposal(self.payload["candidates"], history, self.payload["baseline_id"])

    def evaluate_metrics(self, candidate):
        return evaluate_candidate(self.data["rows"], candidate, self.payload["development"]["fold_ids"])

    def loss(self, metrics):
        return metrics["mse"]

    def target_reached(self, successes):
        target = self.payload["stop"]["target_mse"]
        return bool(successes and target is not None and min(self.loss(r["metrics"]) for r in successes) <= target)

    def outcome_fields(self, best):
        return {"scope": "development_prediction_calibration",
                "selected_development_mse": best["metrics"]["mse"] if best else None,
                "new_formal_result": False}

    def history(self):
        return [_read(p) for p in sorted((self.root / "rounds").glob("*/record.json"))]

    def stop_reason(self):
        history = self.history()
        success = [r for r in history if r["status"] == "evaluated"]
        if history and history[-1]["status"] == "stopped":
            return "model_budget" if history[-1]["reason"] == "model_budget" else "proposer_stop"
        if self.target_reached(success):
            return "target_reached"
        if len(history) >= self.payload["budget"]["rounds"]:
            return "round_budget"
        if sum(bool(_read(p).get("evaluation_reserved")) for p in (self.root / "rounds").glob("*/evaluation.json")) >= self.payload["budget"]["evaluations"]:
            return "evaluation_budget"
        if {c["id"] for c in self.payload["candidates"]} <= {r.get("candidate_id") for r in history}:
            return "candidates_exhausted"
        patience = self.payload["stop"]["patience"]
        if len(history) > patience and all(r.get("improvement", 0) <= self.payload["stop"]["min_improvement"] for r in history[-patience:]):
            return "no_improvement"
        return None

    def prompt(self):
        history = self.history()
        facts = {"question": "比较预声明模型及向零收缩预测的开发区误差", "objective": "per_fold_mean_squared_error_then_equal_mean",
                 "candidates": self.payload["candidates"], "baseline_id": self.payload["baseline_id"],
                 "history": [{k: r[k] for k in ("round", "candidate_id", "parent_id", "status", "metrics", "improvement") if k in r} for r in history]}
        return json.dumps(facts, ensure_ascii=False, allow_nan=False)

    def propose(self, index, env_path=None):
        folder = self.root / "rounds" / f"{index:04d}"
        path = folder / "proposal.json"
        if path.exists():
            return _read(path)
        history = self.history()
        if index != len(history) or self.stop_reason():
            raise ValueError("本轮尚未轮到执行或已达到停止条件")
        if not history:
            proposal = self.fixed_proposal(history)
        elif self.payload["proposer"]["mode"] == "fixed_policy":
            proposal = self.fixed_proposal(history)
        else:
            proposal = self._live(index, env_path)
        write_json(path, proposal)
        return proposal

    def _live(self, index, env_path):
        from .model_client import public_config, request_text
        if env_path is None:
            raise ValueError("live研究必须显式提供.env文件")
        identity = self.payload["proposer"]
        if public_config(env_path) != {"model": identity["model"], "base_url": identity["base_url"].rstrip("/")}:
            raise ValueError(".env模型身份与冻结研究不一致")
        folder = self.root / "model-calls"
        path = folder / f"{index:04d}.json"
        if path.exists():
            receipt = _read(path)
            if receipt["status"] != "completed":
                raise RuntimeError("该模型调用已预约或失败，不自动重复付费")
        else:
            budget = self.payload["budget"]
            calls = [_read(p) for p in folder.glob("*.json")]
            remaining = budget["output_tokens"] - sum(c["reserved_output_tokens"] for c in calls)
            if len(calls) >= budget["model_calls"] or remaining <= 0:
                return {"action": "budget_stop", "reason": "model_budget"}
            maximum = min(remaining, budget["max_output_tokens_per_call"])
            receipt = {"status": "reserved", "reserved_output_tokens": maximum, "prompt": self.prompt(), "round": index}
            write_json(path, receipt)
            try:
                response = request_text(env_path, receipt["prompt"], maximum,
                    instructions='只返回研究JSON。根据已完成轮次的开发指标选择未尝试候选，不改变候选菜单或研究范围。评估格式：{"action":"evaluate","candidate_id":"菜单ID","parent_id":"已评估候选ID或null","reason":"研究理由"}；停止格式：{"action":"stop","reason":"停止理由"}。不得请求test或holdout。')
                if response.get("model") != identity["model"] or not isinstance(response.get("text"), str):
                    raise ValueError("模型响应身份或正文无效")
                usage = response.get("usage") or {}
                if type(usage.get("output_tokens")) is int and usage["output_tokens"] > maximum:
                    raise ValueError("模型返回超过预约输出预算")
                receipt.update(status="completed", text=response["text"], usage={k: v for k, v in usage.items() if k in ("input_tokens", "output_tokens", "total_tokens") and type(v) is int and v >= 0})
                write_json(path, receipt)
            except Exception:
                receipt.update(status="failed", error_code="model_call_failed")
                write_json(path, receipt)
                raise RuntimeError("研究模型调用失败，预算已占用；检查调用收据后处理") from None
        try:
            return json.loads(receipt["text"])
        except ValueError:
            return {"action": "invalid", "reason": "模型未返回有效JSON"}

    def evaluate(self, index, proposal):
        folder = self.root / "rounds" / f"{index:04d}"
        path = folder / "evaluation.json"
        if path.exists():
            receipt = _read(path)
            if receipt["status"] == "reserved":
                pass
            else:
                return receipt
        history = self.history()
        result = {"round": index, "status": "rejected", "reason": "提案字段、父候选或候选范围无效"}
        if not isinstance(proposal, dict):
            write_json(path, result)
            return result
        if proposal.get("action") in ("stop", "budget_stop") and set(proposal) == {"action", "reason"} and isinstance(proposal["reason"], str):
            result.update(status="stopped", reason=proposal["reason"])
        else:
            candidates = {c["id"]: c for c in self.payload["candidates"]}
            tried = {r.get("candidate_id") for r in history}
            parents = {r.get("candidate_id") for r in history if r["status"] == "evaluated"}
            valid = (set(proposal) == {"action", "candidate_id", "parent_id", "reason"} and proposal["action"] == "evaluate"
                     and isinstance(proposal["candidate_id"], str) and proposal["candidate_id"] in candidates
                     and proposal["candidate_id"] not in tried and isinstance(proposal["reason"], str) and proposal["reason"].strip()
                     and (proposal["parent_id"] is None or isinstance(proposal["parent_id"], str) and proposal["parent_id"] in parents))
            if parents and proposal.get("parent_id") is None:
                valid = False
            if valid:
                result.update(candidate_id=proposal["candidate_id"], parent_id=proposal["parent_id"], reason=proposal["reason"], status="reserved", evaluation_reserved=True)
                write_json(path, result)
                try:
                    metric = self.evaluate_metrics(candidates[proposal["candidate_id"]])
                    prior = [self.loss(r["metrics"]) for r in history if r["status"] == "evaluated"]
                    result.update(status="evaluated", metrics=metric, improvement=min(prior) - self.loss(metric) if prior else 0.0)
                except (ValueError, ArithmeticError) as exc:
                    result.update(status="failed", reason="开发评价无法完成", error_type=type(exc).__name__, diagnostic=str(exc))
        write_json(path, result)
        return result

    def record(self, index, evaluation):
        path = self.root / "rounds" / f"{index:04d}" / "record.json"
        if path.exists():
            if _read(path) != evaluation:
                raise ValueError("已记录轮次与恢复评价不一致")
        else:
            write_json(path, evaluation)
        reason = self.stop_reason()
        if reason:
            self.finish(reason)
        return evaluation

    def finish(self, reason):
        output = self.root / "outcome.json"
        if output.exists():
            return _read(output)
        history = self.history()
        success = [r for r in history if r["status"] == "evaluated"]
        best = min(success, key=lambda x: self.loss(x["metrics"])) if success else None
        receipt = {"campaign_id": self.payload["campaign_id"], "status": "completed" if best else "failed",
                   "stop_reason": reason, "source": self.data["provenance"],
                   "development": self.payload["development"], "rounds": history,
                   "selected_candidate_id": best["candidate_id"] if best else None,
                   "holdout_evaluated": False, **self.outcome_fields(best)}
        write_json(output, receipt)
        return receipt


def build_campaign(payload, **kwargs):
    """研究类型决定计算，预算与逐轮记录共用同一实现。"""
    if payload.get("research_kind") == "package":
        from .package_campaign import PackageCampaign
        return PackageCampaign(payload, **kwargs)
    return Campaign(payload, **kwargs)
