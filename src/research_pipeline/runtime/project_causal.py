"""项目扩展实际交付轨迹与核心时间事实。"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence

import pandas as pd
import pyarrow as pa

from research_pipeline.platform.causal_time import (
    CausalTimeContractError,
    validate_feature_time_facts,
    validate_label_time_facts,
)
from research_pipeline.platform.project_causal_contract import (
    CausalPlan, CausalSource, CausalWorkItem, _names, _time,
)


class CausalReadTrace:
    """仅核心持有；按源与分区聚合已交付行和继承状态的保守依赖。"""

    def __init__(self, plan: CausalPlan, item: CausalWorkItem, inherited_lineage: Sequence[Mapping] = (),
                 source_partition_identities: Mapping[str, Mapping[str, Sequence[str]]] | None = None):
        self.plan = plan
        self.item = item
        self._identities = {
            port: {partition: tuple(source_partition_identities[port][partition])
                   if source_partition_identities is not None else (partition,)
                   for partition in partitions}
            for port, partitions in item.source_partitions.items()
        }
        self._facts: dict[tuple[str, str], dict] = {}
        if inherited_lineage and plan.state_scope != "carry":
            raise CausalTimeContractError("independent causal work item 不得继承 state")
        for fact in inherited_lineage:
            self._merge(dict(fact))

    def _merge(self, fact: dict) -> None:
        source = next((source for source in self.plan.sources if source.port == fact.get("port")), None)
        if source is None or fact.get("request_id") != source.request_id:
            raise CausalTimeContractError("causal state lineage 含未允许来源")
        if fact.get("partition_id") not in self.item.source_partitions[source.port]:
            raise CausalTimeContractError("causal state lineage 含未允许分区")
        identities = self._identities[source.port][fact["partition_id"]]
        if tuple(fact.get("source_partition_ids", identities)) != identities:
            raise CausalTimeContractError("causal state lineage 来源内容身份不匹配")
        columns = _names(fact.get("columns"), "lineage columns")
        if not set(columns) <= set(source.columns):
            raise CausalTimeContractError("causal state lineage 含未允许源列")
        first, last, available = (_time(fact.get(name)) for name in (
            "first_observation_time", "last_observation_time", "available_time",
        ))
        if first < self.item.window_start or last > self.item.window_end or first > last:
            raise CausalTimeContractError("causal state/交付 lineage 超出当前窗口")
        if available < last:
            raise CausalTimeContractError("causal 源可见时间早于实际观测")
        if self.plan.kind == "feature" and (last > self.item.decision_time or available > self.item.decision_time):
            raise CausalTimeContractError("Feature lineage 引用了未来数据")
        if self.plan.kind == "label" and first <= self.item.decision_time:
            raise CausalTimeContractError("Label lineage 必须严格晚于决策")
        key = (source.port, fact["partition_id"])
        previous = self._facts.get(key)
        self._facts[key] = {
            "port": source.port, "request_id": source.request_id,
            "partition_id": fact["partition_id"],
            "source_partition_ids": list(identities),
            "columns": sorted(set(columns) | set(previous["columns"] if previous else ())),
            "first_observation_time": min(first, _time(previous["first_observation_time"])).isoformat() if previous else first.isoformat(),
            "last_observation_time": max(last, _time(previous["last_observation_time"])).isoformat() if previous else last.isoformat(),
            "available_time": max(available, _time(previous["available_time"])).isoformat() if previous else available.isoformat(),
        }

    def wrap_input(self, port: str, source_iter: Callable) -> RestrictedCausalInput:
        source = next((source for source in self.plan.sources if source.port == port), None)
        if source is None:
            raise CausalTimeContractError("causal input port 未获允许")
        return RestrictedCausalInput(self, source, source_iter)

    def lineage(self) -> tuple[dict, ...]:
        return tuple({**fact, "columns": list(fact["columns"]),
                      "source_partition_ids": list(fact["source_partition_ids"])}
                     for _, fact in sorted(self._facts.items()))

    def core_time_facts(self) -> pd.DataFrame:
        if not self._facts:
            raise CausalTimeContractError("缺少实际 delivered-row trace 或 inherited state lineage")
        facts = tuple(self._facts.values())
        common = {"decision_time": self.item.decision_time}
        if self.plan.kind == "feature":
            common.update({
                "max_source_observation_time": max(_time(fact["last_observation_time"]) for fact in facts),
                "max_source_available_time": max(_time(fact["available_time"]) for fact in facts),
                "source_partition_ids": sorted({identity for fact in facts for identity in fact["source_partition_ids"]}),
            })
        else:
            common.update({
                "first_actual_observation_time": min(_time(fact["first_observation_time"]) for fact in facts),
                "last_actual_observation_time": max(_time(fact["last_observation_time"]) for fact in facts),
                "available_time": max(_time(fact["available_time"]) for fact in facts),
            })
        result = pd.DataFrame([{**dict(zip(self.plan.key_columns, key)), **common} for key in self.item.key_rows])
        (validate_feature_time_facts if self.plan.kind == "feature" else validate_label_time_facts)(result)
        return result


class RestrictedCausalInput:
    """只暴露冻结的源列和窗口；不提供源路径与任意时间选择器。"""

    def __init__(self, trace: CausalReadTrace, source: CausalSource, source_iter: Callable):
        self.__trace = trace
        self.__source = source
        self.__source_iter = source_iter

    @property
    def port(self) -> str:
        """沿用普通项目输入的端口选择方式，不暴露底层来源路径。"""
        return self.__source.port

    def iter_batches(self, *, columns: Sequence[str] | None = None, batch_size: int = 8192,
                     window_start: object = None, window_end: object = None,
                     partition_ids: Sequence[str] | None = None) -> Iterator[pa.RecordBatch]:
        trace, source = self.__trace, self.__source
        selected = _names(source.columns if columns is None else columns, "causal columns")
        if not set(selected) <= set(source.columns):
            raise CausalTimeContractError("causal 请求了未允许源列")
        if type(batch_size) is not int or not 0 < batch_size <= 65_536:
            raise CausalTimeContractError("causal batch_size 必须介于 1 和 65536")
        if ((window_start is not None and _time(window_start) != trace.item.window_start)
                or (window_end is not None and _time(window_end) != trace.item.window_end)):
            raise CausalTimeContractError("causal iterator 请求超出冻结窗口")
        partitions = trace.item.source_partitions[source.port]
        if partition_ids is not None:
            requested = _names(partition_ids, "causal partition_ids")
            if not set(requested) <= set(partitions):
                raise CausalTimeContractError("causal iterator 请求未允许分区")
            partitions = requested
        physical_columns = tuple(dict.fromkeys((*selected, source.observation_column, source.available_column)))
        for partition_id, batch in self.__source_iter(columns=physical_columns, partition_ids=partitions, batch_size=batch_size):
            if partition_id not in partitions:
                raise CausalTimeContractError("causal 底层交付了未允许分区")
            if not isinstance(batch, pa.RecordBatch) or batch.num_rows > batch_size:
                raise CausalTimeContractError("causal 底层批次超出资源边界")
            frame = batch.select(physical_columns).to_pandas()
            observations = frame[source.observation_column].map(_time)
            available = frame[source.available_column].map(_time)
            in_window = (observations >= trace.item.window_start) & (observations <= trace.item.window_end)
            if trace.plan.kind == "feature":
                in_window &= available <= trace.item.decision_time
            else:
                in_window &= observations > trace.item.decision_time
            if not in_window.any():
                continue
            if (available[in_window] < observations[in_window]).any():
                raise CausalTimeContractError("causal 源可见时间早于实际观测")
            trace._merge({
                "port": source.port, "request_id": source.request_id, "partition_id": partition_id,
                "columns": list(physical_columns),
                "first_observation_time": observations[in_window].min().isoformat(),
                "last_observation_time": observations[in_window].max().isoformat(),
                "available_time": available[in_window].max().isoformat(),
            })
            yield batch.filter(pa.array(in_window.to_numpy())).select(selected)
