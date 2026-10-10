"""从 canonical ResultBundle 独立重建金融守恒与 Bar TCA 事实。"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Mapping

import duckdb
import pyarrow as pa

from research_pipeline.domain.simulation_result import (
    CANONICAL_SIMULATION_COLUMNS,
    CANONICAL_SIMULATION_INTEGER_COLUMNS,
    CANONICAL_SIMULATION_TIMESTAMP_COLUMNS,
)
from research_pipeline.platform import typed_canonical_bytes, typed_canonical_hash
from research_pipeline.results import (
    ResultBundle,
    ResultSnapshot,
    ResultStore,
)
from .errors import EvidenceContractError
from .financial_oracle.bar_tca import verify_tca
from .financial_oracle.canonical import verify_canonical_tables
from .financial_oracle.common import normalized as _normalized
from .financial_oracle.daily_etf import verify_daily_etf_financial_context
from .financial_oracle.daily_futures_support import (
    FUTURES_DECLARATION, futures_support_sources, verify_sealed_futures,
)
from .financial_oracle.minute_context import verify_minute_financial_context
from .financial_oracle.order_lifecycle import (
    LIFECYCLE_DECLARATION, LIFECYCLE_SOURCE_ID, lifecycle_support_source,
    verify_order_lifecycle,
)
from .oracle_workspace import (
    DEFAULT_FINANCIAL_ORACLE_MEMORY_BYTES,
    DEFAULT_FINANCIAL_ORACLE_TEMP_BYTES,
    FinancialOracleBudget,
    OracleTable,
    OracleWorkspace,
    ResultTableSource,
    result_table_source,
)




@dataclass(frozen=True)
class FinancialOracleSemanticContract:
    """由 Result semantic handler 注册表投影的金融复核身份。"""

    canonical_schema_roles: tuple[tuple[str, str], ...]
    bar_tca_schema_roles: tuple[tuple[str, str], ...]
    bar_tca_path_bindings: tuple[tuple[str, str], ...]
    minute_context_schema_roles: tuple[tuple[str, str], ...]
    minute_target_schema_id: str
    adjustment_snapshot_schema_id: str
    required_control_paths: tuple[str, ...]
    minute_context_path: str
    daily_etf_context_path: str
    shared_futures_schema_roles: tuple[tuple[str, str], ...]
    shared_futures_path_bindings: tuple[tuple[str, str], ...]
    daily_futures_context_path: str = "simulation/futures-context.json"
    order_lifecycle_schema_id: str = LIFECYCLE_SOURCE_ID
    order_lifecycle_path_prefix: str = "simulation/context-tables/order-lifecycle"

    def __post_init__(self) -> None:
        role_groups = (
            self.canonical_schema_roles,
            self.bar_tca_schema_roles,
            self.minute_context_schema_roles,
            self.shared_futures_schema_roles,
        )
        if any(
            not items
            or len({role for role, _ in items}) != len(items)
            or len({schema_id for _, schema_id in items}) != len(items)
            for items in role_groups
        ):
            raise EvidenceContractError("金融复核 schema role 合同无效")
        if (
            set(dict(self.bar_tca_path_bindings))
            != {schema_id for _, schema_id in self.bar_tca_schema_roles}
            or len({path for _, path in self.bar_tca_path_bindings})
            != len(self.bar_tca_path_bindings)
        ):
            raise EvidenceContractError("金融复核 Bar TCA 路径合同无效")
        if (
            set(dict(self.shared_futures_path_bindings))
            != {schema_id for _, schema_id in self.shared_futures_schema_roles}
            or len({path for _, path in self.shared_futures_path_bindings})
            != len(self.shared_futures_path_bindings)
        ):
            raise EvidenceContractError("金融复核共享期货路径合同无效")
        if (self.order_lifecycle_schema_id != LIFECYCLE_SOURCE_ID
                or self.order_lifecycle_path_prefix != LIFECYCLE_DECLARATION["path_prefix"]):
            raise EvidenceContractError("金融复核生命周期 schema 或路径合同无效")
        if len(self.required_control_paths) != 5:
            raise EvidenceContractError("金融复核控制文件合同无效")

    @property
    def canonical_schema_ids(self) -> dict[str, str]:
        return dict(self.canonical_schema_roles)

    @property
    def bar_tca_schema_ids(self) -> dict[str, str]:
        return dict(self.bar_tca_schema_roles)

    @property
    def bar_tca_path_by_schema(self) -> dict[str, str]:
        return dict(self.bar_tca_path_bindings)

    @property
    def minute_context_schema_ids(self) -> dict[str, str]:
        return dict(self.minute_context_schema_roles)


    @property
    def shared_futures_schema_ids(self) -> dict[str, str]:
        return dict(self.shared_futures_schema_roles)

    @property
    def shared_futures_path_by_schema(self) -> dict[str, str]:
        return dict(self.shared_futures_path_bindings)


@dataclass(frozen=True)
class FinancialOracleResult:
    oracle_hash: str
    bar_tca_expectations: Mapping[str, object] | None


def verify_result_financial_oracle(
    *,
    store: ResultStore,
    bundle: ResultBundle,
    semantic_contract: FinancialOracleSemanticContract,
    snapshot: ResultSnapshot | None = None,
    budget: FinancialOracleBudget | None = None,
) -> FinancialOracleResult | None:
    """从封存 Result 强制复验已声明的期货账户或 Bar TCA。"""

    declared = {table.schema_id for table in bundle.tables}
    shared_ids = set(semantic_contract.shared_futures_schema_ids.values())
    if declared & shared_ids:
        return _verify_shared_futures_result(
            store=store, bundle=bundle, snapshot=snapshot, budget=budget,
            semantic_contract=semantic_contract,
        )
    tca_schema_ids = semantic_contract.bar_tca_schema_ids
    canonical_schema_ids = semantic_contract.canonical_schema_ids
    minute_context_schema_ids = semantic_contract.minute_context_schema_ids
    tca_ids = set(tca_schema_ids.values())
    if not declared & tca_ids:
        return None
    has_minute_context = any(
        item.source_path == semantic_contract.minute_context_path
        for item in bundle.support_files
    )
    has_daily_etf_context = any(
        item.source_path == semantic_contract.daily_etf_context_path
        for item in bundle.support_files
    )
    has_futures_context = any(item.source_path == semantic_contract.daily_futures_context_path
                              for item in bundle.support_files)
    required = tca_ids | set(canonical_schema_ids.values())
    if not required <= declared:
        raise EvidenceContractError("Bar TCA ResultBundle 缺少规范六表或 TCA 表")
    schema_ids = tuple(canonical_schema_ids.values()) + tuple(
        tca_schema_ids.values()
    )
    if has_minute_context:
        if semantic_contract.minute_target_schema_id not in declared:
            raise EvidenceContractError("分钟 Bar TCA Result 缺少正式 target 表")
        minute_context_ids = tuple(minute_context_schema_ids.values())
        if not set(minute_context_ids) <= declared:
            raise EvidenceContractError("分钟 Bar TCA Result 缺少列式金融上下文")
        schema_ids = (
            *schema_ids,
            semantic_contract.minute_target_schema_id,
            *minute_context_ids,
        )
    if (
        has_minute_context
        and semantic_contract.adjustment_snapshot_schema_id in declared
    ):
        schema_ids = (*schema_ids, semantic_contract.adjustment_snapshot_schema_id)
    control_paths = semantic_contract.required_control_paths
    if has_minute_context:
        control_paths = (*control_paths, semantic_contract.minute_context_path)
    if has_daily_etf_context:
        control_paths = (*control_paths, semantic_contract.daily_etf_context_path)
    if has_futures_context:
        control_paths = (*control_paths, semantic_contract.daily_futures_context_path)
    if snapshot is None:
        snapshot = store.open_snapshot(
            store.result_directory(bundle),
            support_paths=control_paths,
        )
    if (
        snapshot.bundle != bundle
        or not set(schema_ids) <= declared
        or not set(control_paths) <= set(snapshot.support_bytes)
    ):
        raise EvidenceContractError("金融复核缺少同一次 Result 验证快照")
    controls = dict(snapshot.support_bytes)
    if has_daily_etf_context:
        daily_context = _json_bytes(controls[semantic_contract.daily_etf_context_path], "日频金融上下文")
        if "external_cashflow_context" in daily_context:
            from .financial_oracle.external_cashflows import METRIC_SCHEMA_ID
            if METRIC_SCHEMA_ID not in declared:
                raise EvidenceContractError("资金流 Result 缺少正式日频指标表")
            schema_ids = (*schema_ids, METRIC_SCHEMA_ID)

    sources = {
        schema_id: result_table_source(snapshot, schema_id)
        for schema_id in schema_ids
    }
    lifecycle_source = lifecycle_support_source(
        snapshot, _json_bytes(controls[control_paths[0]], "SimulationResult manifest"),
    )
    if lifecycle_source is not None:
        sources[LIFECYCLE_SOURCE_ID] = lifecycle_source
    futures_context = (
        _json_bytes(controls[semantic_contract.daily_futures_context_path], "日频期货金融上下文")
        if has_futures_context else None
    )
    sources.update(futures_support_sources(
        snapshot, _json_bytes(controls[control_paths[0]], "SimulationResult manifest"), futures_context,
    ))
    effective_budget = budget or FinancialOracleBudget()
    try:
        with OracleWorkspace(
            sources,
            budget=effective_budget,
        ) as workspace:
            return _verify_financial_workspace(
                workspace=workspace,
                sources=sources,
                bundle=bundle,
                controls=controls,
                control_paths=control_paths,
                has_minute_context=has_minute_context,
                has_daily_etf_context=has_daily_etf_context,
                semantic_contract=semantic_contract,
            )
    except EvidenceContractError:
        raise
    except (duckdb.Error, MemoryError, pa.ArrowMemoryError, OSError) as exc:
        raise EvidenceContractError(
            f"金融独立复核资源不足或外部扫描失败: {exc}"
        ) from exc


def _verify_shared_futures_result(*, store, bundle, snapshot, budget, semantic_contract):
    """仅用 Result 内十张正式表复验，不读取运行目录或供应商数据库。"""
    from .financial_oracle.shared_futures import verify_shared_futures

    schema_roles = semantic_contract.shared_futures_schema_ids
    path_by_schema = semantic_contract.shared_futures_path_by_schema
    selected = {}
    for role, schema_id in schema_roles.items():
        items = [table for table in bundle.tables if table.schema_id == schema_id]
        if len(items) != 1 or items[0].path_prefix != path_by_schema[schema_id]:
            raise EvidenceContractError(f"共享期货 Result 缺少唯一正式表或路径绑定: {role}")
        selected[role] = items[0]
    if len({(item.source_node_id, item.source_port, item.artifact_key)
            for item in selected.values()}) != 1:
        raise EvidenceContractError("共享期货 Result 十表必须封存同一仿真工件")
    if any(table.schema_id.startswith("research.simulation.") or
           table.schema_id.startswith("research.bar-tca.") for table in bundle.tables):
        raise EvidenceContractError("共享期货 Result 不得混用旧净仓六表或旧 Bar TCA")
    if snapshot is None:
        snapshot = store.open_snapshot(store.result_directory(bundle), support_paths=())
    if snapshot.bundle != bundle:
        raise EvidenceContractError("共享期货金融复核的 Result 快照身份不一致")
    memory = (budget or FinancialOracleBudget()).memory_bytes
    tables, used = {}, 0
    for role, manifest in selected.items():
        source = result_table_source(snapshot, manifest.schema_id)
        batches = []
        for batch in source.iter_batches():
            used += batch.nbytes
            if used * 8 > memory:
                raise EvidenceContractError("共享期货独立复核超出 memory_bytes")
            batches.append(batch)
        tables[role] = pa.Table.from_batches(batches, schema=source.schema)
        if tables[role].num_rows != sum(manifest.row_counts.values()):
            raise EvidenceContractError("共享期货 Result 表行数与封存清单不一致")
    context_table = tables.pop("context")
    if context_table.column_names != ["payload"] or context_table.num_rows != 1:
        raise EvidenceContractError("共享期货 Result 上下文必须为单行 payload")
    payload = context_table.column("payload")[0].as_py()
    if not isinstance(payload, str):
        raise EvidenceContractError("共享期货 Result 上下文 payload 必须为 JSON 字符串")
    context = _json_bytes(payload.encode("utf-8"), "共享期货金融上下文")
    expectations = verify_shared_futures(tables, context)
    return FinancialOracleResult(
        oracle_hash=typed_canonical_hash({
            "contract": "shared-futures-financial-oracle-v1",
            "context": context,
            "tables": {role: manifest.table_manifest_hash for role, manifest in selected.items()},
            "expectations": expectations,
        }),
        bar_tca_expectations=None,
    )


def _verify_financial_workspace(
    *,
    workspace: OracleWorkspace,
    sources: Mapping[str, ResultTableSource],
    bundle: ResultBundle,
    controls: Mapping[str, bytes],
    control_paths: tuple[str, ...],
    has_minute_context: bool,
    has_daily_etf_context: bool,
    semantic_contract: FinancialOracleSemanticContract,
) -> FinancialOracleResult:
    canonical_schema_ids = semantic_contract.canonical_schema_ids
    tca_schema_ids = semantic_contract.bar_tca_schema_ids
    tca_path_by_schema = semantic_contract.bar_tca_path_by_schema
    minute_context_schema_ids = semantic_contract.minute_context_schema_ids
    tca_ids = set(tca_schema_ids.values())
    canonical: dict[str, OracleTable] = {}
    canonical_hashes: dict[str, str] = {}
    canonical_schemas: dict[str, pa.Schema] = {}
    canonical_rows: dict[str, int] = {}
    for name, schema_id in canonical_schema_ids.items():
        source = sources[schema_id]
        canonical[name] = workspace.table(schema_id)
        canonical_hashes[name] = _hash_canonical_source(source, name=name)
        canonical_schemas[name] = source.schema
        canonical_rows[name] = source.row_count

    tca_tables: dict[str, OracleTable] = {}
    tca_table_hashes: dict[str, str] = {}
    tca_schema_hashes: dict[str, str] = {}
    for name, schema_id in tca_schema_ids.items():
        source = sources[schema_id]
        tca_tables[name] = workspace.table(schema_id)
        tca_table_hashes[name] = _hash_tca_source(source)
        tca_schema_hashes[name] = typed_canonical_hash(str(source.schema))

    minute_context_rows = (
        None
        if not has_minute_context
        else {
            name: workspace.table(schema_id)
            for name, schema_id in minute_context_schema_ids.items()
        }
    )
    simulation_manifest = _json_bytes(
        controls[control_paths[0]],
        "SimulationResult manifest",
    )
    account_adjustments = None
    cashflow_oracle = None
    credit_oracle = None
    if has_daily_etf_context:
        daily_context = _json_bytes(controls[semantic_contract.daily_etf_context_path], "日频金融上下文")
        if "credit_context" in daily_context:
            from .financial_oracle.credit_account import CreditContextOracle
            from .financial_oracle.daily_etf import daily_opening_observations
            if (daily_context.get("contract_version") != "research-daily-cash-financial-context-v4"
                    or daily_context.get("account_context") is None or "external_cashflow_context" not in daily_context):
                raise EvidenceContractError("信用Result必须绑定v4账户与净NAV收益事实")
            credit_oracle = CreditContextOracle(context=daily_context["credit_context"], canonical=canonical,
                account_context=daily_context["account_context"], market_observations=daily_opening_observations(daily_context, daily_context["asset_class"]),
                market_artifact_hash=daily_context.get("market_artifact_hash"))
        if "external_cashflow_context" in daily_context:
            from .financial_oracle.daily_etf import daily_opening_observations
            from .financial_oracle.external_cashflows import ExternalCashflowOracle
            if daily_context.get("contract_version") not in {"research-daily-cash-financial-context-v3", "research-daily-cash-financial-context-v4"}:
                raise EvidenceContractError("资金流 Result 必须绑定 v3 日频金融上下文")
            cashflow_oracle = ExternalCashflowOracle(
                context=daily_context["external_cashflow_context"], canonical=canonical,
                market_observations=daily_opening_observations(daily_context, daily_context["asset_class"]),
                market_artifact_hash=daily_context.get("market_artifact_hash"),
                account_context=daily_context.get("account_context"), explicit_context=daily_context.get("explicit_order_context"), asset_class=daily_context["asset_class"],
                corporate_actions=daily_context["corporate_actions"], corporate_action_records=daily_context.get("corporate_action_records", ()), credit_oracle=credit_oracle,
            )
        if daily_context.get("account_context") is not None:
            from .financial_oracle.spot_account import verify_spot_account_context
            account_adjustments = verify_spot_account_context(context=daily_context["account_context"], canonical=canonical,
                allow_zero_opening=cashflow_oracle is not None, cashflow_oracle=cashflow_oracle, credit_oracle=credit_oracle)
        elif cashflow_oracle is not None:
            cashflow_oracle.replay_plain_account()
    if simulation_manifest.get("account_valuation_adjustments") != account_adjustments:
        raise EvidenceContractError("账户估值调整未与独立金融事实绑定")
    verify_canonical_tables(
        canonical, frequency=simulation_manifest.get("semantics", {}).get("frequency"),
        credit_adjustments=account_adjustments if credit_oracle is not None else None,
        account_adjustments=(None if credit_oracle is not None else account_adjustments if account_adjustments is not None or cashflow_oracle is None
                             else {str(row["session"]): 0 for row in canonical["cash"]}),
    )
    tca_manifest = _json_bytes(
        controls[control_paths[2]],
        "Bar TCA manifest",
    )
    oracle_input = _json_bytes(
        controls[control_paths[4]],
        "Bar TCA oracle input",
    )
    policy = oracle_input.get("policy")
    if (
        isinstance(policy, Mapping)
        and policy.get("bar_frequency") == "minute"
        and not has_minute_context
    ):
        raise EvidenceContractError("分钟 Bar TCA Result 缺少金融上下文")
    if (
        isinstance(policy, Mapping)
        and policy.get("asset_class") in {"cn_etf", "cn_stock"}
        and policy.get("bar_frequency") == "daily"
        and not has_daily_etf_context
    ):
        raise EvidenceContractError("现货日频 Bar TCA Result 缺少受控规则上下文")
    try:
        simulation_marker = controls[control_paths[1]].decode("ascii")
        tca_marker = controls[control_paths[3]].decode("ascii")
    except UnicodeDecodeError as exc:
        raise EvidenceContractError("SimulationResult 或 Bar TCA COMMITTED 无效") from exc
    if (
        simulation_marker != simulation_manifest.get("manifest_hash")
        or tca_marker != tca_manifest.get("manifest_hash")
    ):
        raise EvidenceContractError("SimulationResult 或 Bar TCA COMMITTED 漂移")
    _verify_simulation_identity(
        actual_schemas=canonical_schemas,
        actual_rows=canonical_rows,
        actual_hashes=canonical_hashes,
        manifest=simulation_manifest,
        expected_names=set(canonical_schema_ids),
    )
    if LIFECYCLE_SOURCE_ID in sources:
        lifecycle_context = None
        session_bundle = None
        if has_minute_context:
            control = _json_bytes(controls[semantic_contract.minute_context_path], "分钟金融上下文")
            lifecycle_context = control.get("explicit_order_context")
            if lifecycle_context is not None:
                from research_pipeline.domain import SessionPolicyBundle
                session_bundle = SessionPolicyBundle.from_dict(control["session_policy_bundle"])
        elif has_daily_etf_context:
            lifecycle_context = daily_context.get("explicit_order_context")
        elif semantic_contract.daily_futures_context_path in controls:
            control = _json_bytes(controls[semantic_contract.daily_futures_context_path], "日频期货金融上下文")
            lifecycle_context = control.get("explicit_order_context")
        verify_order_lifecycle(
            lifecycle=workspace.table(LIFECYCLE_SOURCE_ID), canonical=canonical,
            frequency=simulation_manifest["semantics"]["frequency"],
            explicit_order_context=lifecycle_context, session_policy_bundle=session_bundle,
        )
    tca_role_by_schema = {
        schema_id: role for role, schema_id in tca_schema_ids.items()
    }
    tca_files: dict[str, str] = {}
    for table in bundle.tables:
        if table.schema_id not in tca_ids:
            continue
        if table.path_prefix != tca_path_by_schema[table.schema_id]:
            raise EvidenceContractError("Bar TCA Result 表路径与语义合同不一致")
        source_prefix = f"tables/{table.table_id}/"
        role = tca_role_by_schema[table.schema_id]
        for path, digest in table.files.items():
            tca_files[f"{role}/{path.removeprefix(source_prefix)}"] = digest
    tca_files["oracle-input.json"] = hashlib.sha256(
        controls[control_paths[4]],
    ).hexdigest()
    expectations = verify_tca(
        canonical=canonical,
        tca_tables=tca_tables,
        simulation_manifest=simulation_manifest,
        tca_manifest=tca_manifest,
        oracle_input=oracle_input,
        expected_tca_files=tca_files,
        expected_tca_schema_hashes={
            name: tca_schema_hashes[name] for name in tca_schema_ids
        },
        expected_tca_table_hashes={
            name: tca_table_hashes[name] for name in tca_schema_ids
        },
        minute_context_tables=minute_context_rows,
    )
    if has_minute_context:
        verify_minute_financial_context(
            context=_json_bytes(
                controls[semantic_contract.minute_context_path], "分钟金融上下文"
            ),
            canonical=canonical,
            simulation_manifest=simulation_manifest,
            oracle_input=oracle_input,
            target_table=sources.get(semantic_contract.minute_target_schema_id),
            adjustment_snapshot_table=sources.get(
                semantic_contract.adjustment_snapshot_schema_id
            ),
            context_tables=minute_context_rows,
        )
    if has_daily_etf_context:
        verify_daily_etf_financial_context(
            context=_json_bytes(
                controls[semantic_contract.daily_etf_context_path],
                "ETF 日频金融上下文",
            ),
            canonical=canonical,
            simulation_manifest=simulation_manifest,
            oracle_input=oracle_input,
            cashflow_oracle=cashflow_oracle, credit_oracle=credit_oracle,
        )
    if cashflow_oracle is not None:
        from .financial_oracle.external_cashflows import METRIC_SCHEMA_ID, verify_cashflow_metrics
        verify_cashflow_metrics(summary=cashflow_oracle.returns.summary(), canonical=canonical,
                                metric_rows=workspace.table(METRIC_SCHEMA_ID))
    if semantic_contract.daily_futures_context_path in controls:
        verify_sealed_futures(
            workspace=workspace, canonical=canonical,
            context=_json_bytes(controls[semantic_contract.daily_futures_context_path], "日频期货金融上下文"),
        )
    return FinancialOracleResult(
        typed_canonical_hash({
            "algorithm": "result-bundle-financial-oracle-v11",
            "result_id": bundle.result_id,
            "canonical_rows": {name: len(rows) for name, rows in canonical.items()},
            "tca_rows": {name: len(rows) for name, rows in tca_tables.items()},
            "minute_context_hash": (
                None if not has_minute_context
                else _json_bytes(
                    controls[semantic_contract.minute_context_path],
                    "分钟金融上下文",
                )["context_hash"]
            ),
            "daily_etf_context_hash": (
                None if not has_daily_etf_context
                else _json_bytes(
                    controls[semantic_contract.daily_etf_context_path],
                    "ETF 日频金融上下文",
                )["context_hash"]
            ),
            "bar_tca_expectations": dict(expectations),
            "order_lifecycle_contract": simulation_manifest.get("order_lifecycle_contract"),
            "futures_context_hash": (None if semantic_contract.daily_futures_context_path not in controls
                else _json_bytes(controls[semantic_contract.daily_futures_context_path], "日频期货金融上下文")["context_hash"]),
        }),
        expectations,
    )


def _hash_canonical_source(
    source: ResultTableSource,
    *,
    name: str,
) -> str:
    expected_columns = set(CANONICAL_SIMULATION_COLUMNS[name])
    if set(source.schema.names) != expected_columns:
        raise EvidenceContractError(f"canonical {name} schema 不匹配")
    schema = _canonical_schema_payload(name)
    digest = hashlib.sha256()
    digest.update(typed_canonical_bytes({
        "table_hash_contract": "research-simulation-table-hash-v2",
        "schema": schema,
    }))
    row_count = 0
    for batch in source.iter_batches():
        for raw_row in batch.to_pylist():
            row = dict(raw_row)
            digest.update(typed_canonical_bytes({
                str(column): _normalized(row[column])
                for column in source.schema.names
            }))
            row_count += 1
    if row_count != source.row_count:
        raise EvidenceContractError(
            f"canonical {name} 行数与 Result manifest 不一致"
        )
    return digest.hexdigest()


def _hash_tca_source(
    source: ResultTableSource,
) -> str:
    digest = hashlib.sha256()
    digest.update(typed_canonical_bytes({
        "table_hash_contract": "research-bar-tca-table-hash-v1",
        "schema": str(source.schema),
    }))
    row_count = 0
    for batch in source.iter_batches():
        for raw_row in batch.to_pylist():
            row = dict(raw_row)
            digest.update(typed_canonical_bytes(row))
            row_count += 1
    if row_count != source.row_count:
        raise EvidenceContractError("Bar TCA 行数与 Result manifest 不一致")
    return digest.hexdigest()


def _json_bytes(content: bytes, label: str) -> Mapping[str, object]:
    try:
        payload = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceContractError(f"{label} 不是有效 JSON") from exc
    if not isinstance(payload, Mapping):
        raise EvidenceContractError(f"{label} 必须是对象")
    return payload


def _verify_simulation_identity(
    *,
    actual_schemas: Mapping[str, pa.Schema],
    actual_rows: Mapping[str, int],
    actual_hashes: Mapping[str, str],
    manifest: Mapping[str, object],
    expected_names: set[str],
) -> None:
    expected_fields = {
        "contract_version", "semantics", "semantics_hash", "source_simulation_hash",
        "table_hashes", "table_rows", "table_schemas", "result_hash", "manifest_hash",
    }
    semantics = manifest.get("semantics", {})
    if (isinstance(semantics, Mapping)
            and semantics.get("contract_version") == "research-simulation-result-semantics-v2"
            and "order_lifecycle_contract" not in manifest):
        raise EvidenceContractError("新仿真语义合同缺少订单生命周期声明")
    if "order_lifecycle_contract" in manifest:
        expected_fields.add("order_lifecycle_contract")
        if manifest["order_lifecycle_contract"] != LIFECYCLE_DECLARATION:
            raise EvidenceContractError("订单生命周期合同声明无效")
    if "futures_context_contract" in manifest:
        expected_fields.add("futures_context_contract")
        declaration = manifest["futures_context_contract"]
        version = declaration.get("contract_version") if isinstance(declaration, Mapping) else None
        if (version not in {"research-futures-daily-context-v1", "research-futures-daily-context-v2", "research-futures-daily-context-v3"}
                or declaration != {**FUTURES_DECLARATION, "contract_version": version}):
            raise EvidenceContractError("日频期货金融上下文声明无效")
    if "account_valuation_adjustments" in manifest:
        expected_fields.add("account_valuation_adjustments")
        if not isinstance(manifest["account_valuation_adjustments"], Mapping):
            raise EvidenceContractError("账户估值调整必须是日期映射")
    if set(manifest) != expected_fields:
        raise EvidenceContractError("SimulationResult manifest schema 无效")
    semantics = manifest["semantics"]
    table_hashes = manifest["table_hashes"]
    table_rows = manifest["table_rows"]
    table_schemas = manifest["table_schemas"]
    if not all(isinstance(value, Mapping) for value in (
        semantics, table_hashes, table_rows, table_schemas,
    )):
        raise EvidenceContractError("SimulationResult manifest 映射字段无效")
    if any(set(value) != expected_names for value in (
        table_hashes, table_rows, table_schemas,
    )):
        raise EvidenceContractError("SimulationResult 六表身份不闭合")
    if (
        set(actual_schemas) != expected_names
        or set(actual_rows) != expected_names
        or set(actual_hashes) != expected_names
    ):
        raise EvidenceContractError("SimulationResult canonical 输入不闭合")
    for name, actual_arrow_schema in actual_schemas.items():
        schema = _canonical_schema_payload(name)
        actual_schema = [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in actual_arrow_schema
        ]
        if actual_schema != schema or table_schemas[name] != schema:
            raise EvidenceContractError("SimulationResult canonical schema 漂移")
        if (
            type(actual_rows[name]) is not int
            or actual_rows[name] < 0
            or table_rows[name] != actual_rows[name]
        ):
            raise EvidenceContractError("SimulationResult canonical 行数漂移")
    if dict(table_hashes) != dict(actual_hashes):
        raise EvidenceContractError("SimulationResult canonical table hash 漂移")
    semantics_hash = typed_canonical_hash(dict(semantics))
    if semantics_hash != manifest["semantics_hash"]:
        raise EvidenceContractError("SimulationResult semantics hash 漂移")
    identity = {
        "contract_version": manifest["contract_version"],
        "semantics_hash": semantics_hash,
        "source_simulation_hash": manifest["source_simulation_hash"],
        "table_hashes": dict(actual_hashes),
    }
    if "order_lifecycle_contract" in manifest:
        identity["order_lifecycle_contract"] = dict(LIFECYCLE_DECLARATION)
    if "futures_context_contract" in manifest:
        identity["futures_context_contract"] = dict(manifest["futures_context_contract"])
    if "account_valuation_adjustments" in manifest:
        identity["account_valuation_adjustments"] = dict(manifest["account_valuation_adjustments"])
    result_hash = typed_canonical_hash(identity)
    if result_hash != manifest["result_hash"]:
        raise EvidenceContractError("SimulationResult result hash 漂移")


def _canonical_schema_payload(name: str) -> list[dict[str, object]]:
    payload = []
    for column in sorted(CANONICAL_SIMULATION_COLUMNS[name]):
        if column == "session":
            data_type = "date32[day]"
        elif column in CANONICAL_SIMULATION_TIMESTAMP_COLUMNS:
            data_type = "timestamp[ns, tz=Asia/Shanghai]"
        elif column in CANONICAL_SIMULATION_INTEGER_COLUMNS:
            data_type = "int64"
        else:
            data_type = "string"
        payload.append({"name": column, "type": data_type, "nullable": True})
    return payload


__all__ = [
    "DEFAULT_FINANCIAL_ORACLE_MEMORY_BYTES",
    "DEFAULT_FINANCIAL_ORACLE_TEMP_BYTES",
    "FinancialOracleBudget",
    "FinancialOracleResult",
    "FinancialOracleSemanticContract",
    "verify_result_financial_oracle",
]
