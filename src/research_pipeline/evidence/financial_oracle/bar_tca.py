"""Bar TCA 正式输入、独立重算、聚合和结果绑定复核。"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP, localcontext
import hashlib
import json
import math
from typing import Callable, Iterable, Mapping, Sequence

import pyarrow as pa

from research_pipeline.platform import typed_canonical_bytes, typed_canonical_hash

from ..errors import EvidenceContractError
from ..oracle_workspace import (
    FINANCIAL_ORACLE_BATCH_SIZE,
    OracleTable,
    OracleWorkspace,
    ResultTableSource,
    quoted_identifier,
)
from .common import (
    context_table_rows as _context_table_rows,
    create_mapping_table as _mapping_table,
    integer as _integer,
    iso as _iso,
    normalized as _normalized,
    normalized_mapping as _normalized_mapping,
    require_no_rows as _require_no_external_rows,
    require_unique as _unique,
)


@dataclass(frozen=True)
class _TypedRowStream:
    factory: Callable[[], Iterable[Mapping[str, object]]]


def _update_typed_canonical_bytes(
    digest: hashlib._Hash,
    value: object,
) -> None:
    digest.update(b'{"codec_version":"canonical-codec-v2-v1","value":')
    _update_typed_fragment(digest, value)
    digest.update(b"}")


def _streaming_typed_canonical_hash(value: object) -> str:
    digest = hashlib.sha256()
    _update_typed_canonical_bytes(digest, value)
    return digest.hexdigest()


def _update_typed_fragment(digest: hashlib._Hash, value: object) -> None:
    if value is None:
        digest.update(b'{"type":"null"}')
        return
    if isinstance(value, bool):
        digest.update(b'{"type":"bool","value":')
        digest.update(b"true" if value else b"false")
        digest.update(b"}")
        return
    if isinstance(value, int):
        digest.update(b'{"type":"int","value":')
        digest.update(json.dumps(str(value)).encode("utf-8"))
        digest.update(b"}")
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise EvidenceContractError("金融复核 canonical payload 不接受 NaN 或 Inf")
        digest.update(b'{"type":"float","value":')
        digest.update(json.dumps(value.hex()).encode("utf-8"))
        digest.update(b"}")
        return
    if isinstance(value, str):
        digest.update(b'{"type":"str","value":')
        digest.update(
            json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )
        )
        digest.update(b"}")
        return
    if isinstance(value, (list, tuple, _TypedRowStream)):
        values = value.factory() if isinstance(value, _TypedRowStream) else value
        digest.update(b'{"type":"list","value":[')
        first = True
        for item in values:
            if not first:
                digest.update(b",")
            _update_typed_fragment(digest, item)
            first = False
        digest.update(b"]}")
        return
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise EvidenceContractError(
                "金融复核 canonical payload 的 mapping key 必须是字符串"
            )
        digest.update(b'{"type":"object","value":{')
        first = True
        for key in sorted(value):
            if not first:
                digest.update(b",")
            digest.update(
                json.dumps(key, ensure_ascii=False, separators=(",", ":")).encode(
                    "utf-8"
                )
            )
            digest.update(b":")
            _update_typed_fragment(digest, value[key])
            first = False
        digest.update(b"}}")
        return
    raise EvidenceContractError(
        f"金融复核 canonical payload 不支持类型: {type(value).__name__}"
    )


def minute_tca_referenced_inputs(
    *,
    canonical: Mapping[str, list[dict[str, object]]],
    context_tables: Mapping[
        str,
        pa.Table | ResultTableSource | list[dict[str, object]],
    ] | None,
    source_ledger_hash: str,
    oracle_input: Mapping[str, object],
) -> tuple[list[dict[str, object]], list[dict[str, object]], str, str]:
    expected_references = {
        "decision_benchmarks": {
            "schema_id": "research.minute-financial-context.decision-benchmarks.v1",
            "path_prefix": "simulation/context-tables/decision-benchmarks",
        },
        "execution_observations": {
            "schema_id": "research.minute-financial-context.execution-observations.v1",
            "path_prefix": "simulation/context-tables/execution-observations",
        },
        "formal_fills": {
            "schema_id": "research.simulation.fills.v1",
            "path_prefix": "simulation/result-contract/fills",
        },
        "formal_orders": {
            "schema_id": "research.simulation.orders.v1",
            "path_prefix": "simulation/result-contract/orders",
        },
    }
    if (
        set(oracle_input) != {
            "contract_version", "policy", "table_references", "partitioning",
            "source_simulation_hash", "source_ledger_hash",
        }
        or oracle_input.get("table_references") != expected_references
        or oracle_input.get("partitioning") != "session"
        or context_tables is None
    ):
        raise EvidenceContractError("Bar TCA 正式表引用无效")
    benchmark_rows = _context_table_rows(
        context_tables["decision_benchmarks"],
        expected_columns={
            "portfolio_id", "order_id", "decision_price_units",
            "available_at", "source_hash",
        },
        label="TCA 决策基准",
    )
    observation_rows = _context_table_rows(
        context_tables["execution_observations"],
        expected_columns={
            "source_fill_id", "arrival_price_units",
            "arrival_price_available_at", "visible_capacity",
            "capacity_available_at",
        },
        label="TCA 成交观察",
    )
    benchmarks = {
        (str(row["portfolio_id"]), str(row["order_id"])): row
        for row in benchmark_rows
    }
    observations = {
        str(row["source_fill_id"]): row for row in observation_rows
    }
    if len(benchmarks) != len(benchmark_rows) or len(observations) != len(
        observation_rows
    ):
        raise EvidenceContractError("Bar TCA 基准或成交观察主键重复")
    orders = []
    for row in canonical["orders"]:
        benchmark = benchmarks.pop(
            (str(row["portfolio_id"]), str(row["order_id"])), None
        )
        if benchmark is None:
            raise EvidenceContractError("Bar TCA 正式订单缺少决策基准")
        orders.append({
            "order_id": str(row["order_id"]),
            "instrument_id": str(row["instrument_id"]),
            "asset_class": str(row["asset_class"]),
            "side": str(row["side"]),
            "requested_quantity": int(row["requested_quantity"]),
            "filled_quantity": int(row["filled_quantity"]),
            "status": str(row["status"]),
            "terminal_reason": row.get("terminal_reason"),
            "decision_time": _iso(row["decision_time"]),
            "submitted_at": _iso(row["submitted_at"]),
            "decision_price_units": int(benchmark["decision_price_units"]),
            "decision_price_available_at": _iso(benchmark["available_at"]),
            "decision_price_source_hash": str(benchmark["source_hash"]),
            "source_order_hash": str(row["source_order_hash"]),
            "_session": _iso(row["session"])[:10],
        })
    if benchmarks:
        raise EvidenceContractError("Bar TCA 决策基准引用未知订单")
    fills = []
    for row in canonical["fills"]:
        fill_id = str(row["fill_id"])
        observation = observations.pop(fill_id, None)
        if observation is None:
            raise EvidenceContractError("Bar TCA 正式 fill 缺少成交观察")
        fills.append({
            "source_fill_id": fill_id,
            "order_id": str(row["order_id"]),
            "instrument_id": str(row["instrument_id"]),
            "asset_class": str(row["asset_class"]),
            "side": str(row["side"]),
            "fill_time": _iso(row["fill_time"]),
            "quantity": int(row["quantity"]),
            "execution_price_units": int(row["execution_price_units"]),
            "formal_fee_units": int(row["fee_units"]),
            "source_fill_hash": str(row["source_fill_hash"]),
            "source_ledger_hash": source_ledger_hash,
            "arrival_price_units": int(observation["arrival_price_units"]),
            "arrival_price_available_at": _iso(
                observation["arrival_price_available_at"]
            ),
            "visible_capacity": int(observation["visible_capacity"]),
            "capacity_available_at": _iso(
                observation["capacity_available_at"]
            ),
            "_session": _iso(row["session"])[:10],
        })
    if observations:
        raise EvidenceContractError("Bar TCA 成交观察引用未知 fill")
    input_digest = hashlib.sha256(b"bar-tca-stream-input-v1\0")
    fill_digest = hashlib.sha256(b"bar-tca-fill-manifest-v1\0")
    sessions = sorted({str(row["_session"]) for row in orders})
    for session in sessions:
        session_orders = sorted(
            (
                {key: value for key, value in row.items() if key != "_session"}
                for row in orders if row["_session"] == session
            ),
            key=lambda row: str(row["order_id"]),
        )
        session_fills = sorted(
            (
                {key: value for key, value in row.items() if key != "_session"}
                for row in fills if row["_session"] == session
            ),
            key=lambda row: str(row["source_fill_id"]),
        )
        input_digest.update(typed_canonical_bytes({
            "orders": session_orders,
            "formal_fills": session_fills,
        }))
        for row in session_fills:
            fill_digest.update(typed_canonical_bytes({
                "source_fill_id": row["source_fill_id"],
                "source_fill_hash": row["source_fill_hash"],
            }))
    policy = oracle_input.get("policy")
    if not isinstance(policy, Mapping):
        raise EvidenceContractError("Bar TCA 表引用缺少 policy")
    clean_orders = [
        {key: value for key, value in row.items() if key != "_session"}
        for row in orders
    ]
    clean_fills = [
        {key: value for key, value in row.items() if key != "_session"}
        for row in fills
    ]
    input_hash = typed_canonical_hash({
        "stream_contract": "bar-tca-stream-input-v1",
        "stream_sha256": input_digest.hexdigest(),
        "policy_hash": typed_canonical_hash(dict(policy)),
        "source_simulation_hash": oracle_input.get("source_simulation_hash"),
        "source_ledger_hash": source_ledger_hash,
    })
    fill_hash = typed_canonical_hash({
        "stream_contract": "bar-tca-fill-manifest-v1",
        "stream_sha256": fill_digest.hexdigest(),
        "fill_count": len(clean_fills),
    })
    return clean_orders, clean_fills, input_hash, fill_hash


@dataclass(frozen=True)
class _FormalTcaTables:
    orders: str
    fills: str
    input_hash: str
    fill_manifest_hash: str
    stream_contract: bool


def _formal_order_schema() -> pa.Schema:
    return pa.schema([
        pa.field("order_id", pa.string()),
        pa.field("instrument_id", pa.string()),
        pa.field("asset_class", pa.string()),
        pa.field("side", pa.string()),
        pa.field("requested_quantity", pa.int64()),
        pa.field("filled_quantity", pa.int64()),
        pa.field("status", pa.string()),
        pa.field("terminal_reason", pa.string()),
        pa.field("decision_time", pa.string()),
        pa.field("submitted_at", pa.string()),
        pa.field("decision_price_units", pa.int64()),
        pa.field("decision_price_available_at", pa.string()),
        pa.field("decision_price_source_hash", pa.string()),
        pa.field("source_order_hash", pa.string()),
    ])


def _formal_fill_schema() -> pa.Schema:
    return pa.schema([
        pa.field("source_fill_id", pa.string()),
        pa.field("order_id", pa.string()),
        pa.field("instrument_id", pa.string()),
        pa.field("asset_class", pa.string()),
        pa.field("side", pa.string()),
        pa.field("fill_time", pa.string()),
        pa.field("quantity", pa.int64()),
        pa.field("execution_price_units", pa.int64()),
        pa.field("formal_fee_units", pa.int64()),
        pa.field("source_fill_hash", pa.string()),
        pa.field("source_ledger_hash", pa.string()),
        pa.field("arrival_price_units", pa.int64()),
        pa.field("arrival_price_available_at", pa.string()),
        pa.field("visible_capacity", pa.int64()),
        pa.field("capacity_available_at", pa.string()),
    ])


def _clean_stream_row(row: Mapping[str, object]) -> dict[str, object]:
    return {
        str(key): _normalized(value)
        for key, value in row.items()
        if key != "_session"
    }


def _prepare_external_formal_tca_tables(
    *,
    workspace: OracleWorkspace,
    canonical: Mapping[str, OracleTable],
    oracle_input: Mapping[str, object],
    minute_context_tables: Mapping[str, object] | None,
    policy_hash: str,
) -> _FormalTcaTables:
    stream_contract = oracle_input.get("contract_version") == (
        "research-bar-tca-table-references-v1"
    )
    if not stream_contract:
        if oracle_input.get("contract_version") != (
            "research-bar-tca-attribution-oracle-input-v2"
        ):
            raise EvidenceContractError("Bar TCA oracle input 版本无效")
        formal_orders = oracle_input.get("orders")
        formal_fills = oracle_input.get("formal_fills")
        if (
            not isinstance(formal_orders, list)
            or not isinstance(formal_fills, list)
            or any(
                not isinstance(row, Mapping)
                for row in (*formal_orders, *formal_fills)
            )
        ):
            raise EvidenceContractError("Bar TCA oracle input schema 无效")
        _mapping_table(
            workspace,
            name="formal_orders",
            rows=formal_orders,
            schema=_formal_order_schema(),
        )
        _mapping_table(
            workspace,
            name="formal_fills",
            rows=formal_fills,
            schema=_formal_fill_schema(),
        )
        source_simulation_hash = oracle_input.get("source_simulation_hash")
        source_ledger_hash = str(oracle_input.get("source_ledger_hash"))
        input_hash = _streaming_typed_canonical_hash({
            "orders": _TypedRowStream(lambda: workspace.iter_rows(
                "formal_orders", order_by=("order_id",)
            )),
            "formal_fills": _TypedRowStream(lambda: workspace.iter_rows(
                "formal_fills", order_by=("source_fill_id",)
            )),
            "policy_hash": policy_hash,
            "source_simulation_hash": source_simulation_hash,
            "source_ledger_hash": source_ledger_hash,
        })
        fill_manifest_hash = _streaming_typed_canonical_hash(
            _TypedRowStream(lambda: (
                {
                    "source_fill_id": row["source_fill_id"],
                    "source_fill_hash": row["source_fill_hash"],
                }
                for row in workspace.iter_rows(
                    "formal_fills", order_by=("source_fill_id",)
                )
            ))
        )
        return _FormalTcaTables(
            "formal_orders",
            "formal_fills",
            input_hash,
            fill_manifest_hash,
            False,
        )

    expected_references = {
        "decision_benchmarks": {
            "schema_id": "research.minute-financial-context.decision-benchmarks.v1",
            "path_prefix": "simulation/context-tables/decision-benchmarks",
        },
        "execution_observations": {
            "schema_id": "research.minute-financial-context.execution-observations.v1",
            "path_prefix": "simulation/context-tables/execution-observations",
        },
        "formal_fills": {
            "schema_id": "research.simulation.fills.v1",
            "path_prefix": "simulation/result-contract/fills",
        },
        "formal_orders": {
            "schema_id": "research.simulation.orders.v1",
            "path_prefix": "simulation/result-contract/orders",
        },
    }
    if (
        set(oracle_input) != {
            "contract_version", "policy", "table_references", "partitioning",
            "source_simulation_hash", "source_ledger_hash",
        }
        or oracle_input.get("table_references") != expected_references
        or oracle_input.get("partitioning") != "session"
        or minute_context_tables is None
    ):
        raise EvidenceContractError("Bar TCA 正式表引用无效")
    benchmarks = minute_context_tables.get("decision_benchmarks")
    observations = minute_context_tables.get("execution_observations")
    if not isinstance(benchmarks, OracleTable) or not isinstance(
        observations, OracleTable
    ):
        raise EvidenceContractError("Bar TCA 正式列式上下文无效")
    if set(benchmarks.schema.names if benchmarks.schema else ()) != {
        "portfolio_id", "order_id", "decision_price_units",
        "available_at", "source_hash",
    } or set(observations.schema.names if observations.schema else ()) != {
        "source_fill_id", "arrival_price_units",
        "arrival_price_available_at", "visible_capacity",
        "capacity_available_at",
    }:
        raise EvidenceContractError("Bar TCA 基准或成交观察 schema 无效")
    _unique(benchmarks, ("portfolio_id", "order_id"), "Bar TCA 决策基准")
    _unique(observations, ("source_fill_id",), "Bar TCA 成交观察")
    orders = canonical["orders"].name
    fills = canonical["fills"].name
    _require_no_external_rows(
        workspace,
        f"""
        SELECT 1 FROM {orders} AS o
        FULL OUTER JOIN {benchmarks.name} AS b
          ON o.portfolio_id = b.portfolio_id AND o.order_id = b.order_id
        WHERE o.order_id IS NULL OR b.order_id IS NULL
        LIMIT 1
        """,
        "Bar TCA 决策基准与正式订单集合不一致",
    )
    _require_no_external_rows(
        workspace,
        f"""
        SELECT 1 FROM {fills} AS f
        FULL OUTER JOIN {observations.name} AS x
          ON f.fill_id = x.source_fill_id
        WHERE f.fill_id IS NULL OR x.source_fill_id IS NULL
        LIMIT 1
        """,
        "Bar TCA 成交观察与正式 fills 集合不一致",
    )
    workspace.execute(f"""
        CREATE VIEW formal_orders AS
        SELECT o.order_id, o.instrument_id, o.asset_class, o.side,
               o.requested_quantity, o.filled_quantity, o.status,
               o.terminal_reason, o.decision_time, o.submitted_at,
               b.decision_price_units,
               b.available_at AS decision_price_available_at,
               b.source_hash AS decision_price_source_hash,
               o.source_order_hash,
               CAST(o.session AS VARCHAR) AS _session
        FROM {orders} AS o
        JOIN {benchmarks.name} AS b
          ON o.portfolio_id = b.portfolio_id AND o.order_id = b.order_id
    """)
    workspace.execute(f"""
        CREATE VIEW formal_fills AS
        SELECT f.fill_id AS source_fill_id, f.order_id, f.instrument_id,
               f.asset_class, f.side, f.fill_time, f.quantity,
               f.execution_price_units, f.fee_units AS formal_fee_units,
               f.source_fill_hash,
               '{str(oracle_input.get("source_ledger_hash")).replace("'", "''")}'
                 AS source_ledger_hash,
               x.arrival_price_units, x.arrival_price_available_at,
               x.visible_capacity, x.capacity_available_at,
               CAST(f.session AS VARCHAR) AS _session
        FROM {fills} AS f
        JOIN {observations.name} AS x ON f.fill_id = x.source_fill_id
    """)
    input_digest = hashlib.sha256(b"bar-tca-stream-input-v1\0")
    fill_digest = hashlib.sha256(b"bar-tca-fill-manifest-v1\0")
    sessions = [
        str(row["_session"])
        for row in workspace.iter_query(
            "SELECT DISTINCT _session FROM formal_orders ORDER BY _session"
        )
    ]
    for session in sessions:
        _update_typed_canonical_bytes(input_digest, {
            "orders": _TypedRowStream(lambda session=session: (
                _clean_stream_row(row)
                for row in workspace.iter_query(
                    "SELECT * FROM formal_orders WHERE _session = ? "
                    "ORDER BY order_id",
                    (session,),
                )
            )),
            "formal_fills": _TypedRowStream(lambda session=session: (
                _clean_stream_row(row)
                for row in workspace.iter_query(
                    "SELECT * FROM formal_fills WHERE _session = ? "
                    "ORDER BY source_fill_id",
                    (session,),
                )
            )),
        })
        for row in workspace.iter_query(
            "SELECT source_fill_id, source_fill_hash FROM formal_fills "
            "WHERE _session = ? ORDER BY source_fill_id",
            (session,),
        ):
            fill_digest.update(typed_canonical_bytes(dict(row)))
    input_hash = typed_canonical_hash({
        "stream_contract": "bar-tca-stream-input-v1",
        "stream_sha256": input_digest.hexdigest(),
        "policy_hash": policy_hash,
        "source_simulation_hash": oracle_input.get("source_simulation_hash"),
        "source_ledger_hash": str(oracle_input.get("source_ledger_hash")),
    })
    fill_manifest_hash = typed_canonical_hash({
        "stream_contract": "bar-tca-fill-manifest-v1",
        "stream_sha256": fill_digest.hexdigest(),
        "fill_count": len(canonical["fills"]),
    })
    return _FormalTcaTables(
        "formal_orders", "formal_fills", input_hash, fill_manifest_hash, True
    )


def _tables_differ(
    workspace: OracleWorkspace,
    *,
    left: str,
    right: str,
    columns: Sequence[str],
) -> bool:
    projection = ", ".join(quoted_identifier(name) for name in columns)
    return workspace.execute(f"""
        SELECT 1 FROM (
          (SELECT {projection} FROM {left}
           EXCEPT ALL
           SELECT {projection} FROM {right})
          UNION ALL
          (SELECT {projection} FROM {right}
           EXCEPT ALL
           SELECT {projection} FROM {left})
        )
        LIMIT 1
    """).fetchone() is not None


def _append_expected_tca_fills(
    workspace: OracleWorkspace,
    *,
    actual: OracleTable,
    formal: _FormalTcaTables,
    policy: Mapping[str, object],
) -> None:
    workspace.execute(
        f"CREATE TABLE expected_tca_fills AS "
        f"SELECT * FROM {actual.name} WHERE false"
    )
    pending: list[dict[str, object]] = []
    query = f"""
        SELECT f.*, o.side AS order_side,
               o.decision_price_units AS order_decision_price_units
        FROM {formal.fills} AS f
        LEFT JOIN {formal.orders} AS o ON f.order_id = o.order_id
        ORDER BY f.source_fill_id
    """
    for row in workspace.iter_query(query):
        if row.get("order_side") is None:
            raise EvidenceContractError("Bar TCA fill 引用未知 order")
        fill = {
            key: value for key, value in row.items()
            if key not in {"order_side", "order_decision_price_units", "_session"}
        }
        order = {
            "side": row["order_side"],
            "decision_price_units": row["order_decision_price_units"],
        }
        expected = _recompute_tca_fills(
            {str(fill["order_id"]): order},
            {str(fill["source_fill_id"]): fill},
            policy,
        )[str(fill["source_fill_id"])]
        pending.append(_normalized_mapping(expected))
        if len(pending) >= FINANCIAL_ORACLE_BATCH_SIZE:
            _insert_arrow_rows(
                workspace,
                target="expected_tca_fills",
                rows=pending,
                schema=actual.schema,
            )
            pending.clear()
    if pending:
        _insert_arrow_rows(
            workspace,
            target="expected_tca_fills",
            rows=pending,
            schema=actual.schema,
        )


def _insert_arrow_rows(
    workspace: OracleWorkspace,
    *,
    target: str,
    rows: Sequence[Mapping[str, object]],
    schema: pa.Schema | None,
) -> None:
    if schema is None:
        raise EvidenceContractError("Bar TCA 目标表缺少 Arrow schema")
    table = pa.Table.from_pylist([dict(row) for row in rows], schema=schema)
    registration = "_financial_oracle_insert"
    assert workspace.connection is not None
    workspace.connection.register(registration, table)
    try:
        workspace.execute(
            f"INSERT INTO {target} BY NAME SELECT * FROM {registration}"
        )
    finally:
        workspace.connection.unregister(registration)


def _verify_external_tca(
    *,
    canonical: Mapping[str, Sequence[dict[str, object]]],
    tca_tables: Mapping[str, Sequence[dict[str, object]]],
    simulation_manifest: Mapping[str, object],
    tca_manifest: Mapping[str, object],
    oracle_input: Mapping[str, object],
    expected_tca_files: Mapping[str, str],
    expected_tca_schema_hashes: Mapping[str, str],
    expected_tca_table_hashes: Mapping[str, str],
    minute_context_tables: Mapping[str, object] | None,
) -> Mapping[str, object]:
    external_canonical = {
        name: rows for name, rows in canonical.items()
        if isinstance(rows, OracleTable)
    }
    external_tca = {
        name: rows for name, rows in tca_tables.items()
        if isinstance(rows, OracleTable)
    }
    workspace = next(iter(external_canonical.values())).workspace
    for name, fields, label in (
        ("orders", ("order_id",), "Bar TCA orders"),
        ("fills", ("source_fill_id",), "Bar TCA fills"),
        ("daily", ("date",), "Bar TCA daily"),
    ):
        _unique(external_tca[name], fields, label)
    if simulation_manifest.get("contract_version") != "research-simulation-result-v1":
        raise EvidenceContractError("SimulationResult manifest 版本无效")
    simulation_unsigned = {
        key: value for key, value in simulation_manifest.items()
        if key != "manifest_hash"
    }
    if typed_canonical_hash(simulation_unsigned) != simulation_manifest.get(
        "manifest_hash"
    ):
        raise EvidenceContractError("SimulationResult manifest hash 不一致")
    stream_contract = tca_manifest.get("contract_version") == (
        "research-bar-tca-attribution-artifact-v3"
    )
    if not stream_contract and tca_manifest.get("contract_version") != (
        "research-bar-tca-attribution-artifact-v2"
    ):
        raise EvidenceContractError("Bar TCA manifest 版本无效")
    expected_manifest_fields = {
        "contract_version", "result_hash", "policy_hash", "input_hash",
        "source_simulation_hash", "source_ledger_hash", "table_rows",
        "schema_hashes", "files", "manifest_hash",
    }
    if stream_contract:
        expected_manifest_fields.add("table_hashes")
    if set(tca_manifest) != expected_manifest_fields:
        raise EvidenceContractError("Bar TCA manifest schema 无效")
    tca_unsigned = {
        key: value for key, value in tca_manifest.items() if key != "manifest_hash"
    }
    if typed_canonical_hash(tca_unsigned) != tca_manifest.get("manifest_hash"):
        raise EvidenceContractError("Bar TCA manifest hash 不一致")
    expected_tca_rows = {name: len(rows) for name, rows in external_tca.items()}
    if (
        tca_manifest.get("table_rows") != expected_tca_rows
        or tca_manifest.get("schema_hashes") != dict(expected_tca_schema_hashes)
        or tca_manifest.get("files") != dict(expected_tca_files)
        or (
            stream_contract
            and tca_manifest.get("table_hashes")
            != dict(expected_tca_table_hashes)
        )
    ):
        raise EvidenceContractError("Bar TCA manifest 与实际表或文件不闭合")
    policy = oracle_input.get("policy")
    if not isinstance(policy, Mapping):
        raise EvidenceContractError("Bar TCA oracle input schema 无效")
    policy_hash = typed_canonical_hash(dict(policy))
    implementation_digest = typed_canonical_hash({
        "formula": "formal-fill-attribution-with-optional-visible-liquidity-v2",
        "impact_model": policy.get("impact_model"),
        "rounding": policy.get("rounding_rule"),
        "contract_version": policy.get("contract_version"),
    })
    if policy.get("implementation_digest") != implementation_digest:
        raise EvidenceContractError("Bar TCA policy implementation digest 不一致")
    formal = _prepare_external_formal_tca_tables(
        workspace=workspace,
        canonical=external_canonical,
        oracle_input=oracle_input,
        minute_context_tables=minute_context_tables,
        policy_hash=policy_hash,
    )
    if formal.stream_contract != stream_contract:
        raise EvidenceContractError("Bar TCA manifest 与 oracle input 版本不一致")
    canonical_orders = external_canonical["orders"].name
    canonical_fills = external_canonical["fills"].name
    _require_no_external_rows(
        workspace,
        f"SELECT 1 FROM {canonical_orders} GROUP BY order_id "
        "HAVING count(*) > 1 LIMIT 1",
        "Bar TCA 要求 canonical order_id 全局唯一",
    )
    _require_no_external_rows(
        workspace,
        f"""
        SELECT 1 FROM {canonical_orders} AS c
        FULL OUTER JOIN {formal.orders} AS f ON c.order_id = f.order_id
        WHERE c.order_id IS NULL OR f.order_id IS NULL
           OR f.instrument_id IS DISTINCT FROM c.instrument_id
           OR f.asset_class IS DISTINCT FROM c.asset_class
           OR f.side IS DISTINCT FROM c.side
           OR f.requested_quantity IS DISTINCT FROM c.requested_quantity
           OR f.filled_quantity IS DISTINCT FROM c.filled_quantity
           OR f.status IS DISTINCT FROM c.status
           OR f.terminal_reason IS DISTINCT FROM c.terminal_reason
           OR CAST(f.decision_time AS TIMESTAMPTZ) IS DISTINCT FROM c.decision_time
           OR CAST(f.submitted_at AS TIMESTAMPTZ) IS DISTINCT FROM c.submitted_at
           OR f.source_order_hash IS DISTINCT FROM c.source_order_hash
        LIMIT 1
        """,
        "Bar TCA oracle order 篡改了 canonical order",
    )
    _require_no_external_rows(
        workspace,
        f"""
        WITH duplicate_source_hash AS (
          SELECT 1 FROM {canonical_fills}
          GROUP BY source_fill_hash HAVING count(*) > 1 LIMIT 1
        ), mismatch AS (
          SELECT 1 FROM {canonical_fills} AS c
          FULL OUTER JOIN {formal.fills} AS f
            ON c.fill_id = f.source_fill_id
          WHERE c.fill_id IS NULL OR f.source_fill_id IS NULL
             OR f.order_id IS DISTINCT FROM c.order_id
             OR f.instrument_id IS DISTINCT FROM c.instrument_id
             OR f.asset_class IS DISTINCT FROM c.asset_class
             OR f.side IS DISTINCT FROM c.side
             OR CAST(f.fill_time AS TIMESTAMPTZ) IS DISTINCT FROM c.fill_time
             OR f.quantity IS DISTINCT FROM c.quantity
             OR f.execution_price_units IS DISTINCT FROM c.execution_price_units
             OR f.formal_fee_units IS DISTINCT FROM c.fee_units
             OR f.source_fill_hash IS DISTINCT FROM c.source_fill_hash
          LIMIT 1
        )
        SELECT 1 FROM duplicate_source_hash
        UNION ALL SELECT 1 FROM mismatch
        LIMIT 1
        """,
        "Bar TCA oracle fills 与 canonical fills 集合或内容不一致",
    )
    ledger_hashes = workspace.execute(
        f"SELECT count(DISTINCT source_ledger_hash), min(source_ledger_hash) "
        f"FROM {formal.fills}"
    ).fetchone()
    if ledger_hashes is None or ledger_hashes[0] != 1 or (
        str(ledger_hashes[1]) != str(oracle_input.get("source_ledger_hash"))
    ):
        raise EvidenceContractError("Bar TCA source ledger 身份不闭合")
    source_simulation_hash = str(oracle_input.get("source_simulation_hash"))
    if source_simulation_hash != str(simulation_manifest.get("result_hash")):
        raise EvidenceContractError("Bar TCA 未绑定实际 SimulationResult")
    _append_expected_tca_fills(
        workspace,
        actual=external_tca["fills"],
        formal=formal,
        policy=policy,
    )
    if _tables_differ(
        workspace,
        left=external_tca["fills"].name,
        right="expected_tca_fills",
        columns=external_tca["fills"].schema.names,
    ):
        raise EvidenceContractError("Bar TCA fill 独立重算不一致")
    workspace.execute(f"""
        CREATE VIEW expected_tca_orders AS
        SELECT o.order_id, o.status, o.terminal_reason,
               o.requested_quantity, o.filled_quantity,
               o.requested_quantity - o.filled_quantity AS remaining_quantity,
               count(f.source_fill_id) AS fill_count,
               CAST(coalesce(sum(f.observed_price_shortfall_units), 0) AS BIGINT)
                 AS observed_price_shortfall_units,
               CAST(coalesce(sum(f.formal_fee_units), 0) AS BIGINT)
                 AS formal_fee_units,
               CAST(coalesce(sum(f.observed_implementation_shortfall_units), 0)
                 AS BIGINT) AS observed_implementation_shortfall_units,
               CASE WHEN count(f.source_fill_id) > 0
                          AND bool_and(f.liquidity_attribution_status = 'computed')
                    THEN 'computed' ELSE 'not_computable' END
                 AS liquidity_attribution_status,
               CASE WHEN count(f.source_fill_id) > 0
                          AND bool_and(f.liquidity_attribution_status = 'computed')
                    THEN CAST(sum(f.modeled_spread_slippage_units) AS BIGINT)
                    ELSE NULL END AS modeled_spread_slippage_units,
               CASE WHEN count(f.source_fill_id) > 0
                          AND bool_and(f.liquidity_attribution_status = 'computed')
                    THEN CAST(sum(f.modeled_impact_units) AS BIGINT)
                    ELSE NULL END AS modeled_impact_units
        FROM {formal.orders} AS o
        LEFT JOIN expected_tca_fills AS f ON o.order_id = f.order_id
        GROUP BY ALL
    """)
    if _tables_differ(
        workspace,
        left=external_tca["orders"].name,
        right="expected_tca_orders",
        columns=external_tca["orders"].schema.names,
    ):
        raise EvidenceContractError("Bar TCA order 汇总独立重算不一致")
    workspace.execute("""
        CREATE VIEW expected_tca_daily AS
        SELECT substr(fill_time, 1, 10) AS date,
               count(*) AS fill_count,
               CAST(sum(observed_price_shortfall_units) AS BIGINT)
                 AS observed_price_shortfall_units,
               CAST(sum(formal_fee_units) AS BIGINT) AS formal_fee_units,
               CAST(sum(observed_implementation_shortfall_units) AS BIGINT)
                 AS observed_implementation_shortfall_units,
               CASE WHEN bool_and(liquidity_attribution_status = 'computed')
                    THEN 'computed' ELSE 'not_computable' END
                 AS liquidity_attribution_status,
               CASE WHEN bool_and(liquidity_attribution_status = 'computed')
                    THEN CAST(sum(modeled_spread_slippage_units) AS BIGINT)
                    ELSE NULL END AS modeled_spread_slippage_units,
               CASE WHEN bool_and(liquidity_attribution_status = 'computed')
                    THEN CAST(sum(modeled_impact_units) AS BIGINT)
                    ELSE NULL END AS modeled_impact_units
        FROM expected_tca_fills
        GROUP BY substr(fill_time, 1, 10)
    """)
    if _tables_differ(
        workspace,
        left=external_tca["daily"].name,
        right="expected_tca_daily",
        columns=external_tca["daily"].schema.names,
    ):
        raise EvidenceContractError("Bar TCA daily 汇总独立重算不一致")
    if len(external_tca["research"]) != 1:
        raise EvidenceContractError("Bar TCA research 汇总必须恰好一行")
    aggregate = workspace.execute("""
        SELECT count(*),
               coalesce(sum(observed_price_shortfall_units), 0),
               coalesce(sum(formal_fee_units), 0),
               coalesce(sum(observed_implementation_shortfall_units), 0),
               CASE WHEN count(*) > 0
                          AND bool_and(liquidity_attribution_status = 'computed')
                    THEN 'computed' ELSE 'not_computable' END,
               coalesce(sum(observed_implementation_shortfall_units
                            - observed_price_shortfall_units
                            - formal_fee_units), 0)
        FROM expected_tca_fills
    """).fetchone()
    assert aggregate is not None
    liquidity_status = str(aggregate[4])
    claim_ceiling = (
        str(policy.get("claim_ceiling"))
        if liquidity_status == "computed"
        else "analysis_only"
    )
    expected_research = {
        "order_count": len(external_canonical["orders"]),
        "fill_count": int(aggregate[0]),
        "observed_price_shortfall_units": int(aggregate[1]),
        "formal_fee_units": int(aggregate[2]),
        "observed_implementation_shortfall_units": int(aggregate[3]),
        "liquidity_attribution_status": liquidity_status,
        "policy_hash": policy_hash,
        "implementation_digest": implementation_digest,
        "rule_snapshot_hash": policy.get("rule_snapshot_hash"),
        "claim_ceiling": claim_ceiling,
        "source_simulation_hash": source_simulation_hash,
        "source_ledger_hash": oracle_input.get("source_ledger_hash"),
        "source_fill_manifest_hash": formal.fill_manifest_hash,
        "input_hash": formal.input_hash,
    }
    research = external_tca["research"][0]
    if any(
        _normalized(research.get(field)) != _normalized(value)
        for field, value in expected_research.items()
    ):
        raise EvidenceContractError("Bar TCA research 汇总独立重算不一致")
    if any(
        tca_manifest.get(field) != expected
        for field, expected in {
            "policy_hash": policy_hash,
            "input_hash": formal.input_hash,
            "source_simulation_hash": source_simulation_hash,
            "source_ledger_hash": oracle_input.get("source_ledger_hash"),
        }.items()
    ):
        raise EvidenceContractError("Bar TCA manifest 上游身份不闭合")
    if formal.stream_contract:
        result_hash = typed_canonical_hash({
            "contract_version": "research-bar-tca-attribution-result-v3",
            "policy_hash": policy_hash,
            "input_hash": formal.input_hash,
            "source_simulation_hash": source_simulation_hash,
            "source_ledger_hash": oracle_input.get("source_ledger_hash"),
            "table_hashes": dict(expected_tca_table_hashes),
        })
    else:
        result_hash = _streaming_typed_canonical_hash({
            "orders": _TypedRowStream(lambda: workspace.iter_rows(
                "expected_tca_orders", order_by=("order_id",)
            )),
            "fills": _TypedRowStream(lambda: workspace.iter_rows(
                "expected_tca_fills", order_by=("source_fill_id",)
            )),
            "daily_rows": _TypedRowStream(lambda: workspace.iter_rows(
                "expected_tca_daily", order_by=("date",)
            )),
            "research_rows": [expected_research],
            "policy_hash": policy_hash,
            "input_hash": formal.input_hash,
            "source_simulation_hash": source_simulation_hash,
            "source_ledger_hash": oracle_input.get("source_ledger_hash"),
            "contract_version": "research-bar-tca-attribution-result-v2",
        })
    if result_hash != tca_manifest.get("result_hash"):
        raise EvidenceContractError("Bar TCA result hash 与实际表不一致")
    return {
        "tca_result_hash": result_hash,
        "tca_artifact_manifest_hash": tca_manifest.get("manifest_hash"),
        "tca_policy_hash": policy_hash,
        "tca_input_hash": formal.input_hash,
        "tca_implementation_digest": implementation_digest,
        "tca_rule_snapshot_hash": policy.get("rule_snapshot_hash"),
        "tca_source_simulation_hash": source_simulation_hash,
        "tca_source_ledger_hash": oracle_input.get("source_ledger_hash"),
        "tca_source_fill_manifest_hash": formal.fill_manifest_hash,
        "claim_ceiling": claim_ceiling,
        "reconciliation_delta_units": int(aggregate[5]),
        "liquidity_attribution_status": liquidity_status,
    }


def verify_tca(
    *,
    canonical: Mapping[str, list[dict[str, object]]],
    tca_tables: Mapping[str, list[dict[str, object]]],
    simulation_manifest: Mapping[str, object],
    tca_manifest: Mapping[str, object],
    oracle_input: Mapping[str, object],
    expected_tca_files: Mapping[str, str],
    expected_tca_schema_hashes: Mapping[str, str],
    expected_tca_table_hashes: Mapping[str, str],
    minute_context_tables: Mapping[
        str,
        pa.Table | ResultTableSource | list[dict[str, object]],
    ] | None,
) -> Mapping[str, object]:
    if all(isinstance(rows, OracleTable) for rows in (
        *canonical.values(),
        *tca_tables.values(),
    )):
        return _verify_external_tca(
            canonical=canonical,
            tca_tables=tca_tables,
            simulation_manifest=simulation_manifest,
            tca_manifest=tca_manifest,
            oracle_input=oracle_input,
            expected_tca_files=expected_tca_files,
            expected_tca_schema_hashes=expected_tca_schema_hashes,
            expected_tca_table_hashes=expected_tca_table_hashes,
            minute_context_tables=minute_context_tables,
        )
    _unique(tca_tables["orders"], ("order_id",), "Bar TCA orders")
    _unique(tca_tables["fills"], ("source_fill_id",), "Bar TCA fills")
    _unique(tca_tables["daily"], ("date",), "Bar TCA daily")
    if simulation_manifest.get("contract_version") != "research-simulation-result-v1":
        raise EvidenceContractError("SimulationResult manifest 版本无效")
    simulation_unsigned = {
        key: value for key, value in simulation_manifest.items() if key != "manifest_hash"
    }
    if typed_canonical_hash(simulation_unsigned) != simulation_manifest.get("manifest_hash"):
        raise EvidenceContractError("SimulationResult manifest hash 不一致")
    stream_contract = tca_manifest.get("contract_version") == (
        "research-bar-tca-attribution-artifact-v3"
    )
    if not stream_contract and tca_manifest.get("contract_version") != (
        "research-bar-tca-attribution-artifact-v2"
    ):
        raise EvidenceContractError("Bar TCA manifest 版本无效")
    expected_manifest_fields = {
        "contract_version", "result_hash", "policy_hash", "input_hash",
        "source_simulation_hash", "source_ledger_hash", "table_rows",
        "schema_hashes", "files", "manifest_hash",
    }
    if stream_contract:
        expected_manifest_fields.add("table_hashes")
    if set(tca_manifest) != expected_manifest_fields:
        raise EvidenceContractError("Bar TCA manifest schema 无效")
    tca_unsigned = {key: value for key, value in tca_manifest.items() if key != "manifest_hash"}
    if typed_canonical_hash(tca_unsigned) != tca_manifest.get("manifest_hash"):
        raise EvidenceContractError("Bar TCA manifest hash 不一致")
    expected_tca_rows = {name: len(rows) for name, rows in tca_tables.items()}
    if (
        tca_manifest.get("table_rows") != expected_tca_rows
        or tca_manifest.get("schema_hashes") != dict(expected_tca_schema_hashes)
        or tca_manifest.get("files") != dict(expected_tca_files)
        or (
            stream_contract
            and tca_manifest.get("table_hashes")
            != dict(expected_tca_table_hashes)
        )
    ):
        raise EvidenceContractError("Bar TCA manifest 与实际表或文件不闭合")
    policy = oracle_input.get("policy")
    if stream_contract:
        if oracle_input.get("contract_version") != (
            "research-bar-tca-table-references-v1"
        ):
            raise EvidenceContractError("Bar TCA 表引用版本无效")
        formal_orders, formal_fills, stream_input_hash, stream_fill_hash = (
            minute_tca_referenced_inputs(
                canonical=canonical,
                context_tables=minute_context_tables,
                source_ledger_hash=str(oracle_input.get("source_ledger_hash")),
                oracle_input=oracle_input,
            )
        )
    else:
        if oracle_input.get("contract_version") != (
            "research-bar-tca-attribution-oracle-input-v2"
        ):
            raise EvidenceContractError("Bar TCA oracle input 版本无效")
        formal_orders = oracle_input.get("orders")
        formal_fills = oracle_input.get("formal_fills")
        stream_input_hash = None
        stream_fill_hash = None
    if not isinstance(policy, Mapping) or not isinstance(formal_orders, list) or not isinstance(formal_fills, list):
        raise EvidenceContractError("Bar TCA oracle input schema 无效")
    if any(not isinstance(row, Mapping) for row in (*formal_orders, *formal_fills)):
        raise EvidenceContractError("Bar TCA oracle 行 schema 无效")
    policy_hash = typed_canonical_hash(dict(policy))
    implementation_digest = typed_canonical_hash({
        "formula": "formal-fill-attribution-with-optional-visible-liquidity-v2",
        "impact_model": policy.get("impact_model"),
        "rounding": policy.get("rounding_rule"),
        "contract_version": policy.get("contract_version"),
    })
    if policy.get("implementation_digest") != implementation_digest:
        raise EvidenceContractError("Bar TCA policy implementation digest 不一致")
    canonical_orders = {str(row["order_id"]): row for row in canonical["orders"]}
    if len(canonical_orders) != len(canonical["orders"]):
        raise EvidenceContractError("Bar TCA 要求 canonical order_id 全局唯一")
    oracle_orders = {str(row["order_id"]): row for row in formal_orders}
    if len(oracle_orders) != len(formal_orders) or set(oracle_orders) != set(canonical_orders):
        raise EvidenceContractError("Bar TCA oracle orders 与 canonical orders 集合不一致")
    for order_id, formal in oracle_orders.items():
        canonical_order = canonical_orders[order_id]
        for field in (
            "instrument_id", "asset_class", "side", "requested_quantity", "filled_quantity",
            "status", "terminal_reason", "decision_time", "submitted_at", "source_order_hash",
        ):
            if _normalized(formal.get(field)) != _normalized(canonical_order.get(field)):
                raise EvidenceContractError("Bar TCA oracle order 篡改了 canonical order")
    canonical_fills = {str(row["fill_id"]): row for row in canonical["fills"]}
    oracle_fills = {str(row["source_fill_id"]): row for row in formal_fills}
    if (
        len(canonical_fills) != len(canonical["fills"])
        or len(oracle_fills) != len(formal_fills)
        or set(canonical_fills) != set(oracle_fills)
        or len({str(row["source_fill_hash"]) for row in canonical["fills"]})
        != len(canonical["fills"])
    ):
        raise EvidenceContractError("Bar TCA oracle fills 与 canonical fills 集合不一致")
    ledger_hashes = {str(row.get("source_ledger_hash")) for row in formal_fills}
    if len(ledger_hashes) != 1 or ledger_hashes != {str(oracle_input.get("source_ledger_hash"))}:
        raise EvidenceContractError("Bar TCA source ledger 身份不闭合")
    for fill_id, formal in oracle_fills.items():
        canonical_fill = canonical_fills[fill_id]
        mapping = {
            "order_id": "order_id", "instrument_id": "instrument_id", "asset_class": "asset_class",
            "side": "side", "fill_time": "fill_time", "quantity": "quantity",
            "execution_price_units": "execution_price_units", "formal_fee_units": "fee_units",
            "source_fill_hash": "source_fill_hash",
        }
        for formal_field, canonical_field in mapping.items():
            if _normalized(formal.get(formal_field)) != _normalized(canonical_fill.get(canonical_field)):
                raise EvidenceContractError("Bar TCA oracle fill 篡改了 canonical fill")
    source_simulation_hash = str(oracle_input.get("source_simulation_hash"))
    if source_simulation_hash != str(simulation_manifest.get("result_hash")):
        raise EvidenceContractError("Bar TCA 未绑定实际 SimulationResult")
    expected_fills = _recompute_tca_fills(oracle_orders, oracle_fills, policy)
    actual_fills = {str(row["source_fill_id"]): row for row in tca_tables["fills"]}
    if set(actual_fills) != set(expected_fills):
        raise EvidenceContractError("Bar TCA 输出 fill 集合与正式 fills 不一致")
    for fill_id, expected in expected_fills.items():
        if _normalized_mapping(actual_fills[fill_id]) != _normalized_mapping(expected):
            raise EvidenceContractError("Bar TCA fill 独立重算不一致")
    expected_orders = _aggregate_tca_orders(oracle_orders, expected_fills)
    actual_orders = {str(row["order_id"]): row for row in tca_tables["orders"]}
    if {
        key: _normalized_mapping(value) for key, value in actual_orders.items()
    } != {
        key: _normalized_mapping(value) for key, value in expected_orders.items()
    }:
        raise EvidenceContractError("Bar TCA order 汇总独立重算不一致")
    expected_daily = _aggregate_tca_daily(expected_fills)
    if [_normalized_mapping(row) for row in tca_tables["daily"]] != [
        _normalized_mapping(row) for row in expected_daily
    ]:
        raise EvidenceContractError("Bar TCA daily 汇总独立重算不一致")
    if len(tca_tables["research"]) != 1:
        raise EvidenceContractError("Bar TCA research 汇总必须恰好一行")
    research = tca_tables["research"][0]
    source_fill_manifest_hash = (
        str(stream_fill_hash)
        if stream_contract
        else typed_canonical_hash([
            {
                "source_fill_id": fill_id,
                "source_fill_hash": expected_fills[fill_id]["source_fill_hash"],
            }
            for fill_id in sorted(expected_fills)
        ])
    )
    input_hash = (
        str(stream_input_hash)
        if stream_contract
        else typed_canonical_hash({
            "orders": [dict(oracle_orders[key]) for key in sorted(oracle_orders)],
            "formal_fills": [dict(oracle_fills[key]) for key in sorted(oracle_fills)],
            "policy_hash": policy_hash,
            "source_simulation_hash": source_simulation_hash,
            "source_ledger_hash": str(oracle_input.get("source_ledger_hash")),
        })
    )
    if any(
        tca_manifest.get(field) != expected
        for field, expected in {
            "policy_hash": policy_hash,
            "input_hash": input_hash,
            "source_simulation_hash": source_simulation_hash,
            "source_ledger_hash": oracle_input.get("source_ledger_hash"),
        }.items()
    ):
        raise EvidenceContractError("Bar TCA manifest 上游身份不闭合")
    liquidity_status = (
        "computed" if expected_fills and all(
            row["liquidity_attribution_status"] == "computed" for row in expected_fills.values()
        ) else "not_computable"
    )
    claim_ceiling = str(policy.get("claim_ceiling")) if liquidity_status == "computed" else "analysis_only"
    expected_research = {
        "order_count": len(expected_orders),
        "fill_count": len(expected_fills),
        "observed_price_shortfall_units": sum(int(row["observed_price_shortfall_units"]) for row in expected_fills.values()),
        "formal_fee_units": sum(int(row["formal_fee_units"]) for row in expected_fills.values()),
        "observed_implementation_shortfall_units": sum(int(row["observed_implementation_shortfall_units"]) for row in expected_fills.values()),
        "liquidity_attribution_status": liquidity_status,
        "policy_hash": policy_hash,
        "implementation_digest": implementation_digest,
        "rule_snapshot_hash": policy.get("rule_snapshot_hash"),
        "claim_ceiling": claim_ceiling,
        "source_simulation_hash": source_simulation_hash,
        "source_ledger_hash": oracle_input.get("source_ledger_hash"),
        "source_fill_manifest_hash": source_fill_manifest_hash,
        "input_hash": input_hash,
    }
    for field, value in expected_research.items():
        if _normalized(research.get(field)) != _normalized(value):
            raise EvidenceContractError("Bar TCA research 汇总独立重算不一致")
    if stream_contract:
        result_hash = typed_canonical_hash({
            "contract_version": "research-bar-tca-attribution-result-v3",
            "policy_hash": policy_hash,
            "input_hash": input_hash,
            "source_simulation_hash": source_simulation_hash,
            "source_ledger_hash": oracle_input.get("source_ledger_hash"),
            "table_hashes": dict(expected_tca_table_hashes),
        })
    else:
        result_hash = typed_canonical_hash({
            "orders": [expected_orders[key] for key in sorted(expected_orders)],
            "fills": [expected_fills[key] for key in sorted(expected_fills)],
            "daily_rows": expected_daily,
            "research_rows": [expected_research],
            "policy_hash": policy_hash,
            "input_hash": input_hash,
            "source_simulation_hash": source_simulation_hash,
            "source_ledger_hash": oracle_input.get("source_ledger_hash"),
            "contract_version": "research-bar-tca-attribution-result-v2",
        })
    expectations = {
        "tca_result_hash": result_hash,
        "tca_artifact_manifest_hash": tca_manifest.get("manifest_hash"),
        "tca_policy_hash": policy_hash,
        "tca_input_hash": input_hash,
        "tca_implementation_digest": implementation_digest,
        "tca_rule_snapshot_hash": policy.get("rule_snapshot_hash"),
        "tca_source_simulation_hash": source_simulation_hash,
        "tca_source_ledger_hash": oracle_input.get("source_ledger_hash"),
        "tca_source_fill_manifest_hash": source_fill_manifest_hash,
        "claim_ceiling": claim_ceiling,
        "reconciliation_delta_units": sum(
            int(row["observed_implementation_shortfall_units"])
            - int(row["observed_price_shortfall_units"])
            - int(row["formal_fee_units"])
            for row in expected_fills.values()
        ),
        "liquidity_attribution_status": liquidity_status,
    }
    if result_hash != tca_manifest.get("result_hash"):
        raise EvidenceContractError("Bar TCA result hash 与实际表不一致")
    return expectations


def _recompute_tca_fills(
    orders: Mapping[str, Mapping[str, object]],
    fills: Mapping[str, Mapping[str, object]],
    policy: Mapping[str, object],
) -> dict[str, dict[str, object]]:
    result = {}
    multiplier = _integer(policy.get("contract_multiplier"), "TCA multiplier", minimum=1)
    for fill_id in sorted(fills):
        fill = fills[fill_id]
        order = orders.get(str(fill["order_id"]))
        if order is None:
            raise EvidenceContractError("Bar TCA fill 引用未知 order")
        direction = 1 if order["side"] == "buy" else -1
        shortfall = direction * (
            int(fill["execution_price_units"]) - int(order["decision_price_units"])
        ) * int(fill["quantity"]) * multiplier
        computed = fill.get("arrival_price_units") is not None and fill.get("visible_capacity") is not None
        participation = spread = impact = exceeded = None
        if computed:
            capacity = _integer(fill["visible_capacity"], "TCA visible_capacity", minimum=1)
            quantity = int(fill["quantity"])
            participation = quantity * 1_000_000 // capacity
            spread_delta = _price_bps(int(fill["arrival_price_units"]), int(policy["spread_slippage_bps"]))
            if policy["impact_model"] == "fixed_bps_v1":
                impact_bps = Decimal(int(policy["fixed_impact_bps"]))
            elif policy["impact_model"] == "sqrt_participation_v1":
                with localcontext() as context:
                    context.prec = 40
                    impact_bps = Decimal(int(policy["sqrt_impact_coefficient_bps"])) * (
                        Decimal(quantity) / Decimal(capacity)
                    ).sqrt()
            else:
                raise EvidenceContractError("Bar TCA impact model 不受支持")
            impact_delta = _price_bps(int(fill["arrival_price_units"]), impact_bps)
            spread = spread_delta * quantity * multiplier
            impact = impact_delta * quantity * multiplier
            exceeded = participation > int(policy["participation_cap_ppm"])
        result[fill_id] = {
            "source_fill_id": fill_id,
            "source_fill_hash": fill["source_fill_hash"],
            "source_ledger_hash": fill["source_ledger_hash"],
            "order_id": fill["order_id"],
            "instrument_id": fill["instrument_id"],
            "fill_time": fill["fill_time"],
            "quantity": fill["quantity"],
            "execution_price_units": fill["execution_price_units"],
            "decision_price_units": order["decision_price_units"],
            "observed_price_shortfall_units": shortfall,
            "formal_fee_units": fill["formal_fee_units"],
            "observed_implementation_shortfall_units": shortfall + int(fill["formal_fee_units"]),
            "liquidity_attribution_status": "computed" if computed else "not_computable",
            "participation_ppm": participation,
            "modeled_participation_limit_exceeded": exceeded,
            "modeled_spread_slippage_units": spread,
            "modeled_impact_units": impact,
        }
    return result


def _aggregate_tca_orders(
    orders: Mapping[str, Mapping[str, object]],
    fills: Mapping[str, Mapping[str, object]],
) -> dict[str, dict[str, object]]:
    result = {}
    for order_id in sorted(orders):
        order = orders[order_id]
        selected = [row for row in fills.values() if str(row["order_id"]) == order_id]
        if sum(int(row["quantity"]) for row in selected) != int(order["filled_quantity"]):
            raise EvidenceContractError("Bar TCA order 与正式 fills 数量不闭合")
        computed = bool(selected) and all(row["liquidity_attribution_status"] == "computed" for row in selected)
        result[order_id] = {
            "order_id": order_id,
            "status": order["status"],
            "terminal_reason": order["terminal_reason"],
            "requested_quantity": order["requested_quantity"],
            "filled_quantity": order["filled_quantity"],
            "remaining_quantity": int(order["requested_quantity"]) - int(order["filled_quantity"]),
            "fill_count": len(selected),
            "observed_price_shortfall_units": sum(int(row["observed_price_shortfall_units"]) for row in selected),
            "formal_fee_units": sum(int(row["formal_fee_units"]) for row in selected),
            "observed_implementation_shortfall_units": sum(int(row["observed_implementation_shortfall_units"]) for row in selected),
            "liquidity_attribution_status": "computed" if computed else "not_computable",
            "modeled_spread_slippage_units": sum(int(row["modeled_spread_slippage_units"]) for row in selected) if computed else None,
            "modeled_impact_units": sum(int(row["modeled_impact_units"]) for row in selected) if computed else None,
        }
    return result


def _aggregate_tca_daily(fills: Mapping[str, Mapping[str, object]]) -> list[dict[str, object]]:
    days = sorted({_iso(row["fill_time"])[:10] for row in fills.values()})
    result = []
    for day in days:
        selected = [row for row in fills.values() if _iso(row["fill_time"])[:10] == day]
        computed = all(row["liquidity_attribution_status"] == "computed" for row in selected)
        result.append({
            "date": day,
            "fill_count": len(selected),
            "observed_price_shortfall_units": sum(int(row["observed_price_shortfall_units"]) for row in selected),
            "formal_fee_units": sum(int(row["formal_fee_units"]) for row in selected),
            "observed_implementation_shortfall_units": sum(int(row["observed_implementation_shortfall_units"]) for row in selected),
            "liquidity_attribution_status": "computed" if computed else "not_computable",
            "modeled_spread_slippage_units": sum(int(row["modeled_spread_slippage_units"]) for row in selected) if computed else None,
            "modeled_impact_units": sum(int(row["modeled_impact_units"]) for row in selected) if computed else None,
        })
    return result


def _price_bps(price_units: int, bps: int | Decimal) -> int:
    return int((Decimal(price_units) * Decimal(bps) / Decimal(10_000)).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP,
    ))


__all__ = ["minute_tca_referenced_inputs", "verify_tca"]
