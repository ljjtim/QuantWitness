"""共享期货算子、独立金融表与正式封存路径。"""

from types import MappingProxyType

SHARED_FUTURES_ARTIFACT_TYPE = "research.shared-futures-simulation.v1"
SHARED_FUTURES_CONTEXT_SCHEMA_ID = "research.shared-futures.context.v1"
SHARED_FUTURES_OPERATOR_FREQUENCIES = MappingProxyType(
    {
        "finance.simulation.shared-futures.daily": "1d",
        "finance.simulation.shared-futures.intraday": "1m",
    }
)
SHARED_FUTURES_TABLE_NAMES = (
    "cash",
    "positions",
    "orders",
    "fills",
    "costs",
    "valuations",
    "reservations",
    "risks",
    "rolls",
)
SHARED_FUTURES_RESULT_PATHS = MappingProxyType(
    {
        name: f"shared_futures/{name}"
        for name in (*SHARED_FUTURES_TABLE_NAMES, "context")
    }
)


def shared_futures_result_tables(
    node_id: str, *, primary_table: str | None = "cash"
) -> list[dict[str, str]]:
    """为同一金融节点生成包含全部支持事实的 ResultSpec 声明。"""
    if primary_table is not None and primary_table not in SHARED_FUTURES_TABLE_NAMES:
        raise ValueError("共享期货 primary_table 必须是九张正式金融表之一")
    return [
        {
            "table_id": f"{node_id}.{name}",
            "role": "primary" if name == primary_table else "diagnostic",
            "source_node_id": node_id,
            "source_port": "simulation",
            "artifact_type": SHARED_FUTURES_ARTIFACT_TYPE,
            "schema_id": f"research.shared-futures.{name}.v1",
            "path_prefix": path,
        }
        for name, path in SHARED_FUTURES_RESULT_PATHS.items()
    ]
