"""日频和已完成分钟行情上的共享期货账户正式算子。"""

from research_pipeline.extensions import OperatorDefinition, ParameterType
from research_pipeline.platform.shared_futures_contracts import (
    SHARED_FUTURES_ARTIFACT_TYPE,
    SHARED_FUTURES_OPERATOR_FREQUENCIES,
)
from ..operator_definition_factory import _GIB, _definition, _parameter


def build_shared_futures_operator_definitions() -> tuple[OperatorDefinition, ...]:
    return tuple(
        _definition(
            name,
            name + ".v1",
            inputs=(("market", "data.columnar-bundle.v1"),)
            + ((("bars", "data.minute-bars.v1"),) if frequency == "1m" else ()),
            outputs=(("simulation", SHARED_FUTURES_ARTIFACT_TYPE),),
            parameters=(
                _parameter("spec", ParameterType.JSON),
                _parameter("market_request_id", ParameterType.STRING),
                _parameter("event_field_bindings", ParameterType.JSON),
                _parameter("max_event_rows", ParameterType.INTEGER),
            ),
            resource_profile={
                "memory_bytes": _GIB,
                "cpu_slots": 1,
                "temp_bytes": 8 * _GIB,
                "wall_seconds": 7_200,
            },
            code_fingerprint="shared-futures-direction-buckets-visible-events-v1",
            capability=SHARED_FUTURES_ARTIFACT_TYPE,
            module_name="research_pipeline.simulation.shared_futures",
            symbol_name="run_shared_futures_simulation",
            implementation_scope="core",
            dependency_modules=(
                "research_pipeline.domain.shared_futures",
                "research_pipeline.domain.shared_futures_result",
                "research_pipeline.domain.sessions",
                "research_pipeline.domain.trading",
                "research_pipeline.domain.values",
                "research_pipeline.domain.order_stream",
                "research_pipeline.simulation.target_execution",
                "research_pipeline.simulation.execution_clock",
                "research_pipeline.simulation.events",
                "research_pipeline.simulation.execution_market",
                "research_pipeline.simulation.shared_futures_result",
                "research_pipeline.simulation.shared_futures_ledger",
                "research_pipeline.simulation.engine",
                "research_pipeline.simulation.broker",
                "research_pipeline.simulation.orders",
                "research_pipeline.simulation.costs",
                "research_pipeline.simulation.margin",
                "research_pipeline.simulation.ledger",
                "research_pipeline.platform.shared_futures_contracts",
            ),
            partition_keys=("account_id",),
            cache_compatibility_mode="numerical",
        )
        for name, frequency in SHARED_FUTURES_OPERATOR_FREQUENCIES.items()
    )
