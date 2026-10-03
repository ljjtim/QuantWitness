"""ETF 日频现金仿真的显式目标和行情合同。"""
from research_pipeline.extensions import OperatorDefinition, ParameterType
from ..operator_definition_factory import _GIB, _definition, _parameter, _choice_parameter


def build_daily_cash_operator_definitions() -> tuple[OperatorDefinition, ...]:
    return (_definition(
        "finance.simulation.daily-cash", "finance.simulation.daily-cash.v1",
        inputs=(("targets", "research.portfolio-targets.v1"), ("market", "data.daily-market.v1")),
        outputs=(("simulation", "research.daily-simulation.v1"),),
        parameters=(
            _choice_parameter("market_rule_profile_id", ParameterType.STRING, ("cn_etf.daily.curated.v1",)),
            _parameter("instrument_codes", ParameterType.STRING_LIST),
            _parameter("bond_etf_codes", ParameterType.STRING_LIST_ALLOW_EMPTY),
            _parameter("equity_etf_codes", ParameterType.STRING_LIST_ALLOW_EMPTY),
            _parameter("commission_ppm", ParameterType.INTEGER),
            _parameter("min_commission_units", ParameterType.INTEGER),
            _parameter("corporate_actions", ParameterType.JSON),
            _parameter("initial_cash_cny", ParameterType.NUMBER),
            _parameter("calendar_id", ParameterType.STRING),
            _parameter("policy_available_at", ParameterType.STRING),
        ),
        resource_profile={"memory_bytes": 4 * _GIB, "cpu_slots": 1,
                          "temp_bytes": 8 * _GIB, "wall_seconds": 3600},
        code_fingerprint="daily-etf-cash-explicit-artifacts-v1",
        capability="research.daily-simulation.v1",
        module_name="research_pipeline.runtime.qlib_portfolio_execution",
        symbol_name="execute_daily_cash_artifact",
        implementation_scope="core",
        dependency_modules=(
            "research_pipeline.simulation.daily_event",
            "research_pipeline.simulation.cash_market",
            "research_pipeline.simulation.cn_etf",
            "research_pipeline.simulation.corporate_actions",
            "research_pipeline.domain.values",
            "research_pipeline.domain.time",
            "research_pipeline.domain.simulation_result",
            "research_pipeline.simulation.events",
            "research_pipeline.simulation.result_contract",
            "research_pipeline.simulation.bar_tca",
            "research_pipeline.runtime.bar_tca_adapter",
            "research_pipeline.domain.rules",
            "research_pipeline.platform.market_rule_defaults",
        ),
    ),)
