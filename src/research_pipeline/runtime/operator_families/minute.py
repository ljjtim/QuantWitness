"""分钟数据、研究、仿真和统计算子定义。"""

from __future__ import annotations

from research_pipeline.extensions import (
    OperatorDefinition,
    ParameterType,
)

from research_pipeline.platform.asset_taxonomy import (
    CANONICAL_ASSET_CLASSES,
    MINUTE_TARGET_ASSET_CLASSES,
)

from ..operator_definition_factory import (
    _GIB,
    _MIB,
    _bar_tca_parameters,
    _choice_parameter,
    _definition,
    _parameter,
)


def build_minute_operator_definitions() -> tuple[OperatorDefinition, ...]:
    return (
        _definition(
            "data.minute.scan",
            "data.minute.scan.v1",
            inputs=(("admission", "data.catalog-admission.v1"),),
            outputs=(("minute_1m", "data.minute-bars.1m.v1"),),
            parameters=(
                _parameter("request_ids", ParameterType.STRING_LIST),
                _parameter("scope_binding_hash", ParameterType.STRING),
                _parameter("max_source_bytes", ParameterType.INTEGER),
                _parameter("max_returned_rows", ParameterType.INTEGER),
                _parameter("max_batch_bytes", ParameterType.INTEGER),
                _parameter("availability_policy_ref", ParameterType.STRING),
            ),
            resource_profile={
                "memory_bytes": 2 * _GIB,
                "cpu_slots": 2,
                "temp_bytes": 8 * _GIB,
                "wall_seconds": 7_200,
            },
            code_fingerprint="minute-scan-plan-v1",
            capability="data.minute-bars.1m.v1",
            module_name="research_pipeline.data_plane.minute_scan",
            symbol_name="build_minute_scan_plan",
            implementation_scope="core",
            partition_keys=("trading_date", "instrument"),
            cache_compatibility_mode="byte_exact",
        ),
        _definition(
            "research.bars.minute_resample",
            "research.bars.minute_resample.v1",
            inputs=(("minute_1m", "data.minute-bars.1m.v1"),),
            outputs=(("bars", "data.minute-bars.v1"),),
            parameters=(
                _choice_parameter(
                    "interval_minutes", ParameterType.INTEGER, (1, 5, 15, 30, 60, 120)
                ),
                _parameter("session_policy_ref", ParameterType.STRING),
                _parameter("quality_policy_refs", ParameterType.STRING_LIST),
                _choice_parameter("adjustment_mode", ParameterType.STRING, ("none",)),
                _choice_parameter(
                    "asset_class", ParameterType.STRING, CANONICAL_ASSET_CLASSES
                ),
                _parameter("availability_policy_ref", ParameterType.STRING),
            ),
            resource_profile={
                "memory_bytes": 2 * _GIB,
                "cpu_slots": 4,
                "temp_bytes": 8 * _GIB,
                "wall_seconds": 7_200,
            },
            code_fingerprint="minute-resample-operator-v1",
            capability="research.minute-bars.v1",
            module_name="research_pipeline.data_plane.minute_resampling",
            symbol_name="execute_minute_resample",
            implementation_scope="core",
            partition_keys=("instrument",),
            cache_compatibility_mode="numerical",
        ),
        _definition(
            "data.minute.adjustment_snapshot",
            "data.minute.adjustment_snapshot.v1",
            inputs=(("data", "data.columnar-bundle.v1"),),
            outputs=(("snapshot", "data.adjustment-factor-snapshot.v1"),),
            parameters=(
                _parameter("factor_request_id", ParameterType.STRING),
                _parameter("corporate_action_request_id", ParameterType.STRING),
                _parameter("instrument_id", ParameterType.STRING),
                _choice_parameter(
                    "asset_class", ParameterType.STRING, ("cn_stock", "cn_etf")
                ),
                _parameter("as_of", ParameterType.STRING),
                _parameter("applicable_start", ParameterType.STRING),
                _parameter("applicable_end", ParameterType.STRING),
                _parameter("availability_policy_ref", ParameterType.STRING),
            ),
            resource_profile={
                "memory_bytes": 512 * _MIB,
                "cpu_slots": 1,
                "temp_bytes": 512 * _MIB,
                "wall_seconds": 600,
            },
            code_fingerprint="minute-adjustment-snapshot-pit-v1",
            capability="data.adjustment-factor-snapshot.v1",
            module_name="research_pipeline.data_plane.minute_adjustment",
            symbol_name="build_adjustment_factor_snapshot",
            implementation_scope="core",
            partition_keys=("instrument",),
            cache_compatibility_mode="byte_exact",
        ),
        _definition(
            "research.bars.minute_adjust",
            "research.bars.minute_adjust.v1",
            inputs=(
                ("bars", "data.minute-bars.v1"),
                ("snapshot", "data.adjustment-factor-snapshot.v1"),
            ),
            outputs=(("bars", "data.minute-bars.v1"),),
            parameters=(
                _choice_parameter("mode", ParameterType.STRING, ("post",)),
                _parameter("relative_tolerance", ParameterType.NUMBER),
                _parameter("availability_policy_ref", ParameterType.STRING),
            ),
            resource_profile={
                "memory_bytes": 2 * _GIB,
                "cpu_slots": 4,
                "temp_bytes": 8 * _GIB,
                "wall_seconds": 7_200,
            },
            code_fingerprint="minute-adjustment-stream-v1",
            capability="research.minute-bars.v1",
            module_name="research_pipeline.data_plane.minute_adjustment",
            symbol_name="execute_minute_adjustment",
            implementation_scope="core",
            partition_keys=("instrument",),
            cache_compatibility_mode="numerical",
        ),
        _definition(
            "research.observation.minute-bars",
            "research.observation.minute-bars.v1",
            inputs=(("bars", "data.minute-bars.v1"),),
            outputs=(("observation", "research.minute-observation.v1"),),
            parameters=(_parameter("request_id", ParameterType.STRING),),
            resource_profile={
                "memory_bytes": 256 * _MIB,
                "cpu_slots": 1,
                "temp_bytes": 256 * _MIB,
                "wall_seconds": 300,
            },
            code_fingerprint="minute-observation-row-count-v1",
            capability="research.minute-observation.v1",
            module_name="research_pipeline.runtime.adapters.minute_data",
            symbol_name="execute_research_observation_minute_bars_v1",
            implementation_scope="core",
            partition_keys=("request_id",),
            cache_compatibility_mode="byte_exact",
        ),
        _definition(
            "research.validity.minute-observation",
            "research.validity.minute-observation.v1",
            inputs=(
                ("minute_1m", "data.minute-bars.1m.v1"),
                ("decision_minute_1m", "data.minute-bars.1m.v1"),
                ("observation", "research.minute-observation.v1"),
            ),
            outputs=(("validity", "research.validity-facts.v1"),),
            parameters=(_parameter("request_id", ParameterType.STRING),),
            resource_profile={
                "memory_bytes": 256 * _MIB,
                "cpu_slots": 1,
                "temp_bytes": 256 * _MIB,
                "wall_seconds": 300,
            },
            code_fingerprint="minute-observation-validity-facts-v1",
            capability="research.validity-facts.v1",
            module_name="research_pipeline.runtime.adapters.minute_data",
            symbol_name="execute_research_validity_minute_observation_v1",
            implementation_scope="core",
            partition_keys=("request_id",),
            cache_compatibility_mode="byte_exact",
        ),
        _definition(
            "research.validity.adjusted-minute-observation",
            "research.validity.adjusted-minute-observation.v1",
            inputs=(
                ("data", "data.columnar-bundle.v1"),
                ("minute_1m", "data.minute-bars.1m.v1"),
                ("decision_minute_1m", "data.minute-bars.1m.v1"),
                ("observation", "research.minute-observation.v1"),
            ),
            outputs=(("validity", "research.validity-facts.v1"),),
            parameters=(_parameter("request_id", ParameterType.STRING),),
            resource_profile={
                "memory_bytes": 256 * _MIB,
                "cpu_slots": 1,
                "temp_bytes": 256 * _MIB,
                "wall_seconds": 300,
            },
            code_fingerprint="adjusted-minute-observation-validity-facts-v1",
            capability="research.validity-facts.v1",
            module_name="research_pipeline.runtime.adapters.minute_data",
            symbol_name="execute_research_validity_adjusted_minute_observation_v1",
            implementation_scope="core",
            partition_keys=("request_id",),
            cache_compatibility_mode="byte_exact",
        ),
        _definition(
            "research.features.intraday",
            "research.features.intraday.v1",
            inputs=(("bars", "data.minute-bars.v1"),),
            outputs=(("features", "research.minute-features.v1"),),
            parameters=(
                _parameter("feature_ids", ParameterType.STRING_LIST),
                _parameter("lookback_bars", ParameterType.INTEGER),
                _parameter("warmup_bars", ParameterType.INTEGER),
                _choice_parameter(
                    "gap_policy", ParameterType.STRING, ("fail", "reset")
                ),
                _parameter("cross_session", ParameterType.BOOLEAN),
                _choice_parameter(
                    "adjustment_mode", ParameterType.STRING, ("none", "post", "pre")
                ),
                _choice_parameter(
                    "asset_class", ParameterType.STRING, CANONICAL_ASSET_CLASSES
                ),
                _choice_parameter(
                    "interval_minutes", ParameterType.INTEGER, (1, 5, 15, 30, 60, 120)
                ),
                _parameter("availability_policy_ref", ParameterType.STRING),
            ),
            resource_profile={
                "memory_bytes": 4 * _GIB,
                "cpu_slots": 4,
                "temp_bytes": 8 * _GIB,
                "wall_seconds": 7_200,
            },
            code_fingerprint="intraday-feature-whitelist-v1",
            capability="research.minute-features.v1",
            module_name="research_pipeline.research.minute_operators",
            symbol_name="build_intraday_features",
            implementation_scope="core",
            partition_keys=("instrument",),
            cache_compatibility_mode="numerical",
        ),
        _definition(
            "research.labels.intraday",
            "research.labels.intraday.v1",
            inputs=(("bars", "data.minute-bars.v1"),),
            outputs=(("labels", "research.minute-labels.v1"),),
            parameters=(
                _choice_parameter(
                    "entry_event", ParameterType.STRING, ("bar_close", "next_bar_open")
                ),
                _choice_parameter(
                    "exit_event", ParameterType.STRING, ("bar_close", "next_bar_open")
                ),
                _parameter("horizon_bars", ParameterType.INTEGER),
                _parameter("overlap", ParameterType.BOOLEAN),
                _choice_parameter(
                    "adjustment_mode", ParameterType.STRING, ("none", "post", "pre")
                ),
                _choice_parameter(
                    "asset_class", ParameterType.STRING, CANONICAL_ASSET_CLASSES
                ),
                _choice_parameter(
                    "interval_minutes", ParameterType.INTEGER, (1, 5, 15, 30, 60, 120)
                ),
                _parameter("availability_policy_ref", ParameterType.STRING),
            ),
            resource_profile={
                "memory_bytes": 4 * _GIB,
                "cpu_slots": 4,
                "temp_bytes": 8 * _GIB,
                "wall_seconds": 7_200,
            },
            code_fingerprint="intraday-label-horizon-v1",
            capability="research.minute-labels.v1",
            module_name="research_pipeline.research.minute_operators",
            symbol_name="build_intraday_labels",
            implementation_scope="core",
            partition_keys=("instrument",),
            cache_compatibility_mode="numerical",
        ),
        _definition(
            "research.signals.intraday",
            "research.signals.intraday.v1",
            inputs=(("features", "research.minute-features.v1"),),
            outputs=(("signals", "research.minute-signals.v1"),),
            parameters=(
                _parameter("feature_id", ParameterType.STRING),
                _choice_parameter(
                    "rule", ParameterType.STRING, ("greater_than", "less_than")
                ),
                _parameter("threshold", ParameterType.NUMBER),
                _parameter("availability_policy_ref", ParameterType.STRING),
            ),
            resource_profile={
                "memory_bytes": 2 * _GIB,
                "cpu_slots": 4,
                "temp_bytes": 4 * _GIB,
                "wall_seconds": 3_600,
            },
            code_fingerprint="intraday-signal-rule-v1",
            capability="research.minute-signals.v1",
            module_name="research_pipeline.research.minute_operators",
            symbol_name="build_intraday_signals",
            implementation_scope="core",
            partition_keys=("instrument",),
            cache_compatibility_mode="numerical",
        ),
        _definition(
            "research.targets.intraday",
            "research.targets.intraday.v1",
            inputs=(("signals", "research.minute-signals.v1"),),
            outputs=(("targets", "research.minute-targets.v1"),),
            parameters=(
                _choice_parameter(
                    "asset_class",
                    ParameterType.STRING,
                    MINUTE_TARGET_ASSET_CLASSES,
                ),
                _parameter("target_quantity_per_signal", ParameterType.INTEGER),
                _parameter("leverage_limit", ParameterType.NUMBER),
                _parameter("availability_policy_ref", ParameterType.STRING),
            ),
            resource_profile={
                "memory_bytes": 1 * _GIB,
                "cpu_slots": 2,
                "temp_bytes": 2 * _GIB,
                "wall_seconds": 1_800,
            },
            code_fingerprint="intraday-signal-to-quantity-target-v1",
            capability="research.minute-targets.v1",
            module_name="research_pipeline.runtime.adapters.minute_research",
            symbol_name="execute_research_targets_intraday_v1",
            implementation_scope="core",
            partition_keys=("instrument",),
            cache_compatibility_mode="numerical",
        ),
        _definition(
            "finance.simulation.intraday",
            "finance.simulation.intraday.v3",
            operator_version="3.0.0",
            inputs=(
                ("bars", "data.minute-bars.v1"),
                ("targets", "research.minute-targets.v1"),
            ),
            outputs=(("simulation", "research.minute-simulation.v1"),),
            parameters=(
                _choice_parameter(
                    "execution_model",
                    ParameterType.STRING,
                    ("next_bar_participation_v1",),
                ),
                _parameter("participation_ppm", ParameterType.INTEGER),
                _choice_parameter(
                    "claim_ceiling", ParameterType.STRING, ("bar_level_research_only",)
                ),
                _parameter("rule_bundle_hash", ParameterType.STRING),
                _parameter("initial_cash_units", ParameterType.INTEGER),
                *_bar_tca_parameters(claim_ceilings=("analysis_only",)),
            ),
            resource_profile={
                "memory_bytes": 4 * _GIB,
                "cpu_slots": 4,
                "temp_bytes": 16 * _GIB,
                "wall_seconds": 14_400,
            },
            code_fingerprint="intraday-target-canonical-result-tca-v3",
            capability="research.minute-simulation.v1",
            module_name="research_pipeline.simulation.minute_execution",
            symbol_name="run_minute_event_simulation",
            implementation_scope="core",
            dependency_modules=(
                "research_pipeline.domain.minute_rule_snapshots",
                "research_pipeline.simulation.cash_market",
                "research_pipeline.simulation.ledger",
                "research_pipeline.simulation.margin",
                "research_pipeline.simulation.result_contract",
                "research_pipeline.simulation.bar_tca",
                "research_pipeline.runtime.bar_tca_adapter",
            ),
            partition_keys=("account_id",),
            cache_compatibility_mode="numerical",
        ),
        _definition(
            "research.statistics.minute",
            "research.statistics.minute.v1",
            inputs=(
                ("labels", "research.minute-labels.v1"),
                ("simulation", "research.minute-simulation.v1"),
            ),
            outputs=(("statistics", "research.minute-statistics.v1"),),
            parameters=(
                _parameter("trial_candidate_ids", ParameterType.STRING_LIST),
                _parameter("train_end_ns", ParameterType.INTEGER),
                _parameter("validation_end_ns", ParameterType.INTEGER),
                _parameter("test_end_ns", ParameterType.INTEGER),
                _parameter("hac_lag", ParameterType.INTEGER),
                _parameter("embargo_ns", ParameterType.INTEGER),
                _choice_parameter(
                    "multiple_testing_method",
                    ParameterType.STRING,
                    ("holm", "benjamini_yekutieli"),
                ),
                _parameter("alpha", ParameterType.NUMBER),
                _parameter("min_test_samples", ParameterType.INTEGER),
                _choice_parameter(
                    "claim_ceiling",
                    ParameterType.STRING,
                    ("historical_intraday_research_observation",),
                ),
            ),
            resource_profile={
                "memory_bytes": 4 * _GIB,
                "cpu_slots": 4,
                "temp_bytes": 8 * _GIB,
                "wall_seconds": 7_200,
            },
            code_fingerprint="minute-statistics-profile-runtime-facts-v2",
            capability="research.minute-statistics.v1",
            module_name="research_pipeline.research.statistics.minute_profile",
            symbol_name="build_minute_statistics_artifact",
            implementation_scope="core",
            dependency_modules=(
                "research_pipeline.research.statistics.covariance",
                "research_pipeline.research.statistics.multiple_testing",
            ),
            partition_keys=("candidate_id",),
            seeded=True,
            cache_compatibility_mode="numerical",
        ),
        _definition(
            "research.validity.minute",
            "research.validity.minute.v1",
            inputs=(
                ("data", "data.columnar-bundle.v1"),
                ("minute_1m", "data.minute-bars.1m.v1"),
                ("simulation", "research.minute-simulation.v1"),
                ("statistics", "research.minute-statistics.v1"),
            ),
            outputs=(("validity", "research.validity-facts.v1"),),
            parameters=(),
            resource_profile={
                "memory_bytes": 2 * _GIB,
                "cpu_slots": 2,
                "temp_bytes": 4 * _GIB,
                "wall_seconds": 3_600,
            },
            code_fingerprint="minute-validity-runtime-facts-v2",
            capability="research.validity-facts.v1",
            module_name="research_pipeline.runtime.operator_graph_evidence",
            symbol_name="build_minute_intraday_validity_facts",
            implementation_scope="core",
            dependency_modules=(
                "research_pipeline.runtime.validity_facts_common",
            ),
        ),
    )
