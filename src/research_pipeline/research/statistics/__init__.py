"""新主线稳健统计、重采样和选择偏差诊断。"""

from importlib import import_module

__all__ = [
    "InferenceResult",
    "StatisticsError",
    "SelectionBiasResult",
    "TestFamilyManifest",
    "adjust_p_values",
    "cluster_robust_mean",
    "bootstrap_batch_replications",
    "iter_bootstrap_index_batches",
    "derive_seed",
    "infer_overlap_lag",
    "newey_west_mean",
    "deflated_sharpe_ratio",
    "effective_trial_count",
    "probabilistic_sharpe_ratio",
    "probability_of_backtest_overfitting",
    "reality_check",
    "MINUTE_STATISTICS_ARTIFACT_VERSION",
    "MINUTE_STATISTICS_CLAIM_CEILING",
    "MINUTE_STATISTICS_OBSERVATION_SCHEMA_ID",
    "MINUTE_STATISTICS_PROFILE_VERSION",
    "MINUTE_STATISTICS_SPLIT_SCHEMA_ID",
    "MinuteLabelReturn",
    "MinuteStatisticsProfile",
    "build_minute_statistics_artifact",
    "derive_minute_overlap_floors",
    "derive_minute_trial_universe_hash",
    "recompute_minute_statistics_artifact",
    "stream_minute_statistics_artifact",
]


_LAZY_EXPORTS = {
    "bootstrap_batch_replications": ".bootstrap",
    "iter_bootstrap_index_batches": ".bootstrap",
    "derive_seed": ".bootstrap",
    "InferenceResult": ".contracts",
    "StatisticsError": ".contracts",
    "cluster_robust_mean": ".covariance",
    "infer_overlap_lag": ".covariance",
    "newey_west_mean": ".covariance",
    "TestFamilyManifest": ".multiple_testing",
    "adjust_p_values": ".multiple_testing",
    "SelectionBiasResult": ".selection_bias",
    "deflated_sharpe_ratio": ".selection_bias",
    "effective_trial_count": ".selection_bias",
    "probabilistic_sharpe_ratio": ".selection_bias",
    "probability_of_backtest_overfitting": ".selection_bias",
    "reality_check": ".selection_bias",
    "MINUTE_STATISTICS_ARTIFACT_VERSION": ".minute_profile",
    "MINUTE_STATISTICS_CLAIM_CEILING": ".minute_profile",
    "MINUTE_STATISTICS_OBSERVATION_SCHEMA_ID": ".minute_profile",
    "MINUTE_STATISTICS_PROFILE_VERSION": ".minute_profile",
    "MINUTE_STATISTICS_SPLIT_SCHEMA_ID": ".minute_profile",
    "MinuteLabelReturn": ".minute_profile",
    "MinuteStatisticsProfile": ".minute_profile",
    "build_minute_statistics_artifact": ".minute_profile",
    "derive_minute_overlap_floors": ".minute_profile",
    "derive_minute_trial_universe_hash": ".minute_profile",
    "recompute_minute_statistics_artifact": ".minute_profile",
    "stream_minute_statistics_artifact": ".minute_profile",
}


def __getattr__(name: str):
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value
