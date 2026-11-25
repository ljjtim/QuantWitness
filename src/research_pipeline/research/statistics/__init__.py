"""新主线稳健统计、重采样和选择偏差诊断。"""

from importlib import import_module

__all__ = [
    "InferenceResult",
    "StatisticsError",
    "TestFamilyManifest",
    "adjust_p_values",
    "newey_west_mean",
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
    "stream_minute_statistics_artifact",
]


_LAZY_EXPORTS = {
    "InferenceResult": ".contracts",
    "StatisticsError": ".contracts",
    "newey_west_mean": ".covariance",
    "TestFamilyManifest": ".multiple_testing",
    "adjust_p_values": ".multiple_testing",
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
    "stream_minute_statistics_artifact": ".minute_profile",
}


def __getattr__(name: str):
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value
