"""新研究主链的无业务状态平台基础。"""

from importlib import import_module

from .canonical import (
    CANONICAL_JSON_V1_VERSION,
    TYPED_CANONICAL_V1_VERSION,
    canonical_json,
    fingerprint,
    typed_canonical_bytes,
    typed_canonical_hash,
)
from .errors import (
    CanonicalEncodingError,
    MainlineError,
    error_code_for_exception,
)


__all__ = [
    "CANONICAL_JSON_V1_VERSION",
    "BUILD_MANIFEST_VERSION",
    "DEPENDENCY_LOCK_VERSION",
    "TYPED_CANONICAL_V1_VERSION",
    "CanonicalEncodingError",
    "CausalTimeContractError",
    "CORE_FEATURE_TIME_COLUMNS",
    "CORE_LABEL_TIME_COLUMNS",
    "CURRENT_MINUTE_CAPABILITY_MANIFEST_HASH",
    "CURRENT_MINUTE_CAPABILITY_MANIFEST_REVISION",
    "BuildManifest",
    "MainlineError",
    "EXECUTION_CAPABILITY_MATRIX",
    "TradingCapabilityError",
    "MINUTE_INTERVALS",
    "MINUTE_CAPABILITY_BINDING_VERSION",
    "MINUTE_CAPABILITY_MANIFEST_VERSION",
    "MINUTE_CAPABILITY_REQUIRED_CONSUMERS",
    "GATE_RECEIPT_VERSION",
    "LOCAL_SUPPLY_CHAIN_STATUS",
    "RELEASE_ACCEPTANCE_INPUT_VERSION",
    "RELEASE_ENVELOPE_VERSION",
    "REQUIRED_GATE_IDS",
    "MinuteInventoryObservation",
    "MinuteAssetCoverage",
    "MinuteCapabilityInstrument",
    "MinuteCapabilityManifest",
    "MinuteCapabilityManifestError",
    "ReleaseAcceptanceInput",
    "ReleaseEnvelope",
    "ReleaseEnvelopeError",
    "ReleaseGateReceipt",
    "verify_release_envelope",
    "canonical_json",
    "attach_core_feature_time_facts",
    "attach_core_label_time_facts",
    "build_core_feature_time_facts",
    "error_code_for_exception",
    "fingerprint",
    "installed_distribution_digest",
    "load_build_manifest",
    "load_minute_capability_manifest",
    "require_declared_trading_capability",
    "typed_canonical_bytes",
    "typed_canonical_hash",
    "validate_feature_time_facts",
    "validate_label_time_facts",
    "verify_dependency_distribution_lock",
]


_BUILD_MANIFEST_EXPORTS = {
    "BUILD_MANIFEST_VERSION",
    "DEPENDENCY_LOCK_VERSION",
    "BuildManifest",
    "installed_distribution_digest",
    "load_build_manifest",
    "verify_dependency_distribution_lock",
}
_MINUTE_CAPABILITY_EXPORTS = {
    "CURRENT_MINUTE_CAPABILITY_MANIFEST_HASH",
    "CURRENT_MINUTE_CAPABILITY_MANIFEST_REVISION",
    "MINUTE_INTERVALS",
    "MINUTE_CAPABILITY_BINDING_VERSION",
    "MINUTE_CAPABILITY_MANIFEST_VERSION",
    "MINUTE_CAPABILITY_REQUIRED_CONSUMERS",
    "MinuteInventoryObservation",
    "MinuteAssetCoverage",
    "MinuteCapabilityInstrument",
    "MinuteCapabilityManifest",
    "MinuteCapabilityManifestError",
    "load_minute_capability_manifest",
}
_TRADING_CAPABILITY_EXPORTS = {
    "EXECUTION_CAPABILITY_MATRIX",
    "TradingCapabilityError",
    "require_declared_trading_capability",
}
_RELEASE_ENVELOPE_EXPORTS = {
    "GATE_RECEIPT_VERSION",
    "LOCAL_SUPPLY_CHAIN_STATUS",
    "RELEASE_ACCEPTANCE_INPUT_VERSION",
    "RELEASE_ENVELOPE_VERSION",
    "REQUIRED_GATE_IDS",
    "ReleaseAcceptanceInput",
    "ReleaseEnvelope",
    "ReleaseEnvelopeError",
    "ReleaseGateReceipt",
    "verify_release_envelope",
}
_CAUSAL_TIME_EXPORTS = {
    "CausalTimeContractError",
    "CORE_FEATURE_TIME_COLUMNS",
    "CORE_LABEL_TIME_COLUMNS",
    "attach_core_feature_time_facts",
    "attach_core_label_time_facts",
    "build_core_feature_time_facts",
    "validate_feature_time_facts",
    "validate_label_time_facts",
}


def __getattr__(name: str):
    """保持公共导出不变，仅在真实消费者访问时加载对应实现。"""
    if name in _BUILD_MANIFEST_EXPORTS:
        module_name = ".build_manifest"
    elif name in _MINUTE_CAPABILITY_EXPORTS:
        module_name = ".minute_reference"
    elif name in _TRADING_CAPABILITY_EXPORTS:
        module_name = ".trading_capabilities"
    elif name in _RELEASE_ENVELOPE_EXPORTS:
        module_name = ".release_envelope"
    elif name in _CAUSAL_TIME_EXPORTS:
        module_name = ".causal_time"
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
