"""预处理、模型与 early stopping 的拟合范围证书。"""

from __future__ import annotations

from dataclasses import dataclass

from research_pipeline.platform.canonical import typed_canonical_hash

from .seeds import SeedManifest
from .splits import SplitFold, SplitManifest, ValidationError


@dataclass(frozen=True)
class FitScopeCertificate:
    component_id: str
    component_kind: str
    split_manifest_hash: str
    outer_fold_id: str
    allowed_fit_ids: tuple[str, ...]
    allowed_early_stopping_ids: tuple[str, ...]
    input_columns: tuple[str, ...]
    parameter_hash: str
    code_hash: str
    environment_hash: str
    seed_manifest_hash: str
    certificate_hash: str


@dataclass(frozen=True)
class FitArtifactBinding:
    certificate_hash: str
    output_artifact_hash: str
    fit_sample_hash: str
    early_stopping_sample_hash: str | None
    binding_hash: str


def issue_fit_scope(
    *,
    component_id: str,
    component_kind: str,
    split_manifest: SplitManifest,
    fold_id: str,
    input_columns: tuple[str, ...],
    parameters: dict[str, object],
    code_hash: str,
    environment_hash: str,
    seed_manifest: SeedManifest,
) -> FitScopeCertificate:
    fold = _fold(split_manifest, fold_id)
    if component_kind not in {"transformer", "feature_selector", "model", "early_stopping_model"}:
        raise ValidationError("component_kind 不受支持")
    if not component_id.strip() or not input_columns or len(set(input_columns)) != len(input_columns):
        raise ValidationError("component_id/input_columns 必须完整且唯一")
    for value, name in ((code_hash, "code_hash"), (environment_hash, "environment_hash")):
        if not value.strip():
            raise ValidationError(f"{name} 不能为空")
    seed_manifest.require_components((component_id,))
    early_ids = fold.validation_ids if component_kind == "early_stopping_model" else ()
    payload = {
        "component_id": component_id,
        "component_kind": component_kind,
        "split_manifest_hash": split_manifest.manifest_hash,
        "outer_fold_id": fold_id,
        "allowed_fit_ids": list(fold.train_ids),
        "allowed_early_stopping_ids": list(early_ids),
        "input_columns": list(input_columns),
        "parameter_hash": typed_canonical_hash(parameters),
        "code_hash": code_hash,
        "environment_hash": environment_hash,
        "seed_manifest_hash": seed_manifest.manifest_hash,
    }
    return FitScopeCertificate(
        component_id, component_kind, split_manifest.manifest_hash, fold_id,
        fold.train_ids, early_ids, input_columns, payload["parameter_hash"],
        code_hash, environment_hash, seed_manifest.manifest_hash,
        typed_canonical_hash(payload),
    )


def bind_fit_artifact(
    certificate: FitScopeCertificate,
    *,
    actual_fit_ids: tuple[str, ...],
    actual_early_stopping_ids: tuple[str, ...] = (),
    output_artifact_hash: str,
) -> FitArtifactBinding:
    if not actual_fit_ids or not set(actual_fit_ids) <= set(certificate.allowed_fit_ids):
        raise ValidationError("实际 fit 样本越过证书允许的训练范围")
    if actual_early_stopping_ids:
        if certificate.component_kind != "early_stopping_model" or not set(actual_early_stopping_ids) <= set(certificate.allowed_early_stopping_ids):
            raise ValidationError("early stopping 只能读取指定 inner validation")
    if not output_artifact_hash.strip():
        raise ValidationError("output_artifact_hash 不能为空")
    fit_hash = typed_canonical_hash(sorted(actual_fit_ids))
    early_hash = typed_canonical_hash(sorted(actual_early_stopping_ids)) if actual_early_stopping_ids else None
    payload = {"certificate_hash": certificate.certificate_hash, "output_artifact_hash": output_artifact_hash, "fit_sample_hash": fit_hash, "early_stopping_sample_hash": early_hash}
    return FitArtifactBinding(certificate.certificate_hash, output_artifact_hash, fit_hash, early_hash, typed_canonical_hash(payload))


def _fold(manifest: SplitManifest, fold_id: str) -> SplitFold:
    matches = [fold for fold in manifest.folds if fold.fold_id == fold_id]
    if len(matches) != 1:
        raise ValidationError("fit scope 必须绑定唯一 fold")
    return matches[0]


__all__ = ["FitArtifactBinding", "FitScopeCertificate", "bind_fit_artifact", "issue_fit_scope"]
