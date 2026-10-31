"""统一解析已封存文件与直接来源引用。"""
from .dataset_files import (
    DATASET_ARTIFACT_REF_VERSION, DatasetArtifactRef, DatasetFilter,
    DirectDatasetRef, VerifiedDataset, ParquetArtifactResolver,
    build_dataset_artifact_ref,
)
from .direct_sources import resolve_direct_dataset


class ArtifactResolver(ParquetArtifactResolver):
    """按来源类型分派读取，统一使用批准根与批次预算。"""

    def resolve(self, reference):
        if isinstance(reference, DirectDatasetRef):
            return resolve_direct_dataset(self, reference)
        return super().resolve(reference)


__all__ = [
    "DATASET_ARTIFACT_REF_VERSION", "ArtifactResolver", "DatasetArtifactRef",
    "DatasetFilter", "DirectDatasetRef", "VerifiedDataset", "build_dataset_artifact_ref",
]
