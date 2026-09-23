"""从 D root seed 派生所有随机组件的稳定 seed manifest。"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac

from research_pipeline.platform.canonical import typed_canonical_hash

from .splits import ValidationError


@dataclass(frozen=True)
class SeedManifest:
    root_seed: int
    research_identity_hash: str
    component_seeds: tuple[tuple[str, int], ...]
    manifest_hash: str

    def require_components(self, component_ids: tuple[str, ...]) -> None:
        registered = {key for key, _ in self.component_seeds}
        missing = set(component_ids) - registered
        if missing:
            raise ValidationError(f"随机组件未登记 seed: {sorted(missing)}")


def build_seed_manifest(*, root_seed: int, research_identity_hash: str, component_ids: tuple[str, ...]) -> SeedManifest:
    if isinstance(root_seed, bool) or not isinstance(root_seed, int) or root_seed < 0:
        raise ValidationError("root_seed 必须是非负整数")
    if not research_identity_hash.strip() or not component_ids or len(set(component_ids)) != len(component_ids) or any(not item.strip() for item in component_ids):
        raise ValidationError("research identity 和随机组件 ID 必须完整且唯一")
    seeds = []
    for component_id in sorted(component_ids):
        digest = hmac.new(str(root_seed).encode("ascii"), f"{research_identity_hash}|{component_id}".encode("utf-8"), hashlib.sha256).digest()
        seeds.append((component_id, int.from_bytes(digest[:8], "big")))
    payload = {"root_seed": root_seed, "research_identity_hash": research_identity_hash, "component_seeds": [[key, value] for key, value in seeds]}
    return SeedManifest(root_seed, research_identity_hash, tuple(seeds), typed_canonical_hash(payload))


__all__ = ["SeedManifest", "build_seed_manifest"]
