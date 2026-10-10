"""分钟来源公开摘要的分发与历史身份验证。"""

from copy import deepcopy
from importlib.resources import files
import json

import pytest

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.minute_reference import (
    CURRENT_MINUTE_CAPABILITY_MANIFEST_HASH,
    MinuteCapabilityManifestError,
    load_minute_capability_manifest,
)


def _public_files(tmp_path):
    resource = files("research_pipeline.platform").joinpath("minute_capabilities")
    for name in ("minute_capability_manifest.v3.json", "minute_inventory.v3.public.json"):
        (tmp_path / name).write_bytes(resource.joinpath(name).read_bytes())
    return tmp_path / "minute_capability_manifest.v3.json"


def _load(path):
    return load_minute_capability_manifest(
        path, expected_manifest_hash=CURRENT_MINUTE_CAPABILITY_MANIFEST_HASH,
    )


def test_public_inventory_preserves_published_manifest_without_local_source(tmp_path):
    path = _public_files(tmp_path)
    assert not path.with_name("minute_inventory.v3.json").exists()
    assert _load(path) == load_minute_capability_manifest()
    public = json.loads(path.with_name("minute_inventory.v3.public.json").read_text(encoding="utf-8"))
    assert public["source_inventory_hash"] == _load(path).inventory_evidence_hash
    forbidden = {"source", "source_stat", "source_facts", "database", "query"}
    assert all(not forbidden.intersection(row) for row in public["inventory"]["observations"])
    assert len(public["inventory"]["observations"]) == 8


@pytest.mark.parametrize("change", ["rows", "source_identity", "missing"])
def test_public_inventory_rejects_tampering_or_missing_projection(tmp_path, change):
    path = _public_files(tmp_path)
    public_path = path.with_name("minute_inventory.v3.public.json")
    if change == "missing":
        public_path.unlink()
    else:
        public = json.loads(public_path.read_text(encoding="utf-8"))
        if change == "rows":
            public["inventory"]["observations"][0]["rows"] += 1
        else:
            public["source_inventory_hash"] = "0" * 64
        public_path.write_text(json.dumps(public), encoding="utf-8")
    with pytest.raises(MinuteCapabilityManifestError, match="公开 inventory"):
        _load(path)


def test_corrupt_original_inventory_cannot_fall_back_to_public_projection(tmp_path):
    path = _public_files(tmp_path)
    path.with_name("minute_inventory.v3.json").write_text("{}", encoding="utf-8")
    with pytest.raises(MinuteCapabilityManifestError, match="只读 inventory 摘要不一致"):
        _load(path)


def test_unpublished_manifest_cannot_borrow_approved_public_inventory(tmp_path):
    path = _public_files(tmp_path)
    payload = deepcopy(json.loads(path.read_text(encoding="utf-8")))
    payload["inventory_evidence_hash"] = "0" * 64
    payload["manifest_hash"] = typed_canonical_hash({key: value for key, value in payload.items() if key != "manifest_hash"})
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(MinuteCapabilityManifestError, match="只读 inventory 缺失"):
        load_minute_capability_manifest(path, expected_manifest_hash=payload["manifest_hash"])


def test_unreadable_original_inventory_cannot_fall_back_to_public_projection(tmp_path, monkeypatch):
    path = _public_files(tmp_path)
    original = path.with_name("minute_inventory.v3.json")
    read_bytes = type(original).read_bytes

    def read_with_denied_original(self):
        if self == original:
            raise PermissionError("原始清单不可读")
        return read_bytes(self)

    monkeypatch.setattr(type(original), "read_bytes", read_with_denied_original)
    with pytest.raises(MinuteCapabilityManifestError, match="只读 inventory 无法读取"):
        _load(path)
