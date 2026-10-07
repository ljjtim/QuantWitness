"""来源材料的只读抽取和复验，不访问数据库或真实模型。"""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from quantwitness_rdagent import source_materials


@pytest.fixture
def archive(tmp_path, monkeypatch):
    source = SimpleNamespace(
        source_id="paper", title="研究原文", url="https://example.invalid/paper",
        limitation=None,
    )
    manifest = {
        "artifact_id": "existing-snapshot", "manifest_hash": "existing-manifest",
        "snapshot_file": "snapshot.bin", "media_type": "application/pdf",
    }
    snapshot = tmp_path / "archive" / manifest["artifact_id"]
    snapshot.mkdir(parents=True)
    (snapshot / "snapshot.bin").write_bytes(b"mock pdf")
    texts = ["第一页\n第一行", "第二页\n第二行", "第三页\n第三行"]
    calls = []
    monkeypatch.setattr(source_materials, "load_research_package", lambda path: SimpleNamespace(sources=[source]))
    def verify(selected_source, root):
        calls.append((selected_source, root))
        return manifest
    monkeypatch.setattr(source_materials, "verify_source_snapshot", verify)
    import pypdf
    def reader(stream):
        assert stream.read() == b"mock pdf"
        return SimpleNamespace(pages=[SimpleNamespace(extract_text=lambda text=text: text) for text in texts])
    monkeypatch.setattr(pypdf, "PdfReader", reader)
    return SimpleNamespace(
        package=tmp_path / "package", root=tmp_path / "archive", source=source,
        manifest=manifest, texts=texts, calls=calls,
    )


def extract(archive, pages=(1, 3)):
    return source_materials.extract_materials(archive.package, archive.root, "paper", pages)


def test_extract_uses_verified_snapshot_and_physical_pages(archive):
    materials = extract(archive)
    assert materials["schema_version"] == "paper-materials-v1"
    assert materials["package_path"] == str(archive.package.resolve())
    assert materials["archive_root"] == str(archive.root.resolve())
    assert materials["snapshot_artifact_id"] == archive.manifest["artifact_id"]
    assert materials["snapshot_manifest_hash"] == archive.manifest["manifest_hash"]
    assert materials["pages"] == [
        {"pdf_page": 1, "lines": ["第一页", "第一行"]},
        {"pdf_page": 3, "lines": ["第三页", "第三行"]},
    ]
    assert any("占位地址" in note for note in materials["review_notes"])
    assert len(archive.calls) == 1
    source_materials.verify_materials(materials)
    assert len(archive.calls) == 2


@pytest.mark.parametrize("pages", [[], [0], [True], [1.0], [1, 1], [3, 1], [4]])
def test_invalid_page_selection_is_rejected(archive, pages):
    with pytest.raises(ValueError, match="页"):
        extract(archive, pages)


@pytest.mark.parametrize("text", [None, "", " \n\t"])
def test_empty_selected_page_requires_manual_review(archive, text):
    archive.texts[0] = text
    with pytest.raises(ValueError, match="人工核对"):
        extract(archive)


def test_missing_source_and_non_pdf_are_rejected(archive):
    with pytest.raises(ValueError, match="不存在来源"):
        source_materials.extract_materials(archive.package, archive.root, "unknown", [1])
    archive.manifest["media_type"] = "text/plain"
    with pytest.raises(ValueError, match="仅支持归档PDF"):
        extract(archive)


@pytest.mark.parametrize("field,value", [
    ("source_title", "不同题录"), ("source_url", "https://other.example/paper"),
    ("snapshot_artifact_id", "other-snapshot"), ("snapshot_manifest_hash", "other-manifest"),
    ("extractor", {"name": "pypdf", "version": "other-version"}),
    ("page_count", 4), ("page_count", True),
])
def test_saved_identity_or_extractor_changes_are_rejected(archive, field, value):
    materials = extract(archive)
    materials[field] = value
    with pytest.raises(ValueError):
        source_materials.verify_materials(materials)


def test_saved_text_and_current_extraction_changes_are_rejected(archive):
    materials = extract(archive)
    changed = deepcopy(materials)
    changed["pages"][0]["lines"][0] = "改写的文本"
    with pytest.raises(ValueError, match="pages"):
        source_materials.verify_materials(changed)
    archive.texts[0] = "新的抽取文本"
    with pytest.raises(ValueError, match="pages"):
        source_materials.verify_materials(materials)


def test_package_reference_and_snapshot_are_reverified(archive, monkeypatch):
    materials = extract(archive)
    archive.source.title = "更新后的包来源"
    with pytest.raises(ValueError, match="source_title"):
        source_materials.verify_materials(materials)
    def invalid_snapshot(source, root):
        raise ValueError("来源快照正文已被篡改")
    monkeypatch.setattr(source_materials, "verify_source_snapshot", invalid_snapshot)
    with pytest.raises(ValueError, match="来源快照正文"):
        source_materials.verify_materials(materials)
