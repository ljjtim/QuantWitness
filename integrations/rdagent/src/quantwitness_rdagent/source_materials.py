"""从已有来源快照抽取指定物理页，并复验保存的文本定位。"""
from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from research_pipeline.packages.source_provenance import verify_source_snapshot
from research_pipeline.packages.store import load_research_package


SCHEMA_VERSION = "paper-materials-v1"


def _selected_pages(pages):
    if not isinstance(pages, (list, tuple)) or not pages:
        raise ValueError("必须指定非空、升序且不重复的1基PDF物理页码")
    if any(type(page) is not int or page < 1 for page in pages):
        raise ValueError("PDF物理页码必须是从1开始的整数")
    selected = list(pages)
    if selected != sorted(set(selected)):
        raise ValueError("PDF物理页码必须升序且不重复")
    return selected


def extract_materials(package_path, archive_root, source_id, pages) -> dict:
    """只读抽取归档PDF的选定页；引文存在不代表公式语义已确认。"""
    selected = _selected_pages(pages)
    package_root = Path(package_path).resolve()
    archive = Path(archive_root).resolve()
    package = load_research_package(package_root)
    source = next((item for item in package.sources if item.source_id == source_id), None)
    if source is None:
        raise ValueError(f"ResearchPackage 中不存在来源：{source_id}")
    manifest = verify_source_snapshot(source, archive)
    if manifest["media_type"] != "application/pdf":
        raise ValueError("材料抽取仅支持归档PDF")
    try:
        import pypdf
    except ImportError as exc:
        raise ValueError("材料抽取需要安装本集成的materials可选依赖") from exc

    snapshot_path = archive / manifest["artifact_id"] / manifest["snapshot_file"]
    extracted = []
    try:
        with snapshot_path.open("rb") as stream:
            reader = pypdf.PdfReader(stream)
            page_count = len(reader.pages)
            if selected[-1] > page_count:
                raise ValueError(f"指定物理页超出PDF范围：共{page_count}页")
            for page_number in selected:
                text = reader.pages[page_number - 1].extract_text()
                if not text or not text.strip():
                    raise ValueError(
                        f"PDF物理页{page_number}没有可提取文本，需人工核对图像或版面；本入口不执行OCR"
                    )
                extracted.append({"pdf_page": page_number, "lines": text.splitlines()})
    except pypdf.errors.PdfReadError as exc:
        raise ValueError("归档PDF无法读取，需人工核对原文件") from exc

    notes = [
        "页码为1基PDF物理页；行号仅对应本次保存的抽取文本。",
        "文本定位只能证明引文存在；公式含义、阅读顺序及图中规则需要人工审阅。",
        "离线快照证明所用文件版本，不证明来源网站地址已核实。",
    ]
    hostname = urlparse(source.url).hostname or ""
    if hostname == "example.invalid" or hostname.endswith(".invalid"):
        notes.append("来源URL为占位地址，出处以已核验的离线PDF为准。")
    if source.limitation:
        notes.append(source.limitation)
    return {
        "schema_version": SCHEMA_VERSION,
        "package_path": str(package_root),
        "archive_root": str(archive),
        "source_id": source.source_id,
        "snapshot_artifact_id": manifest["artifact_id"],
        "snapshot_manifest_hash": manifest["manifest_hash"],
        "source_title": source.title,
        "source_url": source.url,
        "extractor": {"name": "pypdf", "version": pypdf.__version__},
        "page_count": page_count,
        "pages": extracted,
        "review_notes": notes,
    }


def verify_materials(materials) -> None:
    """复验包引用、快照、抽取器版本及逐页原文，不新增摘要身份。"""
    if not isinstance(materials, dict) or materials.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("材料schema_version无效")
    required = ("package_path", "archive_root", "source_id", "pages", "extractor", "page_count")
    if any(key not in materials for key in required):
        raise ValueError("材料缺少来源路径、抽取器或页信息")
    if type(materials["page_count"]) is not int or materials["page_count"] < 1:
        raise ValueError("材料page_count必须是正整数")
    page_records = materials["pages"]
    if not isinstance(page_records, list) or any(
        not isinstance(page, dict) or "pdf_page" not in page or "lines" not in page
        for page in page_records
    ):
        raise ValueError("材料pages必须包含物理页码与行文本")
    pages = _selected_pages([page["pdf_page"] for page in page_records])
    for page in page_records:
        if not isinstance(page["lines"], list) or any(not isinstance(line, str) for line in page["lines"]):
            raise ValueError("材料lines必须是文本列表")
    current = extract_materials(
        materials["package_path"], materials["archive_root"], materials["source_id"], pages
    )
    for key, value in current.items():
        if materials.get(key) != value:
            raise ValueError(f"材料与已核验来源或抽取结果不一致：{key}，需重新提取并确认")
