"""由 pyproject 唯一声明校验 backend 产物的发布元数据。"""

from __future__ import annotations

from configparser import ConfigParser
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
import tarfile
from zipfile import ZipFile

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import Version

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


def _requirement(value: str, extra: str | None = None) -> tuple:
    requirement = Requirement(value)
    if extra is not None:
        marker = f'({requirement.marker}) and extra == "{extra}"' if requirement.marker else f'extra == "{extra}"'
        from packaging.markers import Marker

        requirement.marker = Marker(marker)
    return (
        canonicalize_name(requirement.name), tuple(sorted(canonicalize_name(item) for item in requirement.extras)),
        str(requirement.specifier), requirement.url or "", str(requirement.marker or ""),
    )


def expected_project_metadata(project: Path) -> dict[str, object]:
    """只读取声明；不在工具中维护版本、依赖或入口副本。"""
    declaration = tomllib.loads((project / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    optional = declaration.get("optional-dependencies", {})
    requirements = [_requirement(value) for value in declaration.get("dependencies", ())]
    requirements.extend(_requirement(value, canonicalize_name(extra))
                        for extra, values in optional.items() for value in values)
    return {
        "name": canonicalize_name(declaration["name"]),
        "version": str(Version(declaration["version"])),
        "summary": declaration.get("description", ""),
        "requires_python": str(SpecifierSet(declaration["requires-python"])),
        "requirements": sorted(requirements),
        "extras": sorted(canonicalize_name(extra) for extra in optional),
        "scripts": dict(sorted(declaration.get("scripts", {}).items())),
    }


def _artifact_metadata(content: bytes, entry_points: bytes) -> dict[str, object]:
    metadata = BytesParser(policy=default).parsebytes(content)
    parser = ConfigParser(interpolation=None)
    parser.optionxform = str
    parser.read_string(entry_points.decode("utf-8"))
    return {
        "name": canonicalize_name(metadata["Name"]),
        "version": str(Version(metadata["Version"])),
        "summary": metadata["Summary"] or "",
        "requires_python": str(SpecifierSet(metadata["Requires-Python"] or "")),
        "requirements": sorted(_requirement(value) for value in metadata.get_all("Requires-Dist", [])),
        "extras": sorted(canonicalize_name(extra) for extra in metadata.get_all("Provides-Extra", [])),
        "scripts": dict(sorted(parser.items("console_scripts"))) if parser.has_section("console_scripts") else {},
    }


def verify_distribution_metadata(*, project: Path, wheel: Path, sdist: Path) -> dict[str, object]:
    """元数据任一字段漂移即在产物发布前拒绝。"""
    expected = expected_project_metadata(project)
    with ZipFile(wheel) as archive:
        metadata_names = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
        entries = [name for name in archive.namelist() if name.endswith(".dist-info/entry_points.txt")]
        if len(metadata_names) != 1 or len(entries) != 1:
            raise ValueError("wheel 缺少唯一 metadata 或 entry point")
        wheel_metadata = _artifact_metadata(archive.read(metadata_names[0]), archive.read(entries[0]))
    with tarfile.open(sdist, "r:gz") as archive:
        members = [member for member in archive.getmembers() if member.isfile()]
        metadata_members = [member for member in members if len(Path(member.name).parts) == 2 and member.name.endswith("/PKG-INFO")]
        entry_members = [member for member in members if member.name.endswith(".egg-info/entry_points.txt")]
        if len(metadata_members) != 1 or len(entry_members) != 1:
            raise ValueError("sdist 缺少唯一 metadata 或 entry point")
        metadata_handle, entry_handle = archive.extractfile(metadata_members[0]), archive.extractfile(entry_members[0])
        if metadata_handle is None or entry_handle is None:
            raise ValueError("sdist metadata 无法读取")
        sdist_metadata = _artifact_metadata(metadata_handle.read(), entry_handle.read())
    for kind, observed in (("wheel", wheel_metadata), ("sdist", sdist_metadata)):
        changed = sorted(key for key in expected if expected[key] != observed[key])
        if changed:
            raise ValueError(f"{kind} 发布元数据与 pyproject 不一致: {changed}")
    return expected
