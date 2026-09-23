"""研究输入、输出和数据文件的统一真实路径策略。"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Mapping

from .errors import SnapshotIntegrityError


@dataclass(frozen=True)
class PathRolePolicy:
    """按真实路径校验角色不重叠，允许根内受控链接。"""

    def validate(
        self,
        roles: Mapping[str, str | Path],
        *,
        read_only_roles: tuple[str, ...] = (),
    ) -> Mapping[str, Path]:
        if not roles or any(not isinstance(role, str) or not role.strip() for role in roles):
            raise SnapshotIntegrityError("路径角色不能为空")
        unknown_read_only = set(read_only_roles) - set(roles)
        if unknown_read_only:
            raise SnapshotIntegrityError("只读路径角色未在策略中声明")
        read_only = set(read_only_roles)
        resolved: dict[str, Path] = {}
        for role, raw_path in roles.items():
            path = self._absolute(raw_path, role)
            resolved[role] = path.resolve(strict=False)
        items = sorted(resolved.items())
        for index, (left_role, left) in enumerate(items):
            for right_role, right in items[index + 1:]:
                if left_role in read_only and right_role in read_only:
                    continue
                if self._overlaps(left, right) or self._same_existing_path(left, right):
                    raise SnapshotIntegrityError(
                        f"路径角色冲突: {left_role} 与 {right_role} 不得相等或互为祖先/后代"
                    )
        return MappingProxyType(resolved)

    def resolve_root(self, path: str | Path, *, role: str) -> Path:
        absolute = self._absolute(path, role)
        resolved = absolute.resolve(strict=False)
        if not resolved.is_dir():
            raise SnapshotIntegrityError(f"路径角色 {role} 必须是已存在目录")
        return resolved

    def resolve_contained_path(
        self,
        *,
        allowed_root: str | Path,
        candidate: str | Path,
        root_role: str,
        path_role: str,
        expected_kind: str,
    ) -> Path:
        root = self.resolve_root(allowed_root, role=root_role)
        raw_candidate = Path(candidate)
        lexical = raw_candidate if raw_candidate.is_absolute() else root / raw_candidate
        lexical = self._absolute(lexical, path_role)
        resolved = lexical.resolve(strict=False)
        if not self._contained(root, resolved):
            raise SnapshotIntegrityError(f"路径角色 {path_role} 越出 {root_role}")
        if expected_kind == "file" and not resolved.is_file():
            raise SnapshotIntegrityError(f"路径角色 {path_role} 必须是已存在文件")
        if expected_kind == "directory" and not resolved.is_dir():
            raise SnapshotIntegrityError(f"路径角色 {path_role} 必须是已存在目录")
        if expected_kind not in {"file", "directory", "any"}:
            raise SnapshotIntegrityError("路径类型策略无效")
        return resolved

    def resolve_manifest_files(
        self,
        *,
        allowed_root: str | Path,
        relative_paths: tuple[str, ...],
        root_role: str,
        file_role: str,
    ) -> tuple[Path, ...]:
        if not relative_paths or len(relative_paths) != len(set(relative_paths)):
            raise SnapshotIntegrityError(f"路径角色 {file_role} 的 manifest 文件必须非空且唯一")
        files = []
        for relative in relative_paths:
            normalized = self._safe_relative_path(relative, file_role)
            files.append(
                self.resolve_contained_path(
                    allowed_root=allowed_root,
                    candidate=Path(*normalized.parts),
                    root_role=root_role,
                    path_role=file_role,
                    expected_kind="file",
                )
            )
        return tuple(files)

    def discover_files(
        self,
        root: str | Path,
        *,
        suffix: str,
        root_role: str,
        file_role: str,
    ) -> Mapping[str, Path]:
        resolved_root = self.resolve_root(root, role=root_role)
        found: dict[str, Path] = {}
        for directory, dirnames, filenames in self.walk(resolved_root):
            directory_path = Path(directory)
            for name in sorted(filenames):
                lexical = directory_path / name
                if lexical.suffix.lower() != suffix.lower():
                    continue
                resolved = self.resolve_contained_path(
                    allowed_root=resolved_root,
                    candidate=lexical,
                    root_role=root_role,
                    path_role=file_role,
                    expected_kind="file",
                )
                relative = lexical.relative_to(resolved_root).as_posix()
                if relative in found:
                    raise SnapshotIntegrityError(f"路径角色 {file_role} 发现重复文件")
                found[relative] = resolved
        return MappingProxyType(dict(sorted(found.items())))

    @staticmethod
    def _safe_relative_path(value: object, role: str) -> PurePosixPath:
        if not isinstance(value, str) or not value or "\\" in value:
            raise SnapshotIntegrityError(f"路径角色 {role} 不是安全 POSIX 相对路径")
        path = PurePosixPath(value)
        if path.is_absolute() or ":" in path.parts[0] or any(
            part in {"", ".", ".."} for part in path.parts
        ):
            raise SnapshotIntegrityError(f"路径角色 {role} 不是安全 POSIX 相对路径")
        return path

    @staticmethod
    def _absolute(value: str | Path, role: str) -> Path:
        try:
            path = Path(value).absolute()
        except (OSError, TypeError, ValueError) as exc:
            raise SnapshotIntegrityError(f"路径角色 {role} 无法解析") from exc
        return path

    @staticmethod
    def _contained(root: Path, candidate: Path) -> bool:
        return candidate == root or root in candidate.parents

    @classmethod
    def _overlaps(cls, left: Path, right: Path) -> bool:
        return cls._contained(left, right) or cls._contained(right, left)

    @staticmethod
    def _same_existing_path(left: Path, right: Path) -> bool:
        if not left.exists() or not right.exists():
            return False
        try:
            return left.samefile(right)
        except OSError as exc:
            raise SnapshotIntegrityError("路径角色无法完成 samefile 检查") from exc

    def walk(self, root: str | Path):
        """遍历根内真实目录，保留链接路径名；循环目录不能无限展开。"""
        resolved_root = self.resolve_root(root, role="directory_root")
        for directory, dirnames, filenames in os.walk(resolved_root, followlinks=True):
            current = Path(directory)
            ancestors = {current.resolve()}
            ancestors.update(parent.resolve() for parent in current.parents
                             if parent == resolved_root or resolved_root in parent.parents)
            for name in dirnames:
                target = self.resolve_contained_path(
                    allowed_root=resolved_root, candidate=current / name,
                    root_role="directory_root", path_role="directory", expected_kind="directory",
                )
                if target in ancestors:
                    raise SnapshotIntegrityError("目录链接形成循环")
            yield directory, dirnames, filenames


__all__ = ["PathRolePolicy"]
