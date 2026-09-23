"""单次 Runtime 调用内的工件完整验证生命周期。"""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from contextvars import ContextVar
from types import MappingProxyType
from typing import Callable, Iterator, Mapping, TypeVar

from .errors import SnapshotIntegrityError


_T = TypeVar("_T")
_ACTIVE_VERIFICATION_SESSION: ContextVar[RunScopedArtifactVerification | None] = (
    ContextVar("research_pipeline_artifact_verification_session", default=None)
)


class RunScopedArtifactVerification:
    """由 Runtime Supervisor 持有、不会写入 checkpoint 的短生命周期缓存。"""

    def __init__(self, run_id: str) -> None:
        if not isinstance(run_id, str) or not run_id.strip():
            raise SnapshotIntegrityError("artifact verification run_id 无效")
        self.run_id = run_id
        self._verified: dict[tuple[str, str], object] = {}
        self._full_verification_counts: Counter[tuple[str, str]] = Counter()

    def resolve(
        self,
        namespace: str,
        identity: str,
        verifier: Callable[[], _T],
    ) -> _T:
        """首次调用完整验证；同一 run 后续调用复用冻结结果。"""

        key = self._key(namespace, identity)
        existing = self._verified.get(key)
        if existing is not None:
            return existing  # type: ignore[return-value]
        verified = verifier()
        self._verified[key] = verified
        self._full_verification_counts[key] += 1
        return verified

    def remember(self, namespace: str, identity: str, verified: _T) -> _T:
        """记录刚在本调用栈完成完整检查并原子发布的结果。"""

        key = self._key(namespace, identity)
        existing = self._verified.get(key)
        if existing is not None and existing != verified:
            raise SnapshotIntegrityError("同一 run 的 verified artifact identity 冲突")
        if existing is None:
            self._verified[key] = verified
            self._full_verification_counts[key] += 1
        return verified

    @property
    def full_verification_counts(self) -> Mapping[tuple[str, str], int]:
        return MappingProxyType(dict(self._full_verification_counts))

    def count(self, namespace: str) -> int:
        return sum(
            count
            for (current_namespace, _identity), count in self._full_verification_counts.items()
            if current_namespace == namespace
        )

    @staticmethod
    def _key(namespace: str, identity: str) -> tuple[str, str]:
        if (
            not isinstance(namespace, str)
            or not namespace.strip()
            or not isinstance(identity, str)
            or not identity.strip()
        ):
            raise SnapshotIntegrityError("artifact verification identity 无效")
        return namespace, identity

    def __reduce__(self):
        raise TypeError("RunScopedArtifactVerification 不允许序列化或跨 run 复用")


def current_artifact_verification() -> RunScopedArtifactVerification | None:
    return _ACTIVE_VERIFICATION_SESSION.get()


@contextmanager
def activate_artifact_verification(
    session: RunScopedArtifactVerification,
) -> Iterator[None]:
    """把 Supervisor 会话只绑定到当前普通算子调用栈。"""

    token = _ACTIVE_VERIFICATION_SESSION.set(session)
    try:
        yield
    finally:
        _ACTIVE_VERIFICATION_SESSION.reset(token)


__all__ = [
    "RunScopedArtifactVerification",
    "activate_artifact_verification",
    "current_artifact_verification",
]
