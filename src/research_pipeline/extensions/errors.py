"""受控扩展合同错误。"""

from __future__ import annotations

from typing import Mapping

from research_pipeline.platform.errors import MainlineError


class ExtensionError(MainlineError):
    error_code = "extension_invalid"

    def __init__(
        self,
        message: str,
        *,
        failure_payload: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        if failure_payload is not None:
            self.failure_payload = dict(failure_payload)


__all__ = ["ExtensionError"]
