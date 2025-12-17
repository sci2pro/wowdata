from __future__ import annotations

from typing import Optional


class WowDataUserError(Exception):
    """An instructional error intended for end users.

    Use this for mistakes in the pipeline definition (invalid params, missing columns, etc.).
    It carries a short error code and an optional hint to guide the user.
    """

    def __init__(self, code: str, message: str, *, hint: Optional[str] = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.hint = hint

    def __str__(self) -> str:
        base = f"[{self.code}] {self.message}"
        if self.hint:
            return base + f"\nHint: {self.hint}"
        return base
