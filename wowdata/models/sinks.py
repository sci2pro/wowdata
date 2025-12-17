from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import os

import petl as etl

from wowdata.errors import WowDataUserError
from wowdata.util import _infer_type_from_uri


@dataclass(frozen=True)
class Sink:
    uri: str
    type: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        inferred = self.type or _infer_type_from_uri(self.uri)
        object.__setattr__(self, "type", inferred)
        if self.type is None:
            raise WowDataUserError(
                "E_SINK_TYPE_INFER",
                f"Could not infer Sink type from uri='{self.uri}'.",
                hint="Provide type explicitly, e.g. Sink('out.data', type='csv').",
            )

        if self.type != "csv":
            raise WowDataUserError(
                "E_SINK_TYPE_UNSUPPORTED",
                f"Sink type '{self.type}' is not supported in v0.",
                hint="Currently supported sink types: csv.",
            )

        # Fail fast: ensure the output directory exists and is writable before running the pipeline.
        if self.type == "csv":
            parent = os.path.dirname(self.uri) or "."
            if not os.path.isdir(parent):
                raise WowDataUserError(
                    "E_SINK_DIR_NOT_FOUND",
                    f"Output directory does not exist: '{parent}'.",
                    hint="Create the directory or choose a different output path.",
                )
            if not os.access(parent, os.W_OK):
                raise WowDataUserError(
                    "E_SINK_NOT_WRITABLE",
                    f"Output directory is not writable: '{parent}'.",
                    hint="Check permissions or choose a different output location.",
                )

    def write(self, table) -> None:
        if self.type == "csv":
            try:
                etl.tocsv(table, self.uri, **self.options)
            except FileNotFoundError as e:
                parent = os.path.dirname(self.uri) or "."
                raise WowDataUserError(
                    "E_SINK_DIR_NOT_FOUND",
                    f"Output directory does not exist: '{parent}'.",
                    hint="Create the directory or choose a different output path.",
                ) from e
            except PermissionError as e:
                parent = os.path.dirname(self.uri) or "."
                raise WowDataUserError(
                    "E_SINK_NOT_WRITABLE",
                    f"Cannot write to output directory: '{parent}'.",
                    hint="Check permissions or choose a different output location.",
                ) from e
            except Exception as e:
                raise WowDataUserError(
                    "E_SINK_WRITE",
                    f"Could not write sink '{self.uri}': {type(e).__name__}: {e}",
                    hint="Check file permissions and Sink options (delimiter/encoding).",
                ) from e
            return
        raise WowDataUserError(
            "E_SINK_WRITE_UNSUPPORTED",
            f"Sink type '{self.type}' cannot be written in v0.",
            hint="Currently supported sink types: csv.",
        )

    def __str__(self) -> str:
        return f'Sink("{self.uri}")  kind={self.type}'


