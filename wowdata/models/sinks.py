from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, Any

import petl as etl

from wowdata.errors import WowDataUserError


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

    def write(self, table) -> None:
        if self.type == "csv":
            # PETL tocsv writes table to CSV
            etl.tocsv(table, self.uri, **self.options)
            return
        raise WowDataUserError(
            "E_SINK_WRITE_UNSUPPORTED",
            f"Sink type '{self.type}' cannot be written in v0.",
            hint="Currently supported sink types: csv.",
        )

    def __str__(self) -> str:
        return f'Sink("{self.uri}")  kind={self.type}'


def _infer_type_from_uri(uri: str) -> Optional[str]:
    ext = Path(uri).suffix.lower()
    if ext == ".csv":
        return "csv"
    # todo: extend later: .xlsx, .parquet, db urls, s3://, http(s)://, etc.
    return None
