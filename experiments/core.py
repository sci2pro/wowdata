from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import petl as etl

FrictionlessSchema = Dict[str, Any]


def _infer_type_from_uri(uri: str) -> Optional[str]:
    ext = Path(uri).suffix.lower()
    if ext == ".csv":
        return "csv"
    # extend later: .xlsx, .parquet, db urls, s3://, http(s)://, etc.
    return None


def _normalize_inline_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expect frictionless-ish schema:
      {"fields":[{"name":"age","type":"integer"}, ...]}
    We keep it permissive for now.
    """
    if not isinstance(schema, dict):
        raise TypeError("schema must be a dict")
    if "fields" not in schema:
        # allow passing full frictionless resource descriptor later; for now keep simple
        return {"fields": []}
    if not isinstance(schema["fields"], list):
        raise TypeError("schema['fields'] must be a list")
    return schema


@dataclass(frozen=True)
class Source:
    uri: str
    type: Optional[str] = None
    schema: Optional[Union[str, FrictionlessSchema]] = None
    options: Dict[str, Any] = field(default_factory=dict)

    # --- UX controls (bounded by design) ---
    preview_rows: int = 5
    preview_max_chars: int = 6_000  # prevent huge terminal spam

    def __post_init__(self) -> None:
        inferred = self.type or _infer_type_from_uri(self.uri)
        object.__setattr__(self, "type", inferred)

        if self.type is None:
            raise ValueError(
                f"Could not infer Source type from uri='{self.uri}'. "
                f"Provide type= explicitly (e.g., Source('file.txt', type='csv'))."
            )

        if self.type != "csv":
            raise NotImplementedError(f"Source type '{self.type}' not supported yet (v0 supports csv only).")

        if self.schema is not None and isinstance(self.schema, dict):
            _normalize_inline_schema(self.schema)

    # ---------- PETL table (lazy) ----------
    def table(self):
        """
        Return a PETL table. PETL is lazy for many sources; reading occurs on iteration.
        """
        if self.type == "csv":
            # PETL fromcsv supports kwargs like delimiter, encoding, etc.
            return etl.fromcsv(self.uri, **self.options)
        raise NotImplementedError(self.type)

    # ---------- Peepholes / inspection ----------
    def head(self, n: Optional[int] = None):
        n = n or self.preview_rows
        return etl.head(self.table(), n)

    def _preview_str(self) -> str:
        """
        Bounded preview string. Does NOT load full dataset.
        """
        t = self.head(self.preview_rows)
        s = str(etl.look(t))  # returns a string table view
        if len(s) > self.preview_max_chars:
            s = s[: self.preview_max_chars] + "\n… (truncated)"
        return s

    def __str__(self) -> str:
        kind = self.type or "unknown"
        hdr = f'Source("{self.uri}")  kind={kind}'
        sch = ""
        if self.schema is None:
            sch = "  schema=(none; infer at peepholes/validate)"
        elif isinstance(self.schema, str):
            sch = f"  schema_ref={self.schema}"
        else:
            sch = "  schema=(inline)"
        return hdr + sch + "\nPreview:\n" + self._preview_str()

    # ---------- Pipeline composition ----------
    def __gt__(self, other: Any) -> "Pipeline":
        """
        Source > Transform or Source > Sink creates a Pipeline.
        (Do NOT encourage chained a > b > c in one expression; Python chains comparisons.)
        """
        return Pipeline(self).then(other)


@dataclass(frozen=True)
class Transform:
    op: str
    params: Dict[str, Any] = field(default_factory=dict)

    def apply(self, table, *, context: "PipelineContext"):
        """
        Apply this transform to a PETL table (single-input transforms).
        Multi-input transforms are handled by Pipeline (join/filter_in/union).
        """
        op = self.op
        p = self.params

        if op == "filter":
            # v0: string expressions will be compiled later.
            # Here we keep a placeholder to wire the system; implement parsing next.
            raise NotImplementedError("Transform('filter') execution not implemented yet (needs expr compiler).")

        if op == "select":
            cols = p.get("columns")
            if not isinstance(cols, list) or not cols:
                raise ValueError("select requires params.columns: [..]")
            return etl.cut(table, *cols)

        if op == "drop":
            cols = p.get("columns")
            if not isinstance(cols, list) or not cols:
                raise ValueError("drop requires params.columns: [..]")
            return etl.cutout(table, *cols)

        if op == "cast":
            # v0: cast in PETL is usually via convert/convertall; real typing should be checked by frictionless validate.
            # We'll implement a conservative cast later once we decide coercion rules.
            return table

        if op == "validate":
            # runtime validation checkpoint (frictionless integration comes next)
            context.checkpoints.append(("validate", p))
            return table

        raise NotImplementedError(f"Transform op '{op}' not implemented in v0 executor yet.")

    def __str__(self) -> str:
        return f"Transform(op={self.op}, params={self.params})"


@dataclass(frozen=True)
class Sink:
    uri: str
    type: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        inferred = self.type or _infer_type_from_uri(self.uri)
        object.__setattr__(self, "type", inferred)
        if self.type is None:
            raise ValueError(f"Could not infer Sink type from uri='{self.uri}'. Provide type= explicitly.")
        if self.type != "csv":
            # allow parquet later, etc.
            raise NotImplementedError(f"Sink type '{self.type}' not supported yet (v0 supports csv only).")

    def write(self, table) -> None:
        if self.type == "csv":
            # PETL tocvs writes table to CSV
            etl.tocsv(table, self.uri, **self.options)
            return
        raise NotImplementedError(self.type)

    def __str__(self) -> str:
        return f'Sink("{self.uri}")  kind={self.type}'


@dataclass
class PipelineContext:
    checkpoints: List[Tuple[str, Dict[str, Any]]] = field(default_factory=list)
    # later: frictionless reports, timing, rowcounts, etc.


@dataclass
class Pipeline:
    """
    v0: linear pipelines only.
    """
    start: Source
    steps: List[Union[Transform, Sink]] = field(default_factory=list)

    def then(self, step: Union[Transform, Sink]) -> "Pipeline":
        if not isinstance(step, (Transform, Sink)):
            raise TypeError("Pipeline.then expects a Transform or Sink")
        return Pipeline(self.start, self.steps + [step])

    def __str__(self) -> str:
        parts = [f"Pipeline(start={self.start.uri})"]
        for s in self.steps:
            parts.append(f"  -> {s}")
        return "\n".join(parts)

    def run(self) -> PipelineContext:
        ctx = PipelineContext()
        table = self.start.table()

        for step in self.steps:
            if isinstance(step, Transform):
                table = step.apply(table, context=ctx)
            elif isinstance(step, Sink):
                step.write(table)
            else:
                raise RuntimeError("Unknown step type")

        return ctx
