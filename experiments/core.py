from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import petl as etl

try:
    from frictionless import Resource, Detector
except Exception:  # pragma: no cover
    Resource = None
    Detector = None

FrictionlessSchema = Dict[str, Any]


def _infer_type_from_uri(uri: str) -> Optional[str]:
    ext = Path(uri).suffix.lower()
    if ext == ".csv":
        return "csv"
    # todo: extend later: .xlsx, .parquet, db urls, s3://, http(s)://, etc.
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

    _inferred_schema: Optional[Dict[str, Any]] = field(default=None, init=False, repr=False)
    _schema_warnings: List[str] = field(default_factory=list, init=False, repr=False)

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

    # --- peek to see the schema ---
    def peek_schema(self, *, sample_rows: int = 200, force: bool = False) -> Dict[str, Any]:
        """Infer a Frictionless-like schema from a bounded sample.

        - Never loads full data.
        - Uses Frictionless when available.
        - Caches the inferred schema on the Source instance.
        """
        if self._inferred_schema is not None and not force:
            return self._inferred_schema

        warnings: List[str] = []

        # If user provided an inline schema, prefer it (it is authoritative)
        if isinstance(self.schema, dict):
            sch = _normalize_inline_schema(self.schema)
            object.__setattr__(self, "_inferred_schema", sch)
            object.__setattr__(self, "_schema_warnings", warnings)
            return sch

        # If frictionless isn't available, fall back to an unknown schema
        if Resource is None or Detector is None:
            warnings.append("Frictionless not available; schema inference disabled.")
            sch = {"fields": []}
            object.__setattr__(self, "_inferred_schema", sch)
            object.__setattr__(self, "_schema_warnings", warnings)
            return sch

        # Use Frictionless to infer schema from a bounded sample
        try:
            detector = Detector(sample_size=sample_rows)
            resource = Resource(path=self.uri, detector=detector)
            resource.infer()
            desc = resource.to_descriptor()
            sch = desc.get("schema") or {"fields": []}
            sch = _normalize_inline_schema(sch) if isinstance(sch, dict) else {"fields": []}

            # Heuristic warnings to make inference "safe" for ordinary users
            fields = sch.get("fields", []) if isinstance(sch, dict) else []
            if not fields:
                warnings.append("No fields inferred; the file may be empty or unreadable with current options.")
            else:
                # If frictionless inferred many strings, check whether columns look numeric in the sample
                try:
                    sample = etl.data(etl.head(self.table(), sample_rows))
                    # sample includes header row at index 0
                    if sample and len(sample) > 1:
                        header = list(sample[0])
                        rows = sample[1:]
                        # Count numeric-looking values per column
                        numeric_counts: Dict[str, int] = {h: 0 for h in header}
                        nonempty_counts: Dict[str, int] = {h: 0 for h in header}

                        def _looks_number(v: Any) -> bool:
                            if v is None:
                                return False
                            if isinstance(v, (int, float)):
                                return True
                            if isinstance(v, str):
                                s = v.strip()
                                if s == "":
                                    return False
                                try:
                                    float(s)
                                    return True
                                except Exception:
                                    return False
                            return False

                        for r in rows:
                            for h, v in zip(header, r):
                                if v is None:
                                    continue
                                if isinstance(v, str) and v.strip() == "":
                                    continue
                                nonempty_counts[h] += 1
                                if _looks_number(v):
                                    numeric_counts[h] += 1

                        # Map inferred field types
                        inferred_types = {f.get("name"): f.get("type", "any") for f in fields if isinstance(f, dict)}
                        for h in header:
                            t = inferred_types.get(h)
                            if t == "string":
                                ne = nonempty_counts.get(h, 0)
                                if ne >= 5:
                                    frac = numeric_counts.get(h, 0) / max(ne, 1)
                                    if frac >= 0.8:
                                        warnings.append(
                                            f"Column '{h}' inferred as string but {frac:.0%} of sampled values look numeric; consider cast(types={{'{h}': 'number'}})."
                                        )
                except Exception as _e:
                    # Sampling heuristics are best-effort; don't fail schema inference
                    pass
        except Exception as e:
            warnings.append(f"Schema inference failed: {e}")
            sch = {"fields": []}

        object.__setattr__(self, "_inferred_schema", sch)
        object.__setattr__(self, "_schema_warnings", warnings)
        return sch

    def __str__(self) -> str:
        kind = self.type or "unknown"
        hdr = f'Source("{self.uri}")  kind={kind}'
        if self.schema is None:
            sch = "  schema=(none; infer at peepholes/validate)"
        elif isinstance(self.schema, str):
            sch = f"  schema_ref={self.schema}"
        else:
            sch = "  schema=(inline)"

        inferred = self._inferred_schema
        if inferred is not None:
            fields = inferred.get("fields", []) if isinstance(inferred, dict) else []
            if isinstance(fields, list) and fields:
                pairs: List[str] = []
                for f in fields:
                    if isinstance(f, dict) and isinstance(f.get("name"), str):
                        pairs.append(f"{f['name']}:{f.get('type', 'any')}")
                if pairs:
                    sch += "  inferred={" + ", ".join(pairs[:8])
                    if len(pairs) > 8:
                        sch += ", …"
                    sch += "}"
            warns = self._schema_warnings
            if warns:
                sch += "  warnings=" + str(len(warns))
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
            types = p.get("types")
            if not isinstance(types, dict) or not types:
                raise ValueError("cast requires params.types: {col: type, ...}")

            on_error = p.get("on_error", "fail")
            if on_error not in {"fail", "null", "keep"}:
                raise ValueError("cast params.on_error must be one of: fail, null, keep")

            def _to_int(v: Any) -> Any:
                if v is None:
                    return None
                if isinstance(v, int):
                    return v
                if isinstance(v, float) and v.is_integer():
                    return int(v)
                if isinstance(v, str):
                    s = v.strip()
                    if s == "":
                        return None
                    return int(float(s))
                return int(v)

            def _to_number(v: Any) -> Any:
                if v is None:
                    return None
                if isinstance(v, (int, float)):
                    return float(v)
                if isinstance(v, str):
                    s = v.strip()
                    if s == "":
                        return None
                    return float(s)
                return float(v)

            def _to_bool(v: Any) -> Any:
                if v is None:
                    return None
                if isinstance(v, bool):
                    return v
                if isinstance(v, (int, float)):
                    return bool(v)
                if isinstance(v, str):
                    s = v.strip().lower()
                    if s in {"", "null", "none", "na", "nan"}:
                        return None
                    if s in {"true", "t", "yes", "y", "1"}:
                        return True
                    if s in {"false", "f", "no", "n", "0"}:
                        return False
                raise ValueError(f"Cannot coerce {v!r} to boolean")

            def _to_str(v: Any) -> Any:
                if v is None:
                    return None
                return str(v)

            # date/datetime: keep conservative; expect ISO strings
            from datetime import date, datetime

            def _to_date(v: Any) -> Any:
                if v is None:
                    return None
                if isinstance(v, date) and not isinstance(v, datetime):
                    return v
                if isinstance(v, datetime):
                    return v.date()
                if isinstance(v, str):
                    s = v.strip()
                    if s == "":
                        return None
                    return date.fromisoformat(s)
                raise ValueError(f"Cannot coerce {v!r} to date")

            def _to_datetime(v: Any) -> Any:
                if v is None:
                    return None
                if isinstance(v, datetime):
                    return v
                if isinstance(v, str):
                    s = v.strip()
                    if s == "":
                        return None
                    return datetime.fromisoformat(s)
                raise ValueError(f"Cannot coerce {v!r} to datetime")

            converters = {
                "integer": _to_int,
                "number": _to_number,
                "boolean": _to_bool,
                "string": _to_str,
                "date": _to_date,
                "datetime": _to_datetime,
            }

            def _wrap(conv):
                def f(v: Any) -> Any:
                    try:
                        return conv(v)
                    except Exception:
                        if on_error == "fail":
                            raise
                        if on_error == "null":
                            return None
                        return v  # keep
                return f

            out = table
            for col, typ in types.items():
                if not isinstance(col, str) or not col:
                    raise ValueError("cast types keys must be non-empty strings")
                if not isinstance(typ, str) or typ not in converters:
                    raise ValueError(f"Unsupported cast type for '{col}': {typ!r}")

                out = etl.convert(out, col, _wrap(converters[typ]))

            return out

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
            # PETL tocsv writes table to CSV
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
