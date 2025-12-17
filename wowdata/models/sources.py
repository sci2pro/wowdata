from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Union, Dict, Any, List

import petl as etl

from experiments.core import FrictionlessSchema, _normalize_inline_schema, Resource, Detector
from wowdata.models.pipeline import Pipeline
from wowdata.models.sinks import _infer_type_from_uri
from wowdata.errors import WowDataUserError


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
            raise WowDataUserError(
                "E_SOURCE_TYPE_INFER",
                f"Could not infer Source type from uri='{self.uri}'.",
                hint="Provide type explicitly, e.g. Source('file.txt', type='csv').",
            )

        if self.type != "csv":
            raise WowDataUserError(
                "E_SOURCE_TYPE_UNSUPPORTED",
                f"Source type '{self.type}' is not supported in v0.",
                hint="Currently supported source types: csv.",
            )

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
        raise WowDataUserError(
            "E_SOURCE_TABLE_UNSUPPORTED",
            f"Source type '{self.type}' cannot be materialized as a table in v0.",
            hint="Currently supported source types: csv.",
        )

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
                                            f"Column '{h}' inferred as string but {frac:.0%} of sampled values look numeric; "
                                            "consider an explicit cast, e.g. "
                                            f"cast(types={{'{h}': 'integer'}}) or cast(types={{'{h}': 'number'}})."
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

    def schema_warnings(self) -> List[str]:
        """Warnings produced during the last schema inference pass."""
        return list(self._schema_warnings)

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
