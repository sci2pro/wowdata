from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union, Optional, Type, Callable

import petl as etl

from wowdata.errors import WowDataUserError

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None

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
        raise WowDataUserError(
            "E_SCHEMA_INLINE_TYPE",
            "Inline schema must be a mapping (dict).",
            hint="Example: {'fields': [{'name': 'age', 'type': 'integer'}]}",
        )
    if "fields" not in schema:
        # allow passing full frictionless resource descriptor later; for now keep simple
        return {"fields": []}
    if not isinstance(schema["fields"], list):
        raise WowDataUserError(
            "E_SCHEMA_INLINE_FIELDS",
            "Inline schema['fields'] must be a list.",
            hint="Example: {'fields': [{'name': 'age', 'type': 'integer'}]}",
        )
    return schema


def _source_from_descriptor(desc: Union[str, Dict[str, Any]]) -> Source:
    """Create a Source from a descriptor.

    Supported forms:
      - string URI: "file.csv"
      - mapping: {"uri": "file.csv", "type": "csv", "schema": {...}, "options": {...}}
    """
    if isinstance(desc, str):
        return Source(desc)
    if isinstance(desc, dict):
        uri = desc.get("uri")
        if not isinstance(uri, str) or not uri:
            raise WowDataUserError(
                "E_JOIN_RIGHT",
                "join params.right mapping must include a non-empty 'uri' string.",
                hint="Example: {'uri': 'other.csv', 'type': 'csv', 'options': {...}}",
            )
        return Source(
            uri,
            type=desc.get("type"),
            schema=desc.get("schema"),
            options=desc.get("options") or {},
        )
    raise WowDataUserError(
        "E_JOIN_RIGHT",
        "join params.right must be a URI string or a mapping descriptor.",
        hint="Example: Transform('join', params={'right': 'other.csv', 'on': ['id']})",
    )


def _schema_field_names(schema: Optional[FrictionlessSchema]) -> List[str]:
    if not schema or not isinstance(schema, dict):
        return []
    fields = schema.get("fields")
    if not isinstance(fields, list):
        return []
    out: List[str] = []
    for f in fields:
        if isinstance(f, dict) and isinstance(f.get("name"), str):
            out.append(f["name"])
    return out


def _source_to_ir(src: Source) -> Dict[str, Any]:
    d: Dict[str, Any] = {"uri": src.uri}
    if src.type is not None:
        d["type"] = src.type
    if src.schema is not None:
        d["schema"] = src.schema
    if src.options:
        d["options"] = dict(src.options)
    return d


def _sink_to_ir(sink: Sink) -> Dict[str, Any]:
    d: Dict[str, Any] = {"uri": sink.uri}
    if sink.type is not None:
        d["type"] = sink.type
    if sink.options:
        d["options"] = dict(sink.options)
    return d


def _transform_to_ir(t: Transform) -> Dict[str, Any]:
    d: Dict[str, Any] = {"op": t.op}
    if t.params:
        d["params"] = dict(t.params)
    if t.output_schema_override is not None:
        d["output_schema"] = t.output_schema_override
    return d


def _source_from_ir(d: Dict[str, Any]) -> Source:
    if not isinstance(d, dict):
        raise WowDataUserError(
            "E_IR_SOURCE",
            "IR source must be a mapping.",
            hint="Example: start: {uri: people.csv, type: csv}",
        )
    uri = d.get("uri")
    if not isinstance(uri, str) or not uri:
        raise WowDataUserError(
            "E_IR_SOURCE",
            "IR source requires a non-empty 'uri' string.",
            hint="Example: start: {uri: people.csv}",
        )
    return Source(
        uri,
        type=d.get("type"),
        schema=d.get("schema"),
        options=d.get("options") or {},
    )


def _sink_from_ir(d: Dict[str, Any]) -> Sink:
    if not isinstance(d, dict):
        raise WowDataUserError(
            "E_IR_SINK",
            "IR sink must be a mapping.",
            hint="Example: {sink: {uri: out.csv}}",
        )
    uri = d.get("uri")
    if not isinstance(uri, str) or not uri:
        raise WowDataUserError(
            "E_IR_SINK",
            "IR sink requires a non-empty 'uri' string.",
            hint="Example: {sink: {uri: out.csv}}",
        )
    return Sink(
        uri,
        type=d.get("type"),
        options=d.get("options") or {},
    )


def _transform_from_ir(d: Dict[str, Any]) -> Transform:
    if not isinstance(d, dict):
        raise WowDataUserError(
            "E_IR_TRANSFORM",
            "IR transform must be a mapping.",
            hint="Example: {transform: {op: select, params: {columns: [a,b]}}}",
        )
    op = d.get("op")
    if not isinstance(op, str) or not op:
        raise WowDataUserError(
            "E_IR_TRANSFORM",
            "IR transform requires a non-empty 'op' string.",
            hint="Example: {transform: {op: filter, params: {where: \"…\"}}}",
        )
    params = d.get("params") or {}
    if not isinstance(params, dict):
        raise WowDataUserError(
            "E_IR_TRANSFORM",
            "IR transform 'params' must be a mapping.",
            hint="Example: params: {where: \"…\"}",
        )
    out_schema = d.get("output_schema")
    if out_schema is not None and not isinstance(out_schema, dict):
        raise WowDataUserError(
            "E_IR_TRANSFORM_SCHEMA",
            "IR transform 'output_schema' must be a mapping when provided.",
            hint="Example: output_schema: {fields: [{name: x, type: integer}]}",
        )
    return Transform(op, params=dict(params), output_schema_override=out_schema)


# --- IR normalization helpers ---

def _is_probably_url(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://") or s.startswith("s3://")


def _norm_path(p: str, *, base_dir: Optional[Path]) -> str:
    """Normalize a URI/path relative to a base directory (when provided).

    - Leaves absolute paths and URLs unchanged.
    - If base_dir is provided and p is relative, returns an absolute resolved path.
    """
    if not isinstance(p, str) or not p:
        return p
    if _is_probably_url(p):
        return p
    try:
        pp = Path(p)
    except Exception:
        return p
    if pp.is_absolute():
        return str(pp)
    if base_dir is None:
        return p
    return str((base_dir / pp).resolve())


def _normalize_ir(ir: Any, *, base_dir: Optional[Path]) -> Dict[str, Any]:
    """Normalize IR structure and paths.

    Guarantees:
      - returns a dict with keys: wowdata, pipeline
      - pipeline.start.uri is normalized
      - any sink.uri is normalized
      - any join params.right (string or descriptor.uri) is normalized
      - missing/None options become {}
      - missing transform params become {}

    This does not change semantics; it makes the IR portable and deterministic.
    """
    if not isinstance(ir, dict):
        raise WowDataUserError(
            "E_IR_ROOT",
            "IR must be a mapping at the root.",
            hint="Expected keys: wowdata, pipeline.",
        )

    ir2: Dict[str, Any] = dict(ir)

    # Default version
    if ir2.get("wowdata") is None:
        ir2["wowdata"] = 0

    version = ir2.get("wowdata")
    if version != 0:
        raise WowDataUserError(
            "E_IR_VERSION",
            f"Unsupported IR version: {version!r}.",
            hint="Supported: wowdata: 0",
        )

    pipe = ir2.get("pipeline")
    if not isinstance(pipe, dict):
        raise WowDataUserError(
            "E_IR_PIPELINE",
            "IR requires a 'pipeline' mapping.",
            hint="Example: {wowdata: 0, pipeline: {start: {...}, steps: [...]}}",
        )

    pipe2: Dict[str, Any] = dict(pipe)

    # Normalize start
    start = pipe2.get("start")
    if not isinstance(start, dict):
        raise WowDataUserError(
            "E_IR_SOURCE",
            "IR pipeline.start must be a mapping.",
            hint="Example: start: {uri: people.csv, type: csv}",
        )
    start2: Dict[str, Any] = dict(start)
    u = start2.get("uri")
    if isinstance(u, str):
        start2["uri"] = _norm_path(u, base_dir=base_dir)
    if "options" in start2 and start2["options"] is None:
        start2["options"] = {}
    pipe2["start"] = start2

    # Normalize steps
    steps = pipe2.get("steps")
    if steps is None:
        steps = []
    if not isinstance(steps, list):
        raise WowDataUserError(
            "E_IR_STEPS",
            "IR pipeline.steps must be a list.",
            hint="Example: steps: [{transform: {...}}, {sink: {...}}]",
        )

    norm_steps: List[Dict[str, Any]] = []
    for i, item in enumerate(steps):
        if not isinstance(item, dict) or len(item) != 1:
            raise WowDataUserError(
                "E_IR_STEP",
                f"IR step #{i} must be a mapping with exactly one key: 'transform' or 'sink'.",
                hint=str(item),
            )

        if "sink" in item:
            s = item["sink"]
            if not isinstance(s, dict):
                raise WowDataUserError(
                    "E_IR_SINK",
                    "IR sink must be a mapping.",
                    hint="Example: - sink: {uri: out.csv}",
                )
            s2: Dict[str, Any] = dict(s)
            su = s2.get("uri")
            if isinstance(su, str):
                s2["uri"] = _norm_path(su, base_dir=base_dir)
            if "options" in s2 and s2["options"] is None:
                s2["options"] = {}
            norm_steps.append({"sink": s2})
            continue

        if "transform" in item:
            t = item["transform"]
            if not isinstance(t, dict):
                raise WowDataUserError(
                    "E_IR_TRANSFORM",
                    "IR transform must be a mapping.",
                    hint="Example: - transform: {op: select, params: {...}}",
                )
            t2: Dict[str, Any] = dict(t)
            params = t2.get("params")
            if params is None:
                params = {}
                t2["params"] = params
            if not isinstance(params, dict):
                raise WowDataUserError(
                    "E_IR_TRANSFORM",
                    "IR transform 'params' must be a mapping.",
                    hint="Example: params: {where: \"age >= 30\"}",
                )

            # Join: normalize params.right if present
            if t2.get("op") == "join" and "right" in params:
                r = params.get("right")
                if isinstance(r, str):
                    params["right"] = _norm_path(r, base_dir=base_dir)
                elif isinstance(r, dict):
                    r2: Dict[str, Any] = dict(r)
                    ru = r2.get("uri")
                    if isinstance(ru, str):
                        r2["uri"] = _norm_path(ru, base_dir=base_dir)
                    if "options" in r2 and r2["options"] is None:
                        r2["options"] = {}
                    params["right"] = r2

            norm_steps.append({"transform": t2})
            continue

        raise WowDataUserError(
            "E_IR_STEP",
            f"IR step #{i} must be 'transform' or 'sink'.",
            hint="Example: {transform: {op: select, params: {...}}}",
        )

    pipe2["steps"] = norm_steps

    return {"wowdata": 0, "pipeline": pipe2}


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


# ---------------- Transform implementation registry ----------------

class TransformImpl:
    """Internal implementation for a Transform op.

    Users interact with `Transform(op, params)`.
    Implementations are registered by op name and invoked by `Transform.apply`.
    """

    op: str = ""

    @classmethod
    def validate_params(cls, params: Dict[str, Any], input_schema: Optional[FrictionlessSchema] = None) -> None:
        # default: no validation
        return

    @classmethod
    def apply(cls, table, *, params: Dict[str, Any], context: "PipelineContext"):
        raise WowDataUserError(
            "E_OP_NOT_IMPL",
            f"Transform op '{cls.op}' is not implemented in v0 executor yet.",
            hint="Implement it as a TransformImpl and register it.",
        )

    @classmethod
    def output_schema(cls, input_schema: Optional[FrictionlessSchema], params: Dict[str, Any]) -> Optional[
        FrictionlessSchema]:
        """Infer the output schema given an input schema.

        Return None if the schema cannot be determined statically.
        """
        return input_schema


TRANSFORM_REGISTRY: Dict[str, Type[TransformImpl]] = {}


def register_transform(op: str) -> Callable[[Type[TransformImpl]], Type[TransformImpl]]:
    """Decorator to register a TransformImpl under an op string."""

    def deco(cls: Type[TransformImpl]) -> Type[TransformImpl]:
        cls.op = op
        TRANSFORM_REGISTRY[op] = cls
        return cls

    return deco


# =========================
# Shared expression language (tokenizer + parser)
# Used by filter and derive to keep the DSL consistent.
# =========================

class _ExprTok:
    __slots__ = ("typ", "val", "pos")

    def __init__(self, typ: str, val: Any, pos: int):
        self.typ = typ
        self.val = val
        self.pos = pos


def _expr_tokenize(src: str, *, allow_arith: bool) -> List["_ExprTok"]:
    import re

    _re_ws = re.compile(r"\s+")
    _re_ident = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
    _re_number = re.compile(r"(?:\d+\.\d*|\d*\.\d+|\d+)")
    KEYWORDS = {"and", "or", "not", "true", "false", "null"}

    OPS_2 = {"==", "!=", ">=", "<="}
    OPS_1 = {">", "<", "(", ")"}
    if allow_arith:
        OPS_1 |= {"+", "-", "*", "/"}

    out: List[_ExprTok] = []
    i = 0
    n = len(src)

    while i < n:
        m = _re_ws.match(src, i)
        if m:
            i = m.end()
            continue

        if src[i] in ("'", '"'):
            q = src[i]
            j = i + 1
            buf = []
            while j < n:
                ch = src[j]
                if ch == "\\" and j + 1 < n:
                    buf.append(src[j + 1])
                    j += 2
                    continue
                if ch == q:
                    out.append(_ExprTok("STR", "".join(buf), i))
                    i = j + 1
                    break
                buf.append(ch)
                j += 1
            else:
                raise WowDataUserError(
                    "E_EXPR_PARSE",
                    "Unterminated string literal in expression.",
                    hint=src,
                )
            continue

        two = src[i: i + 2]
        if two in OPS_2:
            out.append(_ExprTok("OP", two, i))
            i += 2
            continue

        if src[i] in OPS_1:
            out.append(_ExprTok("OP", src[i], i))
            i += 1
            continue

        m = _re_number.match(src, i)
        if m:
            s = m.group(0)
            out.append(_ExprTok("NUM", float(s) if ("." in s) else int(s), i))
            i = m.end()
            continue

        m = _re_ident.match(src, i)
        if m:
            s = m.group(0)
            low = s.lower()
            if low in KEYWORDS:
                out.append(_ExprTok("KW", low, i))
            else:
                out.append(_ExprTok("IDENT", s, i))
            i = m.end()
            continue

        raise WowDataUserError(
            "E_EXPR_PARSE",
            f"Unexpected character {src[i]!r} in expression.",
            hint=f"At position {i}: {src}\n" + (" " * i) + "^",
        )

    out.append(_ExprTok("EOF", None, n))
    return out


def _expr_parse(src: str, *, allow_arith: bool) -> Any:
    toks = _expr_tokenize(src, allow_arith=allow_arith)
    k = 0

    def _peek() -> _ExprTok:
        return toks[k]

    def _eat(expected_typ: str, expected_val: Optional[str] = None) -> _ExprTok:
        nonlocal k
        t = toks[k]
        if t.typ != expected_typ:
            raise WowDataUserError(
                "E_EXPR_PARSE",
                f"Expected {expected_typ} but found {t.typ}.",
                hint=f"At position {t.pos}: {src}\n" + (" " * t.pos) + "^",
            )
        if expected_val is not None and t.val != expected_val:
            raise WowDataUserError(
                "E_EXPR_PARSE",
                f"Expected '{expected_val}' but found '{t.val}'.",
                hint=f"At position {t.pos}: {src}\n" + (" " * t.pos) + "^",
            )
        k += 1
        return t

    def parse_expr():
        return parse_or()

    def parse_or():
        node = parse_and()
        while _peek().typ == "KW" and _peek().val == "or":
            _eat("KW", "or")
            rhs = parse_and()
            node = ("or", node, rhs)
        return node

    def parse_and():
        node = parse_cmp()
        while _peek().typ == "KW" and _peek().val == "and":
            _eat("KW", "and")
            rhs = parse_cmp()
            node = ("and", node, rhs)
        return node

    def parse_cmp():
        left = parse_add() if allow_arith else parse_unary()
        if _peek().typ == "OP" and _peek().val in {"==", "!=", ">=", "<=", ">", "<"}:
            op_tok = _eat("OP")
            right = parse_add() if allow_arith else parse_unary()
            return ("cmp", op_tok.val, left, right)
        return left

    def parse_add():
        node = parse_mul()
        while _peek().typ == "OP" and _peek().val in {"+", "-"}:
            op_tok = _eat("OP")
            rhs = parse_mul()
            node = ("bin", op_tok.val, node, rhs)
        return node

    def parse_mul():
        node = parse_unary()
        while _peek().typ == "OP" and _peek().val in {"*", "/"}:
            op_tok = _eat("OP")
            rhs = parse_unary()
            node = ("bin", op_tok.val, node, rhs)
        return node

    def parse_unary():
        if _peek().typ == "KW" and _peek().val == "not":
            _eat("KW", "not")
            inner = parse_unary()
            return ("not", inner)
        if allow_arith and _peek().typ == "OP" and _peek().val == "-":
            _eat("OP", "-")
            inner = parse_unary()
            return ("neg", inner)
        return parse_atom()

    def parse_atom():
        t = _peek()
        if t.typ == "OP" and t.val == "(":
            _eat("OP", "(")
            node = parse_expr()
            _eat("OP", ")")
            return node
        if t.typ == "IDENT":
            _eat("IDENT")
            return ("col", t.val, t.pos)
        if t.typ == "NUM":
            _eat("NUM")
            return ("lit", t.val)
        if t.typ == "STR":
            _eat("STR")
            return ("lit", t.val)
        if t.typ == "KW" and t.val in {"true", "false", "null"}:
            _eat("KW")
            if t.val == "true":
                return ("lit", True)
            if t.val == "false":
                return ("lit", False)
            return ("lit", None)
        raise WowDataUserError(
            "E_EXPR_PARSE",
            f"Unexpected token '{t.val}' in expression.",
            hint=f"At position {t.pos}: {src}\n" + (" " * t.pos) + "^",
        )

    ast = parse_expr()
    _eat("EOF")
    return ast


@register_transform("filter")
class FilterTransform(TransformImpl):
    @classmethod
    def validate_params(cls, params: Dict[str, Any], input_schema: Optional[FrictionlessSchema] = None) -> None:
        where = params.get("where")
        if not isinstance(where, str) or not where.strip():
            raise WowDataUserError(
                "E_FILTER_PARAMS",
                "filter requires params.where as a non-empty expression string.",
                hint="Example: Transform('filter', params={'where': \"age >= 30 and country == 'KE'\"})",
            )
        strict = params.get("strict", True)
        if not isinstance(strict, bool):
            raise WowDataUserError(
                "E_FILTER_PARAMS",
                "filter params.strict must be a boolean.",
                hint="Example: Transform('filter', params={'where': 'age >= 30', 'strict': true})",
            )

    @classmethod
    def apply(cls, table, *, params: Dict[str, Any], context: "PipelineContext"):
        where = params.get("where")
        strict = params.get("strict", True)

        columns = list(etl.header(table))
        colset = set(columns)

        import difflib

        try:
            ast = _expr_parse(where, allow_arith=False)
        except WowDataUserError as e:
            if getattr(e, "code", None) == "E_EXPR_PARSE":
                raise WowDataUserError(
                    "E_FILTER_PARSE",
                    getattr(e, "message", "Invalid filter expression."),
                    hint=getattr(e, "hint", None),
                ) from e
            raise

        # ---------------- Compile/eval ----------------
        def _suggest(col: str) -> str:
            matches = difflib.get_close_matches(col, columns, n=3, cutoff=0.6)
            if matches:
                return f"Did you mean {matches[0]!r}?"
            return "Available columns: " + ", ".join(columns)

        idx_map = {name: i for i, name in enumerate(columns)}

        def _get_col(row: Any, name: str, pos: int) -> Any:
            if name not in colset:
                raise WowDataUserError(
                    "E_FILTER_UNKNOWN_COL",
                    f"Unknown column {name!r} in filter expression.",
                    hint=_suggest(name),
                )
            try:
                return row[name]
            except Exception:
                pass
            if isinstance(row, dict):
                return row.get(name)
            try:
                return row[idx_map[name]]
            except Exception:
                return None

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

        def _to_float(v: Any) -> float:
            if isinstance(v, (int, float)):
                return float(v)
            if isinstance(v, str):
                return float(v.strip())
            return float(v)

        def _eval(node, row: Any) -> Any:
            tag = node[0] if isinstance(node, tuple) else None
            if tag is None:
                return node
            if tag == "lit":
                return node[1]
            if tag == "col":
                _, name, pos = node
                return _get_col(row, name, pos)
            if tag == "and":
                return bool(_eval(node[1], row)) and bool(_eval(node[2], row))
            if tag == "or":
                return bool(_eval(node[1], row)) or bool(_eval(node[2], row))
            if tag == "not":
                return not bool(_eval(node[1], row))
            if tag == "cmp":
                _, opx, left, right = node
                a = _eval(left, row)
                b = _eval(right, row)

                if opx in {"==", "!="}:
                    return (a == b) if opx == "==" else (a != b)
                if a is None or b is None:
                    return False

                if _looks_number(a) and _looks_number(b):
                    af = _to_float(a)
                    bf = _to_float(b)
                    if opx == ">":
                        return af > bf
                    if opx == ">=":
                        return af >= bf
                    if opx == "<":
                        return af < bf
                    if opx == "<=":
                        return af <= bf

                if isinstance(a, str) and isinstance(b, str):
                    if opx == ">":
                        return a > b
                    if opx == ">=":
                        return a >= b
                    if opx == "<":
                        return a < b
                    if opx == "<=":
                        return a <= b

                if strict:
                    raise WowDataUserError(
                        "E_FILTER_TYPE",
                        f"Type mismatch in filter comparison: {a!r} {opx} {b!r}.",
                        hint="Consider applying Transform('cast', ...) earlier in the pipeline.",
                    )
                return False

            raise WowDataUserError(
                "E_FILTER_UNSUPPORTED",
                "Unsupported construct in filter expression.",
                hint="Use comparisons, and/or/not, literals, parentheses, and column names.",
            )

        return etl.select(table, lambda r: bool(_eval(ast, r)))

    @classmethod
    def output_schema(cls, input_schema: Optional[FrictionlessSchema], params: Dict[str, Any]) -> Optional[
        FrictionlessSchema]:
        # filtering does not change schema
        return input_schema


@register_transform("select")
class SelectTransform(TransformImpl):
    @classmethod
    def validate_params(cls, params: Dict[str, Any], input_schema: Optional[FrictionlessSchema] = None) -> None:
        cols = params.get("columns")
        if not isinstance(cols, list) or not cols:
            raise WowDataUserError(
                "E_SELECT_PARAMS",
                "select requires params.columns as a non-empty list of column names.",
                hint="Example: Transform('select', params={'columns': ['person_id', 'age']})",
            )

        if input_schema and isinstance(input_schema, dict):
            fields = input_schema.get("fields")
            if isinstance(fields, list) and fields:
                names = {f.get("name") for f in fields if isinstance(f, dict)}
                missing = [c for c in cols if c not in names]
                if missing:
                    raise WowDataUserError(
                        "E_SELECT_UNKNOWN_COL",
                        "select refers to column(s) not present in the current schema: " + str(missing) + ".",
                        hint="Check spelling/case, or apply select after a transform that introduces these columns.",
                    )

    @classmethod
    def apply(cls, table, *, params: Dict[str, Any], context: "PipelineContext"):
        cols = params.get("columns")
        return etl.cut(table, *cols)

    @classmethod
    def output_schema(cls, input_schema: Optional[FrictionlessSchema], params: Dict[str, Any]) -> Optional[
        FrictionlessSchema]:
        if not input_schema or "fields" not in input_schema:
            return input_schema
        cols = params.get("columns") or []
        fields = [f for f in input_schema.get("fields", []) if f.get("name") in cols]
        return {"fields": fields}


@register_transform("drop")
class DropTransform(TransformImpl):
    @classmethod
    def validate_params(cls, params: Dict[str, Any], input_schema: Optional[FrictionlessSchema] = None) -> None:
        cols = params.get("columns")
        if not isinstance(cols, list) or not cols:
            raise WowDataUserError(
                "E_DROP_PARAMS",
                "drop requires params.columns as a non-empty list of column names.",
                hint="Example: Transform('drop', params={'columns': ['debug_col']})",
            )

        if input_schema and isinstance(input_schema, dict):
            fields = input_schema.get("fields")
            if isinstance(fields, list) and fields:
                names = {f.get("name") for f in fields if isinstance(f, dict)}
                missing = [c for c in cols if c not in names]
                if missing:
                    raise WowDataUserError(
                        "E_DROP_UNKNOWN_COL",
                        "drop refers to column(s) not present in the current schema: " + str(missing) + ".",
                        hint="Check spelling/case, or drop after a transform that introduces these columns.",
                    )

    @classmethod
    def apply(cls, table, *, params: Dict[str, Any], context: "PipelineContext"):
        cols = params.get("columns")
        return etl.cutout(table, *cols)

    @classmethod
    def output_schema(cls, input_schema: Optional[FrictionlessSchema], params: Dict[str, Any]) -> Optional[
        FrictionlessSchema]:
        if not input_schema or "fields" not in input_schema:
            return input_schema
        drop_cols = set(params.get("columns") or [])
        fields = [f for f in input_schema.get("fields", []) if f.get("name") not in drop_cols]
        return {"fields": fields}


@register_transform("cast")
class CastTransform(TransformImpl):
    @classmethod
    def validate_params(cls, params: Dict[str, Any], input_schema: Optional[FrictionlessSchema] = None) -> None:
        types = params.get("types")
        if not isinstance(types, dict) or not types:
            raise WowDataUserError(
                "E_CAST_TYPES",
                "cast requires params.types as a non-empty mapping: {column: type, ...}.",
                hint="Example: Transform('cast', params={'types': {'age': 'integer'}, 'on_error': 'null'})",
            )
        on_error = params.get("on_error", "fail")
        if on_error not in {"fail", "null", "keep"}:
            raise WowDataUserError(
                "E_CAST_ON_ERROR",
                "cast params.on_error must be one of: 'fail', 'null', 'keep'.",
                hint="Example: Transform('cast', params={'types': {...}, 'on_error': 'null'})",
            )

    @classmethod
    def apply(cls, table, *, params: Dict[str, Any], context: "PipelineContext"):
        types = params.get("types")
        on_error = params.get("on_error", "fail")

        header = list(etl.header(table))
        missing = [c for c in types.keys() if c not in header]
        if missing:
            raise WowDataUserError(
                "E_CAST_MISSING_COL",
                "cast refers to column(s) not present in the current table: " + str(missing) + ".",
                hint=(
                    "This usually happens if you applied select/drop before cast. "
                    "Fix by moving cast earlier in the pipeline, or update cast.types to match the selected columns."
                ),
            )

        def _to_int(v: Any) -> Any:
            if v is None:
                return None
            if isinstance(v, int):
                return v
            if isinstance(v, float) and v.is_integer():
                return int(v)
            try:
                if isinstance(v, str):
                    s = v.strip()
                    if s == "":
                        return None
                    return int(float(s))
                return int(v)
            except Exception as e:
                raise WowDataUserError(
                    "E_CAST_COERCE",
                    f"Cannot coerce value {v!r} to integer.",
                    hint="Ensure the column contains numeric values (e.g. 12, 12.0). Use on_error='null' or on_error='keep' to handle messy rows."
                ) from e

        def _to_number(v: Any) -> Any:
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return float(v)
            try:
                if isinstance(v, str):
                    s = v.strip()
                    if s == "":
                        return None
                    return float(s)
                return float(v)
            except Exception as e:
                raise WowDataUserError(
                    "E_CAST_COERCE",
                    f"Cannot coerce value {v!r} to number.",
                    hint="Ensure the column contains numeric values (e.g. 12, 12.5). Use on_error='null' or on_error='keep' to handle messy rows."
                ) from e

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
            raise WowDataUserError(
                "E_CAST_COERCE",
                f"Cannot coerce value {v!r} to boolean.",
                hint="Ensure the column contains values like true/false, yes/no, 1/0, or cast it to string first."
            )

        def _to_str(v: Any) -> Any:
            if v is None:
                return None
            return str(v)

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
                try:
                    return date.fromisoformat(s)
                except Exception as e:
                    raise WowDataUserError(
                        "E_CAST_COERCE",
                        f"Cannot coerce value {v!r} to date.",
                        hint="Ensure the value is an ISO date string (YYYY-MM-DD). Use on_error='null' or on_error='keep' to handle messy rows."
                    ) from e
            raise WowDataUserError(
                "E_CAST_COERCE",
                f"Cannot coerce value {v!r} to date.",
                hint="Ensure the value is an ISO date string (YYYY-MM-DD). Use on_error='null' or on_error='keep' to handle messy rows."
            )

        def _to_datetime(v: Any) -> Any:
            if v is None:
                return None
            if isinstance(v, datetime):
                return v
            if isinstance(v, str):
                s = v.strip()
                if s == "":
                    return None
                try:
                    return datetime.fromisoformat(s)
                except Exception as e:
                    raise WowDataUserError(
                        "E_CAST_COERCE",
                        f"Cannot coerce value {v!r} to datetime.",
                        hint="Ensure the value is an ISO datetime string (e.g. YYYY-MM-DDTHH:MM:SS). Use on_error='null' or on_error='keep' to handle messy rows."
                    ) from e
            raise WowDataUserError(
                "E_CAST_COERCE",
                f"Cannot coerce value {v!r} to datetime.",
                hint="Ensure the value is an ISO datetime string (e.g. YYYY-MM-DDTHH:MM:SS). Use on_error='null' or on_error='keep' to handle messy rows."
            )

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
                except WowDataUserError as e:
                    code = getattr(e, "code", None)
                    if code != "E_CAST_COERCE":
                        # Semantic/config violations must always surface.
                        raise
                    # Value-level coercion failure: apply policy.
                    if on_error == "fail":
                        raise
                    if on_error == "null":
                        return None
                    return v
                except Exception as e:
                    # Unexpected exception: treat as internal bug, never suppress via on_error.
                    raise WowDataUserError(
                        "E_CAST_INTERNAL",
                        f"Internal error while casting value {v!r}: {type(e).__name__}: {e}",
                        hint="This should not happen. Please report a bug with a minimal reproducible example."
                    ) from e

            return f

        out = table
        for col, typ in types.items():
            if not isinstance(col, str) or not col:
                raise WowDataUserError(
                    "E_CAST_KEY",
                    "cast types keys must be non-empty strings.",
                    hint="Example: {'age': 'integer', 'income': 'number'}",
                )
            if not isinstance(typ, str) or typ not in converters:
                raise WowDataUserError(
                    "E_CAST_TYPE_UNSUPPORTED",
                    f"Unsupported cast type for '{col}': {typ!r}.",
                    hint="Supported types: integer, number, boolean, string, date, datetime.",
                )
            out = etl.convert(out, col, _wrap(converters[typ]))

        return out

    @classmethod
    def output_schema(cls, input_schema: Optional[FrictionlessSchema], params: Dict[str, Any]) -> Optional[
        FrictionlessSchema]:
        if not input_schema or "fields" not in input_schema:
            return input_schema
        types = params.get("types") or {}
        out_fields = []
        for f in input_schema.get("fields", []):
            name = f.get("name")
            if name in types:
                nf = dict(f)
                nf["type"] = types[name]
                out_fields.append(nf)
            else:
                out_fields.append(f)
        return {"fields": out_fields}


@register_transform("validate")
class ValidateTransform(TransformImpl):
    @classmethod
    def validate_params(cls, params: Dict[str, Any], input_schema: Optional[FrictionlessSchema] = None) -> None:
        sample_rows = params.get("sample_rows", 5000)
        if not isinstance(sample_rows, int) or sample_rows <= 0:
            raise WowDataUserError(
                "E_VALIDATE_PARAMS",
                "validate params.sample_rows must be a positive integer.",
                hint="Example: Transform('validate', params={'sample_rows': 1000})",
            )

        fail = params.get("fail", True)
        if not isinstance(fail, bool):
            raise WowDataUserError(
                "E_VALIDATE_PARAMS",
                "validate params.fail must be a boolean.",
                hint="Example: Transform('validate', params={'fail': false})",
            )

        strict_schema = params.get("strict_schema", True)
        if not isinstance(strict_schema, bool):
            raise WowDataUserError(
                "E_VALIDATE_PARAMS",
                "validate params.strict_schema must be a boolean.",
                hint="Example: Transform('validate', params={'strict_schema': false})",
            )

    @classmethod
    def apply(cls, table, *, params: Dict[str, Any], context: "PipelineContext"):
        sample_rows = params.get("sample_rows", 5000)
        fail = params.get("fail", True)
        strict_schema = params.get("strict_schema", True)

        # Require Frictionless for validation.
        if Resource is None:
            raise WowDataUserError(
                "E_VALIDATE_IMPORT",
                "Validation requires the 'frictionless' dependency, but it is not available.",
                hint="Install it with: pip install frictionless",
            )

        # Best-available schema (carried through the pipeline).
        sch: Dict[str, Any] = {}
        if isinstance(getattr(context, "schema", None), dict):
            sch = dict(context.schema)  # shallow copy

        # Bounded sample (header + N rows).
        try:
            sample_tbl = etl.head(table, sample_rows + 1)
            header = list(etl.header(sample_tbl))
            data_rows = list(etl.data(sample_tbl))  # data only (no header)
        except Exception as e:
            raise WowDataUserError(
                "E_VALIDATE_READ",
                f"validate could not read a sample of rows for validation: {e}",
                hint="Check that your Source options (delimiter/encoding) are correct.",
            ) from e

        if not data_rows:
            result = {
                "valid": True,
                "rows_checked": 0,
                "errors": 0,
                "warnings": 0,
                "notes": ["No rows to validate (empty table)."],
            }
            context.checkpoints.append(("validate", {"params": dict(params), "result": result}))
            context.validations.append(result)
            return table

        # Frictionless validates best when given records (dicts) keyed by header.
        # Pad/truncate rows to header length for deterministic column alignment.
        def _row_to_record(r):
            rr = list(r)
            if len(rr) < len(header):
                rr = rr + [None] * (len(header) - len(rr))
            if len(rr) > len(header):
                rr = rr[: len(header)]
            return dict(zip(header, rr))

        records = [_row_to_record(r) for r in data_rows]

        # Teach explicitness: if strict_schema and no schema fields, fail loudly.
        fields = sch.get("fields") if isinstance(sch, dict) else None
        expected_types = {}
        if isinstance(fields, list):
            for f in fields:
                if isinstance(f, dict) and isinstance(f.get("name"), str):
                    expected_types[f["name"]] = f.get("type", "any")
        if strict_schema and (not isinstance(fields, list) or not fields):
            raise WowDataUserError(
                "E_VALIDATE_NO_SCHEMA",
                "validate requires a known schema, but none is available.",
                hint=(
                    "Provide an inline schema on Source(..., schema={...}), "
                    "or enable Frictionless inference, or set params.strict_schema=false."
                ),
            )

        # Validate via Frictionless (in-memory).
        try:
            schema_obj = None
            if sch:
                try:
                    # Frictionless expects Schema-like objects; passing a raw dict can crash on some versions.
                    from frictionless import Schema as _FrSchema  # type: ignore
                    schema_obj = _FrSchema.from_descriptor(sch) if hasattr(_FrSchema, "from_descriptor") else _FrSchema(
                        sch)
                except Exception:
                    # Fall back to passing no schema if conversion fails; strict_schema above should catch missing schema.
                    schema_obj = None

            resource = Resource(data=records, schema=schema_obj)
            # Prefer cast-aware validation when supported, so "12" can satisfy integer, etc.
            try:
                report = resource.validate(cast=True)
            except TypeError:
                report = resource.validate()
        except Exception as e:
            raise WowDataUserError(
                "E_VALIDATE_FAILED_TO_RUN",
                f"validate could not run validation: {e}",
                hint="Check that your schema is well-formed and matches the data.",
            ) from e

        # Summarize report into a human-scale artifact.
        try:
            report_desc = report.to_descriptor() if hasattr(report, "to_descriptor") else {}
        except Exception:
            report_desc = {}

        valid = bool(getattr(report, "valid", False))
        tasks = report_desc.get("tasks") or []

        err_list: List[Dict[str, Any]] = []
        warn_list: List[Dict[str, Any]] = []
        for t in tasks:
            if isinstance(t, dict):
                errs = t.get("errors") or []
                if isinstance(errs, list):
                    for er in errs:
                        if isinstance(er, dict):
                            err_list.append(er)
                warns = t.get("warnings") or []
                if isinstance(warns, list):
                    for wr in warns:
                        if isinstance(wr, dict):
                            warn_list.append(wr)

        preview_errors: List[str] = []
        for er in err_list[:5]:
            rown = er.get("rowNumber")
            fieldn = er.get("fieldName")

            # Try to show the actual offending value (from the sampled records).
            chosen_val = None
            if isinstance(rown, int) and rown >= 1 and isinstance(fieldn, str):
                # Frictionless rowNumber is often 1-based and may include the header row (so first data row is rowNumber=2).
                candidate_idxs = [rown - 2, rown - 1]  # header-inclusive first, then data-only fallback

                for idx in candidate_idxs:
                    if 0 <= idx < len(records):
                        rec = records[idx]
                        if isinstance(rec, dict) and fieldn in rec:
                            chosen_val = rec.get(fieldn)
                            break

            # Determine an "actual" type label for teaching-oriented errors.
            def _wow_type(v: Any) -> str:
                if v is None:
                    return "null"
                if isinstance(v, bool):
                    return "boolean"
                if isinstance(v, int) and not isinstance(v, bool):
                    return "integer"
                if isinstance(v, float):
                    return "number"
                try:
                    from datetime import date, datetime
                    if isinstance(v, datetime):
                        return "datetime"
                    if isinstance(v, date):
                        return "date"
                except Exception:
                    pass
                # CSV and most PETL sources yield strings
                return "string"

            expected = expected_types.get(fieldn) if isinstance(fieldn, str) else None
            if expected is not None and isinstance(rown, int) and isinstance(fieldn, str):
                actual = _wow_type(chosen_val)
                preview_errors.append(
                    f'value type is "{actual}" (expected {expected!r}), row {rown}, field {fieldn!r}, value={chosen_val!r}'
                )
                continue

            # Fallback: keep Frictionless note/message if we can't build a precise cell-level message.
            note = er.get("note") or er.get("message") or "Validation error"
            if isinstance(note, str):
                import re
                note = re.sub(r'type is "([A-Za-z0-9_]+)/default"', r'type is "\1"', note)
            loc = []
            if rown is not None:
                loc.append(f"row {rown}")
            if fieldn:
                loc.append(f"field {fieldn!r}")
            where = (", " + ", ".join(loc)) if loc else ""
            preview_errors.append(f"{note}{where}")

        # Remove any stray  artifacts from the result dict
        def _clean_result(d):
            if isinstance(d, dict):
                return {k: _clean_result(v) for k, v in d.items() if k != ""}
            elif isinstance(d, list):
                return [_clean_result(v) for v in d]
            return d

        result = {
            "valid": valid,
            "rows_checked": len(data_rows),
            "errors": len(err_list),
            "warnings": len(warn_list),
            "error_preview": preview_errors,
        }
        result = _clean_result(result)

        context.checkpoints.append(("validate", {"params": dict(params), "result": result}))
        context.validations.append(result)

        if (not valid) and fail:
            hint = (
                "Fix the data or adjust the schema. First issues:\n- " + "\n- ".join(preview_errors)
                if preview_errors
                else "Fix the data or adjust the schema. Inspect context.validations for details."
            )
            raise WowDataUserError(
                "E_VALIDATE_INVALID",
                "Validation failed: the data does not match the expected schema.",
                hint=hint,
            )

        return table


@dataclass(frozen=True)
class Transform:
    op: str
    params: Dict[str, Any] = field(default_factory=dict)
    output_schema_override: Optional[FrictionlessSchema] = None

    def apply(self, table, *, context: "PipelineContext"):
        """
        Apply this transform to a PETL table (single-input transforms).
        Multi-input transforms are handled by Pipeline (join/filter_in/union).
        """
        impl = TRANSFORM_REGISTRY.get(self.op)
        if impl is None:
            raise WowDataUserError(
                "E_OP_NOT_IMPL",
                f"Transform op '{self.op}' is not implemented in v0 executor yet.",
                hint="Supported ops: " + ", ".join(sorted(TRANSFORM_REGISTRY.keys())),
            )

        impl.validate_params(self.params, input_schema=getattr(context, "schema", None))
        return impl.apply(table, params=self.params, context=context)

    def output_schema(self, input_schema: Optional[FrictionlessSchema]) -> Optional[FrictionlessSchema]:
        if self.output_schema_override is not None:
            return self.output_schema_override
        impl = TRANSFORM_REGISTRY.get(self.op)
        if impl is None:
            return input_schema
        return impl.output_schema(input_schema, self.params)

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


@dataclass
class PipelineContext:
    checkpoints: List[Tuple[str, Dict[str, Any]]] = field(default_factory=list)
    schema: Optional[FrictionlessSchema] = None
    validations: List[Dict[str, Any]] = field(default_factory=list)
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
            raise WowDataUserError(
                "E_PIPELINE_STEP",
                "Pipeline.then expects a Transform or Sink.",
                hint="Example: pipe.then(Transform('select', params={...})) or pipe.then(Sink('out.csv')).",
            )

        # Enforce Source -> Transform* -> Sink* mental model: no transforms after a sink.
        if isinstance(step, Transform):
            if any(isinstance(s, Sink) for s in self.steps):
                raise WowDataUserError(
                    "E_PIPELINE_ORDER",
                    "A Transform cannot be added after a Sink.",
                    hint="Move the Sink to the end of the pipeline, or create a new Pipeline starting from the Sink output.",
                )

        return Pipeline(self.start, self.steps + [step])

    def __str__(self) -> str:
        parts = [f"Pipeline(start={self.start.uri})"]
        for s in self.steps:
            parts.append(f"  -> {s}")
        return "\n".join(parts)

    def run(self) -> PipelineContext:
        ctx = PipelineContext()
        ctx.schema = self.start.peek_schema()
        table = self.start.table()

        saw_sink = False
        for i, step in enumerate(self.steps):
            if isinstance(step, Transform):
                if saw_sink:
                    # Defensive: should be prevented by Pipeline.then / from_ir, but enforce at runtime too.
                    raise WowDataUserError(
                        "E_PIPELINE_ORDER",
                        f"Transform '{step.op}' appears after a Sink at step index {i}.",
                        hint="Reorder the pipeline so that all sinks come last.",
                    )

                table = step.apply(table, context=ctx)
                # keep a best-effort running schema for better validation and UI
                ctx.schema = step.output_schema(ctx.schema)

                # Deterministic checkpoint: record what happened after each transform.
                try:
                    hdr = list(etl.header(table))
                except Exception:
                    hdr = []
                try:
                    # Fixed-size preview: header + up to 5 data rows. Deterministic and inspectable.
                    preview_tbl = etl.head(table, 6)
                    preview_rows = list(etl.data(preview_tbl))
                except Exception:
                    preview_rows = []

                ctx.checkpoints.append(
                    (
                        "step",
                        {
                            "index": i,
                            "kind": "transform",
                            "op": step.op,
                            "params": dict(step.params),
                            "header": hdr,
                            "preview": preview_rows,
                        },
                    )
                )

            elif isinstance(step, Sink):
                saw_sink = True
                step.write(table)

                # Deterministic checkpoint: record sink execution.
                ctx.checkpoints.append(
                    (
                        "step",
                        {
                            "index": i,
                            "kind": "sink",
                            "uri": step.uri,
                            "type": step.type,
                            "options": dict(step.options),
                        },
                    )
                )

            else:
                raise WowDataUserError(
                    "E_PIPELINE_STEP_TYPE",
                    "Pipeline contains an unknown step type.",
                    hint="This should not happen if you only add Transform/Sink via Pipeline.then().",
                )

        return ctx

    def schema(self, *, sample_rows: int = 200, force: bool = False) -> Optional[FrictionlessSchema]:
        """Infer the pipeline's output schema without executing the full pipeline.

        - Uses Source.peek_schema() (bounded sample, Frictionless when available)
        - Applies each Transform's output_schema(...) in order
        - Returns None if it cannot be determined statically
        """
        sch: Optional[FrictionlessSchema] = self.start.peek_schema(sample_rows=sample_rows, force=force)

        for step in self.steps:
            if isinstance(step, Transform):
                sch = step.output_schema(sch)
            elif isinstance(step, Sink):
                # sinks don't change schema
                continue
            else:
                return None  # should be unreachable

        return sch

    def to_ir(self) -> Dict[str, Any]:
        """Serialize this pipeline to a YAML-friendly IR (dict)."""
        steps_ir: List[Dict[str, Any]] = []
        for s in self.steps:
            if isinstance(s, Transform):
                steps_ir.append({"transform": _transform_to_ir(s)})
            elif isinstance(s, Sink):
                steps_ir.append({"sink": _sink_to_ir(s)})
            else:
                raise WowDataUserError(
                    "E_IR_STEP",
                    "Pipeline contains an unknown step type; cannot serialize.",
                    hint="Only Transform and Sink steps are supported.",
                )

        return {
            "wowdata": 0,
            "pipeline": {
                "start": _source_to_ir(self.start),
                "steps": steps_ir,
            },
        }

    @classmethod
    def from_ir(cls, ir: Dict[str, Any], *, base_dir: Optional[Path] = None) -> "Pipeline":
        """Deserialize a pipeline from IR (dict)."""
        ir = _normalize_ir(ir, base_dir=base_dir)
        pipe = ir["pipeline"]

        start = _source_from_ir(pipe.get("start") or {})
        steps = pipe.get("steps") or []
        if not isinstance(steps, list):
            raise WowDataUserError(
                "E_IR_STEPS",
                "IR pipeline.steps must be a list.",
                hint="Example: steps: [{transform: {...}}, {sink: {...}}]",
            )

        out = Pipeline(start)
        for i, item in enumerate(steps):
            if not isinstance(item, dict) or len(item) != 1:
                raise WowDataUserError(
                    "E_IR_STEP",
                    f"IR step #{i} must be a mapping with exactly one key: 'transform' or 'sink'.",
                    hint=str(item),
                )
            if "transform" in item:
                out = out.then(_transform_from_ir(item["transform"]))
            elif "sink" in item:
                out = out.then(_sink_from_ir(item["sink"]))
            else:
                raise WowDataUserError(
                    "E_IR_STEP",
                    f"IR step #{i} must be 'transform' or 'sink'.",
                    hint="Example: {transform: {op: select, params: {...}}}",
                )

        return out

    def to_yaml(self, *, lock_schema: bool = False, sample_rows: int = 200, force: bool = False) -> str:
        """Dump IR to YAML string."""
        if yaml is None:
            raise WowDataUserError(
                "E_YAML_IMPORT",
                "PyYAML is not available; cannot serialize to YAML.",
                hint="Install dependency: pip install pyyaml",
            )
        pipe = self.lock_schema(sample_rows=sample_rows, force=force) if lock_schema else self
        return yaml.safe_dump(pipe.to_ir(), sort_keys=False)

    @classmethod
    def from_yaml(cls, text: str, *, base_dir: Optional[Path] = None) -> "Pipeline":
        """Load pipeline from YAML string."""
        if yaml is None:
            raise WowDataUserError(
                "E_YAML_IMPORT",
                "PyYAML is not available; cannot parse YAML.",
                hint="Install dependency: pip install pyyaml",
            )
        try:
            ir = yaml.safe_load(text)
        except Exception as e:
            raise WowDataUserError(
                "E_YAML_PARSE",
                f"Failed to parse YAML: {e}",
                hint="Check indentation and quoting.",
            )
        return cls.from_ir(ir, base_dir=base_dir)

    def save_yaml(self, path: Union[str, Path], *, lock_schema: bool = False, sample_rows: int = 200,
                  force: bool = False) -> None:
        """Write YAML IR to a file."""
        p = Path(path)
        p.write_text(self.to_yaml(lock_schema=lock_schema, sample_rows=sample_rows, force=force), encoding="utf-8")

    @classmethod
    def load_yaml(cls, path: Union[str, Path]) -> "Pipeline":
        """Load YAML IR from a file."""
        p = Path(path)
        return cls.from_yaml(p.read_text(encoding="utf-8"), base_dir=p.parent)

    def lock_schema(self, sample_rows: int = 200, force: bool = False) -> "Pipeline":
        sch = self.start.peek_schema(sample_rows=sample_rows, force=force)
        locked_steps = []
        for step in self.steps:
            if isinstance(step, Transform):
                sch = step.output_schema(sch)
                locked_steps.append(
                    Transform(step.op, params=dict(step.params), output_schema_override=sch)
                )
            else:
                locked_steps.append(step)
        return Pipeline(self.start, locked_steps)


@register_transform("derive")
class DeriveTransform(TransformImpl):
    @classmethod
    def validate_params(cls, params: Dict[str, Any], input_schema: Optional[FrictionlessSchema] = None) -> None:
        new = params.get("new")
        expr = params.get("expr")
        overwrite = params.get("overwrite", False)
        strict = params.get("strict", True)

        if not isinstance(new, str) or not new.strip():
            raise WowDataUserError(
                "E_DERIVE_PARAMS",
                "derive requires params.new as a non-empty column name string.",
                hint="Example: Transform('derive', params={'new': 'is_adult', 'expr': 'age >= 18'})",
            )
        if not isinstance(expr, str) or not expr.strip():
            raise WowDataUserError(
                "E_DERIVE_PARAMS",
                "derive requires params.expr as a non-empty expression string.",
                hint="Example: Transform('derive', params={'new': 'is_adult', 'expr': 'age >= 18'})",
            )
        if not isinstance(overwrite, bool):
            raise WowDataUserError(
                "E_DERIVE_PARAMS",
                "derive params.overwrite must be a boolean.",
                hint="Example: Transform('derive', params={'new': 'x', 'expr': '1', 'overwrite': true})",
            )
        if not isinstance(strict, bool):
            raise WowDataUserError(
                "E_DERIVE_PARAMS",
                "derive params.strict must be a boolean.",
                hint="Example: Transform('derive', params={'new': 'x', 'expr': 'a / b', 'strict': false})",
            )

        # Early schema-aware check for overwrite policy
        if input_schema and isinstance(input_schema, dict):
            fields = input_schema.get("fields")
            if isinstance(fields, list) and fields:
                names = {f.get("name") for f in fields if isinstance(f, dict)}
                if (new in names) and not overwrite:
                    raise WowDataUserError(
                        "E_DERIVE_EXISTS",
                        f"derive would create column '{new}' but it already exists in the current schema.",
                        hint="Set params.overwrite=true to replace it, or choose a different params.new.",
                    )

    @classmethod
    def apply(cls, table, *, params: Dict[str, Any], context: "PipelineContext"):
        new = params.get("new")
        expr = params.get("expr")
        overwrite = params.get("overwrite", False)
        strict = params.get("strict", True)

        columns = list(etl.header(table))
        colset = set(columns)
        idx_map = {name: i for i, name in enumerate(columns)}

        if (new in colset) and not overwrite:
            raise WowDataUserError(
                "E_DERIVE_EXISTS",
                f"derive would create column '{new}' but it already exists.",
                hint="Set params.overwrite=true to replace it, or choose a different params.new.",
            )

        import difflib

        try:
            ast = _expr_parse(expr, allow_arith=True)
        except WowDataUserError as e:
            if getattr(e, "code", None) == "E_EXPR_PARSE":
                raise WowDataUserError(
                    "E_DERIVE_PARSE",
                    getattr(e, "message", "Invalid derive expression."),
                    hint=getattr(e, "hint", None),
                ) from e
            raise

        # ---------------- Eval ----------------
        def _suggest(col: str) -> str:
            matches = difflib.get_close_matches(col, columns, n=3, cutoff=0.6)
            if matches:
                return f"Did you mean {matches[0]!r}?"
            return "Available columns: " + ", ".join(columns)

        def _get_col(row: Any, name: str, pos: int) -> Any:
            if name not in colset:
                raise WowDataUserError(
                    "E_DERIVE_UNKNOWN_COL",
                    f"Unknown column {name!r} in derive expression.",
                    hint=_suggest(name),
                )
            try:
                return row[name]
            except Exception:
                pass
            if isinstance(row, dict):
                return row.get(name)
            try:
                return row[idx_map[name]]
            except Exception:
                return None

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

        def _to_float(v: Any) -> float:
            if isinstance(v, (int, float)):
                return float(v)
            if isinstance(v, str):
                return float(v.strip())
            return float(v)

        def _eval(node, row: Any) -> Any:
            tag = node[0] if isinstance(node, tuple) else None
            if tag is None:
                return node

            if tag == "lit":
                return node[1]
            if tag == "col":
                _, name, pos = node
                return _get_col(row, name, pos)
            if tag == "neg":
                v = _eval(node[1], row)
                if v is None:
                    return None
                if _looks_number(v):
                    return -_to_float(v)
                if strict:
                    raise WowDataUserError(
                        "E_DERIVE_TYPE",
                        f"Cannot apply unary '-' to {v!r}.",
                        hint="Cast the column to a numeric type first.",
                    )
                return None
            if tag == "bin":
                _, opx, left, right = node
                a = _eval(left, row)
                b = _eval(right, row)
                if a is None or b is None:
                    return None

                # numeric math
                if _looks_number(a) and _looks_number(b):
                    af = _to_float(a)
                    bf = _to_float(b)
                    if opx == "+":
                        return af + bf
                    if opx == "-":
                        return af - bf
                    if opx == "*":
                        return af * bf
                    if opx == "/":
                        return af / bf

                # string concatenation with +
                if opx == "+" and isinstance(a, str) and isinstance(b, str):
                    return a + b

                if strict:
                    raise WowDataUserError(
                        "E_DERIVE_TYPE",
                        f"Type mismatch in derive operation: {a!r} {opx} {b!r}.",
                        hint="Consider applying Transform('cast', ...) earlier in the pipeline.",
                    )
                return None
            if tag == "cmp":
                _, opx, left, right = node
                a = _eval(left, row)
                b = _eval(right, row)

                if opx in {"==", "!="}:
                    return (a == b) if opx == "==" else (a != b)
                if a is None or b is None:
                    return False

                if _looks_number(a) and _looks_number(b):
                    af = _to_float(a)
                    bf = _to_float(b)
                    if opx == ">":
                        return af > bf
                    if opx == ">=":
                        return af >= bf
                    if opx == "<":
                        return af < bf
                    if opx == "<=":
                        return af <= bf

                if isinstance(a, str) and isinstance(b, str):
                    if opx == ">":
                        return a > b
                    if opx == ">=":
                        return a >= b
                    if opx == "<":
                        return a < b
                    if opx == "<=":
                        return a <= b

                if strict:
                    raise WowDataUserError(
                        "E_DERIVE_TYPE",
                        f"Type mismatch in derive comparison: {a!r} {opx} {b!r}.",
                        hint="Consider applying Transform('cast', ...) earlier in the pipeline.",
                    )
                return False
            if tag == "and":
                return bool(_eval(node[1], row)) and bool(_eval(node[2], row))
            if tag == "or":
                return bool(_eval(node[1], row)) or bool(_eval(node[2], row))
            if tag == "not":
                return not bool(_eval(node[1], row))

            raise WowDataUserError(
                "E_DERIVE_UNSUPPORTED",
                "Unsupported construct in derive expression.",
                hint="Use literals, column names, + - * /, comparisons, and/or/not, and parentheses.",
            )

        # Add or replace column
        if new in colset:
            # Recompute the entire row with the new column value replaced.
            def _row_with_replaced(r: Tuple[Any, ...]):
                val = _eval(ast, r)
                i = idx_map[new]
                rr = list(r)
                rr[i] = val
                return tuple(rr)

            header_row = [columns]
            header_tbl = etl.wrap(header_row)
            data_tbl = etl.skip(table, 1)
            mapped_tbl = etl.rowmap(data_tbl, _row_with_replaced, header=columns)
            return etl.stack(header_tbl, mapped_tbl)

        return etl.addfield(table, new, lambda r: _eval(ast, r))

    @classmethod
    def output_schema(cls, input_schema: Optional[FrictionlessSchema], params: Dict[str, Any]) -> Optional[
        FrictionlessSchema]:
        if not input_schema or not isinstance(input_schema, dict):
            return input_schema
        fields = input_schema.get("fields")
        if not isinstance(fields, list):
            return input_schema

        new = params.get("new")
        expr = params.get("expr")
        overwrite = params.get("overwrite", False)

        # Best-effort type inference from a trivial parse of expr: literals and boolean-ish roots.
        inferred_type = "any"
        if isinstance(expr, str) and expr.strip():
            s = expr.strip()
            # literal heuristics
            if (len(s) >= 2) and ((s[0] == s[-1] == "'") or (s[0] == s[-1] == '"')):
                inferred_type = "string"
            else:
                try:
                    if "." in s:
                        float(s)
                        inferred_type = "number"
                    else:
                        int(s)
                        inferred_type = "integer"
                except Exception:
                    # boolean-ish operators present
                    if any(tok in s for tok in ("==", "!=", ">=", "<=", ">", "<", " and ", " or ", "not ")):
                        inferred_type = "boolean"

        # Build new fields list
        out_fields: List[Dict[str, Any]] = []
        replaced = False
        for f in fields:
            if isinstance(f, dict) and f.get("name") == new:
                replaced = True
                if overwrite:
                    nf = dict(f)
                    nf["type"] = inferred_type
                    out_fields.append(nf)
                else:
                    out_fields.append(f)
                continue
            out_fields.append(f)

        if isinstance(new, str) and new.strip() and not replaced:
            out_fields.append({"name": new, "type": inferred_type})

        return {"fields": out_fields}


@register_transform("join")
class JoinTransform(TransformImpl):
    @classmethod
    def validate_params(cls, params: Dict[str, Any], input_schema: Optional[FrictionlessSchema] = None) -> None:
        right = params.get("right")
        if not (isinstance(right, str) and right.strip()) and not isinstance(right, dict):
            raise WowDataUserError(
                "E_JOIN_PARAMS",
                "join requires params.right as a URI string or a mapping descriptor.",
                hint="Example: Transform('join', params={'right': 'other.csv', 'on': ['id']})",
            )

        on = params.get("on")
        left_on = params.get("left_on")
        right_on = params.get("right_on")

        if on is not None:
            if not isinstance(on, list) or not on or not all(isinstance(x, str) and x for x in on):
                raise WowDataUserError(
                    "E_JOIN_PARAMS",
                    "join params.on must be a non-empty list of column name strings.",
                    hint="Example: params={'right': 'other.csv', 'on': ['person_id']}",
                )
            if left_on is not None or right_on is not None:
                raise WowDataUserError(
                    "E_JOIN_PARAMS",
                    "join accepts either params.on OR params.left_on/params.right_on, not both.",
                    hint="Use params.on when the key names are the same on both sides.",
                )
        else:
            if left_on is None or right_on is None:
                raise WowDataUserError(
                    "E_JOIN_PARAMS",
                    "join requires either params.on OR both params.left_on and params.right_on.",
                    hint="Example: params={'right': 'other.csv', 'left_on': ['id'], 'right_on': ['person_id']}",
                )
            if not isinstance(left_on, list) or not left_on or not all(isinstance(x, str) and x for x in left_on):
                raise WowDataUserError(
                    "E_JOIN_PARAMS",
                    "join params.left_on must be a non-empty list of column name strings.",
                    hint="Example: left_on: ['id']",
                )
            if not isinstance(right_on, list) or not right_on or not all(isinstance(x, str) and x for x in right_on):
                raise WowDataUserError(
                    "E_JOIN_PARAMS",
                    "join params.right_on must be a non-empty list of column name strings.",
                    hint="Example: right_on: ['person_id']",
                )
            if len(left_on) != len(right_on):
                raise WowDataUserError(
                    "E_JOIN_PARAMS",
                    "join params.left_on and params.right_on must be the same length.",
                    hint=f"Got left_on={left_on} and right_on={right_on}.",
                )

        how = params.get("how", "inner")
        if how not in {"inner", "left", "right", "outer"}:
            raise WowDataUserError(
                "E_JOIN_PARAMS",
                "join params.how must be one of: inner, left, right, outer.",
                hint="Example: Transform('join', params={..., 'how': 'left'})",
            )

        strict_types = params.get("strict_types", True)
        if not isinstance(strict_types, bool):
            raise WowDataUserError(
                "E_JOIN_PARAMS",
                "join params.strict_types must be a boolean.",
                hint="Example: Transform('join', params={..., 'strict_types': false})",
            )

    @classmethod
    def apply(cls, table, *, params: Dict[str, Any], context: "PipelineContext"):
        right_desc = params.get("right")
        on = params.get("on")
        left_on = params.get("left_on")
        right_on = params.get("right_on")
        how = params.get("how", "inner")
        strict_types = params.get("strict_types", True)

        # Resolve join key lists
        if on is not None:
            left_keys = list(on)
            right_keys = list(on)
        else:
            left_keys = list(left_on)
            right_keys = list(right_on)

        # Build right Source
        try:
            right_src = _source_from_descriptor(right_desc)
        except WowDataUserError:
            raise
        except Exception as e:
            raise WowDataUserError(
                "E_JOIN_PARAMS",
                "join params.right could not be interpreted as a source descriptor.",
                hint="Use a URI string like 'other.csv' or a mapping like {'uri': 'other.csv', 'options': {...}}.",
            ) from e

        # Materialize right table safely
        try:
            right_table = right_src.table()
        except WowDataUserError:
            raise
        except Exception as e:
            raise WowDataUserError(
                "E_JOIN_RIGHT_READ",
                "join could not read the right-hand source.",
                hint="Check the right uri/path, type, and options (delimiter/encoding).",
            ) from e

        # Column existence checks (teach before PETL crashes)
        try:
            left_header = list(etl.header(table))
            right_header = list(etl.header(right_table))
        except Exception as e:
            raise WowDataUserError(
                "E_JOIN_READ_HEADERS",
                f"join could not read table headers: {e}",
                hint="Ensure both sources are tabular and readable (CSV delimiter/encoding).",
            ) from e

        left_missing = [c for c in left_keys if c not in left_header]
        right_missing = [c for c in right_keys if c not in right_header]
        if left_missing or right_missing:
            msg_parts = []
            if left_missing:
                msg_parts.append(f"missing on left: {left_missing}")
            if right_missing:
                msg_parts.append(f"missing on right: {right_missing}")
            raise WowDataUserError(
                "E_JOIN_UNKNOWN_COL",
                "join key column(s) not found (" + "; ".join(msg_parts) + ").",
                hint="Check spelling/case. If key names differ, use left_on/right_on.",
            )

        # Type surprise guard (optional)
        def _wow_type(v: Any) -> str:
            if v is None:
                return "null"
            if isinstance(v, bool):
                return "boolean"
            if isinstance(v, int) and not isinstance(v, bool):
                return "integer"
            if isinstance(v, float):
                return "number"
            try:
                from datetime import date, datetime
                if isinstance(v, datetime):
                    return "datetime"
                if isinstance(v, date):
                    return "date"
            except Exception:
                pass
            return "string"

        def _predominant_type(tbl, col: str, hdr: List[str]) -> str:
            counts: Dict[str, int] = {}
            try:
                sample_rows = list(etl.data(etl.head(tbl, 51)))  # 50 data rows
            except Exception:
                return "unknown"

            # index fallback for sequence rows
            try:
                idx = hdr.index(col)
            except Exception:
                idx = None

            for r in sample_rows:
                v = None
                if isinstance(r, dict):
                    v = r.get(col)
                else:
                    try:
                        v = r[col]  # petl Record supports name access
                    except Exception:
                        if idx is not None and isinstance(r, (list, tuple)) and idx < len(r):
                            v = r[idx]
                t = _wow_type(v)
                counts[t] = counts.get(t, 0) + 1

            # ignore nulls if there is any non-null evidence
            if "null" in counts and len(counts) > 1:
                counts2 = dict(counts)
                counts2.pop("null", None)
                counts = counts2 or counts

            return max(counts.items(), key=lambda kv: kv[1])[0] if counts else "unknown"

        if strict_types:
            mismatches = []
            for lk, rk in zip(left_keys, right_keys):
                lt = _predominant_type(table, lk, left_header)
                rt = _predominant_type(right_table, rk, right_header)
                if lt != "unknown" and rt != "unknown" and lt != rt:
                    mismatches.append((lk, rk, lt, rt))
            if mismatches:
                parts = [f"{lk!r}->{rk!r}: left looks {lt!r}, right looks {rt!r}" for (lk, rk, lt, rt) in mismatches]
                raise WowDataUserError(
                    "E_JOIN_KEY_TYPE_MISMATCH",
                    "join key type mismatch (this often causes empty joins or surprising coercions).",
                    hint="; ".join(parts) + ". Fix by casting one side before join, or set strict_types=false.",
                )

        # Execute PETL join, wrapped
        try:
            join_fn = {
                "inner": etl.join,
                "left": etl.leftjoin,
                "right": etl.rightjoin,
                "outer": etl.outerjoin,
            }[how]

            if on is not None:
                out = join_fn(table, right_table, key=left_keys if len(left_keys) > 1 else left_keys[0])
            else:
                # petl join APIs accept lkey/rkey for different key names
                lkey = left_keys if len(left_keys) > 1 else left_keys[0]
                rkey = right_keys if len(right_keys) > 1 else right_keys[0]
                out = join_fn(table, right_table, lkey=lkey, rkey=rkey)

            return out

        except WowDataUserError:
            raise
        except Exception as e:
            # Teach common causes rather than leaking PETL internals
            key_hint = f"keys: left={left_keys} right={right_keys} (how={how})"
            raise WowDataUserError(
                "E_JOIN_FAILED",
                "join failed while combining tables.",
                hint=(
                    f"{key_hint}. Common causes: missing key columns, key type mismatches, "
                    "or duplicate keys creating many-to-many matches. "
                    "Try: (1) validate both sources, (2) cast join keys to the same type, "
                    "(3) inspect duplicates on the join keys."
                ),
            ) from e
