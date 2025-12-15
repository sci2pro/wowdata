from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union, Optional, Type, Callable

import petl as etl

try:
    from frictionless import Resource, Detector
except Exception:  # pragma: no cover
    Resource = None
    Detector = None

FrictionlessSchema = Dict[str, Any]


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

        import re
        import difflib

        # ---------------- Tokenizer ----------------
        _re_ws = re.compile(r"\s+")
        _re_ident = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
        _re_number = re.compile(r"(?:\d+\.\d*|\d*\.\d+|\d+)")

        KEYWORDS = {"and", "or", "not", "true", "false", "null"}

        class _Tok:
            __slots__ = ("typ", "val", "pos")

            def __init__(self, typ: str, val: Any, pos: int):
                self.typ = typ
                self.val = val
                self.pos = pos

        def _tokenize(src: str) -> List[_Tok]:
            out: List[_Tok] = []
            i = 0
            n = len(src)
            while i < n:
                m = _re_ws.match(src, i)
                if m:
                    i = m.end()
                    continue

                # strings: single or double quoted, with basic escapes
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
                            out.append(_Tok("STR", "".join(buf), i))
                            i = j + 1
                            break
                        buf.append(ch)
                        j += 1
                    else:
                        raise WowDataUserError(
                            "E_FILTER_PARSE",
                            "Unterminated string literal in filter expression.",
                            hint=src,
                        )
                    continue

                # operators
                two = src[i: i + 2]
                if two in {"==", "!=", ">=", "<="}:
                    out.append(_Tok("OP", two, i))
                    i += 2
                    continue
                if src[i] in {">", "<", "(", ")"}:
                    out.append(_Tok("OP", src[i], i))
                    i += 1
                    continue

                # number
                m = _re_number.match(src, i)
                if m:
                    s = m.group(0)
                    out.append(_Tok("NUM", float(s) if ("." in s) else int(s), i))
                    i = m.end()
                    continue

                # identifier/keyword
                m = _re_ident.match(src, i)
                if m:
                    s = m.group(0)
                    low = s.lower()
                    if low in KEYWORDS:
                        out.append(_Tok("KW", low, i))
                    else:
                        out.append(_Tok("IDENT", s, i))
                    i = m.end()
                    continue

                raise WowDataUserError(
                    "E_FILTER_PARSE",
                    f"Unexpected character {src[i]!r} in filter expression.",
                    hint=f"At position {i}: {src}\n" + (" " * i) + "^",
                )

            out.append(_Tok("EOF", None, n))
            return out

        toks = _tokenize(where)
        k = 0

        def _peek() -> _Tok:
            return toks[k]

        def _eat(expected_typ: str, expected_val: Optional[str] = None) -> _Tok:
            nonlocal k
            t = toks[k]
            if t.typ != expected_typ:
                raise WowDataUserError(
                    "E_FILTER_PARSE",
                    f"Expected {expected_typ} but found {t.typ}.",
                    hint=f"At position {t.pos}: {where}\n" + (" " * t.pos) + "^",
                )
            if expected_val is not None and t.val != expected_val:
                raise WowDataUserError(
                    "E_FILTER_PARSE",
                    f"Expected '{expected_val}' but found '{t.val}'.",
                    hint=f"At position {t.pos}: {where}\n" + (" " * t.pos) + "^",
                )
            k += 1
            return t

        # ---------------- Parser (precedence: not > cmp > and > or) ----------------
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
            node = parse_not()
            while _peek().typ == "KW" and _peek().val == "and":
                _eat("KW", "and")
                rhs = parse_not()
                node = ("and", node, rhs)
            return node

        def parse_not():
            if _peek().typ == "KW" and _peek().val == "not":
                _eat("KW", "not")
                inner = parse_not()
                return ("not", inner)
            return parse_cmp()

        def parse_cmp():
            left = parse_atom()
            if _peek().typ == "OP" and _peek().val in {"==", "!=", ">=", "<=", ">", "<"}:
                op_tok = _eat("OP")
                right = parse_atom()
                return ("cmp", op_tok.val, left, right)
            return left

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
                "E_FILTER_PARSE",
                f"Unexpected token '{t.val}' in filter expression.",
                hint=f"At position {t.pos}: {where}\n" + (" " * t.pos) + "^",
            )

        ast = parse_expr()
        _eat("EOF")

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
                    return v

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
                    "E_CAST_TYPE",
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
    def apply(cls, table, *, params: Dict[str, Any], context: "PipelineContext"):
        context.checkpoints.append(("validate", params))
        return table


@dataclass(frozen=True)
class Transform:
    op: str
    params: Dict[str, Any] = field(default_factory=dict)

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

        for step in self.steps:
            if isinstance(step, Transform):
                table = step.apply(table, context=ctx)
                # keep a best-effort running schema for better validation and UI
                ctx.schema = step.output_schema(ctx.schema)
            elif isinstance(step, Sink):
                step.write(table)
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

        import re
        import difflib

        # ---------------- Tokenizer ----------------
        _re_ws = re.compile(r"\s+")
        _re_ident = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
        _re_number = re.compile(r"(?:\d+\.\d*|\d*\.\d+|\d+)")

        KEYWORDS = {"and", "or", "not", "true", "false", "null"}

        class _Tok:
            __slots__ = ("typ", "val", "pos")

            def __init__(self, typ: str, val: Any, pos: int):
                self.typ = typ
                self.val = val
                self.pos = pos

        def _tokenize(src: str) -> List[_Tok]:
            out: List[_Tok] = []
            i = 0
            n = len(src)
            while i < n:
                m = _re_ws.match(src, i)
                if m:
                    i = m.end()
                    continue

                # strings: single or double quoted, with basic escapes
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
                            out.append(_Tok("STR", "".join(buf), i))
                            i = j + 1
                            break
                        buf.append(ch)
                        j += 1
                    else:
                        raise WowDataUserError(
                            "E_DERIVE_PARSE",
                            "Unterminated string literal in derive expression.",
                            hint=src,
                        )
                    continue

                # operators (longest-first)
                two = src[i : i + 2]
                if two in {"==", "!=", ">=", "<="}:
                    out.append(_Tok("OP", two, i))
                    i += 2
                    continue

                if src[i] in {">", "<", "(", ")", "+", "-", "*", "/"}:
                    out.append(_Tok("OP", src[i], i))
                    i += 1
                    continue

                # number
                m = _re_number.match(src, i)
                if m:
                    s = m.group(0)
                    out.append(_Tok("NUM", float(s) if ("." in s) else int(s), i))
                    i = m.end()
                    continue

                # identifier/keyword
                m = _re_ident.match(src, i)
                if m:
                    s = m.group(0)
                    low = s.lower()
                    if low in KEYWORDS:
                        out.append(_Tok("KW", low, i))
                    else:
                        out.append(_Tok("IDENT", s, i))
                    i = m.end()
                    continue

                raise WowDataUserError(
                    "E_DERIVE_PARSE",
                    f"Unexpected character {src[i]!r} in derive expression.",
                    hint=f"At position {i}: {src}\n" + (" " * i) + "^",
                )

            out.append(_Tok("EOF", None, n))
            return out

        toks = _tokenize(expr)
        k = 0

        def _peek() -> _Tok:
            return toks[k]

        def _eat(expected_typ: str, expected_val: Optional[str] = None) -> _Tok:
            nonlocal k
            t = toks[k]
            if t.typ != expected_typ:
                raise WowDataUserError(
                    "E_DERIVE_PARSE",
                    f"Expected {expected_typ} but found {t.typ}.",
                    hint=f"At position {t.pos}: {expr}\n" + (" " * t.pos) + "^",
                )
            if expected_val is not None and t.val != expected_val:
                raise WowDataUserError(
                    "E_DERIVE_PARSE",
                    f"Expected '{expected_val}' but found '{t.val}'.",
                    hint=f"At position {t.pos}: {expr}\n" + (" " * t.pos) + "^",
                )
            k += 1
            return t

        # ---------------- Parser (precedence) ----------------
        # or  -> and ( "or" and )*
        # and -> cmp ( "and" cmp )*
        # cmp -> add ( (==|!=|<|<=|>|>=) add )?
        # add -> mul ( (+|-) mul )*
        # mul -> unary ( (*|/) unary )*
        # unary -> ("not" unary) | ("-" unary) | atom
        # atom -> IDENT | NUM | STR | true/false/null | '(' expr ')'

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
            left = parse_add()
            if _peek().typ == "OP" and _peek().val in {"==", "!=", ">=", "<=", ">", "<"}:
                op_tok = _eat("OP")
                right = parse_add()
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
            if _peek().typ == "OP" and _peek().val == "-":
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
                "E_DERIVE_PARSE",
                f"Unexpected token '{t.val}' in derive expression.",
                hint=f"At position {t.pos}: {expr}\n" + (" " * t.pos) + "^",
            )

        ast = parse_expr()
        _eat("EOF")

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
                # r is a tuple row (no header)
                val = _eval(ast, r)
                i = idx_map[new]
                rr = list(r)
                rr[i] = val
                return tuple(rr)

            data_only = etl.select(table, lambda _r: True)
            # Build new table by replacing the data rows; header remains the same
            return etl.stack([etl.header(table)], etl.rowmap(data_only, _row_with_replaced))

        return etl.addfield(table, new, lambda r: _eval(ast, r))

    @classmethod
    def output_schema(cls, input_schema: Optional[FrictionlessSchema], params: Dict[str, Any]) -> Optional[FrictionlessSchema]:
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
                if overwrite:
                    nf = dict(f)
                    nf["type"] = inferred_type
                    out_fields.append(nf)
                    replaced = True
                else:
                    out_fields.append(f)
                continue
            out_fields.append(f)

        if isinstance(new, str) and new.strip() and not replaced:
            out_fields.append({"name": new, "type": inferred_type})

        return {"fields": out_fields}