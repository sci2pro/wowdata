from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union, Optional

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
            where = p.get("where")
            if not isinstance(where, str) or not where.strip():
                raise WowDataUserError(
                    "E_FILTER_PARAMS",
                    "filter requires params.where as a non-empty expression string.",
                    hint="Example: Transform('filter', params={'where': "
                         "\"age >= 30 and country == 'KE'\"})",
                )

            strict = p.get("strict", True)
            if not isinstance(strict, bool):
                raise WowDataUserError(
                    "E_FILTER_PARAMS",
                    "filter params.strict must be a boolean.",
                    hint="Example: Transform('filter', params={'where': 'age >= 30', 'strict': true})",
                )

            columns = list(etl.header(table))
            colset = set(columns)

            import re
            import difflib

            # ---------------- Tokenizer ----------------
            _re_ws = re.compile(r"\s+")
            _re_ident = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
            _re_number = re.compile(r"(?:\d+\.\d*|\d*\.\d+|\d+)")

            # Operators ordered longest-first
            OPS = {"==", "!=", ">=", "<=", ">", "<", "(", ")"}
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
                    two = src[i:i+2]
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
            # AST nodes are tuples: (tag, ...)

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
                if t.typ == "KW":
                    if t.val in {"true", "false", "null"}:
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

                # record/dict-like rows
                try:
                    return row[name]
                except Exception:
                    pass

                if isinstance(row, dict):
                    return row.get(name)

                # tuple/list rows
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

                    # Null semantics: comparisons with None are False except ==/!=
                    if opx in {"==", "!="}:
                        return (a == b) if opx == "==" else (a != b)
                    if a is None or b is None:
                        return False

                    # numeric coercion if both look numeric
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

                    # string-to-string comparisons
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

            # Filter directly on the PETL table. `etl.select` preserves the header and
            # applies the predicate to data rows.
            filtered = etl.select(table, lambda r: bool(_eval(ast, r)))
            return filtered

        if op == "select":
            cols = p.get("columns")
            if not isinstance(cols, list) or not cols:
                raise WowDataUserError(
                    "E_SELECT_PARAMS",
                    "select requires params.columns as a non-empty list of column names.",
                    hint="Example: Transform('select', params={'columns': ['person_id', 'age']})",
                )
            return etl.cut(table, *cols)

        if op == "drop":
            cols = p.get("columns")
            if not isinstance(cols, list) or not cols:
                raise WowDataUserError(
                    "E_DROP_PARAMS",
                    "drop requires params.columns as a non-empty list of column names.",
                    hint="Example: Transform('drop', params={'columns': ['debug_col']})",
                )
            return etl.cutout(table, *cols)

        if op == "cast":
            types = p.get("types")
            if not isinstance(types, dict) or not types:
                raise WowDataUserError(
                    "E_CAST_TYPES",
                    "cast requires params.types as a non-empty mapping: {column: type, ...}.",
                    hint="Example: Transform('cast', params={'types': {'age': 'integer'}, 'on_error': 'null'})",
                )

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

            on_error = p.get("on_error", "fail")
            if on_error not in {"fail", "null", "keep"}:
                raise WowDataUserError(
                    "E_CAST_ON_ERROR",
                    "cast params.on_error must be one of: 'fail', 'null', 'keep'.",
                    hint="Example: Transform('cast', params={'types': {...}, 'on_error': 'null'})",
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

        if op == "validate":
            # runtime validation checkpoint (frictionless integration comes next)
            context.checkpoints.append(("validate", p))
            return table

        raise WowDataUserError(
            "E_OP_NOT_IMPL",
            f"Transform op '{op}' is not implemented in v0 executor yet.",
            hint="Implement it in Transform.apply, or restrict your pipeline to supported ops.",
        )

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
        table = self.start.table()

        for step in self.steps:
            if isinstance(step, Transform):
                table = step.apply(table, context=ctx)
            elif isinstance(step, Sink):
                step.write(table)
            else:
                raise WowDataUserError(
                    "E_PIPELINE_STEP_TYPE",
                    "Pipeline contains an unknown step type.",
                    hint="This should not happen if you only add Transform/Sink via Pipeline.then().",
                )

        return ctx
