from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

FL_TYPES = {"integer", "number", "string", "boolean", "date", "datetime", "any"}


@dataclass
class Issue:
    code: str
    message: str
    node_id: Optional[str] = None
    path: Optional[str] = None


def _field_map(schema: Dict[str, Any]) -> Dict[str, str]:
    """Return {name: type}."""
    out = {}
    for f in schema.get("fields", []):
        name = f.get("name")
        typ = f.get("type", "any")
        if isinstance(name, str) and name:
            out[name] = typ if typ in FL_TYPES else "any"
    return out


def _schema_from_map(m: Dict[str, str]) -> Dict[str, Any]:
    return {"fields": [{"name": k, "type": v} for k, v in m.items()]}


def _toposort(nodes: Set[str], edges: List[Tuple[str, str]]) -> List[str]:
    indeg = {n: 0 for n in nodes}
    outs = {n: [] for n in nodes}
    for a, b in edges:
        outs[a].append(b)
        indeg[b] += 1
    q = [n for n, d in indeg.items() if d == 0]
    order = []
    while q:
        n = q.pop()
        order.append(n)
        for nxt in outs[n]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                q.append(nxt)
    return order


class IRSchemaInferencerV0:
    """
    Propagates schemas through the pipeline graph and labels each transform's output schema.

    Rules are intentionally conservative:
    - If an expression introduces unknown type, output type becomes 'any'
    - When columns are missing, we warn and do best-effort
    """

    def __init__(self, *, default_unknown_type: str = "any"):
        self.default_unknown_type = default_unknown_type if default_unknown_type in FL_TYPES else "any"

    def infer(self, ir: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], List[Issue]]:
        issues: List[Issue] = []

        nodes = ir.get("nodes", [])
        edges_raw = ir.get("edges", [])

        nodes_by_id: Dict[str, Dict[str, Any]] = {}
        for node in nodes:
            if isinstance(node, dict) and isinstance(node.get("id"), str):
                nodes_by_id[node["id"]] = node

        edges: List[Tuple[str, str]] = []
        for e in edges_raw:
            if isinstance(e, list) and len(e) == 2:
                a, b = e[0], e[1]
                if a in nodes_by_id and b in nodes_by_id:
                    edges.append((a, b))

        # adjacency helpers
        in_edges: Dict[str, List[str]] = {nid: [] for nid in nodes_by_id}
        for a, b in edges:
            in_edges[b].append(a)

        order = _toposort(set(nodes_by_id.keys()), edges)

        # This stores the inferred output schema for dataset-producing nodes (source + transform)
        out_schema: Dict[str, Dict[str, Any]] = {}

        for nid in order:
            node = nodes_by_id[nid]
            kind = node.get("kind")

            # ---- Sources: schema comes from spec.schema if provided, else unknown until runtime ----
            if kind == "source":
                spec = node.get("spec", {}) if isinstance(node.get("spec"), dict) else {}
                schema = spec.get("schema")

                if isinstance(schema, dict):
                    # assume already frictionless-like; normalize into {fields:[{name,type}]}
                    fields = schema.get("fields")
                    if isinstance(fields, list):
                        # trust, but normalize types
                        m = {}
                        for f in fields:
                            if isinstance(f, dict) and isinstance(f.get("name"), str):
                                t = f.get("type", self.default_unknown_type)
                                m[f["name"]] = t if t in FL_TYPES else self.default_unknown_type
                        out_schema[nid] = _schema_from_map(m)
                    else:
                        out_schema[nid] = {"fields": [], "meta": {
                            "warnings": ["Inline schema missing 'fields'; treating as unknown."]}}
                        issues.append(
                            Issue("W_SCHEMA_SHAPE", "Inline source schema missing fields; treating as unknown.", nid,
                                  "spec.schema"))
                elif isinstance(schema, str):
                    # reference to schema file: you can resolve it elsewhere; here we mark unknown but note reference
                    out_schema[nid] = {"fields": [], "meta": {"schema_ref": schema, "warnings": [
                        "Schema ref not resolved in inferencer; treating as unknown until resolved."]}}
                else:
                    out_schema[nid] = {"fields": [],
                                       "meta": {"warnings": ["No schema provided; inferred at runtime (peephole)."]}}

                continue

            # ---- Sinks: no output schema ----
            if kind == "sink":
                continue

            # ---- Transforms: compute schema from inputs ----
            if kind != "transform":
                continue

            op = node.get("op")
            params = node.get("params", {}) if isinstance(node.get("params"), dict) else {}
            ins = in_edges.get(nid, [])
            if not ins:
                continue

            # For v0: treat first incoming as "left/main"
            left_id = ins[0]
            left_schema = out_schema.get(left_id, {"fields": []})
            left_map = _field_map(left_schema)

            def warn(msg: str, path: str = "params") -> None:
                issues.append(Issue("W_INFER", msg, nid, path))

            # --- helpers: require columns exist (best effort) ---
            def ensure_cols_exist(cols: List[str]) -> None:
                for c in cols:
                    if c not in left_map:
                        warn(f"Column '{c}' not present in input schema; proceeding anyway.", "params")

            # default: pass-through
            new_map = dict(left_map)

            # === Row-only ops (schema unchanged) ===
            if op in {"filter", "exclude", "limit", "sample", "distinct", "dedupe", "fill", "replace", "validate",
                      "profile"}:
                out_schema[nid] = _schema_from_map(new_map)
                continue

            # === Column selection & shaping ===
            if op == "select":
                cols = params.get("columns") or []
                if isinstance(cols, list):
                    ensure_cols_exist([c for c in cols if isinstance(c, str)])
                    new_map = {c: left_map.get(c, self.default_unknown_type) for c in cols if isinstance(c, str)}
                out_schema[nid] = _schema_from_map(new_map)
                continue

            if op == "drop":
                cols = params.get("columns") or []
                if isinstance(cols, list):
                    for c in cols:
                        if isinstance(c, str) and c in new_map:
                            del new_map[c]
                out_schema[nid] = _schema_from_map(new_map)
                continue

            if op == "rename":
                mapping = params.get("mapping") or {}
                if isinstance(mapping, dict):
                    for old, new in mapping.items():
                        if isinstance(old, str) and isinstance(new, str):
                            if old in new_map:
                                new_map[new] = new_map.pop(old)
                            else:
                                warn(f"rename refers to missing column '{old}'.", "params.mapping")
                out_schema[nid] = _schema_from_map(new_map)
                continue

            if op == "reorder":
                cols = params.get("columns") or []
                rest = params.get("rest", "keep")
                if isinstance(cols, list):
                    cols = [c for c in cols if isinstance(c, str)]
                    ensure_cols_exist(cols)
                    ordered = {c: new_map.get(c, self.default_unknown_type) for c in cols}
                    if rest == "keep":
                        for c, t in new_map.items():
                            if c not in ordered:
                                ordered[c] = t
                    new_map = ordered
                out_schema[nid] = _schema_from_map(new_map)
                continue

            # === Types & computed columns ===
            if op == "cast":
                types = params.get("types") or {}
                if isinstance(types, dict):
                    for c, t in types.items():
                        if isinstance(c, str):
                            if c not in new_map:
                                warn(f"cast refers to missing column '{c}'.", "params.types")
                                new_map[c] = self.default_unknown_type
                            new_map[c] = t if isinstance(t, str) and t in FL_TYPES else self.default_unknown_type
                out_schema[nid] = _schema_from_map(new_map)
                continue

            if op == "compute":
                assign = params.get("assign") or {}
                # v0: we do not parse expressions here -> type becomes 'any' unless user later casts
                if isinstance(assign, dict):
                    for out_col, expr in assign.items():
                        if isinstance(out_col, str):
                            new_map[out_col] = self.default_unknown_type
                out_schema[nid] = _schema_from_map(new_map)
                continue

            if op == "split":
                column = params.get("column")
                into = params.get("into") or []
                if isinstance(column, str):
                    if column not in new_map:
                        warn(f"split refers to missing column '{column}'.", "params.column")
                    # v0: split yields strings
                    if isinstance(into, list):
                        for outc in into:
                            if isinstance(outc, str):
                                new_map[outc] = "string"
                out_schema[nid] = _schema_from_map(new_map)
                continue

            if op == "merge_columns":
                into = params.get("into")
                if isinstance(into, str) and into:
                    # merging yields string label by default
                    new_map[into] = "string"
                out_schema[nid] = _schema_from_map(new_map)
                continue

            # === Grouping ===
            if op == "groupby_agg":
                by = params.get("by") or []
                agg = params.get("agg") or {}
                out_m: Dict[str, str] = {}

                if isinstance(by, list):
                    for c in by:
                        if isinstance(c, str):
                            out_m[c] = new_map.get(c, self.default_unknown_type)

                # v0: parse simple func(col) and decide type coarsely
                if isinstance(agg, dict):
                    for outc, spec in agg.items():
                        if not isinstance(outc, str):
                            continue
                        if not isinstance(spec, str):
                            out_m[outc] = self.default_unknown_type
                            continue

                        # basic heuristics
                        s = spec.strip()
                        if s.startswith("count(") or s.startswith("nunique("):
                            out_m[outc] = "integer"
                        elif s.startswith(("sum(", "mean(", "median(", "min(", "max(")):
                            # could be integer or number; we conservatively output number
                            out_m[outc] = "number"
                        else:
                            out_m[outc] = self.default_unknown_type

                out_schema[nid] = _schema_from_map(out_m)
                continue

            # === Multi-source ===
            if op in {"join", "filter_in", "exclude_in"}:
                right_ref = params.get("right")
                if isinstance(right_ref, str) and right_ref in out_schema:
                    right_map = _field_map(out_schema[right_ref])
                else:
                    right_map = {}
                    warn("Right dataset schema not available; join semantics may be under-inferred.", "params.right")

                if op in {"filter_in", "exclude_in"}:
                    # schema unchanged from left
                    out_schema[nid] = _schema_from_map(new_map)
                    continue

                # join: merge maps, applying suffixes when collisions occur
                suffixes = params.get("suffixes") or ["_left", "_right"]
                if not (isinstance(suffixes, list) and len(suffixes) == 2 and all(
                        isinstance(x, str) for x in suffixes)):
                    suffixes = ["_left", "_right"]

                left_suf, right_suf = suffixes[0], suffixes[1]
                merged: Dict[str, str] = {}

                # include all left columns
                for c, t in left_map.items():
                    merged[c] = t

                # merge right columns with collision handling
                for c, t in right_map.items():
                    if c in merged:
                        # rename both sides
                        if (c + left_suf) not in merged:
                            merged[c + left_suf] = merged.pop(c)
                        merged[c + right_suf] = t
                    else:
                        merged[c] = t

                out_schema[nid] = _schema_from_map(merged)
                continue

            if op == "union":
                # union yields combined schema depending on align
                others = params.get("others") or []
                align = params.get("align", "by_name")
                fill_missing = bool(params.get("fill_missing", True))

                schemas = [left_map]
                if isinstance(others, list):
                    for ref in others:
                        if isinstance(ref, str) and ref in out_schema:
                            schemas.append(_field_map(out_schema[ref]))
                        else:
                            warn(f"Union other '{ref}' schema not available; may be under-inferred.", "params.others")

                if align == "strict":
                    # strict: require exact same keys; we conservatively keep left schema and warn if mismatch
                    keys0 = set(schemas[0].keys())
                    for i, m in enumerate(schemas[1:], start=1):
                        if set(m.keys()) != keys0:
                            warn("Union strict align with mismatched columns; schema inference keeps left schema.",
                                 "params.align")
                    out_schema[nid] = _schema_from_map(left_map)
                else:
                    # by_name: union of all column names; type becomes 'any' if conflicts
                    all_cols: Set[str] = set()
                    for m in schemas:
                        all_cols |= set(m.keys())

                    merged: Dict[str, str] = {}
                    for c in sorted(all_cols):
                        types = {m.get(c) for m in schemas if c in m}
                        types.discard(None)
                        if len(types) == 1:
                            merged[c] = next(iter(types)) or self.default_unknown_type
                        else:
                            merged[c] = self.default_unknown_type
                            if len(types) > 1:
                                warn(f"Union type conflict for column '{c}' -> inferred as 'any'.", "params.others")

                    # if fill_missing is false, missing columns may drop at runtime; warn.
                    if not fill_missing:
                        warn(
                            "Union with fill_missing=false may drop missing columns at runtime; schema may over-approximate.",
                            "params.fill_missing")
                    out_schema[nid] = _schema_from_map(merged)

                continue

            if op == "python":
                # Unknown: python can do anything.
                warn("python transform makes schema inference impossible; output schema set to unknown.", "op")
                out_schema[nid] = {"fields": [], "meta": {"warnings": ["python transform: unknown schema"]}}
                continue

            # Fallback for unknown ops
            warn(f"Unknown op '{op}' for schema inference; passing through input schema.", "op")
            out_schema[nid] = _schema_from_map(new_map)

        return out_schema, issues
