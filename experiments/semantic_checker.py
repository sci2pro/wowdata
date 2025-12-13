from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple, Union


@dataclass
class Issue:
    code: str
    message: str
    node_id: Optional[str] = None
    path: Optional[str] = None


def _is_nonempty_str(x: Any) -> bool:
    return isinstance(x, str) and x.strip() != ""


def _as_list(x: Any) -> Optional[List[str]]:
    if isinstance(x, list) and all(_is_nonempty_str(i) for i in x):
        return [i.strip() for i in x]
    return None


def _as_str(x: Any) -> Optional[str]:
    if _is_nonempty_str(x):
        return x.strip()
    return None


def _has_dupes(items: List[str]) -> bool:
    return len(set(items)) != len(items)


class IRSemanticCheckerV0:
    """
    Semantic validation pass for WowData IR.
    Assumes JSON Schema + static checker have already run.
    """

    def __init__(self, *, allow_python: bool = False):
        self.allow_python = allow_python

    def check(self, ir: Dict[str, Any]) -> List[Issue]:
        issues: List[Issue] = []
        nodes = ir.get("nodes", [])
        nodes_by_id = {n.get("id"): n for n in nodes if isinstance(n, dict) and _is_nonempty_str(n.get("id"))}

        for nid, node in nodes_by_id.items():
            if node.get("kind") != "transform":
                continue

            op = node.get("op")
            params = node.get("params", {})
            if not isinstance(op, str) or not isinstance(params, dict):
                continue  # schema/static checker will already flag this

            # ---- helpers ----
            def err(code: str, msg: str, path: str) -> None:
                issues.append(Issue(code, msg, node_id=nid, path=path))

            def check_columns_unique(cols: Any, path: str) -> Optional[List[str]]:
                lst = _as_list(cols)
                if lst is None:
                    return None
                if _has_dupes(lst):
                    err("E_DUP_COLS", "Duplicate column names are not allowed here.", path)
                return lst

            # ---- op-specific checks ----
            if op in {"select", "drop", "reorder"}:
                check_columns_unique(params.get("columns"), "params.columns")

            if op == "distinct":
                by = params.get("by")
                if by is not None:
                    check_columns_unique(by, "params.by")

            if op == "rename":
                mapping = params.get("mapping")
                if isinstance(mapping, dict):
                    # keys and values must be non-empty
                    bad_keys = [k for k in mapping.keys() if not _is_nonempty_str(k)]
                    bad_vals = [v for v in mapping.values() if not _is_nonempty_str(v)]
                    if bad_keys:
                        err("E_RENAME_KEY", "Rename mapping contains an empty/invalid source column name.", "params.mapping")
                    if bad_vals:
                        err("E_RENAME_VAL", "Rename mapping contains an empty/invalid target column name.", "params.mapping")

                    # values must be unique (avoid collisions)
                    vals = [str(v).strip() for v in mapping.values() if _is_nonempty_str(v)]
                    if _has_dupes(vals):
                        err("E_RENAME_COLLIDE", "Rename mapping targets must be unique (would create duplicate columns).", "params.mapping")

            if op == "split":
                into = check_columns_unique(params.get("into"), "params.into") or []
                sep = params.get("sep")
                pattern = params.get("pattern")

                if (sep is None or sep == "") and (pattern is None or pattern == ""):
                    err("E_SPLIT_NEED_DELIM", "Split requires either 'sep' or 'pattern'.", "params")

                # 'into' should not be empty and should have at least 2 columns in most cases
                if into and len(into) < 2:
                    err("E_SPLIT_INTO", "Split 'into' should name at least 2 output columns.", "params.into")

            if op == "merge_columns":
                check_columns_unique(params.get("columns"), "params.columns")
                into = params.get("into")
                if not _is_nonempty_str(into):
                    err("E_MERGE_INTO", "merge_columns requires a non-empty 'into' column name.", "params.into")

            if op == "compute":
                assign = params.get("assign")
                if isinstance(assign, dict):
                    for k in assign.keys():
                        if not _is_nonempty_str(k):
                            err("E_ASSIGN_NAME", "Compute assignment has an empty/invalid target column name.", "params.assign")

            if op in {"join", "filter_in", "exclude_in"}:
                lk = params.get("left_key")
                rk = params.get("right_key")

                lk_str = _as_str(lk)
                rk_str = _as_str(rk)
                lk_list = _as_list(lk)
                rk_list = _as_list(rk)

                if lk_str and rk_list:
                    err("E_KEY_SHAPE", "left_key is a string but right_key is a list; they must match in shape.", "params.right_key")
                if lk_list and rk_str:
                    err("E_KEY_SHAPE", "left_key is a list but right_key is a string; they must match in shape.", "params.left_key")
                if lk_list and rk_list:
                    if len(lk_list) != len(rk_list):
                        err("E_KEY_LEN", "left_key and right_key lists must be the same length.", "params")
                    if _has_dupes(lk_list) or _has_dupes(rk_list):
                        err("E_KEY_DUP", "Join/filter keys must not contain duplicate columns.", "params")

            if op == "union":
                others = params.get("others")
                others_list = _as_list(others)
                if others_list is not None:
                    if _has_dupes(others_list):
                        err("E_UNION_DUP", "Union 'others' contains duplicates.", "params.others")

            if op == "groupby_agg":
                by = check_columns_unique(params.get("by"), "params.by") or []
                agg = params.get("agg")
                if isinstance(agg, dict):
                    out_cols = [k.strip() for k in agg.keys() if _is_nonempty_str(k)]
                    if _has_dupes(out_cols):
                        err("E_AGG_DUP_OUT", "Aggregation output column names must be unique.", "params.agg")

                    # v0 recommendation: forbid collisions between by-keys and agg outputs
                    overlap = set(by).intersection(set(out_cols))
                    if overlap:
                        err(
                            "E_AGG_COLLIDE",
                            f"Aggregation output columns collide with group-by keys: {sorted(overlap)}.",
                            "params"
                        )

            if op == "python":
                if not self.allow_python:
                    err("E_PYTHON_DISABLED", "The 'python' escape hatch is disabled for this pipeline.", "op")

        return issues