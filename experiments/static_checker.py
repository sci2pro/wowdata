from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Set, Optional


@dataclass
class Issue:
    code: str
    message: str
    node_id: Optional[str] = None
    path: Optional[str] = None  # e.g. "nodes[3].params.right"


class IRStaticCheckerV0:
    SINGLE_INPUT_OPS: Set[str] = {
        "filter", "exclude", "limit", "sample", "distinct",
        "select", "drop", "rename", "reorder",
        "cast", "compute", "fill", "replace", "split", "merge_columns",
        "groupby_agg", "dedupe",
        "validate", "profile",
    }

    MULTI_INPUT_OPS_RIGHT: Set[str] = {"join", "filter_in", "exclude_in"}
    MULTI_INPUT_OPS_OTHERS: Set[str] = {"union"}

    VALID_KINDS: Set[str] = {"source", "transform", "sink"}

    def __init__(self, *, enforce_single_output: bool = True, enforce_connected: bool = False):
        self.enforce_single_output = enforce_single_output
        self.enforce_connected = enforce_connected

    def check(self, ir: Dict[str, Any]) -> List[Issue]:
        issues: List[Issue] = []

        nodes = ir.get("nodes", [])
        edges = ir.get("edges", [])

        # Build node map + uniqueness
        nodes_by_id: Dict[str, Dict[str, Any]] = {}
        for i, node in enumerate(nodes):
            node_id = node.get("id")
            if not isinstance(node_id, str) or not node_id:
                issues.append(Issue("E_NODE_ID", "Node id must be a non-empty string.", path=f"nodes[{i}].id"))
                continue
            if node_id in nodes_by_id:
                issues.append(Issue("E_DUP_NODE_ID", f"Duplicate node id '{node_id}'.", node_id=node_id))
                continue
            nodes_by_id[node_id] = node

        # Build adjacency
        in_edges: Dict[str, List[str]] = {nid: [] for nid in nodes_by_id}
        out_edges: Dict[str, List[str]] = {nid: [] for nid in nodes_by_id}

        for j, e in enumerate(edges):
            if not (isinstance(e, list) and len(e) == 2):
                issues.append(Issue("E_EDGE_SHAPE", "Edge must be a 2-item list [from, to].", path=f"edges[{j}]"))
                continue
            src, dst = e[0], e[1]
            if src not in nodes_by_id:
                issues.append(Issue("E_EDGE_SRC_MISSING", f"Edge source '{src}' not found.", path=f"edges[{j}][0]"))
                continue
            if dst not in nodes_by_id:
                issues.append(Issue("E_EDGE_DST_MISSING", f"Edge target '{dst}' not found.", path=f"edges[{j}][1]"))
                continue
            out_edges[src].append(dst)
            in_edges[dst].append(src)

        # Kind/spec sanity
        for nid, node in nodes_by_id.items():
            kind = node.get("kind")
            if kind not in self.VALID_KINDS:
                issues.append(Issue("E_KIND", f"Invalid kind '{kind}'.", node_id=nid, path=f"nodes[{nid}].kind"))
                continue
            if kind in ("source", "sink"):
                spec = node.get("spec", {})
                uri = spec.get("uri") if isinstance(spec, dict) else None
                if not isinstance(uri, str) or not uri:
                    issues.append(Issue("E_URI", "Source/Sink spec.uri must be a non-empty string.", node_id=nid))

        # Directional constraints
        for nid, node in nodes_by_id.items():
            kind = node.get("kind")
            if kind == "source" and in_edges[nid]:
                issues.append(Issue("E_SOURCE_IN", "Source nodes cannot have incoming edges.", node_id=nid))
            if kind == "sink" and out_edges[nid]:
                issues.append(Issue("E_SINK_OUT", "Sink nodes cannot have outgoing edges.", node_id=nid))
            if kind == "transform" and len(in_edges[nid]) == 0:
                issues.append(
                    Issue("E_TRANSFORM_IN", "Transform nodes must have at least one incoming edge.", node_id=nid))

        # Optional single-output rule (keeps pipelines linear for ordinary users)
        if self.enforce_single_output:
            for nid, outs in out_edges.items():
                if len(outs) > 1:
                    issues.append(
                        Issue("E_BRANCHING", "Branching is not allowed in v0 (multiple outgoing edges).", node_id=nid))

        # Validate dataset refs and transform arity
        for nid, node in nodes_by_id.items():
            if node.get("kind") != "transform":
                continue

            op = node.get("op")
            params = node.get("params", {})
            if not isinstance(op, str) or not op:
                issues.append(Issue("E_OP", "Transform op must be a non-empty string.", node_id=nid))
                continue
            if not isinstance(params, dict):
                issues.append(Issue("E_PARAMS", "Transform params must be an object.", node_id=nid))
                continue

            # Helper: validate datasetRef target is valid and not a sink
            def check_dataset_ref(ref: str, path: str) -> None:
                if ref not in nodes_by_id:
                    issues.append(
                        Issue("E_REF_MISSING", f"Referenced dataset '{ref}' not found.", node_id=nid, path=path))
                    return
                if nodes_by_id[ref].get("kind") == "sink":
                    issues.append(
                        Issue("E_REF_SINK", "Transforms cannot reference sinks as datasets.", node_id=nid, path=path))

            incoming = in_edges.get(nid, [])

            if op in self.SINGLE_INPUT_OPS:
                if len(incoming) != 1:
                    issues.append(Issue("E_ARITY", f"Transform '{op}' expects exactly 1 input.", node_id=nid))

            elif op in self.MULTI_INPUT_OPS_RIGHT:
                right = params.get("right")
                if not isinstance(right, str) or not right:
                    issues.append(
                        Issue("E_RIGHT", f"Transform '{op}' requires params.right.", node_id=nid, path="params.right"))
                else:
                    check_dataset_ref(right, "params.right")
                    # Enforce wiring: right must be an incoming edge
                    if right not in incoming:
                        issues.append(
                            Issue("E_RIGHT_EDGE", f"Referenced right='{right}' must also be an incoming edge.",
                                  node_id=nid))
                # Expect exactly 2 inputs in v0
                if len(set(incoming)) != 2:
                    issues.append(
                        Issue("E_ARITY", f"Transform '{op}' expects exactly 2 distinct inputs (left + right).",
                              node_id=nid))

            elif op in self.MULTI_INPUT_OPS_OTHERS:
                others = params.get("others")
                if not (isinstance(others, list) and all(isinstance(x, str) and x for x in others)):
                    issues.append(
                        Issue("E_OTHERS", "Transform 'union' requires params.others: [datasetRef...].", node_id=nid,
                              path="params.others"))
                    others_list: List[str] = []
                else:
                    others_list = others
                    for k, ref in enumerate(others_list):
                        check_dataset_ref(ref, f"params.others[{k}]")
                        if ref not in incoming:
                            issues.append(Issue("E_OTHER_EDGE", f"Union other='{ref}' must also be an incoming edge.",
                                                node_id=nid))

                # Expect 1 (main) + len(others)
                expected = 1 + len(set(others_list))
                if len(set(incoming)) != expected:
                    issues.append(
                        Issue("E_ARITY", f"Transform 'union' expects {expected} distinct inputs.", node_id=nid))

            else:
                # Unknown op should already be caught by JSON Schema, but keep a guard
                issues.append(Issue("E_UNKNOWN_OP", f"Unknown transform op '{op}'.", node_id=nid))

        # Cycle detection (Kahn)
        issues.extend(self._check_cycles(nodes_by_id, in_edges, out_edges))

        # Optional connectivity: every sink reachable from some source
        if self.enforce_connected:
            issues.extend(self._check_connectivity(nodes_by_id, out_edges))

        return issues

    def _check_cycles(
            self,
            nodes_by_id: Dict[str, Dict[str, Any]],
            in_edges: Dict[str, List[str]],
            out_edges: Dict[str, List[str]],
    ) -> List[Issue]:
        issues: List[Issue] = []
        indeg = {nid: len(set(ins)) for nid, ins in in_edges.items()}
        queue = [nid for nid, d in indeg.items() if d == 0]
        visited = 0

        while queue:
            nid = queue.pop()
            visited += 1
            for nxt in set(out_edges.get(nid, [])):
                indeg[nxt] -= 1
                if indeg[nxt] == 0:
                    queue.append(nxt)

        if visited != len(nodes_by_id):
            issues.append(Issue("E_CYCLE", "Pipeline graph contains a cycle (not a DAG)."))
        return issues

    def _check_connectivity(
            self,
            nodes_by_id: Dict[str, Dict[str, Any]],
            out_edges: Dict[str, List[str]],
    ) -> List[Issue]:
        issues: List[Issue] = []
        sources = [nid for nid, n in nodes_by_id.items() if n.get("kind") == "source"]
        sinks = [nid for nid, n in nodes_by_id.items() if n.get("kind") == "sink"]

        # BFS from all sources
        seen: Set[str] = set()
        stack = list(sources)
        while stack:
            nid = stack.pop()
            if nid in seen:
                continue
            seen.add(nid)
            stack.extend(out_edges.get(nid, []))

        for sk in sinks:
            if sk not in seen:
                issues.append(Issue("E_UNREACHABLE_SINK", "Sink is not reachable from any source.", node_id=sk))
        return issues
