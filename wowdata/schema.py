from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, Optional, List

from wowdata.errors import WowDataUserError
from wowdata.models.sources import Source
from wowdata.models.sinks import Sink
from wowdata.models.transforms import Transform
from wowdata.util import _norm_path, FrictionlessSchema


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
