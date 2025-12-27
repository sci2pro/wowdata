from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Union, Optional, Dict, Any, Tuple

import petl as etl

from wowdata.errors import WowDataUserError
from wowdata.models.sinks import Sink
from wowdata.models.sources import Source
from wowdata.models.transforms import Transform, _transform_from_ir
from wowdata.schema import _source_to_ir, _sink_to_ir, _transform_to_ir, _source_from_ir, \
    _sink_from_ir, _normalize_ir
from wowdata.util import FrictionlessSchema

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None


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

    def preflight(self) -> None:
        """
        Validate sources and sinks before executing the pipeline.

        This checks:
        - Source readability / existence
        - Sink writability / destination validity
        - Pipeline ordering invariants (defensive)
        """
        # Validate source early
        try:
            self.start.preflight()
        except AttributeError:
            # Backward compatibility: Source may already fail-fast in __post_init__
            pass

        saw_sink = False
        for i, step in enumerate(self.steps):
            if isinstance(step, Transform):
                if saw_sink:
                    raise WowDataUserError(
                        "E_PIPELINE_ORDER",
                        f"Transform '{step.op}' appears after a Sink at step index {i}.",
                        hint="Reorder the pipeline so that all sinks come last.",
                    )
            elif isinstance(step, Sink):
                saw_sink = True
                try:
                    step.preflight()
                except AttributeError:
                    # Sink may already fail-fast in __post_init__
                    pass
            else:
                raise WowDataUserError(
                    "E_PIPELINE_STEP_TYPE",
                    "Pipeline contains an unknown step type.",
                    hint="This should not happen if you only add Transform/Sink via Pipeline.then().",
                )

    def run(self) -> PipelineContext:
        self.preflight()
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

    def to_yaml(
        self,
        path: Optional[Union[str, Path]] = None,
        *,
        lock_schema: bool = False,
        sample_rows: int = 200,
        force: bool = False,
    ) -> str:
        """Dump IR to YAML string. If `path` is provided, also write the file."""
        if yaml is None:
            raise WowDataUserError(
                "E_YAML_IMPORT",
                "PyYAML is not available; cannot serialize to YAML.",
                hint="Install dependency: pip install pyyaml",
            )
        pipe = self.lock_schema(sample_rows=sample_rows, force=force) if lock_schema else self
        text = yaml.safe_dump(pipe.to_ir(), sort_keys=False)
        if path is not None:
            p = Path(path)
            p.write_text(text, encoding="utf-8")
        return text

    @classmethod
    def from_yaml(cls, text_or_path: Union[str, Path], *, base_dir: Optional[Path] = None) -> "Pipeline":
        """Load pipeline from YAML string or file path."""
        if yaml is None:
            raise WowDataUserError(
                "E_YAML_IMPORT",
                "PyYAML is not available; cannot parse YAML.",
                hint="Install dependency: pip install pyyaml",
            )
        # Accept path-like input for convenience
        if isinstance(text_or_path, (str, Path)):
            p = Path(text_or_path)
            if p.exists():
                base_dir = base_dir or p.parent
                text = p.read_text(encoding="utf-8")
            else:
                text = str(text_or_path)
        else:
            text = text_or_path  # type: ignore[assignment]
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


@dataclass
class PipelineContext:
    checkpoints: List[Tuple[str, Dict[str, Any]]] = field(default_factory=list)
    schema: Optional[FrictionlessSchema] = None
    validations: List[Dict[str, Any]] = field(default_factory=list)
    # later: frictionless reports, timing, rowcounts, etc.
