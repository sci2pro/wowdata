from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from wowdata import Pipeline, WowDataUserError

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_PIPELINE_PARSE = 3
EXIT_PIPELINE_RUNTIME = 4


def _default_locked_path(pipeline_path: str) -> Path:
    p = Path(pipeline_path)
    if p.suffix in {".yaml", ".yml"}:
        return p.with_name(f"{p.stem}.locked{p.suffix}")
    return p.with_name(f"{p.name}.locked.yaml")


def _load_pipeline(path: str, base_dir: Optional[str]) -> Pipeline:
    resolved_base_dir = Path(base_dir) if base_dir else None
    return Pipeline.from_yaml(path, base_dir=resolved_base_dir)


def _print_user_error(err: WowDataUserError) -> None:
    print(str(err), file=sys.stderr)


def _print_json(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=False))


def _cmd_run(args: argparse.Namespace) -> int:
    try:
        pipe = _load_pipeline(args.pipeline, args.base_dir)
    except WowDataUserError as e:
        _print_user_error(e)
        return EXIT_PIPELINE_PARSE

    try:
        ctx = pipe.run()
    except WowDataUserError as e:
        _print_user_error(e)
        return EXIT_PIPELINE_RUNTIME

    sinks = [s for s in pipe.steps if getattr(s, "uri", None) is not None and s.__class__.__name__ == "Sink"]

    if args.json:
        payload: Dict[str, Any] = {
            "ok": True,
            "command": "run",
            "pipeline": args.pipeline,
            "steps": len(pipe.steps),
            "sinks": [s.uri for s in sinks],
            "checkpoints": len(ctx.checkpoints),
            "validations": ctx.validations,
        }
        if args.show_checkpoints:
            payload["checkpoint_data"] = ctx.checkpoints
        _print_json(payload)
        return EXIT_OK

    if not args.quiet:
        print(f"Run complete: {args.pipeline}")
        print(f"Steps: {len(pipe.steps)}")
        if sinks:
            print("Sinks:")
            for s in sinks:
                print(f"- {s.uri}")
        print(f"Checkpoints: {len(ctx.checkpoints)}")
        if ctx.validations:
            print(f"Validations: {len(ctx.validations)}")

    if args.show_checkpoints:
        print(json.dumps(ctx.checkpoints, indent=2, default=str))

    return EXIT_OK


def _cmd_validate(args: argparse.Namespace) -> int:
    try:
        pipe = _load_pipeline(args.pipeline, args.base_dir)
        pipe.preflight()
    except WowDataUserError as e:
        _print_user_error(e)
        return EXIT_PIPELINE_PARSE

    if args.json:
        _print_json(
            {
                "ok": True,
                "command": "validate",
                "pipeline": args.pipeline,
                "steps": len(pipe.steps),
            }
        )
    else:
        print(f"Pipeline is valid: {args.pipeline}")
        print(f"Steps: {len(pipe.steps)}")

    return EXIT_OK


def _cmd_schema(args: argparse.Namespace) -> int:
    try:
        pipe = _load_pipeline(args.pipeline, args.base_dir)
        sch = pipe.schema(sample_rows=args.sample_rows, force=args.force)
    except WowDataUserError as e:
        _print_user_error(e)
        return EXIT_PIPELINE_PARSE

    if args.json:
        _print_json(
            {
                "ok": True,
                "command": "schema",
                "pipeline": args.pipeline,
                "schema": sch,
            }
        )
    else:
        print(f"Schema for: {args.pipeline}")
        print(json.dumps(sch, indent=2, sort_keys=False))

    return EXIT_OK


def _cmd_lock_schema(args: argparse.Namespace) -> int:
    out_path = Path(args.output) if args.output else _default_locked_path(args.pipeline)

    try:
        pipe = _load_pipeline(args.pipeline, args.base_dir)
        pipe.to_yaml(
            path=out_path,
            lock_schema=True,
            sample_rows=args.sample_rows,
            force=args.force,
        )
    except WowDataUserError as e:
        _print_user_error(e)
        return EXIT_PIPELINE_PARSE

    if args.json:
        _print_json(
            {
                "ok": True,
                "command": "lock-schema",
                "pipeline": args.pipeline,
                "output": str(out_path),
            }
        )
    else:
        print(f"Schema-locked pipeline written: {out_path}")

    return EXIT_OK


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wowdata",
        description="WowData CLI for running YAML pipelines.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    run_p = subparsers.add_parser("run", help="Run a YAML pipeline.")
    run_p.add_argument("pipeline", help="Path to pipeline YAML file.")
    run_p.add_argument("--base-dir", help="Base directory for relative paths in YAML.")
    run_p.add_argument("--json", action="store_true", help="Print machine-readable JSON output.")
    run_p.add_argument("--quiet", action="store_true", help="Suppress normal status output.")
    run_p.add_argument("--show-checkpoints", action="store_true", help="Print checkpoint details after run.")
    run_p.set_defaults(func=_cmd_run)

    validate_p = subparsers.add_parser("validate", help="Validate pipeline YAML and preflight I/O paths.")
    validate_p.add_argument("pipeline", help="Path to pipeline YAML file.")
    validate_p.add_argument("--base-dir", help="Base directory for relative paths in YAML.")
    validate_p.add_argument("--json", action="store_true", help="Print machine-readable JSON output.")
    validate_p.set_defaults(func=_cmd_validate)

    schema_p = subparsers.add_parser("schema", help="Infer output schema without full execution.")
    schema_p.add_argument("pipeline", help="Path to pipeline YAML file.")
    schema_p.add_argument("--base-dir", help="Base directory for relative paths in YAML.")
    schema_p.add_argument("--sample-rows", type=int, default=200, help="Rows to sample for schema inference.")
    schema_p.add_argument("--force", action="store_true", help="Recompute schema even if cached.")
    schema_p.add_argument("--json", action="store_true", help="Print machine-readable JSON output.")
    schema_p.set_defaults(func=_cmd_schema)

    lock_p = subparsers.add_parser("lock-schema", help="Write a schema-locked YAML pipeline.")
    lock_p.add_argument("pipeline", help="Path to pipeline YAML file.")
    lock_p.add_argument("-o", "--output", help="Output path for locked YAML.")
    lock_p.add_argument("--base-dir", help="Base directory for relative paths in YAML.")
    lock_p.add_argument("--sample-rows", type=int, default=200, help="Rows to sample for schema inference.")
    lock_p.add_argument("--force", action="store_true", help="Recompute schema even if cached.")
    lock_p.add_argument("--json", action="store_true", help="Print machine-readable JSON output.")
    lock_p.set_defaults(func=_cmd_lock_schema)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
