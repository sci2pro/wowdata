# CLI Reference (v0)

This page documents the WowData command-line interface.

## Index

- [`wow` and `wowdata`](#wow-and-wowdata)
- [`run`](#run)
- [`validate`](#validate)
- [`schema`](#schema)
- [`lock-schema`](#lock-schema)
- [`Exit Codes`](#exit-codes)

<a id="wow-and-wowdata"></a>
??? info "wow and wowdata"

    WowData installs two equivalent entry points:

    - `wow`
    - `wowdata`

    They expose the same commands and options.

    Example:

    ```bash
    wow run pipeline.yaml
    wowdata run pipeline.yaml
    ```

    **Common flags**

    These appear on multiple commands:

    - `--base-dir PATH`: resolve relative paths in YAML against a specific base directory
    - `--json`: print machine-readable JSON output instead of human-oriented text

    **When To Use It**

    Use the CLI when you want to:

    - run YAML pipelines without writing Python code
    - validate pipeline structure and I/O configuration
    - inspect inferred output schema
    - write schema-locked pipeline YAML

<a id="run"></a>
??? info "run"

    Execute a YAML pipeline end to end.

    **Signature**

    `wow run PIPELINE [--base-dir PATH] [--json] [--quiet] [--show-checkpoints]`

    Example:

    ```bash
    wow run examples/climate_heat_events.yaml --base-dir examples
    ```

    **Arguments**

    `PIPELINE`

    Required.

    Path to the pipeline YAML file.

    `--base-dir PATH`

    Optional.

    Resolve relative paths inside YAML against `PATH`.

    `--json`

    Optional.

    Print a machine-readable JSON summary instead of standard text output.

    `--quiet`

    Optional.

    Suppress the normal status summary.

    `--show-checkpoints`

    Optional.

    Print checkpoint details after the run.

    In JSON mode, this adds `checkpoint_data` to the output payload.

    **Behavior**

    - loads the YAML pipeline
    - executes the pipeline via `Pipeline.run()`
    - reports sink paths, checkpoint count, and validation summaries

<a id="validate"></a>
??? info "validate"

    Parse pipeline YAML and run preflight validation without executing the full pipeline.

    **Signature**

    `wow validate PIPELINE [--base-dir PATH] [--json]`

    Example:

    ```bash
    wow validate pipeline.yaml
    ```

    **Behavior**

    - loads the pipeline YAML
    - runs `Pipeline.preflight()`
    - checks structure and source/sink path viability
    - does not execute transforms or write sinks

    Use this when you want a cheap sanity check before a real run.

<a id="schema"></a>
??? info "schema"

    Infer the output schema without running the full pipeline.

    **Signature**

    `wow schema PIPELINE [--base-dir PATH] [--sample-rows N] [--force] [--json]`

    Example:

    ```bash
    wow schema pipeline.yaml --json
    ```

    **Arguments**

    `--sample-rows N`

    Optional. Default: `200`.

    Number of rows to sample for source schema inference.

    `--force`

    Optional.

    Recompute schema inference even if cached values exist.

    **Behavior**

    - loads the pipeline
    - calls `Pipeline.schema(...)`
    - returns the best-effort output schema without executing sinks

<a id="lock-schema"></a>
??? info "lock-schema"

    Write a schema-locked YAML pipeline.

    **Signature**

    `wow lock-schema PIPELINE [-o OUTPUT] [--base-dir PATH] [--sample-rows N] [--force] [--json]`

    Example:

    ```bash
    wow lock-schema pipeline.yaml -o pipeline.locked.yaml
    ```

    **Arguments**

    `-o, --output`

    Optional.

    Output path for the locked YAML file.

    If omitted, WowData derives a default `.locked.yaml`-style path.

    `--sample-rows N`

    Optional. Default: `200`.

    Number of rows to sample during schema inference.

    `--force`

    Optional.

    Recompute schema inference even if cached values exist.

    **Behavior**

    - loads the pipeline
    - computes schema snapshots across transforms
    - writes a YAML file with explicit per-transform `output_schema`

    Use this when you want schema-aware behavior to remain stable after serialization.

<a id="exit-codes"></a>
??? note "Exit Codes"

    WowData uses these CLI exit codes:

    - `0`: success
    - `2`: CLI usage error
    - `3`: pipeline parse or load failure
    - `4`: pipeline runtime failure

    In practice:

    - malformed YAML usually returns `3`
    - invalid runtime behavior during `run` usually returns `4`

## See Also

- [Pipeline](pipeline.md)
- [Sources](sources.md)
- [Transforms](transforms.md)
- [Sinks](sinks.md)
- [Errors](errors.md)
