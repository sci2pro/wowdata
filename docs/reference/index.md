# Reference

Use this section for API and format lookup.

For runnable end-to-end samples, see the checked-in `examples/` directory and the examples guide. From the
repo root, run them with `wow run examples/<pipeline>.yaml --base-dir examples`.

## Sections

??? info "Sources"

    Start here when you need to understand pipeline inputs.

    The sources reference covers:

    - `Source(...)` constructor arguments
    - inline schema and CSV reader options
    - source-type inference and current v0 constraints
    - schema inference behavior

    Go to: [Sources](sources.md)

??? info "Transforms"

    Start here when you need to understand row and column operations inside a pipeline.

    The transforms reference covers:

    - `cast`, `select`, `derive`, `filter`, `drop`, `string`, `validate`, and `join`
    - per-transform signatures
    - Python and YAML examples
    - argument-by-argument explanations
    - the shared Expression DSL

    Go to: [Transforms](transforms.md)

??? info "Transform Object"

    Start here when you need to understand the public `Transform(...)` constructor used in Python.

    This page covers:

    - `Transform(op, params=None, output_schema_override=None)`
    - how `Transform` relates to the per-operation transform catalog
    - schema override behavior in locked pipelines

    Go to: [Transform Object](transform-object.md)

??? info "Pipeline"

    Start here when you need to understand how WowData workflows are composed and executed.

    The pipeline reference covers:

    - `Pipeline(...)` construction
    - `then(...)`, `run()`, and `preflight()`
    - schema inference and schema locking
    - YAML and IR serialization methods
    - `PipelineContext`

    Go to: [Pipeline](pipeline.md)

??? info "YAML / IR Format"

    Start here when you need to understand WowData's serialized pipeline format.

    This page covers:

    - top-level YAML structure
    - source, transform, and sink descriptor shapes
    - normalization rules applied during load
    - the relationship between YAML and internal IR dictionaries

    Go to: [YAML / IR Format](yaml-ir.md)

??? info "CLI"

    Start here when you need to run WowData from the shell.

    The CLI reference covers:

    - `run`
    - `validate`
    - `schema`
    - `lock-schema`
    - common flags and exit codes

    Go to: [CLI](cli.md)

??? info "Sinks"

    Start here when you need to understand pipeline outputs.

    The sinks reference covers:

    - `Sink(...)` constructor arguments
    - output-type inference and current v0 constraints
    - writer options and directory requirements
    - sink write behavior and failure modes

    Go to: [Sinks](sinks.md)

??? info "Errors"

    Start here when you need to interpret WowData error codes.

    The errors reference covers:

    - common user-facing error codes
    - the major source, transform, pipeline, and sink failure families

    Go to: [Errors](errors.md)

## Repository Copy

A compact repository-level version is also available at
[REFERENCE.md](https://github.com/sci2pro/wowdata/blob/main/REFERENCE.md).
