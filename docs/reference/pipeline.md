# Pipeline Reference (v0)

This page documents the `Pipeline` model that connects a `Source`, zero or more `Transform`s, and one or more `Sink`s.

## Index

- [`Pipeline`](#pipeline)
- [`PipelineContext`](#pipeline-context)

<a id="pipeline"></a>
??? info "Pipeline"

    Define and execute a linear WowData workflow.

    **Signature**

    `Pipeline(start, steps=[])`

    **Examples**

    === "Python"

        ```python
        from wowdata import Pipeline, Sink, Source, Transform

        pipe = (
            Pipeline(Source("people.csv"))
            .then(Transform("cast", params={"types": {"age": "integer"}, "on_error": "null"}))
            .then(Transform("filter", params={"where": "age >= 18"}))
            .then(Sink("adults.csv"))
        )
        ```

    === "YAML"

        ```yaml
        wowdata: 0
        pipeline:
          start:
            uri: people.csv
            type: csv
          steps:
            - transform:
                op: cast
                params:
                  types:
                    age: integer
                  on_error: "null"
            - transform:
                op: filter
                params:
                  where: "age >= 18"
            - sink:
                uri: adults.csv
                type: csv
        ```

    **Arguments**

    `start`

    Required.

    The pipeline source. This must be a `Source` object in Python, or a valid `start:` descriptor in YAML.

    `steps`

    Optional. Default: `[]`.

    A list of `Transform` and `Sink` steps.

    In normal Python usage, you will usually build this list with `.then(...)` rather than passing `steps` directly.

    **Pipeline Shape**

    v0 pipelines are linear only:

    - one `Source`
    - zero or more `Transform`s
    - one or more `Sink`s at the end

    Ordering rule:

    - transforms cannot appear after a sink

    If you violate this ordering, WowData raises `E_PIPELINE_ORDER`.

    **Common Methods**

    `then(step)`

    Add a `Transform` or `Sink` and return a new pipeline value.

    Example:

    ```python
    pipe = Pipeline(Source("people.csv")).then(Transform("select", params={"columns": ["age"]}))
    ```

    `preflight()`

    Validate sources, sinks, and pipeline ordering before execution.

    This is called automatically by `run()`.

    `run()`

    Execute the pipeline end to end and return a `PipelineContext`.

    During execution:

    - source schema is initialized from `start.peek_schema()`
    - each transform updates the running table and best-effort schema
    - each sink writes the current table
    - checkpoints are recorded along the way

    `schema(sample_rows=200, force=False)`

    Infer the pipeline's output schema without running the full pipeline.

    This uses:

    - `Source.peek_schema(...)`
    - each transform's `output_schema(...)`

    `to_ir()`, `from_ir(...)`

    Serialize to and from WowData's internal YAML-friendly dict format.

    `to_yaml(...)`, `from_yaml(...)`

    Serialize to and from YAML text or YAML files.

    `save_yaml(...)`, `load_yaml(...)`

    Convenience wrappers for writing or reading pipeline YAML files.

    `lock_schema(sample_rows=200, force=False)`

    Return a new pipeline where each transform carries an explicit `output_schema`.

    This is useful when you want schema-aware validation to remain stable after serialization, especially around joins and other transforms that add or reshape columns.

    **YAML Notes**

    `from_yaml(...)` accepts either:

    - a file path
    - raw YAML text

    Relative paths inside YAML are normalized relative to the YAML file location when loaded from disk.

    **When To Use It**

    Use `Pipeline` whenever you want to:

    - compose sources, transforms, and sinks in Python
    - serialize a pipeline to YAML
    - load and run a pipeline from YAML
    - inspect execution checkpoints and validations after a run

    **See also**

    - [Sources](sources.md)
    - [Transforms](transforms.md)
    - [Sinks](sinks.md)
    - [Errors](errors.md)

<a id="pipeline-context"></a>
??? note "PipelineContext"

    `Pipeline.run()` returns a `PipelineContext`.

    Main fields:

    - `checkpoints`: step-by-step execution artifacts, including transform previews and sink records
    - `schema`: best-effort running schema after the last executed transform
    - `validations`: summaries produced by the `validate` transform

    This makes the pipeline runtime more inspectable, especially for teaching, debugging, and CLI output.
