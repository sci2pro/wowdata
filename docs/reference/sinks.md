# Sink Reference (v0)

This page documents the `Sink` model used to write pipeline output.

## Index

- [`Sink`](#sink)

<a id="sink"></a>
??? info "Sink"

    Define where a pipeline writes its output table.

    **Signature**

    `Sink(uri, type=None, options=None)`

    **Examples**

    === "Python"

        ```python
        from wowdata import Pipeline, Sink, Source

        pipe = Pipeline(Source("people.csv")).then(
            Sink(
                "people_out.csv",
                options={"delimiter": ","},
            )
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
            - sink:
                uri: people_out.csv
                type: csv
                options:
                  delimiter: ","
        ```

    **Arguments**

    `uri`

    Required.

    The output location. In v0 this is expected to be a CSV file path.

    Behavior notes:

    - if `type` is omitted, WowData tries to infer it from the file extension
    - the parent output directory must already exist
    - the parent output directory must be writable

    `type`

    Optional.

    The sink type. If omitted, WowData infers it from `uri`.

    In v0, the only supported sink type is:

    - `csv`

    If the type cannot be inferred, or an unsupported type is given, sink construction fails.

    `options`

    Optional. Default: `{}`.

    Extra options passed to the underlying CSV writer.

    Typical examples include:

    - `delimiter`
    - `encoding`

    These are passed through to PETL's CSV writing behavior.

    **Behavior**

    - `Sink.write()` writes the current table to the configured output path
    - sink construction fails early if the target directory does not exist
    - sink construction fails early if the target directory is not writable
    - write errors are wrapped in WowData user-facing errors

    **Operational Notes**

    Directory handling:

    - WowData does not create missing directories for you in v0
    - create the output directory before running the pipeline

    Output format:

    - only CSV sinks are supported in v0
    - if you need another format, you must convert after export or extend the codebase

    **When To Use It**

    Use `Sink` at the end of a pipeline when you want to persist the final table.

    Typical patterns:

    - write a cleaned CSV for downstream analysis
    - write a teaching example output from a small pipeline
    - write a normalized dataset after `cast`, `derive`, `join`, or `select`

    **See also**

    - [Sources](sources.md)
    - [Transforms](transforms.md)
    - [Errors](errors.md)
