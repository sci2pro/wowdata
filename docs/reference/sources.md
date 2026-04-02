# Source Reference (v0)

This page documents the `Source` model used at the start of a WowData pipeline.

## Index

- [`Source`](#source)

<a id="source"></a>
??? info "Source"

    Define where a pipeline reads its input table from.

    **Signature**

    `Source(uri, type=None, schema=None, options=None)`

    **Examples**

    === "Python"

        ```python
        from wowdata import Pipeline, Sink, Source

        pipe = Pipeline(
            Source(
                "people.csv",
                schema={
                    "fields": [
                        {"name": "person_id", "type": "string"},
                        {"name": "age", "type": "integer"},
                        {"name": "country", "type": "string"},
                    ]
                },
                options={"delimiter": ","},
            )
        ).then(Sink("people_out.csv"))
        ```

    === "YAML"

        ```yaml
        wowdata: 0
        pipeline:
          start:
            uri: people.csv
            type: csv
            schema:
              fields:
                - name: person_id
                  type: string
                - name: age
                  type: integer
                - name: country
                  type: string
            options:
              delimiter: ","
          steps:
            - sink:
                uri: people_out.csv
                type: csv
        ```

    **Arguments**

    `uri`

    Required.

    The source location. In v0 this is expected to be a local CSV file path.

    Accepted forms:

    - string path such as `"people.csv"`
    - path-like object such as `Path("people.csv")` in Python

    Behavior notes:

    - path-like inputs are normalized to strings
    - if `type` is omitted, WowData tries to infer it from the file extension
    - CSV files are checked for existence early, at source construction time

    `type`

    Optional.

    The source type. If omitted, WowData infers it from `uri`.

    In v0, the only supported source type is:

    - `csv`

    If the type cannot be inferred, or an unsupported type is given, source construction fails.

    `schema`

    Optional.

    Schema information for the source.

    Supported forms:

    - inline schema mapping, typically a frictionless-style descriptor with `fields`
    - schema reference string

    Common inline example:

    ```python
    {
        "fields": [
            {"name": "age", "type": "integer"},
            {"name": "country", "type": "string"},
        ]
    }
    ```

    Providing schema helps with:

    - better preflight validation in transforms such as `select`, `drop`, and `string`
    - stricter `validate` behavior
    - clearer pipeline intent

    `options`

    Optional. Default: `{}`.

    Extra options passed to the underlying CSV reader.

    Typical examples include:

    - `delimiter`
    - `encoding`

    These are passed through to PETL's CSV loading behavior.

    **Behavior**

    - `Source.table()` returns a PETL table
    - CSV sources fail fast if the file does not exist
    - source reading errors are wrapped in WowData user-facing errors
    - `peek_schema()` performs best-effort schema inference and caches the result
    - `schema_warnings()` returns warnings collected during schema inference

    **Schema Inference**

    `peek_schema()` inspects the source and returns a best-effort schema descriptor.

    Important notes:

    - inference is cached unless forced
    - it depends on the optional `frictionless` dependency
    - if Frictionless is unavailable, WowData returns an empty schema and records a warning
    - WowData may add heuristic warnings when string columns look mostly numeric, to suggest an explicit `cast`

    **When To Use It**

    Use `Source` whenever you are starting a pipeline from a tabular file.

    Typical patterns:

    - declare an inline schema when you want stronger validation and clearer teaching value
    - rely on type inference only for quick exploratory work
    - set `options` when the CSV is not using the default delimiter or encoding

    **See also**

    - [Transforms](transforms.md)
    - [Errors](errors.md)
