# Transform Object Reference (v0)

This page documents the public `Transform` object used in Python pipelines.

Use this page when you need to understand the `Transform(...)` constructor itself.

Use [Transforms](transforms.md) when you need the per-operation reference for `cast`, `filter`, `join`, `string`, and the other transform types.

## Index

- [`Transform`](#transform)
- [`output_schema_override`](#output-schema-override)

<a id="transform"></a>
??? info "Transform"

    Represent one pipeline step that transforms a table.

    **Signature**

    `Transform(op, params=None, output_schema_override=None)`

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

    `op`

    Required.

    The transform operation name.

    Common examples:

    - `cast`
    - `select`
    - `derive`
    - `filter`
    - `drop`
    - `string`
    - `validate`
    - `join`

    See the full operation catalog in [Transforms](transforms.md).

    `params`

    Optional. Default: `{}`.

    A mapping of operation-specific parameters.

    Example:

    ```python
    Transform("select", params={"columns": ["person_id", "age"]})
    ```

    The meaning of `params` depends on `op`.

    `output_schema_override`

    Optional.

    An explicit schema descriptor to use instead of normal best-effort schema inference.

    This is mainly used in schema-locked pipelines and advanced cases where a transform changes the shape of data in a way that should be recorded explicitly.

    **Behavior**

    - `Transform.apply(...)` validates parameters and applies the operation to the current table
    - `Transform.output_schema(...)` returns the transform's best-effort output schema
    - if `output_schema_override` is present, it takes precedence over inferred output schema
    - `Transform` itself is the public wrapper; the concrete operation logic lives behind the registered transform implementation for `op`

    **When To Use It**

    Use `Transform(...)` in Python whenever you are building a pipeline programmatically.

    Typical examples:

    - `Transform("cast", params={...})`
    - `Transform("filter", params={...})`
    - `Transform("join", params={...})`

    **See also**

    - [Transforms](transforms.md)
    - [Pipeline](pipeline.md)
    - [Sources](sources.md)
    - [Sinks](sinks.md)

<a id="output-schema-override"></a>
??? note "output_schema_override"

    `output_schema_override` is the main advanced constructor argument on `Transform`.

    You will most often encounter it when using:

    - `Pipeline.lock_schema(...)`
    - `Pipeline.to_yaml(lock_schema=True, ...)`
    - schema-locked YAML files produced by the CLI

    In those cases, WowData stores explicit per-transform output schemas so that later validation and serialization remain stable.

## See Also

- [Transforms](transforms.md)
- [Pipeline](pipeline.md)
- [CLI](cli.md)
- [Errors](errors.md)
