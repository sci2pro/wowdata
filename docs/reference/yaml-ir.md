# YAML / IR Format Reference (v0)

This page documents WowData's serialized pipeline format as used in YAML and internal IR dictionaries.

Use this page when you are:

- authoring pipeline YAML directly
- inspecting `Pipeline.to_ir()` or `Pipeline.to_yaml()` output
- trying to understand how WowData normalizes pipeline definitions when loading them

## Index

- [`Top-Level Structure`](#top-level-structure)
- [`Source Descriptor`](#source-descriptor)
- [`Transform Step`](#transform-step)
- [`Sink Step`](#sink-step)
- [`Normalization Rules`](#normalization-rules)

<a id="top-level-structure"></a>
??? info "Top-Level Structure"

    A WowData pipeline YAML file serializes to a mapping with two top-level keys:

    - `wowdata`
    - `pipeline`

    Minimal example:

    ```yaml
    wowdata: 0
    pipeline:
      start:
        uri: people.csv
        type: csv
      steps:
        - transform:
            op: select
            params:
              columns: [person_id, age]
        - sink:
            uri: out.csv
            type: csv
    ```

    Notes:

    - the supported version in v0 is `wowdata: 0`
    - `pipeline` must be a mapping
    - `steps` must be a list

<a id="source-descriptor"></a>
??? info "Source Descriptor"

    The pipeline start is stored under `pipeline.start`.

    Shape:

    ```yaml
    start:
      uri: people.csv
      type: csv
      schema:
        fields:
          - name: age
            type: integer
      options:
        delimiter: ","
    ```

    Main keys:

    - `uri`: required
    - `type`: optional if it can be inferred
    - `schema`: optional
    - `options`: optional

    This shape corresponds to the public [Source](sources.md) model.

<a id="transform-step"></a>
??? info "Transform Step"

    Each transform step is wrapped in a `transform:` mapping.

    Shape:

    ```yaml
    - transform:
        op: cast
        params:
          types:
            age: integer
          on_error: "null"
    ```

    Main keys inside the transform descriptor:

    - `op`: required transform operation name
    - `params`: optional mapping of operation-specific arguments
    - `output_schema`: optional explicit schema override

    Example with a schema-locked transform:

    ```yaml
    - transform:
        op: derive
        params:
          new: is_adult
          expr: "age >= 18"
        output_schema:
          fields:
            - name: age
              type: integer
            - name: is_adult
              type: boolean
    ```

    This shape corresponds to the public [Transform Object](transform-object.md) model.

<a id="sink-step"></a>
??? info "Sink Step"

    Each sink step is wrapped in a `sink:` mapping.

    Shape:

    ```yaml
    - sink:
        uri: out.csv
        type: csv
        options:
          delimiter: ","
    ```

    Main keys:

    - `uri`: required
    - `type`: optional if it can be inferred
    - `options`: optional

    This shape corresponds to the public [Sink](sinks.md) model.

<a id="normalization-rules"></a>
??? info "Normalization Rules"

    When WowData loads YAML or IR, it normalizes the structure before building a `Pipeline`.

    Important normalization behaviors:

    - if `wowdata` is missing, it defaults to `0`
    - relative source and sink paths are normalized relative to the YAML file location when loaded from disk
    - missing transform `params` become `{}` rather than `null`
    - missing `options` on sources and sinks become `{}` rather than `null`

    Join-specific normalization:

    - `join.params.right` is normalized relative to the base directory when it is a path
    - if the right-hand descriptor is a mapping, its `uri` is normalized too

    YAML ergonomics:

    - unquoted YAML `null` for `cast.on_error` is normalized to the `"null"` policy
    - YAML 1.1 can treat unquoted `on:` as a boolean key in some loaders; WowData normalizes that for join params

    Path/text loading behavior:

    - `Pipeline.from_yaml(...)` accepts either a path or raw YAML text
    - multiline document-like strings are treated as YAML text, not as filesystem paths

## IR And YAML

In practice:

- YAML is the human-authored surface
- IR is the normalized dict shape used by `Pipeline.to_ir()` and `Pipeline.from_ir(...)`

If you are authoring by hand, write YAML.

If you are inspecting programmatically, use `to_ir()` or `to_yaml()`.

## See Also

- [CLI](cli.md)
- [Pipeline](pipeline.md)
- [Transform Object](transform-object.md)
- [Transforms](transforms.md)
- [Errors](errors.md)
