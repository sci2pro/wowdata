# Errors

WowData™ raises instructional `WowDataUserError` exceptions.

Each error has:

- a stable code (for recognition and automation)
- a clear message
- a remediation hint

Example:

```text
[E_SOURCE_NOT_FOUND] Source file not found: 'missing.csv'.
Hint: Check the path, working directory, and filename.
```

## `WowDataUserError`

This is the main user-facing exception type exposed by the package.

It carries:

- `code`: a stable machine-friendly error code
- `message`: a short explanation
- `hint`: an optional remediation hint

This design is meant to make errors easier to teach from, easier to automate around, and easier to search for in documentation and tests.

## Error Families

??? info "Source Errors"

    These errors come from source construction, source reading, or source schema handling.

    Common examples:

    - `E_SOURCE_URI_TYPE`: invalid source URI type
    - `E_SOURCE_TYPE_INFER`: source type could not be inferred
    - `E_SOURCE_TYPE_UNSUPPORTED`: unsupported source type
    - `E_SOURCE_NOT_FOUND`: source file missing
    - `E_SOURCE_READ`: source could not be opened or read
    - `E_SOURCE_TABLE_UNSUPPORTED`: unsupported source table materialization

    Typical fixes:

    - check the input path
    - provide `type="csv"` explicitly when inference is ambiguous
    - verify delimiter and encoding options

??? info "Sink Errors"

    These errors come from sink construction or writing output.

    Common examples:

    - `E_SINK_TYPE_INFER`: sink type could not be inferred
    - `E_SINK_TYPE_UNSUPPORTED`: unsupported sink type
    - `E_SINK_DIR_NOT_FOUND`: output directory does not exist
    - `E_SINK_NOT_WRITABLE`: output directory is not writable
    - `E_SINK_WRITE`: generic sink write failure
    - `E_SINK_WRITE_UNSUPPORTED`: unsupported sink write operation

    Typical fixes:

    - create the output directory first
    - check permissions
    - confirm the sink path ends in `.csv` for v0

??? info "Pipeline And YAML Errors"

    These errors come from pipeline structure, YAML parsing, IR normalization, or serialization.

    Common examples:

    - `E_PIPELINE_STEP`: invalid step type passed to `Pipeline.then(...)`
    - `E_PIPELINE_ORDER`: transform appears after sink
    - `E_PIPELINE_STEP_TYPE`: unknown step type inside a pipeline
    - `E_YAML_IMPORT`: YAML library unavailable
    - `E_YAML_PARSE`: YAML parsing failed
    - `E_IR_ROOT`, `E_IR_PIPELINE`, `E_IR_SOURCE`, `E_IR_SINK`, `E_IR_STEP`, `E_IR_STEPS`, `E_IR_TRANSFORM`, `E_IR_VERSION`: invalid serialized pipeline structure

    Typical fixes:

    - check YAML indentation and quoting
    - keep pipelines linear in v0
    - ensure serialized pipeline objects match the expected WowData structure

??? info "Cast Errors"

    These errors come from `cast`.

    Common examples:

    - `E_CAST_TYPES`: invalid `types` mapping
    - `E_CAST_KEY`: invalid cast mapping key
    - `E_CAST_TYPE_UNSUPPORTED`: unsupported target type
    - `E_CAST_ON_ERROR`: invalid cast error policy
    - `E_CAST_MISSING_COL`: cast refers to a missing column
    - `E_CAST_COERCE`: a value could not be coerced
    - `E_CAST_INTERNAL`: unexpected internal failure while casting

??? info "Expression And Derive/Filter Errors"

    These errors come from the shared expression system used by `derive` and `filter`.

    Shared parse-level example:

    - `E_EXPR_PARSE`: low-level expression parse failure

    Derive examples:

    - `E_DERIVE_PARAMS`
    - `E_DERIVE_EXISTS`
    - `E_DERIVE_PARSE`
    - `E_DERIVE_UNKNOWN_COL`
    - `E_DERIVE_TYPE`
    - `E_DERIVE_UNSUPPORTED`

    Filter examples:

    - `E_FILTER_PARAMS`
    - `E_FILTER_PARSE`
    - `E_FILTER_UNKNOWN_COL`
    - `E_FILTER_TYPE`
    - `E_FILTER_UNSUPPORTED`

    Typical fixes:

    - check column spelling and case
    - use the shared Expression DSL correctly
    - `cast` earlier so numeric comparisons or arithmetic behave as expected

??? info "Select And Drop Errors"

    These errors come from column-shaping transforms.

    Common examples:

    - `E_SELECT_PARAMS`
    - `E_SELECT_UNKNOWN_COL`
    - `E_DROP_PARAMS`
    - `E_DROP_UNKNOWN_COL`

    Typical fixes:

    - verify the column names against the current schema
    - make sure the transform is placed after any step that creates the needed columns

??? info "String Errors"

    These errors come from the `string` transform family.

    Common examples:

    - `E_STRING_PARAMS`: invalid action parameters
    - `E_STRING_PATTERN`: invalid regex pattern
    - `E_STRING_GROUP`: invalid regex capture group
    - `E_STRING_UNKNOWN_COL`: missing source column
    - `E_STRING_EXISTS`: target column collision
    - `E_STRING_ACTION`: runtime error inside a string action

    Typical fixes:

    - confirm the action-specific arguments are correct
    - check regex groups when using `regex_extract`
    - set `overwrite=true` if `new` targets an existing column intentionally

??? info "Validate Errors"

    These errors come from the `validate` transform.

    Common examples:

    - `E_VALIDATE_PARAMS`: invalid validate parameters
    - `E_VALIDATE_IMPORT`: Frictionless is unavailable
    - `E_VALIDATE_NO_SCHEMA`: validation requires schema but none is known
    - `E_VALIDATE_READ`: sampling failed
    - `E_VALIDATE_FAILED_TO_RUN`: validation engine failed to execute
    - `E_VALIDATE_INVALID`: validation completed and the data was invalid

    Typical fixes:

    - install `frictionless`
    - provide or infer schema before validating
    - set `strict_schema=false` when appropriate

??? info "Join Errors"

    These errors come from `join`.

    Common examples:

    - `E_JOIN_PARAMS`: invalid join configuration
    - `E_JOIN_RIGHT` / `E_JOIN_RIGHT_READ`: invalid or unreadable right-hand source
    - `E_JOIN_READ_HEADERS`: headers could not be read
    - `E_JOIN_UNKNOWN_COL`: join key column missing on one side
    - `E_JOIN_KEY_TYPE_MISMATCH`: likely left/right key type mismatch
    - `E_JOIN_FAILED`: join execution failed

    Typical fixes:

    - use `on` when key names match
    - use `left_on` / `right_on` when key names differ
    - `cast` join keys so both sides use compatible types
    - inspect duplicate keys if a join behaves unexpectedly

## Practical Guidance

When reading a WowData error:

1. look at the `code` first
2. read the short message for the failure category
3. follow the `Hint:` text before changing code blindly

If you are using the CLI:

- parse/load failures typically surface before execution
- runtime transform and sink failures surface during `run`

For full code/message coverage, see tests in `wowdata/tests/` and compact table in
[REFERENCE.md](https://github.com/sci2pro/wowdata/blob/main/REFERENCE.md).
