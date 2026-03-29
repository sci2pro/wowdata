# WowData™ Reference

## CLI

Primary command:

```bash
wow --help
```

Fallback command:

```bash
wowdata --help
```

Commands:

- `run`: execute a YAML pipeline
- `validate`: parse + preflight checks
- `schema`: infer output schema without full run
- `lock-schema`: write schema-locked YAML

Repository examples:

```bash
wow run examples/climate_heat_events.yaml --base-dir examples
wow run examples/climate_rainfall_alerts.yaml --base-dir examples
```

Use `--base-dir examples` when running the checked-in sample YAML files from the repo root so relative
CSV paths resolve against the `examples/` directory.

Exit codes:

- `0`: success
- `2`: CLI usage error
- `3`: pipeline parse/validation error
- `4`: pipeline runtime execution error

## Transform Reference (v0)

| Transform | Required params | Optional params | Example |
|-----------|-----------------|-----------------|---------|
| `cast` | `types` mapping `{column: type}` | `on_error`: `fail` (default) \| `null` \| `keep` | `Transform("cast", params={"types": {"age": "integer"}, "on_error": "null"})` |
| `select` | `columns`: list of column names | — | `Transform("select", params={"columns": ["person_id", "age"]})` |
| `derive` | `new`: column name, `expr`: expression string | `overwrite` (bool, default `False`), `strict` (bool, default `True`) | `Transform("derive", params={"new": "is_adult", "expr": "age >= 18", "overwrite": True})` |
| `filter` | `where`: expression string | `strict` (bool, default `True`) | `Transform("filter", params={"where": "age >= 30 and country == 'KE'"})` |
| `drop` | `columns`: list of column names | — | `Transform("drop", params={"columns": ["debug_col"]})` |
| `string` | `column`, `action`, `pattern` | `new`, `overwrite` (bool, default `False`), `repl` (for `regex_replace`), `group` (for `regex_extract`, default `0`) | `Transform("string", params={"column": "Price", "action": "regex_replace", "pattern": "[^0-9.]+", "repl": ""})` |
| `validate` | — | `sample_rows` (int, default `5000`), `fail` (bool, default `True`), `strict_schema` (bool, default `True`) | `Transform("validate", params={"sample_rows": 1000, "fail": False})` |
| `join` | `right`: URI or descriptor, `on`: column/list of columns | `how` (`inner` default/`left`), `right_on`, `suffixes` (`("_left","_right")` default), `options` (dict) | `Transform("join", params={"right": "other.csv", "on": ["id"], "how": "left"})` |

Notes:

- Expression params (`expr`, `where`) use the same DSL as `filter`/`derive` (logical ops, comparisons, literals, column names).
- Types accepted by `cast` align with frictionless types (`integer`, `number`, `string`, etc.).
- `string.action` currently supports `regex_replace` and `regex_extract`.
- `validate` requires the optional `frictionless` dependency to be installed.
- Canonical `cast.on_error` values are string enums; unquoted YAML `null` is accepted and normalized to `"null"`.
- Unquoted YAML `on:` in join params is accepted even when the parser treats it as a boolean key.

String cleaning example:

```yaml
- transform:
    op: string
    params:
      column: Price
      action: regex_replace
      pattern: "[^0-9.]+"
      repl: ""
```

## YAML IR Shape

```yaml
wowdata: 0
pipeline:
  start:
    uri: input.csv
    type: csv
  steps:
    - transform:
        op: filter
        params:
          where: "age >= 18"
    - sink:
        uri: out.csv
        type: csv
```

## Errors That Teach

User-facing errors are raised as `WowDataUserError` with a stable code and hint.

Example:

```text
[E_SOURCE_NOT_FOUND] Source file not found: 'missing.csv'.
Hint: Check the path, working directory, and filename.
```

## Error Codes (full table)

| Code | Example message | Action |
|------|-----------------|--------|
| E_SOURCE_URI_TYPE | `Source uri must be a string or path-like; got int.` | Pass a string or `pathlib.Path`. |
| E_SOURCE_TYPE_INFER | `Could not infer Source type from uri='file'.` | Add a `.csv` suffix or pass `type="csv"`. |
| E_SOURCE_TYPE_UNSUPPORTED | `Source type 'json' is not supported in v0.` | Use `csv` or wait for future support. |
| E_SOURCE_NOT_FOUND | `Source file not found: 'missing.csv'.` | Fix the path or working directory. |
| E_SOURCE_TABLE_UNSUPPORTED | `Source type 'x' cannot be materialized as a table in v0.` | Use `csv` sources only. |
| E_SOURCE_READ | `Could not open source 'file.csv': ...` | Check permissions/encoding/options. |
| E_JOIN_RIGHT | `join params.right mapping must include a non-empty 'uri' string.` | Provide a valid `right` URI/descriptor. |
| E_SINK_TYPE_INFER | `Could not infer Sink type from uri='out'.` | Add a `.csv` suffix or pass `type="csv"`. |
| E_SINK_TYPE_UNSUPPORTED | `Sink type 'json' is not supported in v0.` | Use `csv` sinks only. |
| E_SINK_DIR_NOT_FOUND | `Output directory does not exist: '/path'.` | Create the directory or choose another path. |
| E_SINK_NOT_WRITABLE | `Output directory is not writable: '/path'.` | Fix permissions or pick a writable location. |
| E_SINK_WRITE | `Could not write sink 'out.csv': ...` | Check permissions and sink options. |
| E_SINK_WRITE_UNSUPPORTED | `Sink type 'x' cannot be written in v0.` | Use `csv` sinks only. |
| E_PIPELINE_STEP | `Pipeline.then expects a Transform or Sink.` | Only add `Transform` or `Sink` steps. |
| E_PIPELINE_ORDER | `A Transform cannot be added after a Sink.` | Move sinks to the end. |
| E_PIPELINE_STEP_TYPE | `Pipeline contains an unknown step type.` | Inspect steps; ensure only Transform/Sink. |
| E_IR_STEP / E_IR_STEPS | `IR step #0 must be ...` / `IR pipeline.steps must be a list.` | Fix YAML/IR structure. |
| E_IR_ROOT / E_IR_PIPELINE / E_IR_VERSION | `IR must be a mapping...` / `Unsupported IR version.` | Use `{wowdata: 0, pipeline: {...}}` shape. |
| E_IR_SOURCE | `IR source requires a non-empty 'uri' string.` | Provide a valid start source URI. |
| E_IR_SINK | `IR sink must be a mapping.` | Provide `{sink: {uri: out.csv}}`. |
| E_IR_TRANSFORM / E_IR_TRANSFORM_SCHEMA | `IR transform must be a mapping.` / `Invalid output_schema override.` | Fix transform entries in IR. |
| E_YAML_IMPORT | `PyYAML is not available; cannot serialize/parse YAML.` | Install `pyyaml`. |
| E_YAML_PARSE | `Failed to parse YAML: ...` | Fix YAML indentation/format. |
| E_SCHEMA_INLINE_TYPE / E_SCHEMA_INLINE_FIELDS | `Inline schema must be a mapping.` / `schema['fields'] must be a list.` | Pass a valid schema dict. |
| E_CAST_TYPES | `cast requires params.types as a non-empty mapping.` | Provide the `types` dict. |
| E_CAST_ON_ERROR | `cast params.on_error must be one of ...` | Use `fail`, `null`, or `keep`. |
| E_CAST_TYPE_UNSUPPORTED | `cast type 'x' is not supported.` | Use supported types (integer, number, string, etc.). |
| E_CAST_KEY / E_CAST_MISSING_COL | `Unknown column in cast types...` | Cast only existing columns. |
| E_CAST_COERCE / E_CAST_INTERNAL | `Could not coerce value...` | Fix data or choose `on_error='null'/'keep'`. |
| E_SELECT_PARAMS | `select requires params.columns...` | Provide a non-empty columns list. |
| E_SELECT_UNKNOWN_COL | `select refers to column(s) not present...` | Adjust column names/order. |
| E_DERIVE_PARAMS | `derive requires params.new/expr...` | Provide `new` and `expr`. |
| E_DERIVE_EXISTS | `derive would create column 'x' but it already exists...` | Set `overwrite=true` or choose a new name. |
| E_DERIVE_PARSE / E_DERIVE_TYPE / E_DERIVE_UNKNOWN_COL / E_DERIVE_UNSUPPORTED | Expression errors or bad types. | Fix the expression and column names; cast before derive if needed. |
| E_FILTER_PARAMS | `filter requires params.where...` | Provide a non-empty filter expression. |
| E_FILTER_PARSE / E_EXPR_PARSE | Parse errors in expressions. | Fix syntax; use column names, literals, and/or/not. |
| E_FILTER_UNKNOWN_COL | `Unknown column 'x' in filter expression.` | Correct column name(s). |
| E_FILTER_TYPE | `Type mismatch in filter comparison...` | Cast columns or set `strict=false`. |
| E_FILTER_UNSUPPORTED | `Unsupported construct in filter expression.` | Stick to comparisons, and/or/not, parentheses. |
| E_DROP_PARAMS | `drop requires params.columns...` | Provide columns to drop. |
| E_DROP_UNKNOWN_COL | `drop refers to column(s) not present...` | Drop only existing columns. |
| E_STRING_PARAMS | `string requires params.column/action/pattern...` | Provide a valid string transform config. |
| E_STRING_PATTERN | `Invalid string regex pattern: ...` | Fix the regex syntax. |
| E_STRING_GROUP | `string regex_extract group 'x' was not found...` | Use an existing group index or group name. |
| E_STRING_UNKNOWN_COL / E_STRING_EXISTS | Missing source column or target column collision. | Fix the column name, or set `overwrite=true`. |
| E_VALIDATE_PARAMS | `validate params.sample_rows must be a positive integer.` | Fix validate parameters. |
| E_VALIDATE_IMPORT | `Validation requires the 'frictionless' dependency...` | Install `frictionless`. |
| E_VALIDATE_READ | `validate could not read a sample of rows...` | Check source options/permissions. |
| E_VALIDATE_NO_SCHEMA / E_VALIDATE_INVALID / E_VALIDATE_FAILED_TO_RUN | Validation failed or schema missing. | Inspect validation report; adjust schema/data. |
| E_JOIN_PARAMS | `join requires params.right and params.on...` | Provide `right` and `on` columns. |
| E_JOIN_KEY_TYPE_MISMATCH | `join key columns must be strings or list of strings.` | Fix `on`/`right_on` types. |
| E_JOIN_UNKNOWN_COL / E_JOIN_READ_HEADERS / E_JOIN_RIGHT_READ | Issues reading/joining right table. | Fix column names and ensure right source is readable. |
| E_JOIN_FAILED | `join failed: ...` | Check join parameters and data integrity. |
| E_OP_NOT_IMPL | `Transform op 'x' is not implemented in v0 executor yet.` | Use supported ops or implement/register it. |
| E_IR_TRANSFORM_SCHEMA | `Invalid output_schema override: ...` | Provide a valid schema dict for overrides. |

Keep this table handy when authoring pipelines; the Action column is the fastest way to resolve most failures.
