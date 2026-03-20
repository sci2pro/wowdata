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

Exit codes:

- `0`: success
- `2`: CLI usage error
- `3`: pipeline parse/validation error
- `4`: pipeline runtime execution error

## Transforms (v0)

| Transform | Required params | Optional params |
|---|---|---|
| `cast` | `types` mapping `{column: type}` | `on_error`: `fail` (default) \| `null` \| `keep` |
| `select` | `columns`: list of column names | — |
| `derive` | `new`, `expr` | `overwrite` (bool), `strict` (bool) |
| `filter` | `where` | `strict` (bool) |
| `drop` | `columns` | — |
| `validate` | — | `sample_rows`, `fail`, `strict_schema` |
| `join` | `right` and key spec (`on` or `left_on`/`right_on`) | `how`, `strict_types` |

Notes:

- `on_error` is canonically a string enum; unquoted YAML `null` is tolerated and normalized to the `"null"` policy.
- YAML `join` key `on` is normalized even when YAML parses it as a boolean key.

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

## Errors

User-facing errors are raised as `WowDataUserError` with a stable code and hint:

```text
[E_SOURCE_NOT_FOUND] Source file not found: 'missing.csv'.
Hint: Check the path, working directory, and filename.
```

See docs site reference pages for expanded details:

- `docs/reference/transforms.md`
- `docs/reference/errors.md`
