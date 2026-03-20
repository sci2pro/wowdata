# Transforms (v0)

| Transform | Required params | Optional params | Notes |
|---|---|---|---|
| `cast` | `types` mapping `{column: type}` | `on_error`: `fail` (default) \| `null` \| `keep` | Coerces values to target type. |
| `select` | `columns` list | — | Keeps only specified columns. |
| `derive` | `new`, `expr` | `overwrite`, `strict` | Adds/replaces derived column from expression DSL. |
| `filter` | `where` | `strict` | Keeps rows matching expression DSL predicate. |
| `drop` | `columns` list | — | Removes specified columns. |
| `validate` | — | `sample_rows`, `fail`, `strict_schema` | Validates sampled rows against schema. |
| `join` | `right` + key spec | `how`, `strict_types` | Joins left table to right source. |

## YAML Ergonomics

- Canonical `cast.on_error` values are strings: `fail`, `null`, `keep`.
- Unquoted YAML `null` for `on_error` is accepted and normalized to `"null"`.
- Unquoted YAML key `on:` in join params is accepted even when parser treats it as a boolean key.

## Expression DSL

Used by `filter.where` and `derive.expr`:

- logical: `and`, `or`, `not`
- comparisons: `==`, `!=`, `>`, `>=`, `<`, `<=`
- literals: strings, numbers, booleans, `null`
- parentheses for grouping
- arithmetic in `derive`: `+`, `-`, `*`, `/`
