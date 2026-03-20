# Transform Reference (v0)

| Transform | Required params | Optional params | Example |
|-----------|-----------------|-----------------|---------|
| `cast` | `types` mapping `{column: type}` | `on_error`: `fail` (default) \| `null` \| `keep` | `Transform("cast", params={"types": {"age": "integer"}, "on_error": "null"})` |
| `select` | `columns`: list of column names | — | `Transform("select", params={"columns": ["person_id", "age"]})` |
| `derive` | `new`: column name, `expr`: expression string | `overwrite` (bool, default `False`), `strict` (bool, default `True`) | `Transform("derive", params={"new": "is_adult", "expr": "age >= 18", "overwrite": True})` |
| `filter` | `where`: expression string | `strict` (bool, default `True`) | `Transform("filter", params={"where": "age >= 30 and country == 'KE'"})` |
| `drop` | `columns`: list of column names | — | `Transform("drop", params={"columns": ["debug_col"]})` |
| `validate` | — | `sample_rows` (int, default `5000`), `fail` (bool, default `True`), `strict_schema` (bool, default `True`) | `Transform("validate", params={"sample_rows": 1000, "fail": False})` |
| `join` | `right`: URI or descriptor, `on`: column/list of columns | `how` (`inner` default/`left`), `right_on`, `suffixes` (`("_left","_right")` default), `options` (dict) | `Transform("join", params={"right": "other.csv", "on": ["id"], "how": "left"})` |

Notes:
- Expression params (`expr`, `where`) use the same DSL as `filter`/`derive` (logical ops, comparisons, literals, column names).
- Types accepted by `cast` align with frictionless types (`integer`, `number`, `string`, etc.).
- `validate` requires the optional `frictionless` dependency to be installed.

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
