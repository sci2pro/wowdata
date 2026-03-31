# Transform Reference (v0)

| Transform | Required params | Optional params | Example |
|-----------|-----------------|-----------------|---------|
| `cast` | `types` mapping `{column: type}` | `on_error`: `fail` (default) \| `null` \| `keep` | `Transform("cast", params={"types": {"age": "integer"}, "on_error": "null"})` |
| `select` | `columns`: list of column names | — | `Transform("select", params={"columns": ["person_id", "age"]})` |
| `derive` | `new`: column name, `expr`: expression string | `overwrite` (bool, default `False`), `strict` (bool, default `True`) | `Transform("derive", params={"new": "is_adult", "expr": "age >= 18", "overwrite": True})` |
| `filter` | `where`: expression string | `strict` (bool, default `True`) | `Transform("filter", params={"where": "age >= 30 and country == 'KE'"})` |
| `drop` | `columns`: list of column names | — | `Transform("drop", params={"columns": ["debug_col"]})` |
| `string` | `column`, `action` | `new`, `overwrite` (bool, default `False`), regex params (`pattern`, `repl`, `group`), and action-specific params such as `chars`, `sep`, `prefix`, `suffix`, `old`, `new_value`, `count`, `args`, `kwargs`, `encoding`, `errors`, `width` | `Transform("string", params={"column": "Price", "action": "regex_replace", "pattern": "[^0-9.]+", "repl": ""})` |
| `validate` | — | `sample_rows` (int, default `5000`), `fail` (bool, default `True`), `strict_schema` (bool, default `True`) | `Transform("validate", params={"sample_rows": 1000, "fail": False})` |
| `join` | `right`: URI or descriptor, `on`: column/list of columns | `how` (`inner` default/`left`), `right_on`, `suffixes` (`("_left","_right")` default), `options` (dict) | `Transform("join", params={"right": "other.csv", "on": ["id"], "how": "left"})` |

Notes:
- Expression params (`expr`, `where`) use the same DSL as `filter`/`derive` (logical ops, comparisons, literals, column names).
- Types accepted by `cast` align with frictionless types (`integer`, `number`, `string`, etc.).
- `string.action` supports `regex_replace`, `regex_extract`, `capitalize`, `casefold`, `encode`, `format`, `lower`, `lstrip`, `partition`, `removeprefix`, `removesuffix`, `replace`, `rpartition`, `rstrip`, `split`, `strip`, `swapcase`, `title`, `upper`, and `zfill`.
- `validate` requires the optional `frictionless` dependency to be installed.

## String Actions

All `string` actions require:

- `column`: source column name
- `action`: operation name

Common optional params:

- `new`: write to a new column instead of replacing `column`
- `overwrite`: allow replacing an existing target column

Action-specific params:

| Action | Extra params | Result type | Example |
|--------|--------------|-------------|---------|
| `regex_replace` | `pattern`, `repl` (default `""`) | `string` | `{"column": "Price", "action": "regex_replace", "pattern": "[^0-9.]+", "repl": ""}` |
| `regex_extract` | `pattern`, `group` (default `0`) | `string` | `{"column": "size", "action": "regex_extract", "pattern": "([0-9]+(?:\\.[0-9]+)?)", "group": 1}` |
| `capitalize` | — | `string` | `{"column": "name", "action": "capitalize"}` |
| `casefold` | — | `string` | `{"column": "email", "action": "casefold"}` |
| `encode` | `encoding` (default `"utf-8"`), `errors` (default `"strict"`) | `any` | `{"column": "payload", "action": "encode", "encoding": "utf-8"}` |
| `format` | `args` (list/tuple), `kwargs` (mapping) | `string` | `{"column": "template", "action": "format", "kwargs": {"name": "Ada"}}` |
| `lower` | — | `string` | `{"column": "city", "action": "lower"}` |
| `lstrip` | `chars` | `string` | `{"column": "code", "action": "lstrip", "chars": "0 "}` |
| `partition` | `sep` | `any` | `{"column": "full_code", "action": "partition", "sep": "-"}` |
| `removeprefix` | `prefix` | `string` | `{"column": "sku", "action": "removeprefix", "prefix": "SKU-"}` |
| `removesuffix` | `suffix` | `string` | `{"column": "filename", "action": "removesuffix", "suffix": ".csv"}` |
| `replace` | `old`, `new_value`, `count` | `string` | `{"column": "title", "action": "replace", "old": "_", "new_value": " "}` |
| `rpartition` | `sep` | `any` | `{"column": "path", "action": "rpartition", "sep": "/"}` |
| `rstrip` | `chars` | `string` | `{"column": "code", "action": "rstrip", "chars": " ."}` |
| `split` | `sep`, `maxsplit` | `any` | `{"column": "tags", "action": "split", "sep": ",", "maxsplit": 2}` |
| `strip` | `chars` | `string` | `{"column": "name", "action": "strip"}` |
| `swapcase` | — | `string` | `{"column": "headline", "action": "swapcase"}` |
| `title` | — | `string` | `{"column": "headline", "action": "title"}` |
| `upper` | — | `string` | `{"column": "country", "action": "upper"}` |
| `zfill` | `width` | `string` | `{"column": "postal_code", "action": "zfill", "width": 5}` |

Notes:

- `split`, `partition`, `rpartition`, and `encode` produce non-string values, so their inferred schema type is `any`.
- `format` uses Python-style `str.format(...)` semantics.
- `replace` uses `new_value` for the replacement text so `new` remains reserved for the target column name.

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
