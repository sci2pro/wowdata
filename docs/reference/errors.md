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

## High-frequency codes

- `E_SOURCE_NOT_FOUND`: source file missing
- `E_SINK_DIR_NOT_FOUND`: output directory missing
- `E_CAST_ON_ERROR`: invalid cast error policy
- `E_FILTER_PARSE`: invalid filter expression
- `E_JOIN_PARAMS`: missing/invalid join keys or right descriptor
- `E_YAML_PARSE`: invalid YAML syntax
- `E_PIPELINE_ORDER`: transform appears after sink

For full code/message coverage, see tests in `wowdata/tests/` and compact table in [REFERENCE.md](../../REFERENCE.md).
