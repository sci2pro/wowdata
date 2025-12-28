# **WowData™**

**WowData™ makes complex data engineering tasks easy for ordinary users.**

WowData™ is a human-centred data wrangling and pipeline framework designed to make advanced data preparation *understandable*, *teachable*, and *inspectable*—without sacrificing power.

It is built for people who need to work with real data, not just programmers.

---

## **Why WowData™?**

Most data tools assume that if you are touching data, you are already an expert.

WowData™ rejects this assumption.

We believe that:

* data engineering is a foundational skill,

* learning should be fast and durable,

* and tools should teach rather than intimidate.

WowData™ is designed so that **anyone can build, read, and reason about a data pipeline**—even when that pipeline performs non-trivial work.

---

## **Core Concepts**

WowData™ is built on four stable, universal concepts:

* **Source** — where data comes from

* **Transform** — how data changes

* **Sink** — where data goes

* **Pipeline** — how everything fits together

These concepts are deliberately persistent and reused everywhere.

We avoid hidden helpers, magical shortcuts, and proliferating abstractions.

If you understand these four ideas, you understand WowData™.

---

## **Example**

```python
from core import Source, Transform, Sink, Pipeline

pipe = (  
    Pipeline(Source("people.csv"))  
    .then(Transform("cast", params={"types": {"age": "integer"}, "on_error": "null"}))  
    .then(Transform("filter", params={"where": "age >= 30 and country == 'KE'"}))  
    .then(Sink("out_filtered.csv"))  
)

pipe.run()
```

This pipeline:

1. reads a CSV file,

2. explicitly casts a column,

3. filters rows using a small, teachable expression language,

4. and writes the result to disk.

Nothing is hidden. Nothing is inferred without consent.

---

## **Learning-First by Design**

WowData™ makes deliberate trade-offs to accelerate learning:

* A **small, closed vocabulary** of concepts

* A **restricted expression language** that can be mastered quickly

* **Explicit transforms** instead of silent automation

* **Deterministic checkpoints** for inspection

* A serializable pipeline that can be read like a recipe

If a feature makes learning harder—even if it saves time—we reject it.

---

## **Example: Learning-First Explicitness**

```python
from wowdata import Source, Transform, Sink, Pipeline

pipe = (
    Pipeline(Source("people.csv"))
    .then(
        Transform(
            "cast",
            params={
                "types": {"age": "integer"},
                "on_error": "null"   # explicit choice, not hidden behaviour
            }
        )
    )
    .then(
        Transform(
            "filter",
            params={
                "where": "age >= 18"
            }
        )
    )
    .then(Sink("adults.csv"))
)

pipe.run()
pipe.to_yaml("pipeline.yaml")
```

---

## **Serialization That Humans Can Read**

Every WowData™ pipeline can be serialized into a human-readable form.

Serialization is not configuration for machines—it is a **cognitive artifact** for people.

Our goal is that an ordinary user can:

* read a serialized pipeline,

* understand what it does,

* explain it to someone else,

* and safely modify it.

### Example: Human-Readable IR Serialization

The same pipeline can be represented as a simple, inspectable Intermediate Representation (IR) saved as `pipeline.yaml`:

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
          on_error: null
    - transform:
        op: filter
        params:
          where: "age >= 18"
    - sink:
        uri: adults.csv
        type: csv
```

This IR is deliberately verbose and stable.
It is designed to be read, reviewed, versioned, and edited by humans — not generated once and forgotten.

Because the IR mirrors the core concepts (Source → Transform → Sink),
anyone who understands WowData™ can understand what this pipeline does.

---

### Example: Loading a Pipeline from IR (YAML)

A serialized pipeline can be loaded back into WowData™ and executed:

```python
from wowdata import Pipeline

pipe = Pipeline.from_yaml("pipeline.yaml")
pipe.run()
```

This allows pipelines to be:
- authored or reviewed as YAML,
- stored in version control,
- shared between users or systems,
- and executed without modifying Python code.

The same IR is used by both the programmatic API and future graphical tools.

---

## Transform Reference (v0)

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

---

## **Errors That Teach**

In WowData™, error messages are part of the interface.

Every user-facing error:

1. explains what went wrong,

2. explains why it happened,

3. suggests what to do next.

Errors are designed to teach correct mental models, not expose internal stack traces.

### **Example: Errors That Teach**

```python
from wowdata import Source, Sink, Pipeline

pipe = Pipeline(Source("missing.csv")).then(Sink("out.csv"))
pipe.run()
```

produces the following error

```shell
wowdata.errors.WowDataUserError: [E_SOURCE_NOT_FOUND] Source file not found: 'missing.csv'.
Hint: Check the path, working directory, and filename. If the file is elsewhere, pass an absolute path.
```

---

## Error Codes (what they mean + what to do)

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

Keep this table handy when authoring pipelines; the “Action” column is the quickest way to resolve each error.

---

## **Built on the Best**

WowData™ does not reinvent proven tools.

Instead, it **piggybacks on best-in-class ecosystems**:

* mature ETL engines,

* established data modelling and validation frameworks,

* battle-tested execution backends.

Our contribution is an opinionated, human-centred layer that makes these tools usable by more people.

---

## **Open Source, Unapologetically**

WowData™ is open source by principle, not convenience.

We believe that tools shaping how people think about data must be:

* transparent,

* inspectable,

* extensible,

* and owned by the community.

---

## **A Graphical Future**

WowData™ is pipeline-first, not code-first or GUI-first.

We plan to build a **world-class graphical application** that:

* visualizes the same pipelines,

* manipulates the same underlying representation,

* and never diverges from the conceptual model.

The graphical interface will not be a toy—it will be another way of seeing the same truth.

---

## **What WowData™ Is Not**

WowData™ is not:

* a low-code gimmick,

* a black-box automation tool,

* a thin wrapper around someone else’s API,

* or a system that only experts can use safely.

If forced to choose between power and clarity, **we choose clarity**.

---

## **Status**

WowData™ is under active development.

The API is opinionated and evolving, but the core principles are stable.

If these ideas resonate with you, contributions and discussion are welcome.

You can install the bleeding edge by running: `pip install git+https://github.com/sci2pro/wowdata`

---

**WowData™ is not trying to make data engineering smaller.**

**It is trying to make it thinkable.**
