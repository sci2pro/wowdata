# Transform Reference (v0)

This page is organized one transform at a time. Start from the index, then jump to the transform you want.

## Index

- [`cast`](#cast)
- [`select`](#select)
- [`derive`](#derive)
- [`filter`](#filter)
- [`drop`](#drop)
- [`string`](#string)
- [`validate`](#validate)
- [`join`](#join)
- [`Expression DSL`](#expression-dsl)

<a id="cast"></a>
??? info "Cast"

    Convert one or more columns to explicit types such as `integer`, `number`, or `string`.

    **Signature**

    `Transform("cast", params={"types": {"column_name": "type"}, "on_error": "fail"})`

    **Examples**

    === "Python"

        ```python
        from wowdata import Pipeline, Sink, Source, Transform

        pipe = (
            Pipeline(Source("people.csv"))
            .then(
                Transform(
                    "cast",
                    params={
                        "types": {
                            "age": "integer",
                            "income": "number",
                        },
                        "on_error": "null",
                    },
                )
            )
            .then(Sink("people_clean.csv"))
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
                    income: number
                  on_error: "null"
            - sink:
                uri: people_clean.csv
                type: csv
        ```

    **Arguments**

    `types`

    Required.

    A mapping from column name to target type.

    Example:

    ```python
    {"age": "integer", "income": "number"}
    ```

    Each key is the name of an existing column. Each value is the type to coerce into.

    Common target types include:

    - `integer`
    - `number`
    - `string`
    - `boolean`
    - `date`
    - `datetime`

    These align with the frictionless-style types used elsewhere in WowData.

    `on_error`

    Optional. Default: `fail`.

    Controls what happens when a value cannot be converted to the requested type.

    Accepted values:

    - `fail`: stop the pipeline with an error
    - `null`: replace invalid values with null
    - `keep`: keep the original value unchanged

    Example:

    ```python
    {"types": {"age": "integer"}, "on_error": "null"}
    ```

    YAML note:

    - unquoted YAML `null` is accepted and normalized to the `"null"` policy

    **Behavior**

    - `cast` updates values in place; it does not create new columns
    - multiple columns can be cast in a single transform
    - output schema inference follows the requested target types
    - if a referenced column is missing, validation fails before execution

    **When To Use It**

    Use `cast` when values arrive as strings but should behave as typed data in later steps.

    Typical follow-up transforms include:

    - `filter` on numeric comparisons like `age >= 18`
    - `derive` expressions using numbers or booleans
    - `validate` when you want typed values checked against schema expectations

    **See also**

    - [`filter`](#filter)
    - [`derive`](#derive)
    - [`validate`](#validate)

<a id="select"></a>
??? info "Select"

    Keep only the listed columns, in the order you specify.

    **Signature**

    `Transform("select", params={"columns": ["column_a", "column_b"]})`

    **Examples**

    === "Python"

        ```python
        from wowdata import Pipeline, Sink, Source, Transform

        pipe = (
            Pipeline(Source("people.csv"))
            .then(
                Transform(
                    "select",
                    params={
                        "columns": ["person_id", "age", "country"],
                    },
                )
            )
            .then(Sink("people_selected.csv"))
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
                op: select
                params:
                  columns: [person_id, age, country]
            - sink:
                uri: people_selected.csv
                type: csv
        ```

    **Arguments**

    `columns`

    Required.

    A non-empty list of column names to keep.

    Example:

    ```python
    {"columns": ["person_id", "age", "country"]}
    ```

    The order matters. The output table keeps columns in the order listed here.

    **Behavior**

    - `select` removes every column not listed in `columns`
    - the output column order follows the `columns` list, not the input table order
    - if schema information is available, missing column names are caught before execution
    - output schema inference keeps only the selected fields

    **When To Use It**

    Use `select` near the end of a pipeline when you want to:

    - publish a smaller output table
    - remove intermediate working columns after `derive`, `string`, or `join`
    - present columns in a specific order for downstream users or systems

    A common pattern is:

    - clean and derive values first
    - use `select` to keep only the final output columns

    **See also**

    - [`drop`](#drop)
    - [`derive`](#derive)
    - [`join`](#join)

<a id="derive"></a>
??? info "Derive"

    Compute a new column from an expression.

    **Signature**

    `Transform("derive", params={"new": "column_name", "expr": "age >= 18", "overwrite": False, "strict": True})`

    **Examples**

    === "Python"

        ```python
        from wowdata import Pipeline, Sink, Source, Transform

        pipe = (
            Pipeline(Source("people.csv"))
            .then(
                Transform(
                    "derive",
                    params={
                        "new": "is_adult",
                        "expr": "age >= 18",
                    },
                )
            )
            .then(Sink("people_enriched.csv"))
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
                op: derive
                params:
                  new: is_adult
                  expr: "age >= 18"
            - sink:
                uri: people_enriched.csv
                type: csv
        ```

    **Arguments**

    `new`

    Required.

    The name of the column to create.

    Example:

    ```python
    {"new": "is_adult", "expr": "age >= 18"}
    ```

    If the column already exists, `derive` will fail unless `overwrite` is set to `true`.

    `expr`

    Required.

    The expression used to compute each row's new value.

    Example:

    ```python
    {"new": "age_next_year", "expr": "age + 1"}
    ```

    `expr` uses the shared [`Expression DSL`](#expression-dsl).

    `overwrite`

    Optional. Default: `False`.

    Controls whether `derive` may replace an existing column with the same name as `new`.

    Accepted values:

    - `false`: fail if `new` already exists
    - `true`: replace the existing column

    `strict`

    Optional. Default: `True`.

    Controls how type mismatches are handled during expression evaluation.

    Accepted values:

    - `true`: raise an error on unsupported operations or comparisons
    - `false`: return a fallback result instead of failing

    In practice:

    - invalid arithmetic usually becomes `null`
    - invalid comparisons usually become `false`

    **Behavior**

    - `derive` can create a new column or replace an existing one when `overwrite=true`
    - arithmetic supports `+`, `-`, `*`, `/`
    - string concatenation is supported with `+`
    - comparisons and boolean operators use the shared [`Expression DSL`](#expression-dsl)
    - unknown column names raise an error with a suggestion when possible
    - output schema inference attempts a best-effort type guess for simple expressions

    **When To Use It**

    Use `derive` when you need row-level computed values such as:

    - boolean flags like `is_adult`
    - numeric calculations like `rain_mm / 10`
    - string assembly like `first_name + ' ' + last_name`

    A common pattern is:

    - `cast` first so numeric and date-like columns behave predictably
    - `derive` computed columns
    - `select` only the final fields you want to keep

    **See also**

    - [`cast`](#cast)
    - [`filter`](#filter)
    - [`select`](#select)

<a id="filter"></a>
??? info "Filter"

    Keep only the rows whose expression evaluates to true.

    **Signature**

    `Transform("filter", params={"where": "age >= 30 and country == 'KE'", "strict": True})`

    **Examples**

    === "Python"

        ```python
        from wowdata import Pipeline, Sink, Source, Transform

        pipe = (
            Pipeline(Source("people.csv"))
            .then(
                Transform(
                    "filter",
                    params={
                        "where": "age >= 30 and country == 'KE'",
                    },
                )
            )
            .then(Sink("people_filtered.csv"))
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
                op: filter
                params:
                  where: "age >= 30 and country == 'KE'"
            - sink:
                uri: people_filtered.csv
                type: csv
        ```

    **Arguments**

    `where`

    Required.

    The row-level expression used to decide whether each row should be kept.

    Example:

    ```python
    {"where": "age >= 30 and country == 'KE'"}
    ```

    `where` uses the shared [`Expression DSL`](#expression-dsl).

    `strict`

    Optional. Default: `True`.

    Controls what happens when a comparison cannot be evaluated cleanly.

    Accepted values:

    - `true`: raise an error on incompatible comparisons
    - `false`: treat incompatible comparisons as `false`

    This is useful when some rows may contain dirty values that you want to skip rather than fail on.

    **Behavior**

    - `filter` keeps rows where the `where` expression evaluates to true
    - `filter` does not change the table schema
    - the [`Expression DSL`](#expression-dsl) supports comparisons, literals, parentheses, and `and` / `or` / `not`
    - unlike `derive`, `filter` does not support arithmetic operators in the expression
    - unknown column names raise an error with a suggestion when possible

    **When To Use It**

    Use `filter` when you want to restrict the dataset before later transforms or before writing output.

    Typical examples include:

    - keeping adults with `age >= 18`
    - keeping quality-approved rows with `qc_flag == 'A'`
    - keeping records for one geography such as `country == 'KE'`

    A common pattern is:

    - `cast` first so numeric comparisons behave correctly
    - `filter` to keep only the relevant rows
    - `derive` or `select` afterward on the reduced dataset

    **See also**

    - [`cast`](#cast)
    - [`derive`](#derive)
    - [`validate`](#validate)

<a id="drop"></a>
??? info "Drop"

    Remove one or more columns from the table.

    **Signature**

    `Transform("drop", params={"columns": ["debug_col", "raw_note"]})`

    **Examples**

    === "Python"

        ```python
        from wowdata import Pipeline, Sink, Source, Transform

        pipe = (
            Pipeline(Source("people.csv"))
            .then(
                Transform(
                    "drop",
                    params={
                        "columns": ["debug_col", "raw_note"],
                    },
                )
            )
            .then(Sink("people_clean.csv"))
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
                op: drop
                params:
                  columns: [debug_col, raw_note]
            - sink:
                uri: people_clean.csv
                type: csv
        ```

    **Arguments**

    `columns`

    Required.

    A non-empty list of column names to remove.

    Example:

    ```python
    {"columns": ["debug_col", "raw_note"]}
    ```

    If schema information is available, missing column names are caught before execution.

    **Behavior**

    - `drop` removes every column listed in `columns`
    - the remaining columns keep their original order
    - if schema information is available, unknown column names raise a validation error
    - output schema inference removes the dropped fields

    **When To Use It**

    Use `drop` when you want to discard temporary or sensitive columns while keeping the rest of the table unchanged.

    Typical examples include:

    - removing raw source columns after normalization
    - removing debug or trace columns before writing output
    - removing helper columns that were only needed for intermediate calculations

    A common pattern is:

    - `derive` or `string` to build cleaned fields
    - `drop` the raw helper columns you no longer want
    - `select` afterward if you also want to reorder the final columns

    **See also**

    - [`select`](#select)
    - [`derive`](#derive)
    - [`string`](#string)

<a id="string"></a>
??? info "String"

    Apply a string operation to one column, either in place or into a new target column.

    **Signature**

    `Transform("string", params={"column": "raw_name", "action": "strip", "new": "clean_name", "overwrite": False})`

    **Examples**

    === "Python"

        ```python
        from wowdata import Pipeline, Sink, Source, Transform

        pipe = (
            Pipeline(Source("people.csv"))
            .then(
                Transform(
                    "string",
                    params={
                        "column": "raw_name",
                        "action": "strip",
                        "new": "clean_name",
                    },
                )
            )
            .then(Sink("people_clean.csv"))
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
                op: string
                params:
                  column: raw_name
                  action: strip
                  new: clean_name
            - sink:
                uri: people_clean.csv
                type: csv
        ```

    **Arguments**

    `column`

    Required.

    The source column whose values will be transformed.

    `action`

    Required.

    The string operation to apply.

    Supported actions:

    - `regex_replace`
    - `regex_extract`
    - `capitalize`
    - `casefold`
    - `encode`
    - `format`
    - `lower`
    - `lstrip`
    - `partition`
    - `removeprefix`
    - `removesuffix`
    - `replace`
    - `rpartition`
    - `rstrip`
    - `split`
    - `strip`
    - `swapcase`
    - `title`
    - `upper`
    - `zfill`

    `new`

    Optional.

    Write the result into a new column instead of replacing `column`.

    `overwrite`

    Optional. Default: `False`.

    Controls whether `string` may write into an existing target column when `new` is used.

    If `new` points to an existing column and `overwrite` is not `true`, validation fails.

    **Behavior**

    - `string` coerces non-null values to strings before applying the action
    - null values remain null
    - if `new` is omitted, the source column is updated in place
    - if `new` is provided, the result is written to that target column
    - unknown source columns are caught early when schema information is available

    Result type note:

    - `split`, `partition`, `rpartition`, and `encode` produce non-string values, so their inferred schema type is `any`
    - most other actions produce strings

    ### Action Families

    ??? note "Regex Actions"

        Use regexes when you need pattern-based cleanup or extraction.

        **`regex_replace`**

        Replaces all regex matches with `repl`.

        Extra arguments:

        - `pattern`: required regex pattern
        - `repl`: replacement string, default `""`

        Example:

        === "Python"

            ```python
            Transform(
                "string",
                params={
                    "column": "price_raw",
                    "action": "regex_replace",
                    "pattern": r"[^0-9.]+",
                    "repl": "",
                },
            )
            ```

        === "YAML"

            ```yaml
            - transform:
                op: string
                params:
                  column: price_raw
                  action: regex_replace
                  pattern: "[^0-9.]+"
                  repl: ""
            ```

        **`regex_extract`**

        Extracts a regex match or capture group.

        Extra arguments:

        - `pattern`: required regex pattern
        - `group`: optional capture group, default `0`; may be an integer or named group

        If nothing matches, the result is `null`.

        Important:

        - use parentheses to define the part you want to capture
        - without parentheses, there is no numbered capture group beyond group `0`
        - group `0` means "the whole match"

        Example: extracting the numeric part from `F1=0.934 (validated)`

        === "Python"

            ```python
            Transform(
                "string",
                params={
                    "column": "reported_score",
                    "action": "regex_extract",
                    "pattern": r"F1=([0-9]+(?:\.[0-9]+)?)",
                    "group": 1,
                    "new": "f1_value",
                },
            )
            ```

        === "YAML"

            ```yaml
            - transform:
                op: string
                params:
                  column: reported_score
                  action: regex_extract
                  pattern: "F1=([0-9]+(?:\\.[0-9]+)?)"
                  group: 1
                  new: f1_value
            ```

        With input `F1=0.934 (validated)`, this produces `0.934`.

        If you wrote `pattern: "F1=[0-9]+(?:\\.[0-9]+)?"` and still asked for `group: 1`, it would fail because there is no capture group. To return the whole match instead, use `group: 0`.

    ??? note "Casing And Trimming"

        These actions are useful for normalization without introducing regexes.

        Casing actions:

        - `capitalize`
        - `casefold`
        - `lower`
        - `swapcase`
        - `title`
        - `upper`

        Trimming actions:

        - `strip`
        - `lstrip`
        - `rstrip`

        Extra arguments for trim actions:

        - `chars`: optional character set to remove; if omitted, whitespace is used

        Example:

        === "Python"

            ```python
            Transform("string", params={"column": "district_raw", "action": "strip"})
            Transform("string", params={"column": "district_raw", "action": "title", "new": "district"})
            ```

        === "YAML"

            ```yaml
            - transform:
                op: string
                params:
                  column: district_raw
                  action: strip
            - transform:
                op: string
                params:
                  column: district_raw
                  action: title
                  new: district
            ```

    ??? note "Splitting And Partitioning"

        These actions break a string into structured pieces.

        - `split`
        - `partition`
        - `rpartition`

        Extra arguments:

        - `split`: optional `sep`, optional `maxsplit`
        - `partition`: required `sep`
        - `rpartition`: required `sep`

        Notes:

        - `split` returns a list
        - `partition` and `rpartition` return a 3-part tuple
        - because these are not plain strings, the inferred output type is `any`

        Example:

        === "Python"

            ```python
            Transform(
                "string",
                params={"column": "tags", "action": "split", "sep": ",", "new": "tag_list"},
            )
            Transform(
                "string",
                params={"column": "station_code", "action": "partition", "sep": "-", "new": "station_parts"},
            )
            Transform(
                "string",
                params={"column": "path", "action": "rpartition", "sep": "/", "new": "path_parts"},
            )
            ```

        === "YAML"

            ```yaml
            - transform:
                op: string
                params:
                  column: tags
                  action: split
                  sep: ","
                  new: tag_list
            - transform:
                op: string
                params:
                  column: station_code
                  action: partition
                  sep: "-"
                  new: station_parts
            - transform:
                op: string
                params:
                  column: path
                  action: rpartition
                  sep: "/"
                  new: path_parts
            ```

        Example results:

        - `"climate,rainfall,alert"` becomes `["climate", "rainfall", "alert"]`
        - `"KE-047-NRB"` becomes `("KE", "-", "047-NRB")`
        - `"reports/weekly/ew08.csv"` becomes `("reports/weekly", "/", "ew08.csv")`

    ??? note "Prefix, Suffix, And Replacement"

        These actions modify strings without regex syntax.

        - `removeprefix`: requires `prefix`
        - `removesuffix`: requires `suffix`
        - `replace`: requires `old` and `new_value`, optional `count`

        Note:

        - `replace` uses `new_value` instead of `new` because `new` is reserved for the target column name

        Example:

        === "Python"

            ```python
            Transform(
                "string",
                params={"column": "sku", "action": "removeprefix", "prefix": "SKU-", "new": "sku_clean"},
            )
            Transform(
                "string",
                params={"column": "filename", "action": "removesuffix", "suffix": ".csv", "new": "file_stub"},
            )
            Transform(
                "string",
                params={"column": "title", "action": "replace", "old": "_", "new_value": " "},
            )
            ```

        === "YAML"

            ```yaml
            - transform:
                op: string
                params:
                  column: sku
                  action: removeprefix
                  prefix: "SKU-"
                  new: sku_clean
            - transform:
                op: string
                params:
                  column: filename
                  action: removesuffix
                  suffix: ".csv"
                  new: file_stub
            - transform:
                op: string
                params:
                  column: title
                  action: replace
                  old: "_"
                  new_value: " "
            ```

        Example results:

        - `"SKU-00123"` becomes `"00123"`
        - `"weekly_report.csv"` becomes `"weekly_report"`
        - `"HOME_APPLIANCES"` becomes `"HOME APPLIANCES"`

    ??? note "Formatting, Encoding, And Padding"

        These actions cover templating, byte conversion, and width-based normalization.

        **`format`**

        Applies Python-style `str.format(...)`.

        Extra arguments:

        - `args`: optional list or tuple
        - `kwargs`: optional mapping with string keys

        This includes the standard Python format mini-language inside the template string itself.

        Official reference:

        - [Python format specification mini-language](https://docs.python.org/3/library/string.html#format-specification-mini-language)

        Example:

        === "Python"

            ```python
            Transform(
                "string",
                params={
                    "column": "template",
                    "action": "format",
                    "kwargs": {
                        "site": "Nairobi",
                        "week": 8,
                        "positivity": 0.1375,
                    },
                    "new": "bulletin_line",
                },
            )
            ```

        === "YAML"

            ```yaml
            - transform:
                op: string
                params:
                  column: template
                  action: format
                  kwargs:
                    site: Nairobi
                    week: 8
                    positivity: 0.1375
                  new: bulletin_line
            ```

        If `template` contains:

        ```text
        Site {site} | EW{week:02d} | positivity {positivity:.1%}
        ```

        the result will be:

        ```text
        Site Nairobi | EW08 | positivity 13.8%
        ```

        This means you can use format specifiers such as:

        - `:02d` for zero-padded integers
        - `:.1f` for fixed decimal places
        - `:.1%` for percentages
        - alignment and width specifiers such as `:>10` when needed

        **`encode`**

        Converts the string to bytes.

        Extra arguments:

        - `encoding`: optional, default `"utf-8"`
        - `errors`: optional, default `"strict"`

        Because `encode` returns bytes, the inferred output type is `any`.

        **`zfill`**

        Left-pads the string with zeroes.

        Extra arguments:

        - `width`: required integer

        Example results:

        - `encode` with `"hello"` and UTF-8 produces `b"hello"`
        - `zfill` with `"7"` and `width: 3` produces `"007"`

    **When To Use It**

    Use `string` when you need lightweight text normalization inside the pipeline, especially before `cast`, `derive`, `join`, or `select`.

    Typical examples include:

    - trimming and title-casing names
    - extracting values from messy labels
    - splitting delimited text into token lists
    - padding identifiers such as district or postal codes

    **See also**

    - [`cast`](#cast)
    - [`derive`](#derive)
    - [`select`](#select)

<a id="validate"></a>
??? info "Validate"

    Validate the current table against the best available schema and record a human-readable summary in pipeline context.

    **Signature**

    `Transform("validate", params={"sample_rows": 5000, "fail": True, "strict_schema": True})`

    **Examples**

    === "Python"

        ```python
        from wowdata import Pipeline, Sink, Source, Transform

        pipe = (
            Pipeline(
                Source(
                    "people.csv",
                    schema={
                        "fields": [
                            {"name": "age", "type": "integer"},
                            {"name": "country", "type": "string"},
                        ]
                    },
                )
            )
            .then(Transform("validate", params={"sample_rows": 1000, "fail": False}))
            .then(Sink("people_checked.csv"))
        )
        ```

    === "YAML"

        ```yaml
        wowdata: 0
        pipeline:
          start:
            uri: people.csv
            type: csv
            schema:
              fields:
                - name: age
                  type: integer
                - name: country
                  type: string
          steps:
            - transform:
                op: validate
                params:
                  sample_rows: 1000
                  fail: false
            - sink:
                uri: people_checked.csv
                type: csv
        ```

    **Arguments**

    `sample_rows`

    Optional. Default: `5000`.

    The maximum number of data rows to validate from the current table.

    Validation uses a bounded sample rather than scanning the full dataset by default.

    `fail`

    Optional. Default: `True`.

    Controls whether invalid data should stop the pipeline.

    Accepted values:

    - `true`: raise an error if validation fails
    - `false`: keep the table flowing and record the validation result in context

    `strict_schema`

    Optional. Default: `True`.

    Controls whether validation requires a known schema before it can run.

    Accepted values:

    - `true`: require schema information and fail if none is available
    - `false`: allow validation to run with weaker assumptions

    **Behavior**

    - `validate` does not change the table schema or row values
    - it samples up to `sample_rows` rows from the current table
    - it uses the best available carried schema from the pipeline context
    - it records a validation summary in `context.validations` and a checkpoint entry in `context.checkpoints`
    - when `fail=true`, invalid data raises `E_VALIDATE_INVALID`
    - when `fail=false`, the pipeline continues and you can inspect the validation summary afterward

    Dependency note:

    - `validate` requires the optional `frictionless` dependency

    Empty-table note:

    - if there are no data rows, validation records a successful result with zero rows checked

    **When To Use It**

    Use `validate` when you want a teachable checkpoint in the pipeline that confirms whether cleaned data matches expected types and shape.

    Typical cases include:

    - after `cast` to confirm type expectations were achieved
    - before writing a final dataset to catch schema drift
    - in exploratory pipelines where you want a report without necessarily failing the run

    A common pattern is:

    - declare or infer schema
    - clean data with `cast`, `string`, `derive`, or `join`
    - run `validate`
    - choose `fail=true` for strict pipelines or `fail=false` for auditing workflows

    **See also**

    - [`cast`](#cast)
    - [`join`](#join)
    - [`filter`](#filter)

<a id="join"></a>
??? info "Join"

    Combine the current table with another source using one or more key columns.

    **Signature**

    `Transform("join", params={"right": "other.csv", "on": ["id"], "how": "left", "strict_types": True})`

    **Examples**

    === "Python"

        ```python
        from wowdata import Pipeline, Sink, Source, Transform

        pipe = (
            Pipeline(Source("people.csv"))
            .then(
                Transform(
                    "join",
                    params={
                        "right": "countries.csv",
                        "left_on": ["country"],
                        "right_on": ["country_code"],
                        "how": "left",
                    },
                )
            )
            .then(Sink("people_enriched.csv"))
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
                op: join
                params:
                  right: countries.csv
                  left_on: [country]
                  right_on: [country_code]
                  how: left
            - sink:
                uri: people_enriched.csv
                type: csv
        ```

    **Arguments**

    `right`

    Required.

    The right-hand source to join against.

    This may be:

    - a URI string such as `"countries.csv"`
    - a mapping descriptor such as `{"uri": "countries.csv", "type": "csv", "options": {...}}`

    `on`

    Required when the key names are the same on both sides.

    A non-empty list of join key column names shared by the left and right tables.

    Example:

    ```python
    {"right": "stations.csv", "on": ["station_id"]}
    ```

    `left_on`, `right_on`

    Required together when the key names differ between the two sources.

    These must both be non-empty lists and must have the same length.

    Example:

    ```python
    {
        "right": "countries.csv",
        "left_on": ["country"],
        "right_on": ["country_code"],
    }
    ```

    `how`

    Optional. Default: `inner`.

    Join type. Accepted values:

    - `inner`
    - `left`
    - `right`
    - `outer`

    `strict_types`

    Optional. Default: `True`.

    Controls whether WowData should check for likely key-type mismatches before joining.

    Accepted values:

    - `true`: fail early if the left and right join keys appear to have different predominant types
    - `false`: skip the early type guard

    This is useful because mismatched key types often produce confusing empty joins.

    **Behavior**

    - `join` reads the right-hand source at execution time
    - it checks that the requested key columns exist on both sides before calling PETL join operations
    - if `strict_types=true`, it samples key values on both sides to catch likely type mismatches
    - if the join fails, WowData wraps the error with hints about common causes such as missing keys, type mismatches, or duplicate keys

    Key-selection rule:

    - use `on` when the key names are the same on both tables
    - use `left_on` and `right_on` when the key names differ
    - do not provide both styles at once

    Schema note:

    - runtime joins can add columns from the right-hand source
    - if later transforms need schema-aware validation on those joined columns, consider using schema locking or an explicit `output_schema`

    **When To Use It**

    Use `join` when the current table needs enrichment from a second dataset.

    Typical examples include:

    - attaching country names to country codes
    - attaching station metadata to measurements
    - attaching facility metadata to a line list

    A common pattern is:

    - `cast` key columns so the join types line up
    - `join` the enrichment source
    - `derive`, `drop`, or `select` afterward to shape the final result

    **See also**

    - [`cast`](#cast)
    - [`select`](#select)
    - [`validate`](#validate)

??? note "YAML Ergonomics"

    - Canonical `cast.on_error` values are strings: `fail`, `null`, `keep`.
    - Unquoted YAML `null` for `on_error` is accepted and normalized to `"null"`.
    - Unquoted YAML key `on:` in join params is accepted even when parser treats it as a boolean key.

<a id="expression-dsl"></a>
??? note "Expression DSL"

    Used by `filter.where` and `derive.expr`.

    Supported syntax:

    - logical: `and`, `or`, `not`
    - comparisons: `==`, `!=`, `>`, `>=`, `<`, `<=`
    - literals: strings, numbers, booleans, `null`
    - parentheses for grouping
    - arithmetic in `derive`: `+`, `-`, `*`, `/`

    **Examples**

    Literals:

    - string literal: `'KE'`
    - integer literal: `18`
    - number literal: `3.14`
    - boolean literal: `true`
    - null literal: `null`

    Comparisons:

    - `age >= 18`
    - `country == 'KE'`
    - `status != 'archived'`

    Boolean logic:

    - `age >= 18 and country == 'KE'`
    - `country == 'KE' or country == 'UG'`
    - `not archived_flag`

    Parentheses:

    - `(country == 'KE' or country == 'UG') and age >= 18`

    Arithmetic in `derive`:

    - `age + 1`
    - `rain_mm / 10`
    - `(cases_a + cases_b) / 2`

    String concatenation in `derive`:

    - `first_name + ' ' + last_name`

    **Where You Can Use These**

    In `filter`:

    ```yaml
    - transform:
        op: filter
        params:
          where: "(country == 'KE' or country == 'UG') and age >= 18"
    ```

    In `derive`:

    ```yaml
    - transform:
        op: derive
        params:
          new: full_name
          expr: "first_name + ' ' + last_name"
    - transform:
        op: derive
        params:
          new: incidence_flag
          expr: "cases >= 20"
    - transform:
        op: derive
        params:
          new: age_next_year
          expr: "age + 1"
    ```

    **Notes**

    - arithmetic operators are supported in `derive`, but not in `filter`
    - string concatenation with `+` is supported in `derive`
    - unknown column names raise a user-facing error with a suggestion when possible
    - type mismatches may fail or fall back depending on each transform's `strict` setting
