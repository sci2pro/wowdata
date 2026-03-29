# Getting Started

## Install

```bash
pip install wowdata
```

For local development:

```bash
git clone https://github.com/sci2pro/wowdata.git
cd wowdata
pip install -e .[dev]
```

## First Pipeline (Python)

```python
from wowdata import Pipeline, Sink, Source, Transform

pipe = (
    Pipeline(Source("people.csv"))
    .then(Transform("cast", params={"types": {"age": "integer"}, "on_error": "null"}))
    .then(Transform("filter", params={"where": "age >= 18"}))
    .then(Sink("adults.csv"))
)

pipe.run()
```

## First Pipeline (YAML + CLI)

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
          where: "age >= 18"
    - sink:
        uri: adults.csv
        type: csv
```

Run it:

```bash
wow run pipeline.yaml
```

Fallback command:

```bash
wowdata run pipeline.yaml
```

## Repository Examples

The repository includes ready-to-run sample pipelines and data files in `examples/`.

From the repo root:

```bash
wow run examples/climate_heat_events.yaml --base-dir examples
wow run examples/climate_rainfall_alerts.yaml --base-dir examples
```

Or run from inside the directory:

```bash
cd examples
wow run climate_heat_events.yaml
```

## CLI (v0)

WowData™ includes a CLI for running YAML-serialized pipelines.

After installing the package, use:

```shell
wow --help
```

If `wow` conflicts with another tool in your environment, use the fallback command:

```shell
wowdata --help
```

### Commands

1. `wow run pipeline.yaml` (fallback: `wowdata run pipeline.yaml`)
   - Executes the pipeline end-to-end.
   - Returns non-zero on runtime failures.

2. `wow validate pipeline.yaml` (fallback: `wowdata validate pipeline.yaml`)
   - Parses YAML + IR and runs preflight checks on source/sink paths.

3. `wow schema pipeline.yaml` (fallback: `wowdata schema pipeline.yaml`)
   - Infers output schema without full pipeline execution.

4. `wow lock-schema pipeline.yaml -o pipeline.locked.yaml` (fallback: `wowdata lock-schema ...`)
   - Writes a schema-locked YAML by embedding per-transform `output_schema`.

### Common flags

- `--base-dir PATH` resolve relative paths in YAML from a specific directory.
- `--json` print machine-readable JSON output.
- `--sample-rows N` used by `schema` and `lock-schema` for bounded inference.
- `--force` recompute schema inference even if cached.

### CLI examples

```shell
# Run a serialized pipeline
wow run pipeline.yaml

# Run a repository example from the repo root
wow run examples/climate_heat_events.yaml --base-dir examples

# Validate structure and file paths before execution
wow validate pipeline.yaml

# Print inferred output schema as JSON
wow schema pipeline.yaml --json

# Save a locked pipeline snapshot
wow lock-schema pipeline.yaml -o pipeline.locked.yaml
```

### Exit codes

- `0`: success
- `2`: CLI usage error
- `3`: pipeline parse/validation error
- `4`: pipeline runtime execution error
