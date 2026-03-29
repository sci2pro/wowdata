# WowData™

[![PyPI version](https://img.shields.io/pypi/v/wowdata)](https://pypi.org/project/wowdata/)
[![Python versions](https://img.shields.io/pypi/pyversions/wowdata)](https://pypi.org/project/wowdata/)
[![Coverage](https://codecov.io/gh/sci2pro/wowdata/branch/main/graph/badge.svg)](https://codecov.io/gh/sci2pro/wowdata)

WowData™ is a human-centred data wrangling and pipeline framework designed to make real-world data cleanup understandable, teachable, and inspectable.

## Install

From PyPI:

```bash
pip install wowdata
```

From source (editable):

```bash
git clone https://github.com/sci2pro/wowdata.git
cd wowdata
pip install -e .
```

## Quick Start

Create a small input CSV:

```csv
person_id,age,country
1,30,KE
2,17,UG
3,41,KE
```

Run with Python API:

```python
from wowdata import Pipeline, Sink, Source, Transform

pipe = (
    Pipeline(Source("people.csv"))
    .then(Transform("cast", params={"types": {"age": "integer"}, "on_error": "null"}))
    .then(Transform("filter", params={"where": "age >= 18 and country == 'KE'"}))
    .then(Sink("adults_ke.csv"))
)

pipe.run()
```

Run from YAML with CLI:

```bash
wow run pipeline.yaml
```

Fallback command if `wow` conflicts in your shell:

```bash
wowdata run pipeline.yaml
```

## Repository Examples

The repository now includes runnable sample pipelines and data files under `examples/`.

From the repo root, you can run them directly with `--base-dir` so relative CSV paths resolve correctly:

```bash
wow run examples/climate_heat_events.yaml --base-dir examples
wow run examples/climate_rainfall_alerts.yaml --base-dir examples
```

You can also `cd examples` and run the same YAML files from there.

## Documentation

- Philosophy: [docs/philosophy.md](docs/philosophy.md)
- Examples: [EXAMPLES.md](EXAMPLES.md)
- Reference: [REFERENCE.md](REFERENCE.md)
- Docs site source: [docs/](docs/) + [mkdocs.yml](mkdocs.yml)

To preview docs locally:

```bash
pip install -e .[docs]
mkdocs serve
```

The same docs can be published to GitHub Pages (for `wowdata.github.io`).
