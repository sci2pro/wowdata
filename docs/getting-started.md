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
