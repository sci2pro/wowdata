# WowData™ Examples

This page contains runnable, domain-focused examples for WowData™.

All example YAML files and CSV fixtures are checked into the repository under `examples/`. From the repo
root, run them with:

```bash
wow run examples/climate_heat_events.yaml --base-dir examples
wow run examples/climate_rainfall_alerts.yaml --base-dir examples
wow run examples/ml_model_candidates.yaml --base-dir examples
```

You can also `cd examples` and run the YAML files directly there.

## Climate Demo (Heat Events)

This example shows how WowData™ can help non-specialist climate teams clean station observations and produce an
analysis-ready heat-event dataset from YAML.

What the pipeline does:
- casts messy numeric fields (`tmax_c`, `prcp_mm`) with explicit error handling
- filters to quality-approved records (`qc_flag == 'A'`)
- keeps only heat-event rows (`tmax_c >= 40`)
- joins station metadata (name/country) from a second CSV
- derives a simple risk flag (`is_extreme`)
- writes a clean output CSV

Example input data (`climate_observations_raw.csv`):

| station_id | date | tmax_c | prcp_mm | qc_flag |
|---|---|---|---|---|
| ST001 | 2025-01-12 | 39.4 | 0.0 | A |
| ST001 | 2025-01-13 | 41.2 | 1.2 | A |
| ST002 | 2025-01-13 | not_available | 0.0 | A |
| ST003 | 2025-01-14 | 36.8 |  | A |
| ST004 | 2025-01-14 | 42.7 | 0.0 | B |
| ST005 | 2025-01-14 | 40.1 | 5.3 | A |

Station metadata (`climate_stations.csv`):

| station_id | station_name | country | elevation_m |
|---|---|---|---|
| ST001 | Nairobi Central | KE | 1661 |
| ST002 | Garissa | KE | 151 |
| ST003 | Mombasa | KE | 50 |
| ST005 | Kisumu | KE | 1131 |

Pipeline (`climate_heat_events.yaml`):

```yaml
wowdata: 0
pipeline:
  start:
    uri: climate_observations_raw.csv
    type: csv
  steps:
    - transform:
        op: cast
        params:
          types:
            tmax_c: number
            prcp_mm: number
          on_error: "null"
    - transform:
        op: filter
        params:
          where: "qc_flag == 'A'"
    - transform:
        op: filter
        params:
          where: "tmax_c >= 40"
    - transform:
        op: join
        params:
          right: climate_stations.csv
          on: [station_id]
          how: left
    - transform:
        op: derive
        params:
          new: is_extreme
          expr: "tmax_c >= 42"
    - sink:
        uri: climate_heat_events.csv
        type: csv
```

Run it from the pipeline directory:

```shell
wow run climate_heat_events.yaml
```

Fallback command:

```shell
wowdata run climate_heat_events.yaml
```

Expected output (`climate_heat_events.csv`):

| station_id | date | tmax_c | prcp_mm | qc_flag | station_name | country | elevation_m | is_extreme |
|---|---|---|---|---|---|---|---|---|
| ST001 | 2025-01-13 | 41.2 | 1.2 | A | Nairobi Central | KE | 1661 | False |
| ST005 | 2025-01-14 | 40.1 | 5.3 | A | Kisumu | KE | 1131 | False |

## Climate Demo (Rainfall Deficit Alerts)

This example mirrors a common early-warning workflow: compare observed rainfall to station normals and flag unusually
dry conditions.

What the pipeline does:
- casts messy rainfall values (`rain_mm`) with explicit error handling
- filters to quality-approved records
- joins rainfall normals by station (`normal_mm`)
- derives a deficit amount (`deficit_mm = normal_mm - rain_mm`)
- derives an alert flag (`is_deficit_alert`) for large deficits
- writes a cleaned, analysis-ready CSV

Observed rainfall (`climate_rainfall_obs.csv`):

| station_id | date | rain_mm | qc_flag |
|---|---|---|---|
| ST101 | 2025-03-01 | 2.5 | A |
| ST102 | 2025-03-01 | trace | A |
| ST103 | 2025-03-01 | 18.0 | A |
| ST104 | 2025-03-01 | 0.0 | B |
| ST101 | 2025-03-02 | 1.0 | A |
| ST103 | 2025-03-02 | 22.0 | A |

Station rainfall normals (`climate_rainfall_normals.csv`):

| station_id | normal_mm |
|---|---|
| ST101 | 25.0 |
| ST102 | 12.0 |
| ST103 | 20.0 |
| ST104 | 8.0 |

Pipeline (`climate_rainfall_alerts.yaml`):

```yaml
wowdata: 0
pipeline:
  start:
    uri: climate_rainfall_obs.csv
    type: csv
  steps:
    - transform:
        op: cast
        params:
          types:
            rain_mm: number
          on_error: "null"
    - transform:
        op: filter
        params:
          where: "qc_flag == 'A'"
    - transform:
        op: join
        params:
          right: climate_rainfall_normals.csv
          on: [station_id]
          how: left
    - transform:
        op: derive
        params:
          new: deficit_mm
          expr: "normal_mm - rain_mm"
    - transform:
        op: derive
        params:
          new: is_deficit_alert
          expr: "deficit_mm >= 20"
    - sink:
        uri: climate_rainfall_alerts.csv
        type: csv
```

Run it:

```shell
wow run climate_rainfall_alerts.yaml
```

Fallback command:

```shell
wowdata run climate_rainfall_alerts.yaml
```

Expected output (`climate_rainfall_alerts.csv`):

| station_id | date | rain_mm | qc_flag | normal_mm | deficit_mm | is_deficit_alert |
|---|---|---|---|---|---|---|
| ST101 | 2025-03-01 | 2.5 | A | 25.0 | 22.5 | True |
| ST102 | 2025-03-01 |  | A | 12.0 |  | False |
| ST103 | 2025-03-01 | 18.0 | A | 20.0 | 2.0 | False |
| ST101 | 2025-03-02 | 1.0 | A | 25.0 | 24.0 | True |
| ST103 | 2025-03-02 | 22.0 | A | 20.0 | -2.0 | False |

## Machine Learning Demo (Experiment Registry Cleanup)

This example uses a fully synthetic model-experiment registry to show how `string` can extract structured fields
from messy text before `cast`, `filter`, and `derive` are applied.

What the pipeline does:
- extracts a `domain` from experiment names like `vision::RedwoodNet-v2 [aug=heavy]`
- extracts a clean `model_family` identifier from the same messy label
- extracts numeric F1 values from strings like `F1=0.934 (validated)`
- extracts numeric training duration from strings like `184 min (gpu)` and `96min`
- casts extracted numeric strings into analysis-ready numbers
- filters to ship-ready runs that meet a minimum quality threshold
- derives a simple fast-track flag for short, high-performing runs

Synthetic input data (`ml_experiment_runs_raw.csv`):

| run_id | experiment_name | reported_f1 | train_minutes_raw | status_note | owner |
|---|---|---|---|---|---|
| RUN-001 | vision::RedwoodNet-v2 [aug=heavy] | F1=0.912* | 184 min (gpu) | ship-ready | A. Imani |
| RUN-002 | vision::RedwoodNet-v3 [aug=light] | F1=0.887? | ~201 min | needs-review | A. Imani |
| RUN-003 | tabular::QuartzBoost_v1 | score missing | n/a | blocked | K. Soto |
| RUN-004 | audio::WavePatch-7 | F1=0.934 (validated) | 96min | ship-ready | R. Hale |
| RUN-005 | nlp::InkLattice-small | F1=0.901* | 143 minutes | ship-ready | M. Okafor |
| RUN-006 | vision::OrbitMixer-v5 | F1=0.879 | 188 min | archived | R. Hale |

Pipeline (`ml_model_candidates.yaml`):

```yaml
wowdata: 0
pipeline:
  start:
    uri: ml_experiment_runs_raw.csv
    type: csv
  steps:
    - transform:
        op: string
        params:
          column: experiment_name
          action: regex_extract
          pattern: "^([a-z]+)::"
          group: 1
          new: domain
    - transform:
        op: string
        params:
          column: experiment_name
          action: regex_extract
          pattern: "::([A-Za-z][A-Za-z0-9_-]*)"
          group: 1
          new: model_family
    - transform:
        op: string
        params:
          column: reported_f1
          action: regex_extract
          pattern: "([0-9]+(?:\\.[0-9]+)?)"
          group: 1
          new: f1_clean
    - transform:
        op: string
        params:
          column: train_minutes_raw
          action: regex_extract
          pattern: "([0-9]+(?:\\.[0-9]+)?)"
          group: 1
          new: train_minutes
    - transform:
        op: cast
        params:
          types:
            f1_clean: number
            train_minutes: number
          on_error: "null"
    - transform:
        op: filter
        params:
          where: "status_note == 'ship-ready'"
    - transform:
        op: filter
        params:
          where: "f1_clean >= 0.9"
    - transform:
        op: derive
        params:
          new: is_fast_track
          expr: "f1_clean >= 0.93 and train_minutes <= 120"
    - transform:
        op: select
        params:
          columns: [run_id, domain, model_family, f1_clean, train_minutes, owner, is_fast_track]
    - sink:
        uri: ml_model_candidates.csv
        type: csv
```

Run it:

```shell
wow run ml_model_candidates.yaml
```

Fallback command:

```shell
wowdata run ml_model_candidates.yaml
```

Expected output (`ml_model_candidates.csv`):

| run_id | domain | model_family | f1_clean | train_minutes | owner | is_fast_track |
|---|---|---|---|---|---|---|
| RUN-001 | vision | RedwoodNet-v2 | 0.912 | 184.0 | A. Imani | False |
| RUN-004 | audio | WavePatch-7 | 0.934 | 96.0 | R. Hale | True |
| RUN-005 | nlp | InkLattice-small | 0.901 | 143.0 | M. Okafor | False |
