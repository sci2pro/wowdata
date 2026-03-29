# Climate Examples

The YAML files and CSV inputs for these examples live in the repository `examples/` directory. From the
repo root, run them with `--base-dir examples` so relative file references resolve correctly.

## Heat Events

This workflow cleans station observations and produces an analysis-ready heat-events dataset.

### Inputs

`climate_observations_raw.csv`:

| station_id | date | tmax_c | prcp_mm | qc_flag |
|---|---|---|---|---|
| ST001 | 2025-01-12 | 39.4 | 0.0 | A |
| ST001 | 2025-01-13 | 41.2 | 1.2 | A |
| ST002 | 2025-01-13 | not_available | 0.0 | A |
| ST003 | 2025-01-14 | 36.8 |  | A |
| ST004 | 2025-01-14 | 42.7 | 0.0 | B |
| ST005 | 2025-01-14 | 40.1 | 5.3 | A |

`climate_stations.csv`:

| station_id | station_name | country | elevation_m |
|---|---|---|---|
| ST001 | Nairobi Central | KE | 1661 |
| ST002 | Garissa | KE | 151 |
| ST003 | Mombasa | KE | 50 |
| ST005 | Kisumu | KE | 1131 |

### Pipeline

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

Run:

```bash
wow run examples/climate_heat_events.yaml --base-dir examples
```

### Expected Output

`climate_heat_events.csv`:

| station_id | date | tmax_c | prcp_mm | qc_flag | station_name | country | elevation_m | is_extreme |
|---|---|---|---|---|---|---|---|---|
| ST001 | 2025-01-13 | 41.2 | 1.2 | A | Nairobi Central | KE | 1661 | False |
| ST005 | 2025-01-14 | 40.1 | 5.3 | A | Kisumu | KE | 1131 | False |

## Rainfall Deficit Alerts

This workflow compares observed rainfall to station normals and flags large deficits.

### Inputs

`climate_rainfall_obs.csv`:

| station_id | date | rain_mm | qc_flag |
|---|---|---|---|
| ST101 | 2025-03-01 | 2.5 | A |
| ST102 | 2025-03-01 | trace | A |
| ST103 | 2025-03-01 | 18.0 | A |
| ST104 | 2025-03-01 | 0.0 | B |
| ST101 | 2025-03-02 | 1.0 | A |
| ST103 | 2025-03-02 | 22.0 | A |

`climate_rainfall_normals.csv`:

| station_id | normal_mm |
|---|---|
| ST101 | 25.0 |
| ST102 | 12.0 |
| ST103 | 20.0 |
| ST104 | 8.0 |

### Pipeline

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

Run:

```bash
wow run examples/climate_rainfall_alerts.yaml --base-dir examples
```

### Expected Output

`climate_rainfall_alerts.csv`:

| station_id | date | rain_mm | qc_flag | normal_mm | deficit_mm | is_deficit_alert |
|---|---|---|---|---|---|---|
| ST101 | 2025-03-01 | 2.5 | A | 25.0 | 22.5 | True |
| ST102 | 2025-03-01 |  | A | 12.0 |  | False |
| ST103 | 2025-03-01 | 18.0 | A | 20.0 | 2.0 | False |
| ST101 | 2025-03-02 | 1.0 | A | 25.0 | 24.0 | True |
| ST103 | 2025-03-02 | 22.0 | A | 20.0 | -2.0 | False |
