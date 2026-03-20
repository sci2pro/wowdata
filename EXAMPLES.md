# WowData™ Examples

This page contains runnable, domain-focused examples for WowData™.

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
