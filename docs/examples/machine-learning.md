# Machine Learning Examples

## Experiment Registry Cleanup

This workflow cleans a dirty, synthetic model-experiment registry and produces a shortlist of candidate runs.

### Inputs

`ml_experiment_runs_raw.csv`:

| run_id | experiment_name | reported_f1 | train_minutes_raw | status_note | owner |
|---|---|---|---|---|---|
| RUN-001 | vision::RedwoodNet-v2 [aug=heavy] | F1=0.912* | 184 min (gpu) | ship-ready | A. Imani |
| RUN-002 | vision::RedwoodNet-v3 [aug=light] | F1=0.887? | ~201 min | needs-review | A. Imani |
| RUN-003 | tabular::QuartzBoost_v1 | score missing | n/a | blocked | K. Soto |
| RUN-004 | audio::WavePatch-7 | F1=0.934 (validated) | 96min | ship-ready | R. Hale |
| RUN-005 | nlp::InkLattice-small | F1=0.901* | 143 minutes | ship-ready | M. Okafor |
| RUN-006 | vision::OrbitMixer-v5 | F1=0.879 | 188 min | archived | R. Hale |

### Pipeline

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

Run:

```bash
wow run ml_model_candidates.yaml
```

### Expected Output

`ml_model_candidates.csv`:

| run_id | domain | model_family | f1_clean | train_minutes | owner | is_fast_track |
|---|---|---|---|---|---|---|
| RUN-001 | vision | RedwoodNet-v2 | 0.912 | 184.0 | A. Imani | False |
| RUN-004 | audio | WavePatch-7 | 0.934 | 96.0 | R. Hale | True |
| RUN-005 | nlp | InkLattice-small | 0.901 | 143.0 | M. Okafor | False |
