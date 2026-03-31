# Epidemiology Examples

These are draft examples for epidemiological data workflows. They are written to exercise the newer `string` operations and become runnable once the `string` transform implementation is present on this branch.

## Line List Cleanup

This workflow cleans a simple outbreak line list and enriches it with facility metadata.

### Inputs

`epi_line_list_raw.csv`:

| case_id | patient_name_raw | sex_raw | age_raw | onset_date_raw | district_code_raw | facility_label | symptom_blob | outcome_note | classification_note |
|---|---|---|---|---|---|---|---|---|---|
| CL-001 | `  ama njoroge ` | ` f ` | 34 | 2025-03-02 | ` dist-7 ` | `site:HC01|north wing` | `fever;cough;headache` | `admitted-case` | `confirmed` |
| CL-002 | `JOHN  OTIENO` | `M` | `not_reported` | 2025-03-03 | `DIST-12` | `site:HC02|field tent` | `fever` | `home-isolation` | `probable` |
| CL-003 | `  liya  Hassan  ` | ` Female ` | 17 | 2025-03-04 | `dist-3` | `site:HC99|triage` | `rash;fever` | `transferred-out` | `suspected` |

`epi_sites.csv`:

| facility_code | facility_name | county |
|---|---|---|
| HC01 | Kijiji Health Centre | Kisumu |
| HC02 | River Road Clinic | Nairobi |
| HC99 | Lakeside Triage Post | Homa Bay |

### Pipeline

`epi_line_list_cleanup.yaml`:

```yaml
wowdata: 0
pipeline:
  start:
    uri: epi_line_list_raw.csv
    type: csv
  steps:
    - transform:
        op: string
        params: {column: patient_name_raw, action: strip}
    - transform:
        op: string
        params: {column: patient_name_raw, action: replace, old: "  ", new_value: " "}
    - transform:
        op: string
        params: {column: patient_name_raw, action: title, new: patient_name}
    - transform:
        op: string
        params: {column: sex_raw, action: strip}
    - transform:
        op: string
        params: {column: sex_raw, action: upper, new: sex}
    - transform:
        op: string
        params: {column: district_code_raw, action: strip}
    - transform:
        op: string
        params: {column: district_code_raw, action: upper}
    - transform:
        op: string
        params: {column: district_code_raw, action: removeprefix, prefix: "DIST-", new: district_code}
    - transform:
        op: string
        params: {column: district_code, action: zfill, width: 3}
    - transform:
        op: string
        params: {column: facility_label, action: removeprefix, prefix: "site:", new: facility_compact}
    - transform:
        op: string
        params: {column: facility_compact, action: partition, sep: "|", new: facility_parts}
    - transform:
        op: string
        params:
          column: facility_compact
          action: regex_extract
          pattern: "^([A-Z0-9]+)"
          group: 1
          new: facility_code
    - transform:
        op: string
        params: {column: symptom_blob, action: split, sep: ";", new: symptom_tokens}
    - transform:
        op: string
        params: {column: outcome_note, action: replace, old: "-", new_value: " ", new: outcome_clean}
    - transform:
        op: string
        params: {column: outcome_clean, action: title}
    - transform:
        op: string
        params: {column: classification_note, action: capitalize, new: classification}
    - transform:
        op: cast
        params:
          types: {age_raw: integer}
          on_error: "null"
    - transform:
        op: join
        params:
          right: epi_sites.csv
          on: [facility_code]
          how: left
    - transform:
        op: derive
        params:
          new: is_admitted
          expr: "outcome_clean == 'Admitted Case'"
    - transform:
        op: select
        params:
          columns: [case_id, patient_name, sex, age_raw, onset_date_raw, district_code, facility_code, facility_name, county, symptom_tokens, outcome_clean, classification, is_admitted]
    - sink:
        uri: epi_line_list_clean.csv
        type: csv
```

### Expected Output

`epi_line_list_clean.csv`:

| case_id | patient_name | sex | age_raw | onset_date_raw | district_code | facility_code | facility_name | county | symptom_tokens | outcome_clean | classification | is_admitted |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| CL-001 | Ama Njoroge | F | 34 | 2025-03-02 | 007 | HC01 | Kijiji Health Centre | Kisumu | `['fever', 'cough', 'headache']` | Admitted Case | Confirmed | True |
| CL-002 | John Otieno | M |  | 2025-03-03 | 012 | HC02 | River Road Clinic | Nairobi | `['fever']` | Home Isolation | Probable | False |
| CL-003 | Liya Hassan | FEMALE | 17 | 2025-03-04 | 003 | HC99 | Lakeside Triage Post | Homa Bay | `['rash', 'fever']` | Transferred Out | Suspected | False |

## Weekly Incidence Cleanup

This workflow cleans district-level weekly incidence inputs before dashboarding or bulletin generation.

### Inputs

`epi_weekly_incidence_raw.csv`:

| district_code_raw | district_label_raw | epi_week_raw | cases_raw | report_file | bulletin_path | status_note | investigator_email_raw |
|---|---|---|---|---|---|---|---|
| ` dist-7 ` | `KISUMU COUNTY ` | `ew07` | 14 | `week07.csv` | `bulletins/weekly/ew07` | `draft. ` | `Surveillance@MoH.GO.KE` |
| `dist-12` | `nairobi county` | `EW08` | 27 | `week08.csv` | `bulletins/weekly/ew08` | `final.` | `Field.Team@MoH.GO.KE` |
| `dist-3` | ` HOMA BAY COUNTY` | `ew09` | 9 | `week09.csv` | `bulletins/weekly/ew09` | `provisional..` | `Rapid.Response@MoH.GO.KE` |

### Pipeline

`epi_weekly_incidence_cleanup.yaml`:

```yaml
wowdata: 0
pipeline:
  start:
    uri: epi_weekly_incidence_raw.csv
    type: csv
  steps:
    - transform:
        op: string
        params: {column: district_label_raw, action: strip}
    - transform:
        op: string
        params: {column: district_label_raw, action: title, new: district_label}
    - transform:
        op: string
        params: {column: district_label, action: casefold, new: district_key}
    - transform:
        op: string
        params: {column: district_code_raw, action: strip}
    - transform:
        op: string
        params: {column: district_code_raw, action: lower}
    - transform:
        op: string
        params: {column: district_code_raw, action: removeprefix, prefix: "dist-", new: district_code}
    - transform:
        op: string
        params: {column: district_code, action: zfill, width: 3}
    - transform:
        op: string
        params: {column: epi_week_raw, action: upper}
    - transform:
        op: string
        params: {column: epi_week_raw, action: removeprefix, prefix: "EW", new: epi_week_num}
    - transform:
        op: string
        params: {column: epi_week_num, action: zfill, width: 2}
    - transform:
        op: string
        params: {column: report_file, action: removesuffix, suffix: ".csv", new: report_stub}
    - transform:
        op: string
        params: {column: bulletin_path, action: rpartition, sep: "/", new: bulletin_parts}
    - transform:
        op: string
        params: {column: status_note, action: rstrip, chars: ". ", new: status_clean}
    - transform:
        op: string
        params: {column: status_clean, action: capitalize}
    - transform:
        op: string
        params: {column: investigator_email_raw, action: casefold, new: investigator_email}
    - transform:
        op: cast
        params:
          types: {cases_raw: integer}
          on_error: "null"
    - transform:
        op: derive
        params:
          new: incidence_flag
          expr: "cases_raw >= 20"
    - transform:
        op: select
        params:
          columns: [district_code, district_label, district_key, epi_week_num, cases_raw, report_stub, bulletin_parts, status_clean, investigator_email, incidence_flag]
    - sink:
        uri: epi_weekly_incidence_clean.csv
        type: csv
```

### Expected Output

`epi_weekly_incidence_clean.csv`:

| district_code | district_label | district_key | epi_week_num | cases_raw | report_stub | bulletin_parts | status_clean | investigator_email | incidence_flag |
|---|---|---|---|---|---|---|---|---|---|
| 007 | Kisumu County | kisumu county | 07 | 14 | week07 | `('bulletins/weekly', '/', 'ew07')` | Draft | surveillance@moh.go.ke | False |
| 012 | Nairobi County | nairobi county | 08 | 27 | week08 | `('bulletins/weekly', '/', 'ew08')` | Final | field.team@moh.go.ke | True |
| 003 | Homa Bay County | homa bay county | 09 | 9 | week09 | `('bulletins/weekly', '/', 'ew09')` | Provisional | rapid.response@moh.go.ke | False |

## Additional String Snippets For Epidemiological Data

These smaller snippets are useful when you need an operation that is less common in the two full workflows above.

### `lstrip`

```yaml
- transform:
    op: string
    params:
      column: household_code
      action: lstrip
      chars: " 0"
```

Example: `" 00042"` becomes `"42"`.

### `rpartition`

```yaml
- transform:
    op: string
    params:
      column: specimen_path
      action: rpartition
      sep: "/"
      new: specimen_path_parts
```

Example: `"uploads/specimens/S-204.csv"` becomes `("uploads/specimens", "/", "S-204.csv")`.

### `format`

```yaml
- transform:
    op: string
    params:
      column: bulletin_template
      action: format
      kwargs:
        district: Kisumu County
        week: "07"
      new: bulletin_message
```

Example: `"EW {week}: {district} reported elevated incidence"` becomes `"EW 07: Kisumu County reported elevated incidence"`.

### `encode`

```yaml
- transform:
    op: string
    params:
      column: payload
      action: encode
      encoding: utf-8
      new: payload_bytes
```

Example: `"case-summary"` becomes `b"case-summary"`.

### `swapcase`

```yaml
- transform:
    op: string
    params:
      column: qa_marker
      action: swapcase
```

Example: `"DrAfT"` becomes `"dRaFt"`.
