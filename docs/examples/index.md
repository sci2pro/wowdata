# Examples

Domain-focused walkthroughs show complete flows from raw CSVs to cleaned outputs.

The repository also includes the referenced YAML pipelines and CSV fixtures in `examples/`, so you can
run them as-is:

```bash
wow run examples/climate_heat_events.yaml --base-dir examples
wow run examples/climate_rainfall_alerts.yaml --base-dir examples
wow run examples/ml_model_candidates.yaml --base-dir examples
wow run examples/epi_line_list_cleanup.yaml --base-dir examples
wow run examples/epi_weekly_incidence_cleanup.yaml --base-dir examples
```

- [Climate](climate.md)
- [Epidemiology](epidemiology.md)
- [Machine Learning](machine-learning.md)
- [String Operations](string-operations.md)
- Repository copy: [EXAMPLES.md](https://github.com/sci2pro/wowdata/blob/main/EXAMPLES.md)
