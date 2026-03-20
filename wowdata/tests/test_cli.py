import json

from wowdata.cli import (
    EXIT_OK,
    EXIT_PIPELINE_PARSE,
    EXIT_PIPELINE_RUNTIME,
    main,
)


def _write_file(path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _make_valid_pipeline_yaml(tmp_path):
    people = tmp_path / "people.csv"
    out = tmp_path / "out.csv"
    pipeline = tmp_path / "pipeline.yaml"

    _write_file(
        people,
        "age,country\n30,KE\n25,UG\n",
    )

    _write_file(
        pipeline,
        f"""\
wowdata: 0
pipeline:
  start:
    uri: {people}
    type: csv
    schema:
      fields:
        - name: age
          type: string
        - name: country
          type: string
  steps:
    - transform:
        op: cast
        params:
          types:
            age: integer
    - sink:
        uri: {out}
        type: csv
""",
    )
    return pipeline, out


def _make_invalid_runtime_pipeline_yaml(tmp_path):
    people = tmp_path / "people.csv"
    out = tmp_path / "out.csv"
    pipeline = tmp_path / "bad_runtime.yaml"

    _write_file(
        people,
        "age,country\n30,KE\n25,UG\n",
    )

    _write_file(
        pipeline,
        f"""\
wowdata: 0
pipeline:
  start:
    uri: {people}
    type: csv
  steps:
    - transform:
        op: filter
        params:
          where: missing_col > 1
    - sink:
        uri: {out}
        type: csv
""",
    )
    return pipeline


def test_cli_run_success(tmp_path, capsys):
    pipeline, out = _make_valid_pipeline_yaml(tmp_path)

    rc = main(["run", str(pipeline)])
    captured = capsys.readouterr()

    assert rc == EXIT_OK
    assert out.exists()
    assert "Run complete" in captured.out


def test_cli_validate_success(tmp_path, capsys):
    pipeline, _ = _make_valid_pipeline_yaml(tmp_path)

    rc = main(["validate", str(pipeline)])
    captured = capsys.readouterr()

    assert rc == EXIT_OK
    assert "Pipeline is valid" in captured.out


def test_cli_schema_json_output(tmp_path, capsys):
    pipeline, _ = _make_valid_pipeline_yaml(tmp_path)

    rc = main(["schema", str(pipeline), "--json"])
    captured = capsys.readouterr()

    assert rc == EXIT_OK
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    fields = payload["schema"]["fields"]
    assert any(f.get("name") == "age" and f.get("type") == "integer" for f in fields)


def test_cli_lock_schema_writes_yaml(tmp_path, capsys):
    pipeline, _ = _make_valid_pipeline_yaml(tmp_path)
    locked = tmp_path / "pipeline.locked.yaml"

    rc = main(["lock-schema", str(pipeline), "--output", str(locked)])
    captured = capsys.readouterr()

    assert rc == EXIT_OK
    assert locked.exists()
    assert "Schema-locked pipeline written" in captured.out
    assert "output_schema" in locked.read_text(encoding="utf-8")


def test_cli_run_runtime_error_returns_runtime_exit_code(tmp_path, capsys):
    pipeline = _make_invalid_runtime_pipeline_yaml(tmp_path)

    rc = main(["run", str(pipeline)])
    captured = capsys.readouterr()

    assert rc == EXIT_PIPELINE_RUNTIME
    assert "E_FILTER_UNKNOWN_COL" in captured.err


def test_cli_validate_parse_error_returns_parse_exit_code(tmp_path, capsys):
    bad = tmp_path / "bad.yaml"
    _write_file(bad, "wowdata: [")

    rc = main(["validate", str(bad)])
    captured = capsys.readouterr()

    assert rc == EXIT_PIPELINE_PARSE
    assert "E_YAML_PARSE" in captured.err
