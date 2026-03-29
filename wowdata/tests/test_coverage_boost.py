import json
import runpy
import sys
from pathlib import Path

import petl as etl
import pytest

import wowdata.cli as cli
import wowdata.models.transforms as mt
import wowdata.schema as ws
import wowdata.util as wu
from wowdata import Pipeline, Sink, Source, Transform, WowDataUserError
from wowdata.models.pipeline import PipelineContext
from wowdata.models.transforms import (
    TransformImpl,
    _expr_parse,
    _expr_tokenize,
    _transform_from_ir,
    register_transform,
)


def _tbl(data):
    return etl.wrap(data)


def test_error_str_without_hint():
    err = WowDataUserError("E_X", "broken")
    assert str(err) == "[E_X] broken"


def test_util_path_helpers_and_inline_schema_errors(tmp_path):
    base = tmp_path
    assert wu._is_probably_url("http://x")
    assert wu._is_probably_url("https://x")
    assert wu._is_probably_url("s3://bucket/key")
    assert not wu._is_probably_url("local.csv")

    assert wu._norm_path("", base_dir=base) == ""
    assert wu._norm_path("https://example.com/a.csv", base_dir=base) == "https://example.com/a.csv"
    abs_path = str((tmp_path / "a.csv").resolve())
    assert wu._norm_path(abs_path, base_dir=base) == abs_path
    assert wu._norm_path("rel.csv", base_dir=None) == "rel.csv"
    assert wu._norm_path("rel.csv", base_dir=base).endswith("rel.csv")
    assert wu._infer_type_from_uri("data.csv") == "csv"
    assert wu._infer_type_from_uri("data.txt") is None
    assert wu._normalize_inline_schema({"fields": []}) == {"fields": []}
    assert wu._normalize_inline_schema({"x": 1}) == {"fields": []}

    with pytest.raises(WowDataUserError) as ex:
        wu._normalize_inline_schema([])  # type: ignore[arg-type]
    assert ex.value.code == "E_SCHEMA_INLINE_TYPE"

    with pytest.raises(WowDataUserError) as ex:
        wu._normalize_inline_schema({"fields": "bad"})  # type: ignore[dict-item]
    assert ex.value.code == "E_SCHEMA_INLINE_FIELDS"

    class BadPath:
        def __fspath__(self):
            return self

    orig_path = wu.Path

    def raising_path(value):
        if value == "boom":
            raise TypeError("bad path")
        return orig_path(value)

    monkey = pytest.MonkeyPatch()
    monkey.setattr(wu, "Path", raising_path)
    try:
        assert wu._norm_path("boom", base_dir=base) == "boom"
    finally:
        monkey.undo()


def test_schema_helpers_and_ir_normalization(tmp_path):
    src_csv = tmp_path / "people.csv"
    src_csv.write_text("id\n1\n", encoding="utf-8")
    out_csv = tmp_path / "out.csv"
    right_csv = tmp_path / "right.csv"
    right_csv.write_text("id\n1\n", encoding="utf-8")

    src = Source(str(src_csv), type="csv", schema={"fields": [{"name": "id", "type": "integer"}]}, options={"delimiter": ","})
    sink = Sink(str(out_csv), type="csv", options={"encoding": "utf-8"})
    tr = Transform("cast", params={"types": {"id": "integer"}}, output_schema_override={"fields": [{"name": "id"}]})

    assert ws._schema_field_names(None) == []
    assert ws._schema_field_names({"fields": "bad"}) == []
    assert ws._schema_field_names({"fields": [{"name": "a"}, {"bad": 1}]}) == ["a"]
    assert ws._source_to_ir(src)["options"] == {"delimiter": ","}
    assert ws._sink_to_ir(sink)["options"] == {"encoding": "utf-8"}
    assert ws._transform_to_ir(tr)["output_schema"] == {"fields": [{"name": "id"}]}

    assert isinstance(ws._source_from_ir({"uri": str(src_csv)}), Source)
    assert isinstance(ws._sink_from_ir({"uri": str(out_csv)}), Sink)
    with pytest.raises(WowDataUserError):
        ws._source_from_ir([])  # type: ignore[arg-type]
    with pytest.raises(WowDataUserError):
        ws._source_from_ir({})
    with pytest.raises(WowDataUserError):
        ws._sink_from_ir([])  # type: ignore[arg-type]
    with pytest.raises(WowDataUserError):
        ws._sink_from_ir({})

    ir = {
        "pipeline": {
            "start": {"uri": "people.csv", "options": None},
            "steps": [
                {"sink": {"uri": "out.csv", "options": None}},
                {"transform": {"op": "join", "params": {True: ["id"], "right": {"uri": "right.csv", "options": None}}}},
                {"transform": {"op": "cast", "params": {"types": {"id": "integer"}, "on_error": None}}},
            ],
        }
    }
    norm = ws._normalize_ir(ir, base_dir=tmp_path)
    assert norm["wowdata"] == 0
    assert norm["pipeline"]["start"]["options"] == {}
    assert norm["pipeline"]["steps"][0]["sink"]["options"] == {}
    assert norm["pipeline"]["steps"][1]["transform"]["params"]["on"] == ["id"]
    assert norm["pipeline"]["steps"][1]["transform"]["params"]["right"]["options"] == {}
    assert norm["pipeline"]["steps"][2]["transform"]["params"]["on_error"] == "null"

    ir2 = {"wowdata": 0, "pipeline": {"start": {"uri": "people.csv"}, "steps": None}}
    assert ws._normalize_ir(ir2, base_dir=tmp_path)["pipeline"]["steps"] == []
    ir3 = {"wowdata": 0, "pipeline": {"start": {"uri": "people.csv"}, "steps": [{"transform": {"op": "select", "params": None}}]}}
    assert ws._normalize_ir(ir3, base_dir=tmp_path)["pipeline"]["steps"][0]["transform"]["params"] == {}

    with pytest.raises(WowDataUserError):
        ws._normalize_ir([], base_dir=tmp_path)  # type: ignore[arg-type]
    with pytest.raises(WowDataUserError):
        ws._normalize_ir({"wowdata": 1, "pipeline": {}}, base_dir=tmp_path)
    with pytest.raises(WowDataUserError):
        ws._normalize_ir({"wowdata": 0, "pipeline": []}, base_dir=tmp_path)  # type: ignore[dict-item]
    with pytest.raises(WowDataUserError):
        ws._normalize_ir({"wowdata": 0, "pipeline": {"start": [], "steps": []}}, base_dir=tmp_path)  # type: ignore[dict-item]
    with pytest.raises(WowDataUserError):
        ws._normalize_ir({"wowdata": 0, "pipeline": {"start": {"uri": "a.csv"}, "steps": {}}}, base_dir=tmp_path)  # type: ignore[dict-item]
    with pytest.raises(WowDataUserError):
        ws._normalize_ir({"wowdata": 0, "pipeline": {"start": {"uri": "a.csv"}, "steps": [1]}}, base_dir=tmp_path)  # type: ignore[list-item]
    with pytest.raises(WowDataUserError):
        ws._normalize_ir({"wowdata": 0, "pipeline": {"start": {"uri": "a.csv"}, "steps": [{"sink": []}]}}, base_dir=tmp_path)  # type: ignore[list-item]
    with pytest.raises(WowDataUserError):
        ws._normalize_ir({"wowdata": 0, "pipeline": {"start": {"uri": "a.csv"}, "steps": [{"transform": []}]}}, base_dir=tmp_path)  # type: ignore[list-item]
    with pytest.raises(WowDataUserError):
        ws._normalize_ir({"wowdata": 0, "pipeline": {"start": {"uri": "a.csv"}, "steps": [{"transform": {"op": "x", "params": []}}]}}, base_dir=tmp_path)  # type: ignore[list-item]
    with pytest.raises(WowDataUserError):
        ws._normalize_ir({"wowdata": 0, "pipeline": {"start": {"uri": "a.csv"}, "steps": [{"weird": {}}]}}, base_dir=tmp_path)


def test_cli_helpers_and_main_paths(tmp_path, capsys, monkeypatch):
    people = tmp_path / "people.csv"
    people.write_text("age\n30\n", encoding="utf-8")
    out = tmp_path / "out.csv"
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        f"""\
wowdata: 0
pipeline:
  start:
    uri: {people}
    type: csv
  steps:
    - sink:
        uri: {out}
        type: csv
""",
        encoding="utf-8",
    )

    assert cli._default_locked_path("a.yaml").name == "a.locked.yaml"
    assert cli._default_locked_path("a.txt").name == "a.txt.locked.yaml"
    pipe = cli._load_pipeline(str(pipeline), str(tmp_path))
    assert isinstance(pipe, Pipeline)
    cli._print_json({"ok": True})
    assert json.loads(capsys.readouterr().out)["ok"] is True
    cli._print_user_error(WowDataUserError("E_X", "bad"))
    assert "E_X" in capsys.readouterr().err

    rc = cli.main(["run", str(pipeline), "--json", "--show-checkpoints"])
    payload = json.loads(capsys.readouterr().out)
    assert rc == cli.EXIT_OK
    assert payload["command"] == "run"
    assert "checkpoint_data" in payload

    rc = cli.main(["run", str(pipeline), "--quiet", "--show-checkpoints"])
    captured = capsys.readouterr()
    assert rc == cli.EXIT_OK
    assert "Run complete" not in captured.out
    assert "[" in captured.out

    rc = cli.main(["validate", str(pipeline), "--json"])
    assert rc == cli.EXIT_OK
    assert json.loads(capsys.readouterr().out)["command"] == "validate"

    rc = cli.main(["schema", str(pipeline)])
    captured = capsys.readouterr()
    assert rc == cli.EXIT_OK
    assert "Schema for:" in captured.out

    rc = cli.main(["lock-schema", str(pipeline), "--json"])
    assert rc == cli.EXIT_OK
    assert json.loads(capsys.readouterr().out)["command"] == "lock-schema"

    bad = tmp_path / "bad.yaml"
    bad.write_text("wowdata: [", encoding="utf-8")
    assert cli.main(["run", str(bad)]) == cli.EXIT_PIPELINE_PARSE
    assert cli.main(["schema", str(bad)]) == cli.EXIT_PIPELINE_PARSE
    assert cli.main(["lock-schema", str(bad)]) == cli.EXIT_PIPELINE_PARSE
    assert "E_YAML_PARSE" in capsys.readouterr().err

    class ValidationPipe(Pipeline):
        def run(self):
            ctx = super().run()
            ctx.validations.append({"valid": True})
            return ctx

    monkeypatch.setattr(cli, "_load_pipeline", lambda path, base_dir: ValidationPipe(pipe.start, pipe.steps))
    rc = cli.main(["run", str(pipeline)])
    captured = capsys.readouterr()
    assert rc == cli.EXIT_OK
    assert "Validations: 1" in captured.out

    argv_prev = sys.argv[:]
    sys.argv = ["wowdata", "validate", str(pipeline)]
    try:
        with pytest.raises(SystemExit) as ex:
            runpy.run_module("wowdata.cli", run_name="__main__")
        assert ex.value.code == 0
    finally:
        sys.argv = argv_prev


def test_transform_base_helpers_and_ir_parsing():
    class DummyImpl(TransformImpl):
        pass

    DummyImpl.validate_params({}, None)
    assert DummyImpl.output_schema({"fields": []}, {}) == {"fields": []}
    with pytest.raises(WowDataUserError) as ex:
        DummyImpl.apply([], params={}, context=PipelineContext())
    assert ex.value.code == "E_OP_NOT_IMPL"

    @register_transform("cov_dummy")
    class CovDummy(TransformImpl):
        @classmethod
        def apply(cls, table, *, params, context):
            return table

    t = Transform("cov_dummy", params={"x": 1})
    assert "cov_dummy" in str(t)
    assert list(t.apply(_tbl([("a",), (1,)]), context=PipelineContext())) == [("a",), (1,)]
    assert t.output_schema({"fields": []}) == {"fields": []}
    t2 = Transform("cov_dummy", output_schema_override={"fields": [{"name": "x"}]})
    assert t2.output_schema(None) == {"fields": [{"name": "x"}]}

    with pytest.raises(WowDataUserError):
        Transform("missing").apply(_tbl([("a",), (1,)]), context=PipelineContext())
    assert Transform("missing").output_schema({"fields": [{"name": "a"}]}) == {"fields": [{"name": "a"}]}

    toks = _expr_tokenize(r"'a\'b' and true or null", allow_arith=True)
    assert [t.typ for t in toks] == ["STR", "KW", "KW", "KW", "KW", "EOF"]
    assert _expr_parse("(a + -1) >= 2 and not false", allow_arith=True)
    assert _expr_parse("a == 1 or b != 2", allow_arith=False)

    with pytest.raises(WowDataUserError):
        _expr_tokenize("'unterminated", allow_arith=False)
    with pytest.raises(WowDataUserError):
        _expr_tokenize("@", allow_arith=False)
    with pytest.raises(WowDataUserError):
        _expr_parse("(a", allow_arith=False)
    with pytest.raises(WowDataUserError):
        _expr_parse("(a]", allow_arith=False)
    with pytest.raises(WowDataUserError):
        _expr_parse("true false", allow_arith=False)
    with pytest.raises(WowDataUserError):
        _expr_parse(")", allow_arith=False)
    assert _expr_parse("true", allow_arith=False) == ("lit", True)
    assert _expr_parse("false", allow_arith=False) == ("lit", False)
    assert _expr_parse("null", allow_arith=False) == ("lit", None)

    assert _transform_from_ir({"op": "cast", "params": {"types": {"a": "integer"}}}).op == "cast"
    with pytest.raises(WowDataUserError):
        _transform_from_ir([])  # type: ignore[arg-type]
    with pytest.raises(WowDataUserError):
        _transform_from_ir({})
    with pytest.raises(WowDataUserError):
        _transform_from_ir({"op": "x", "params": 1})  # type: ignore[dict-item]
    with pytest.raises(WowDataUserError):
        _transform_from_ir({"op": "x", "output_schema": []})  # type: ignore[dict-item]
