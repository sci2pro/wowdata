import json
import runpy
import sys
import types
from datetime import date, datetime
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


def _cell(value):
    return (lambda: value).__closure__[0]


def _nested_fn(parent, name, **freevars):
    code = next(
        c for c in parent.__code__.co_consts if isinstance(c, types.CodeType) and c.co_name == name
    )
    closure = tuple(_cell(freevars[var]) for var in code.co_freevars)
    return types.FunctionType(code, parent.__globals__, name=name, closure=closure)


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
        _expr_parse("(a(", allow_arith=False)
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


def test_cast_nested_helpers_and_wrapper_branches():
    class Intish:
        def __int__(self):
            return 7

    class Floatish:
        def __float__(self):
            return 2.5

    to_int = _nested_fn(mt.CastTransform.apply, "_to_int")
    to_number = _nested_fn(mt.CastTransform.apply, "_to_number")
    to_bool = _nested_fn(mt.CastTransform.apply, "_to_bool")
    to_date = _nested_fn(mt.CastTransform.apply, "_to_date", date=date, datetime=datetime)
    to_datetime = _nested_fn(mt.CastTransform.apply, "_to_datetime", datetime=datetime)
    wrap_fail = _nested_fn(mt.CastTransform.apply, "_wrap", on_error="fail")
    wrap_null = _nested_fn(mt.CastTransform.apply, "_wrap", on_error="null")
    wrap_keep = _nested_fn(mt.CastTransform.apply, "_wrap", on_error="keep")

    assert to_int(None) is None
    assert to_int(4.0) == 4
    assert to_int(Intish()) == 7
    with pytest.raises(WowDataUserError):
        to_int("bad")

    assert to_number(None) is None
    assert to_number(Floatish()) == 2.5
    with pytest.raises(WowDataUserError):
        to_number(object())

    assert to_bool(None) is None
    assert to_bool(True) is True
    assert to_bool("no") is False
    with pytest.raises(WowDataUserError):
        to_bool("maybe")

    now = datetime(2024, 1, 2, 3, 4, 5)
    assert to_date(None) is None
    assert to_date(now) == now.date()
    with pytest.raises(WowDataUserError):
        to_date("bad-date")
    with pytest.raises(WowDataUserError):
        to_date(123)

    assert to_datetime(None) is None
    with pytest.raises(WowDataUserError):
        to_datetime("bad-datetime")
    with pytest.raises(WowDataUserError):
        to_datetime(123)

    with pytest.raises(WowDataUserError) as ex:
        wrap_fail(lambda v: (_ for _ in ()).throw(WowDataUserError("E_CAST_COERCE", "nope")))("x")
    assert ex.value.code == "E_CAST_COERCE"
    assert wrap_null(lambda v: (_ for _ in ()).throw(WowDataUserError("E_CAST_COERCE", "nope")))("x") is None
    assert wrap_keep(lambda v: (_ for _ in ()).throw(WowDataUserError("E_CAST_COERCE", "nope")))("x") == "x"
    with pytest.raises(WowDataUserError) as ex:
        wrap_fail(lambda v: (_ for _ in ()).throw(WowDataUserError("E_OTHER", "nope")))("x")
    assert ex.value.code == "E_OTHER"
    with pytest.raises(WowDataUserError) as ex:
        wrap_fail(lambda v: (_ for _ in ()).throw(RuntimeError("boom")))("x")
    assert ex.value.code == "E_CAST_INTERNAL"


def test_derive_and_filter_remaining_internal_branches(monkeypatch):
    class Numish:
        def __float__(self):
            return 9.5

    derive_looks = _nested_fn(mt.DeriveTransform.apply, "_looks_number")
    derive_float = _nested_fn(mt.DeriveTransform.apply, "_to_float")
    filter_looks = _nested_fn(mt.FilterTransform.apply, "_looks_number")
    filter_float = _nested_fn(mt.FilterTransform.apply, "_to_float")

    assert derive_looks(None) is False
    assert derive_looks("   ") is False
    assert derive_looks(object()) is False
    assert derive_float(True) == 1.0
    assert derive_float(Numish()) == 9.5

    assert filter_looks(None) is False
    assert filter_looks("   ") is False
    assert filter_looks(object()) is False
    assert filter_float(True) == 1.0
    assert filter_float(Numish()) == 9.5

    with pytest.raises(WowDataUserError) as ex:
        list(Transform("derive", params={"new": "x", "expr": "alpah + 1"}).apply(_tbl([("alpha",), ("1",)]), context=PipelineContext()))
    assert ex.value.code == "E_DERIVE_UNKNOWN_COL"
    assert "Did you mean" in (ex.value.hint or "")

    monkeypatch.setattr(mt, "_expr_parse", lambda expr, allow_arith: (_ for _ in ()).throw(WowDataUserError("E_X", "boom")))
    with pytest.raises(WowDataUserError) as ex:
        Transform("derive", params={"new": "x", "expr": "a"}).apply(_tbl([("a",), ("1",)]), context=PipelineContext())
    assert ex.value.code == "E_X"
    monkeypatch.undo()

    class RowDict(dict):
        def __getitem__(self, key):
            raise KeyError(key)

    def _derive_value(expr, row, **params):
        monkey = pytest.MonkeyPatch()
        monkey.setattr(mt.etl, "header", lambda table: ["a", "b"])
        seen = []

        def fake_addfield(table, new, fn):
            seen.append(fn(row))
            return _tbl([("a", new), ("1", seen[-1])])

        monkey.setattr(mt.etl, "addfield", fake_addfield)
        try:
            Transform("derive", params={"new": "x", "expr": expr, **params}).apply(
                _tbl([("a", "b"), ("1", "2")]), context=PipelineContext()
            )
        finally:
            monkey.undo()
        return seen[-1]

    assert _derive_value("-a", RowDict(a=None, b="2")) is None
    assert _derive_value("-a", RowDict(a="oops", b="2"), strict=False) is None
    assert _derive_value("a - b", RowDict(a="3", b="2")) == 1.0
    assert _derive_value("a * b", RowDict(a="3", b="2")) == 6.0
    assert _derive_value("a / b", RowDict(a="3", b="2")) == 1.5
    assert _derive_value("a < b", RowDict(a="1", b="2")) is True
    assert _derive_value("a <= b", RowDict(a="1", b="2")) is True
    assert _derive_value("a < b", RowDict(a="x", b="2"), strict=False) is False
    assert _derive_value("a >= b", RowDict(a="a", b="b")) is False
    assert _derive_value("a < b", RowDict(a="a", b="b")) is True
    assert _derive_value("a <= b", RowDict(a="a", b="b")) is True
    assert _derive_value("a != b", RowDict(a="a", b="b")) is True
    assert _derive_value("a and b or not a", RowDict(a="a", b="b")) is True
    assert _derive_value("not a", RowDict(a="", b="b")) is True
    assert _derive_value("a", ()) is None

    with pytest.raises(WowDataUserError) as ex:
        list(Transform("derive", params={"new": "x", "expr": "a + b"}).apply(_tbl([("a", "b"), ("x", 1)]), context=PipelineContext()))
    assert ex.value.code == "E_DERIVE_TYPE"
    with pytest.raises(WowDataUserError) as ex:
        list(Transform("derive", params={"new": "x", "expr": "a > b"}).apply(_tbl([("a", "b"), ("x", 1)]), context=PipelineContext()))
    assert ex.value.code == "E_DERIVE_TYPE"
    assert _derive_value("a > b", RowDict(a=object(), b="2"), strict=False) is False

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(mt, "_expr_parse", lambda expr, allow_arith: 5)
    out = list(Transform("derive", params={"new": "x", "expr": "a"}).apply(_tbl([("a",), ("1",)]), context=PipelineContext()))
    assert out[1][1] == 5
    monkeypatch.undo()

    assert Transform("derive", params={"new": "a", "expr": "1", "overwrite": False}).output_schema(
        {"fields": [{"name": "a", "type": "string"}]}
    )["fields"][0]["type"] == "string"

    with pytest.raises(WowDataUserError) as ex:
        list(Transform("filter", params={"where": "alpah == 1"}).apply(_tbl([("alpha",), ("1",)]), context=PipelineContext()))
    assert ex.value.code == "E_FILTER_UNKNOWN_COL"
    assert "Did you mean" in (ex.value.hint or "")

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(mt, "_expr_parse", lambda expr, allow_arith: (_ for _ in ()).throw(WowDataUserError("E_X", "boom")))
    with pytest.raises(WowDataUserError) as ex:
        Transform("filter", params={"where": "a == 1"}).apply(_tbl([("a",), ("1",)]), context=PipelineContext())
    assert ex.value.code == "E_X"
    monkeypatch.undo()

    def _filter_value(where, row, **params):
        monkey = pytest.MonkeyPatch()
        monkey.setattr(mt.etl, "header", lambda table: ["a", "b"])
        seen = []

        def fake_select(table, fn):
            seen.append(fn(row))
            return _tbl([("a", "b"), ("1", "2")])

        monkey.setattr(mt.etl, "select", fake_select)
        try:
            Transform("filter", params={"where": where, **params}).apply(
                _tbl([("a", "b"), ("1", "2")]), context=PipelineContext()
            )
        finally:
            monkey.undo()
        return seen[-1]

    assert _filter_value("a <= b", RowDict(a="1", b="2")) is True
    assert _filter_value("a >= b", RowDict(a="a", b="b")) is False
    assert _filter_value("a > b", RowDict(a="b", b="a")) is True
    assert _filter_value("a < b", RowDict(a="a", b="b")) is True
    assert _filter_value("a <= b", RowDict(a="a", b="a")) is True
    assert _filter_value("a and b", RowDict(a="a", b="b")) is True
    assert _filter_value("a > b", RowDict(a=object(), b="2"), strict=False) is False
    assert _filter_value("a", ()) is False

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(mt, "_expr_parse", lambda expr, allow_arith: 5)
    out = list(Transform("filter", params={"where": "a"}).apply(_tbl([("a",), ("1",)]), context=PipelineContext()))
    assert out == [("a",), ("1",)]
    monkeypatch.undo()


def test_validate_and_join_remaining_branches(monkeypatch):
    wow_type_validate = _nested_fn(mt.ValidateTransform.apply, "_wow_type")
    wow_type_join = _nested_fn(mt.JoinTransform.apply, "_wow_type")

    assert wow_type_validate(None) == "null"
    assert wow_type_validate(True) == "boolean"
    assert wow_type_validate(1) == "integer"
    assert wow_type_validate(1.5) == "number"
    assert wow_type_validate(datetime(2024, 1, 1, 1, 1, 1)) == "datetime"
    assert wow_type_validate(date(2024, 1, 1)) == "date"
    assert wow_type_validate(object()) == "string"

    assert wow_type_join(None) == "null"
    assert wow_type_join(True) == "boolean"
    assert wow_type_join(1) == "integer"
    assert wow_type_join(1.5) == "number"
    assert wow_type_join(datetime(2024, 1, 1, 1, 1, 1)) == "datetime"
    assert wow_type_join(date(2024, 1, 1)) == "date"
    assert wow_type_join(object()) == "string"

    with pytest.raises(WowDataUserError) as ex:
        Transform("validate", params={"fail": "bad"}).apply(_tbl([("a",), ("1",)]), context=PipelineContext())
    assert ex.value.code == "E_VALIDATE_PARAMS"
    with pytest.raises(WowDataUserError) as ex:
        Transform("validate", params={"strict_schema": "bad"}).apply(_tbl([("a",), ("1",)]), context=PipelineContext())
    assert ex.value.code == "E_VALIDATE_PARAMS"

    class DummyReport:
        valid = False

        def to_descriptor(self):
            return {
                "tasks": [
                    {
                        "errors": [
                            {"note": "generic bad", "rowNumber": 9, "fieldName": "missing"},
                            {"message": "msg only"},
                        ],
                        "warnings": [],
                    }
                ]
            }

    class DummyResource:
        def __init__(self, data=None, schema=None):
            self.data = data
            self.schema = schema

        def validate(self, *args, **kwargs):
            return DummyReport()

    monkeypatch.setattr(mt, "Resource", DummyResource)
    monkeypatch.setattr(mt.etl, "head", lambda table, n: [("a", "b"), ("1",), (True, 2.0)])
    monkeypatch.setattr(mt.etl, "header", lambda table: ["a", "b"])
    monkeypatch.setattr(mt.etl, "data", lambda t: [("1",), (True, 2.0)])
    ctx = PipelineContext(schema={"fields": [{"name": "a", "type": "integer"}, {"name": "b", "type": "number"}]})
    list(Transform("validate", params={"fail": False}).apply(_tbl([("a", "b"), ("1", "2")]), context=ctx))
    assert "generic bad" in ctx.validations[-1]["error_preview"][0]
    assert "msg only" in ctx.validations[-1]["error_preview"][1]

    class FailingResource:
        def __init__(self, data=None, schema=None):
            raise RuntimeError("boom")

    monkeypatch.setattr(mt, "Resource", FailingResource)
    with pytest.raises(WowDataUserError) as ex:
        list(Transform("validate", params={"fail": False, "strict_schema": False}).apply(_tbl([("a",), ("1",)]), context=PipelineContext()))
    assert ex.value.code == "E_VALIDATE_FAILED_TO_RUN"

    fake_frictionless = types.SimpleNamespace(
        Schema=type(
            "Schema",
            (),
            {"from_descriptor": classmethod(lambda cls, desc: (_ for _ in ()).throw(RuntimeError("bad schema")))}
        )
    )
    monkeypatch.setitem(sys.modules, "frictionless", fake_frictionless)
    monkeypatch.setattr(mt, "Resource", DummyResource)
    ctx_schema = PipelineContext(schema={"fields": [{"name": "a", "type": "integer"}]})
    list(Transform("validate", params={"fail": False}).apply(_tbl([("a",), ("1",)]), context=ctx_schema))

    real_import = __import__

    def broken_import(name, *args, **kwargs):
        if name == "datetime":
            raise RuntimeError("boom")
        return real_import(name, *args, **kwargs)

    import builtins

    monkeypatch.setattr(builtins, "__import__", broken_import)
    assert wow_type_validate("x") == "string"
    assert wow_type_join("x") == "string"
    monkeypatch.setattr(builtins, "__import__", real_import)

    predominant = _nested_fn(mt.JoinTransform.apply, "_predominant_type", _wow_type=wow_type_join)
    monkeypatch.setattr(mt.etl, "head", lambda tbl, n: tbl)
    monkeypatch.setattr(mt.etl, "data", lambda t: t)
    assert predominant([{"id": "x"}], "id", ["id"]) == "string"
    assert predominant([(None,), ("x",)], "id", ["id"]) == "string"
    assert predominant([("x",)], "id", []) == "null"
    monkeypatch.setattr(mt.etl, "data", lambda t: (_ for _ in ()).throw(RuntimeError("boom")))
    assert predominant([("x",)], "id", ["id"]) == "unknown"
    monkeypatch.setattr(mt.etl, "data", lambda t: t)

    with pytest.raises(WowDataUserError) as ex:
        Transform("join", params={"right": "x.csv", "on": ["id"], "left_on": ["id"], "right_on": ["id"]}).apply(
            _tbl([("id",), ("1",)]), context=PipelineContext()
        )
    assert ex.value.code == "E_JOIN_PARAMS"
    with pytest.raises(WowDataUserError) as ex:
        Transform("join", params={"right": "x.csv", "left_on": ["id"]}).apply(
            _tbl([("id",), ("1",)]), context=PipelineContext()
        )
    assert ex.value.code == "E_JOIN_PARAMS"

    monkeypatch.setattr(mt, "_source_from_descriptor", lambda desc: (_ for _ in ()).throw(WowDataUserError("E_SRC", "bad")))
    with pytest.raises(WowDataUserError) as ex:
        Transform("join", params={"right": "x.csv", "on": ["id"]}).apply(_tbl([("id",), ("1",)]), context=PipelineContext())
    assert ex.value.code == "E_SRC"

    class BadRight:
        def table(self):
            raise WowDataUserError("E_RIGHT", "bad")

    monkeypatch.setattr(mt, "_source_from_descriptor", lambda desc: BadRight())
    with pytest.raises(WowDataUserError) as ex:
        Transform("join", params={"right": "x.csv", "on": ["id"]}).apply(_tbl([("id",), ("1",)]), context=PipelineContext())
    assert ex.value.code == "E_RIGHT"

    left = "LEFT"
    right = "RIGHT"

    class RightSource:
        def table(self):
            return right

    monkeypatch.setattr(mt, "_source_from_descriptor", lambda desc: RightSource())
    monkeypatch.setattr(mt.etl, "header", lambda table: ["id"])
    monkeypatch.setattr(mt.etl, "head", lambda table, n: table)
    monkeypatch.setattr(mt.etl, "data", lambda t: [(1,)] if t == "LEFT" else [("1",)])
    with pytest.raises(WowDataUserError) as ex:
        Transform("join", params={"right": "x.csv", "on": ["id"]}).apply(left, context=PipelineContext())
    assert ex.value.code == "E_JOIN_KEY_TYPE_MISMATCH"

    monkeypatch.setattr(mt.etl, "join", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(WowDataUserError) as ex:
        Transform("join", params={"right": "x.csv", "on": ["id"], "strict_types": False}).apply(
            _tbl([("id",), ("1",)]), context=PipelineContext()
        )
    assert ex.value.code == "E_JOIN_FAILED"

    monkeypatch.setattr(mt.etl, "join", lambda *args, **kwargs: (_ for _ in ()).throw(WowDataUserError("E_JOIN_DIRECT", "boom")))
    with pytest.raises(WowDataUserError) as ex:
        Transform("join", params={"right": "x.csv", "on": ["id"], "strict_types": False}).apply(
            _tbl([("id",), ("1",)]), context=PipelineContext()
        )
    assert ex.value.code == "E_JOIN_DIRECT"
