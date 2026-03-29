from datetime import date, datetime

import petl as etl
import pytest

from wowdata import Transform, WowDataUserError
from wowdata.models.pipeline import PipelineContext
import wowdata.models.transforms as mt


def _tbl(data):
    return etl.wrap(data)


def test_cast_covers_supported_types_and_output_schema():
    tbl = _tbl(
        [
            ("i", "n", "b", "s", "d", "dt"),
            ("12", "12.5", "yes", 5, "2024-01-02", "2024-01-02T03:04:05"),
            ("", "", "", None, "", ""),
            (12, 4.0, 0, "x", date(2024, 1, 3), datetime(2024, 1, 3, 4, 5, 6)),
        ]
    )
    t = Transform(
        "cast",
        params={
            "types": {
                "i": "integer",
                "n": "number",
                "b": "boolean",
                "s": "string",
                "d": "date",
                "dt": "datetime",
            }
        },
    )
    out = list(t.apply(tbl, context=PipelineContext()))
    assert out[1] == (12, 12.5, True, "5", date(2024, 1, 2), datetime(2024, 1, 2, 3, 4, 5))
    assert out[2] == (None, None, None, None, None, None)
    assert out[3][0:4] == (12, 4.0, False, "x")
    sch = t.output_schema({"fields": [{"name": "i", "type": "string"}, {"name": "x", "type": "string"}]})
    assert sch == {"fields": [{"name": "i", "type": "integer"}, {"name": "x", "type": "string"}]}
    assert t.output_schema(None) is None


def test_cast_validation_and_failure_branches(monkeypatch):
    with pytest.raises(WowDataUserError) as ex:
        Transform("cast", params={"types": {"a": "integer"}, "on_error": "bad"}).apply(
            _tbl([("a",), ("1",)]), context=PipelineContext()
        )
    assert ex.value.code == "E_CAST_ON_ERROR"

    original_convert = mt.etl.convert

    def boom(table, col, fn):
        fn("1")
        raise RuntimeError("unexpected")

    monkeypatch.setattr(mt.etl, "convert", boom)
    with pytest.raises(RuntimeError):
        Transform("cast", params={"types": {"a": "integer"}}).apply(_tbl([("a",), ("1",)]), context=PipelineContext())
    monkeypatch.setattr(mt.etl, "convert", original_convert)


def test_select_and_drop_output_schema_branches():
    ctx = PipelineContext(schema={"fields": [{"name": "a"}, {"name": "b"}]})
    with pytest.raises(WowDataUserError) as ex:
        Transform("select", params={"columns": ["x"]}).apply(_tbl([("a", "b"), (1, 2)]), context=ctx)
    assert ex.value.code == "E_SELECT_UNKNOWN_COL"
    assert Transform("select", params={"columns": ["b"]}).output_schema({"fields": [{"name": "a"}, {"name": "b"}]}) == {
        "fields": [{"name": "b"}]
    }
    assert Transform("select", params={"columns": ["b"]}).output_schema(None) is None

    assert Transform("drop", params={"columns": ["b"]}).output_schema({"fields": [{"name": "a"}, {"name": "b"}]}) == {
        "fields": [{"name": "a"}]
    }
    assert Transform("drop", params={"columns": ["b"]}).output_schema(None) is None


def test_derive_branches_and_output_schema():
    tbl = _tbl([("a", "b"), ("2", "3"), ("x", "4"), (None, "5")])
    out = list(Transform("derive", params={"new": "c", "expr": "a + b"}).apply(tbl, context=PipelineContext()))
    assert out[1][2] == 5.0

    out2 = list(Transform("derive", params={"new": "c", "expr": "a - 'x'", "strict": False}).apply(tbl, context=PipelineContext()))
    assert out2[1][2] is None

    out3 = list(Transform("derive", params={"new": "c", "expr": "'a' + 'b'"}).apply(tbl, context=PipelineContext()))
    assert out3[1][2] == "ab"

    out4 = list(Transform("derive", params={"new": "a", "expr": "-b", "overwrite": True}).apply(tbl, context=PipelineContext()))
    assert {row[0] for row in out4[1:]} == {-4.0, -5.0}

    out5 = list(Transform("derive", params={"new": "flag", "expr": "a > b", "strict": False}).apply(tbl, context=PipelineContext()))
    assert out5[1][2] is False

    with pytest.raises(WowDataUserError) as ex:
        list(Transform("derive", params={"new": "x", "expr": "-a"}).apply(_tbl([("a",), ("oops",)]), context=PipelineContext()))
    assert ex.value.code == "E_DERIVE_TYPE"

    with pytest.raises(WowDataUserError) as ex:
        Transform("derive", params={"new": "x", "expr": "1", "overwrite": "bad"}).apply(
            _tbl([("a",), (1,)]), context=PipelineContext()
        )
    assert ex.value.code == "E_DERIVE_PARAMS"

    with pytest.raises(WowDataUserError) as ex:
        Transform("derive", params={"new": "x", "expr": "1", "strict": "bad"}).apply(
            _tbl([("a",), (1,)]), context=PipelineContext()
        )
    assert ex.value.code == "E_DERIVE_PARAMS"

    ctx = PipelineContext(schema={"fields": [{"name": "x"}]})
    with pytest.raises(WowDataUserError) as ex:
        Transform("derive", params={"new": "x", "expr": "1"}).apply(_tbl([("a",), (1,)]), context=ctx)
    assert ex.value.code == "E_DERIVE_EXISTS"

    base_schema = {"fields": [{"name": "old", "type": "string"}]}
    assert Transform("derive", params={"new": "s", "expr": "'x'"}).output_schema(base_schema)["fields"][-1]["type"] == "string"
    assert Transform("derive", params={"new": "n", "expr": "1.2"}).output_schema(base_schema)["fields"][-1]["type"] == "number"
    assert Transform("derive", params={"new": "i", "expr": "1"}).output_schema(base_schema)["fields"][-1]["type"] == "integer"
    assert Transform("derive", params={"new": "b", "expr": "old == 'x'"}).output_schema(base_schema)["fields"][-1]["type"] == "boolean"
    assert Transform("derive", params={"new": "old", "expr": "1", "overwrite": True}).output_schema(base_schema)["fields"][0]["type"] == "integer"
    assert Transform("derive", params={"new": "x", "expr": "1"}).output_schema(None) is None
    assert Transform("derive", params={"new": "x", "expr": "1"}).output_schema({"fields": "bad"}) == {"fields": "bad"}


def test_filter_branches_and_output_schema():
    tbl = _tbl([("a", "b"), ("2", "x"), ("3", "y"), ("4", "z")])
    out = list(Transform("filter", params={"where": "a >= 3 or not (b == 'x')"}).apply(tbl, context=PipelineContext()))
    assert out == [("a", "b"), ("3", "y"), ("4", "z")]

    out2 = list(Transform("filter", params={"where": "a < 3", "strict": False}).apply(tbl, context=PipelineContext()))
    assert out2 == [("a", "b"), ("2", "x")]

    with pytest.raises(WowDataUserError) as ex:
        Transform("filter", params={"where": "a > 1", "strict": "bad"}).apply(tbl, context=PipelineContext())
    assert ex.value.code == "E_FILTER_PARAMS"

    with pytest.raises(WowDataUserError) as ex:
        list(Transform("filter", params={"where": "a > 1"}).apply(_tbl([("a",), ("x",)]), context=PipelineContext()))
    assert ex.value.code == "E_FILTER_TYPE"

    assert Transform("filter", params={"where": "a == 1"}).output_schema({"fields": [{"name": "a"}]}) == {
        "fields": [{"name": "a"}]
    }


def test_string_validation_apply_and_output_schema():
    ctx = PipelineContext(schema={"fields": [{"name": "txt"}, {"name": "out"}]})
    with pytest.raises(WowDataUserError):
        Transform("string", params={"column": "", "action": "regex_replace", "pattern": "x"}).apply(
            _tbl([("txt",), ("x",)]), context=ctx
        )
    with pytest.raises(WowDataUserError):
        Transform("string", params={"column": "txt", "action": "bad", "pattern": "x"}).apply(
            _tbl([("txt",), ("x",)]), context=ctx
        )
    with pytest.raises(WowDataUserError):
        Transform("string", params={"column": "txt", "action": "regex_replace", "pattern": "("}).apply(
            _tbl([("txt",), ("x",)]), context=ctx
        )
    with pytest.raises(WowDataUserError):
        Transform("string", params={"column": "txt", "action": "regex_replace", "pattern": "x", "new": ""}).apply(
            _tbl([("txt",), ("x",)]), context=ctx
        )
    with pytest.raises(WowDataUserError):
        Transform("string", params={"column": "txt", "action": "regex_replace", "pattern": "x", "overwrite": "bad"}).apply(
            _tbl([("txt",), ("x",)]), context=ctx
        )
    with pytest.raises(WowDataUserError):
        Transform("string", params={"column": "txt", "action": "regex_replace", "pattern": "x", "repl": 1}).apply(
            _tbl([("txt",), ("x",)]), context=ctx
        )
    with pytest.raises(WowDataUserError):
        Transform("string", params={"column": "txt", "action": "regex_extract", "pattern": "(x)", "group": []}).apply(
            _tbl([("txt",), ("x",)]), context=ctx
        )
    with pytest.raises(WowDataUserError):
        Transform("string", params={"column": "txt", "action": "regex_extract", "pattern": "(x)", "group": "missing"}).apply(
            _tbl([("txt",), ("x",)]), context=PipelineContext(schema={"fields": [{"name": "txt"}]})
        )
    with pytest.raises(WowDataUserError):
        Transform("string", params={"column": "txt", "action": "regex_replace", "pattern": "x", "new": "out"}).apply(
            _tbl([("txt",), ("x",)]), context=ctx
        )

    tbl = _tbl([("txt",), ("abc123",), (None,), ("zzz",)])
    out = list(
        Transform(
            "string",
            params={"column": "txt", "action": "regex_extract", "pattern": "(?P<num>\\d+)", "group": "num", "new": "num", "overwrite": True},
        ).apply(tbl, context=PipelineContext())
    )
    assert out == [("txt", "num"), ("abc123", "123"), (None, None), ("zzz", None)]

    sch = Transform("string", params={"column": "txt", "action": "regex_replace", "pattern": "x"}).output_schema(
        {"fields": [{"name": "txt", "type": "any"}]}
    )
    assert sch["fields"][0]["type"] == "string"
    sch2 = Transform("string", params={"column": "txt", "action": "regex_extract", "pattern": "(x)", "new": "new"}).output_schema(
        {"fields": [{"name": "txt", "type": "any"}]}
    )
    assert sch2["fields"][-1]["name"] == "new"
    assert Transform("string", params={"column": "txt", "action": "regex_extract", "pattern": "(x)"}).output_schema(None) is None
    assert Transform("string", params={"column": "txt", "action": "regex_extract", "pattern": "(x)"}).output_schema({"fields": "bad"}) == {
        "fields": "bad"
    }


def test_validate_and_join_deeper_branches(monkeypatch, tmp_path):
    orig_head = mt.etl.head
    orig_header = mt.etl.header
    orig_data = mt.etl.data

    class DummyReport:
        def __init__(self, valid, desc=None, boom=False):
            self.valid = valid
            self._desc = desc or {}
            self._boom = boom

        def to_descriptor(self):
            if self._boom:
                raise RuntimeError("bad desc")
            return self._desc

    class DummyResource:
        def __init__(self, data=None, schema=None):
            self.data = data
            self.schema = schema

        def validate(self, *args, **kwargs):
            if kwargs.get("cast") is True:
                raise TypeError("no cast kw")
            return DummyReport(
                False,
                {
                    "tasks": [
                        {
                            "errors": [
                                {"rowNumber": 2, "fieldName": "a", "note": 'type is "integer/default"'},
                                {"rowNumber": 3, "fieldName": "a", "message": "bad"},
                            ],
                            "warnings": [{"note": "warn"}],
                        }
                    ]
                },
            )

    monkeypatch.setattr(mt, "Resource", DummyResource)
    monkeypatch.setattr(mt.etl, "head", lambda table, n: [("a",), ("1",), ("2", "extra")])
    monkeypatch.setattr(mt.etl, "header", lambda table: ["a"])
    monkeypatch.setattr(mt.etl, "data", lambda t: [("1",), ("2", "extra")])

    ctx = PipelineContext(schema={"fields": [{"name": "a", "type": "integer"}]})
    tbl = _tbl([("a",), ("1",), ("2",)])
    out = list(Transform("validate", params={"strict_schema": False, "fail": False}).apply(tbl, context=ctx))
    assert out == [("a",), ("1",), ("2",)]
    assert ctx.validations[-1]["warnings"] == 1
    assert "value type is" in ctx.validations[-1]["error_preview"][0]

    monkeypatch.setattr(
        mt,
        "Resource",
        lambda data=None, schema=None: type("R", (), {"validate": lambda self, cast=True: DummyReport(True, boom=True)})(),
    )
    ctx2 = PipelineContext(schema={"fields": [{"name": "a", "type": "string"}]})
    assert list(Transform("validate", params={"strict_schema": False, "fail": False}).apply(tbl, context=ctx2))[-1] == ("2",)

    monkeypatch.setattr(mt.etl, "head", lambda table, n: [("a",)])
    monkeypatch.setattr(mt.etl, "header", lambda table: ["a"])
    monkeypatch.setattr(mt.etl, "data", lambda t: [])
    empty_ctx = PipelineContext(schema={"fields": [{"name": "a", "type": "string"}]})
    empty_tbl = _tbl([("a",)])
    assert list(Transform("validate", params={"strict_schema": False}).apply(empty_tbl, context=empty_ctx)) == [("a",)]
    assert empty_ctx.validations[-1]["rows_checked"] == 0

    monkeypatch.setattr(mt.etl, "head", orig_head)
    monkeypatch.setattr(mt.etl, "header", orig_header)
    monkeypatch.setattr(mt.etl, "data", orig_data)

    left = tmp_path / "left.csv"
    right = tmp_path / "right.csv"
    left.write_text("id,l\n1,A\n2,B\n", encoding="utf-8")
    right.write_text("rid,r\n1,X\n2,Y\n", encoding="utf-8")
    out_join = list(
        Transform("join", params={"right": str(right), "left_on": ["id"], "right_on": ["rid"], "how": "left"}).apply(
            etl.fromcsv(left), context=PipelineContext()
        )
    )
    assert out_join[1] == ("1", "A", "X")

    with pytest.raises(WowDataUserError):
        Transform("join", params={"right": str(right), "left_on": [], "right_on": ["rid"]}).apply(
            _tbl(etl.fromcsv(left)), context=PipelineContext()
        )
    with pytest.raises(WowDataUserError):
        Transform("join", params={"right": str(right), "left_on": ["id"], "right_on": ["rid"], "strict_types": "bad"}).apply(
            _tbl(etl.fromcsv(left)), context=PipelineContext()
        )

    monkeypatch.setattr(mt, "_source_from_descriptor", lambda desc: (_ for _ in ()).throw(RuntimeError("bad")))
    with pytest.raises(WowDataUserError) as ex:
        Transform("join", params={"right": str(right), "on": ["id"]}).apply(etl.fromcsv(left), context=PipelineContext())
    assert ex.value.code == "E_JOIN_PARAMS"


def test_derive_and_filter_internal_branches_via_monkeypatched_petl(monkeypatch):
    class RowDict(dict):
        def __getitem__(self, key):
            raise KeyError(key)

    captured = []

    def fake_addfield(table, new, fn):
        captured.append(fn(RowDict(a="7")))
        captured.append(fn(["8"]))
        captured.append(fn((None,)))
        return _tbl([("a", new), ("x", captured[-1])])

    monkeypatch.setattr(mt.etl, "addfield", fake_addfield)
    monkeypatch.setattr(mt.etl, "header", lambda table: ["a"])

    Transform("derive", params={"new": "b", "expr": "a"}).apply(_tbl([("a",), ("1",)]), context=PipelineContext())
    assert captured == ["7", "8", None]

    monkeypatch.setattr(mt, "_expr_parse", lambda expr, allow_arith: ("weird",))
    with pytest.raises(WowDataUserError) as ex:
        Transform("derive", params={"new": "b", "expr": "a"}).apply(_tbl([("a",), ("1",)]), context=PipelineContext())
    assert ex.value.code == "E_DERIVE_UNSUPPORTED"
    monkeypatch.undo()

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(mt.etl, "header", lambda table: ["a"])
    selected = []

    def fake_select(table, fn):
        selected.append(fn(RowDict(a="7")))
        selected.append(fn(["8"]))
        selected.append(fn((None,)))
        return _tbl([("a",), ("1",)])

    monkeypatch.setattr(mt.etl, "select", fake_select)
    Transform("filter", params={"where": "a == '7' or not (a == '8')"}).apply(_tbl([("a",), ("1",)]), context=PipelineContext())
    assert selected == [True, False, True]

    monkeypatch.setattr(mt, "_expr_parse", lambda expr, allow_arith: ("weird",))
    with pytest.raises(WowDataUserError) as ex:
        list(Transform("filter", params={"where": "a == 1"}).apply(_tbl([("a",), ("1",)]), context=PipelineContext()))
    assert ex.value.code == "E_FILTER_UNSUPPORTED"
    monkeypatch.undo()
