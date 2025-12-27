import json

import petl as etl
import pytest

from wowdata import Transform, WowDataUserError
from wowdata.models.pipeline import PipelineContext
from wowdata.models.transforms import TRANSFORM_REGISTRY


def _tbl(data):
    return etl.wrap(data)


# ---------- cast ----------
def test_cast_requires_types():
    """cast validation rejects missing params.types mapping."""
    with pytest.raises(WowDataUserError) as ex:
        Transform("cast", params={}).apply(_tbl([("a",)]), context=PipelineContext())
    assert getattr(ex.value, "code", None) == "E_CAST_TYPES"


def test_cast_on_error_null(tmp_path):
    """cast replaces errors with null when on_error='null'."""
    tbl = _tbl([("a",), ("x",), ("3",)])
    t = Transform("cast", params={"types": {"a": "integer"}, "on_error": "null"})

    out = list(t.apply(tbl, context=PipelineContext()))
    assert out == [("a",), (None,), (3,)]


def test_cast_missing_column_errors():
    """cast raises when a referenced column is missing."""
    tbl = _tbl([("b",), (1,)])
    t = Transform("cast", params={"types": {"a": "integer"}})
    with pytest.raises(WowDataUserError) as ex:
        list(t.apply(tbl, context=PipelineContext()))
    assert getattr(ex.value, "code", None) == "E_CAST_MISSING_COL"


def test_cast_unsupported_type_and_bad_key():
    """cast rejects non-string keys and unsupported type names."""
    tbl = _tbl([(None,), (1,)])
    with pytest.raises(WowDataUserError) as ex:
        Transform("cast", params={"types": {None: "integer"}}).apply(tbl, context=PipelineContext())  # type: ignore[arg-type]
    assert getattr(ex.value, "code", None) == "E_CAST_KEY"

    tbl2 = _tbl([("a",), (1,)])
    with pytest.raises(WowDataUserError) as ex:
        Transform("cast", params={"types": {"a": "uuid"}}).apply(tbl2, context=PipelineContext())
    assert getattr(ex.value, "code", None) == "E_CAST_TYPE_UNSUPPORTED"


def test_cast_on_error_keep_returns_original():
    """cast with on_error='keep' preserves original value on coercion failures."""
    tbl = _tbl([("a",), ("oops",)])
    t = Transform("cast", params={"types": {"a": "integer"}, "on_error": "keep"})
    out = list(t.apply(tbl, context=PipelineContext()))
    assert out[1][0] == "oops"


# ---------- select ----------
def test_select_requires_columns():
    """select validation rejects missing params.columns."""
    with pytest.raises(WowDataUserError) as ex:
        Transform("select", params={}).apply(_tbl([("a", 1)]), context=PipelineContext())
    assert getattr(ex.value, "code", None) == "E_SELECT_PARAMS"


def test_select_happy_path():
    """select cuts table to requested columns."""
    tbl = _tbl([("a", "b"), (1, 2)])
    t = Transform("select", params={"columns": ["b"]})
    out = list(t.apply(tbl, context=PipelineContext()))
    assert out == [("b",), (2,)]


# ---------- derive ----------
def test_derive_requires_expr_and_new():
    """derive validation rejects missing new/expr."""
    with pytest.raises(WowDataUserError):
        Transform("derive", params={"new": "", "expr": "1"}).apply(_tbl([("a", 1)]), context=PipelineContext())
    with pytest.raises(WowDataUserError):
        Transform("derive", params={"new": "x", "expr": ""}).apply(_tbl([("a", 1)]), context=PipelineContext())


def test_derive_overwrites_when_allowed():
    """derive can overwrite existing column when overwrite=True."""
    tbl = _tbl([("a",), (1,), (2,)])
    t = Transform("derive", params={"new": "a", "expr": "2 * 2", "overwrite": True})
    out = list(t.apply(tbl, context=PipelineContext()))
    assert out[1][0] == 4.0


def test_derive_existing_column_without_overwrite():
    """derive raises when overwriting existing column without overwrite flag."""
    tbl = _tbl([("a",), (1,)])
    t = Transform("derive", params={"new": "a", "expr": "1"})
    with pytest.raises(WowDataUserError) as ex:
        t.apply(tbl, context=PipelineContext())
    assert getattr(ex.value, "code", None) == "E_DERIVE_EXISTS"


def test_derive_parse_error_and_unknown_column():
    """derive wraps expression parse errors and unknown columns."""
    tbl = _tbl([("a",), (1,)])
    with pytest.raises(WowDataUserError) as ex:
        Transform("derive", params={"new": "b", "expr": "a ==", "overwrite": True}).apply(
            tbl, context=PipelineContext()
        )
    assert getattr(ex.value, "code", None) == "E_DERIVE_PARSE"

    with pytest.raises(WowDataUserError) as ex:
        list(Transform("derive", params={"new": "b", "expr": "missing + 1"}).apply(tbl, context=PipelineContext()))
    assert getattr(ex.value, "code", None) == "E_DERIVE_UNKNOWN_COL"


# ---------- filter ----------
def test_filter_requires_where():
    """filter validation rejects missing params.where."""
    with pytest.raises(WowDataUserError) as ex:
        Transform("filter", params={}).apply(_tbl([("a", 1)]), context=PipelineContext())
    assert getattr(ex.value, "code", None) == "E_FILTER_PARAMS"


def test_filter_strict_type_mismatch():
    """filter raises type error when strict=True and comparisons mismatch."""
    tbl = _tbl([("a",), ("x",), ("y",)])
    t = Transform("filter", params={"where": "a > 1", "strict": True})
    with pytest.raises(WowDataUserError) as ex:
        list(t.apply(tbl, context=PipelineContext()))
    assert getattr(ex.value, "code", None) == "E_FILTER_TYPE"


def test_filter_parse_error_and_unknown_column():
    """filter wraps parse errors and rejects unknown columns."""
    tbl = _tbl([("a",), (1,)])
    with pytest.raises(WowDataUserError) as ex:
        Transform("filter", params={"where": "a ==", "strict": True}).apply(tbl, context=PipelineContext())
    assert getattr(ex.value, "code", None) == "E_FILTER_PARSE"

    with pytest.raises(WowDataUserError) as ex:
        list(Transform("filter", params={"where": "missing > 1"}).apply(tbl, context=PipelineContext()))
    assert getattr(ex.value, "code", None) == "E_FILTER_UNKNOWN_COL"


def test_filter_relaxed_type_mismatch():
    """filter skips non-comparable rows when strict=False."""
    tbl = _tbl([("a",), ("x",), ("2",)])
    t = Transform("filter", params={"where": "a > 1", "strict": False})
    out = list(t.apply(tbl, context=PipelineContext()))
    # Only the numeric-looking row passes
    assert out == [("a",), ("2",)]


# ---------- drop ----------
def test_drop_requires_columns():
    """drop validation rejects missing params.columns."""
    with pytest.raises(WowDataUserError) as ex:
        Transform("drop", params={}).apply(_tbl([("a", 1)]), context=PipelineContext())
    assert getattr(ex.value, "code", None) == "E_DROP_PARAMS"


def test_drop_happy_path():
    """drop removes specified columns."""
    tbl = _tbl([("a", "b"), (1, 2)])
    t = Transform("drop", params={"columns": ["b"]})
    out = list(t.apply(tbl, context=PipelineContext()))
    assert out == [("a",), (1,)]


def test_drop_unknown_column_with_schema():
    """drop validation catches dropping non-existent columns via schema."""
    ctx = PipelineContext(schema={"fields": [{"name": "a"}]})
    with pytest.raises(WowDataUserError) as ex:
        Transform("drop", params={"columns": ["b"]}).apply(_tbl([("a",), (1,)]), context=ctx)
    assert getattr(ex.value, "code", None) == "E_DROP_UNKNOWN_COL"


# ---------- validate ----------
def test_validate_rejects_bad_params():
    """validate rejects invalid sample_rows."""
    with pytest.raises(WowDataUserError):
        Transform("validate", params={"sample_rows": -1}).apply(_tbl([("a", 1)]), context=PipelineContext())


def test_validate_import_error(monkeypatch):
    """validate raises E_VALIDATE_IMPORT when frictionless is missing."""
    tbl = _tbl([("a",), (1,)])
    t = Transform("validate")

    monkeypatch.setattr("wowdata.models.transforms.Resource", None)
    with pytest.raises(WowDataUserError) as ex:
        list(t.apply(tbl, context=PipelineContext()))
    assert getattr(ex.value, "code", None) == "E_VALIDATE_IMPORT"


def test_validate_no_schema(monkeypatch):
    """validate raises when strict_schema and no schema is available."""
    class DummyResource:
        def __init__(self, data=None, schema=None):
            pass

    monkeypatch.setattr("wowdata.models.transforms.Resource", DummyResource)
    ctx = PipelineContext(schema={})
    tbl = _tbl([("a",), (1,)])
    t = Transform("validate", params={"strict_schema": True})

    with pytest.raises(WowDataUserError) as ex:
        t.apply(tbl, context=ctx)
    assert getattr(ex.value, "code", None) == "E_VALIDATE_NO_SCHEMA"


def test_validate_read_error(monkeypatch):
    """validate wraps sample read errors as E_VALIDATE_READ."""
    class DummyResource:
        def __init__(self, data=None, schema=None):
            pass

    monkeypatch.setattr("wowdata.models.transforms.Resource", DummyResource)
    monkeypatch.setattr("wowdata.models.transforms.etl.head", lambda table, n: (_ for _ in ()).throw(RuntimeError("bad")))

    ctx = PipelineContext(schema={"fields": [{"name": "a"}]})
    tbl = _tbl([("a",), (1,)])
    t = Transform("validate", params={"strict_schema": False})

    with pytest.raises(WowDataUserError) as ex:
        t.apply(tbl, context=ctx)
    assert getattr(ex.value, "code", None) == "E_VALIDATE_READ"


def test_validate_invalid_failure(monkeypatch):
    """validate raises E_VALIDATE_INVALID when report is not valid and fail=True."""
    class DummyReport:
        valid = False

        def to_descriptor(self):
            return {"tasks": [{"errors": [{"note": "bad", "rowNumber": 2, "fieldName": "a"}]}]}

    class DummyResource:
        def __init__(self, data=None, schema=None):
            pass

        def validate(self, cast=True):
            return DummyReport()

    monkeypatch.setattr("wowdata.models.transforms.Resource", DummyResource)
    ctx = PipelineContext(schema={"fields": [{"name": "a", "type": "string"}]})
    tbl = _tbl([("a",), ("x",)])
    t = Transform("validate", params={"strict_schema": False})

    with pytest.raises(WowDataUserError) as ex:
        t.apply(tbl, context=ctx)
    assert getattr(ex.value, "code", None) == "E_VALIDATE_INVALID"


# ---------- join ----------
def test_join_requires_right(monkeypatch):
    """join validation rejects missing params.right."""
    from wowdata.models import transforms as mt

    tbl = _tbl([("id",), (1,)])
    ctx = PipelineContext(schema={"fields": [{"name": "id"}]})
    t = Transform("join", params={"on": ["id"]})
    with pytest.raises(WowDataUserError) as ex:
        t.apply(tbl, context=ctx)
    assert getattr(ex.value, "code", None) in {"E_JOIN_RIGHT", "E_JOIN_PARAMS"}


def test_join_happy_path(tmp_path):
    """join performs a simple inner join on id."""
    left = tmp_path / "left.csv"
    right = tmp_path / "right.csv"
    left.write_text("id,name\n1,A\n2,B\n", encoding="utf-8")
    right.write_text("id,age\n1,30\n2,40\n", encoding="utf-8")

    t = Transform("join", params={"right": str(right), "on": ["id"]})
    ctx = PipelineContext(schema={"fields": [{"name": "id"}]})
    out = list(t.apply(_tbl(etl.fromcsv(left)), context=ctx))
    assert out[1] == ("1", "A", "30")


def test_join_param_combinations_and_errors(monkeypatch):
    """join validate_params catches invalid combinations and key lengths."""
    tbl = _tbl([("id",), (1,)])
    ctx = PipelineContext(schema={"fields": [{"name": "id"}]})

    with pytest.raises(WowDataUserError):
        Transform("join", params={"right": "r.csv", "on": [], "left_on": ["id"]}).apply(tbl, context=ctx)
    with pytest.raises(WowDataUserError):
        Transform("join", params={"right": "r.csv", "left_on": ["id"], "right_on": []}).apply(tbl, context=ctx)
    with pytest.raises(WowDataUserError):
        Transform("join", params={"right": "r.csv", "left_on": ["id"], "right_on": ["id", "b"]}).apply(
            tbl, context=ctx
        )
    with pytest.raises(WowDataUserError):
        Transform("join", params={"right": "r.csv", "left_on": ["id"], "right_on": ["id"], "how": "weird"}).apply(
            tbl, context=ctx
        )


def test_join_read_errors(monkeypatch):
    """join wraps right read/header/key errors."""
    class DummySrc:
        def __init__(self, tbl):
            self.tbl = tbl

        def table(self):
            return self.tbl

    left_tbl = _tbl([("id",), (1,)])
    right_tbl = _tbl([("other",), (2,)])  # missing key

    # right table read failure
    monkeypatch.setattr("wowdata.models.transforms._source_from_descriptor", lambda desc: DummySrc(_ for _ in ()).table())
    with pytest.raises(WowDataUserError) as ex:
        Transform("join", params={"right": "bad.csv", "on": ["id"]}).apply(left_tbl, context=PipelineContext())
    assert getattr(ex.value, "code", None) in {"E_JOIN_RIGHT_READ", "E_JOIN_PARAMS"}

    # header read failure
    monkeypatch.setattr("wowdata.models.transforms._source_from_descriptor", lambda desc: DummySrc(right_tbl))
    monkeypatch.setattr("wowdata.models.transforms.etl.header", lambda table: (_ for _ in ()).throw(RuntimeError("hdr")))
    with pytest.raises(WowDataUserError) as ex:
        Transform("join", params={"right": "bad.csv", "on": ["id"]}).apply(left_tbl, context=PipelineContext())
    assert getattr(ex.value, "code", None) == "E_JOIN_READ_HEADERS"

    # unknown column
    monkeypatch.setattr("wowdata.models.transforms.etl.header", lambda table: ["other"])
    with pytest.raises(WowDataUserError) as ex:
        Transform("join", params={"right": "bad.csv", "on": ["id"]}).apply(left_tbl, context=PipelineContext())
    assert getattr(ex.value, "code", None) == "E_JOIN_UNKNOWN_COL"


# ---------- registry ----------
def test_transform_registry_contains_expected_ops():
    """Registry contains all shipped transform ops."""
    assert {"cast", "select", "derive", "filter", "drop", "validate", "join"}.issubset(set(TRANSFORM_REGISTRY.keys()))
