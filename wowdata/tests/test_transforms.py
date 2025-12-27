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


# ---------- registry ----------
def test_transform_registry_contains_expected_ops():
    """Registry contains all shipped transform ops."""
    assert {"cast", "select", "derive", "filter", "drop", "validate", "join"}.issubset(set(TRANSFORM_REGISTRY.keys()))
