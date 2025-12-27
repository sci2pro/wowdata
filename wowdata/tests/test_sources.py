import pytest

from wowdata import WowDataUserError, Source


def _write_csv(path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_source_missing_file_fails_fast(tmp_path):
    missing = tmp_path / "missing.csv"
    with pytest.raises(WowDataUserError) as ex:
        Source(str(missing))
    err = ex.value
    assert getattr(err, "code", None) == "E_SOURCE_NOT_FOUND"
    # The message should help users locate the problem quickly
    assert "not found" in str(err).lower() or "missing" in str(err).lower()


def test_source_existing_csv_can_read_header(tmp_path):
    p = tmp_path / "people.csv"
    _write_csv(
        p,
        "person_id,age,country\n"
        "1,30,KE\n"
        "2,41,UG\n",
    )

    s = Source(str(p))
    tbl = s.table()

    # PETL tables are lazy; forcing header read ensures the file can be opened.
    import petl as etl

    assert list(etl.header(tbl)) == ["person_id", "age", "country"]


def test_source_table_wraps_file_not_found_defensively(tmp_path, monkeypatch):
    """
    Defensive test: even if a file-not-found slips past construction-time checks,
    Source.table() should raise WowDataUserError rather than leaking FileNotFoundError.
    """
    missing = tmp_path / "missing.csv"
    s = Source.__new__(Source)  # bypass __init__/__post_init__
    # Minimal attributes required by Source.table() for csv
    object.__setattr__(s, "uri", missing)
    object.__setattr__(s, "type", "csv")
    object.__setattr__(s, "options", {})

    with pytest.raises(WowDataUserError) as ex:
        _ = s.table()  # should wrap open failure
    assert getattr(ex.value, "code", None) == "E_SOURCE_NOT_FOUND"


def test_source_accepts_pathlib_paths_and_normalizes(tmp_path):
    p = tmp_path / "data.csv"
    _write_csv(p, "a\n1\n")

    s = Source(p)  # pass a pathlib.Path

    assert isinstance(s.uri, str)
    assert s.uri == str(p)
    assert s.type == "csv"


def test_source_rejects_non_string_uri_type():
    with pytest.raises(WowDataUserError) as ex:
        Source(123)  # type: ignore[arg-type]
    assert getattr(ex.value, "code", None) == "E_SOURCE_URI_TYPE"


def test_source_infer_type_failure_raises(tmp_path):
    p = tmp_path / "data"  # no extension => cannot infer type
    p.write_text("a\n1\n", encoding="utf-8")

    with pytest.raises(WowDataUserError) as ex:
        Source(str(p))
    assert getattr(ex.value, "code", None) == "E_SOURCE_TYPE_INFER"


def test_source_rejects_unsupported_explicit_type(tmp_path):
    p = tmp_path / "data.csv"
    _write_csv(p, "a\n1\n")

    with pytest.raises(WowDataUserError) as ex:
        Source(str(p), type="json")
    assert getattr(ex.value, "code", None) == "E_SOURCE_TYPE_UNSUPPORTED"


def test_source_normalizes_inline_schema(tmp_path):
    p = tmp_path / "data.csv"
    _write_csv(p, "a\n1\n")
    inline_schema = {"fields": [{"name": "a", "type": "integer"}]}

    s = Source(str(p), schema=inline_schema)

    # Ensure inline schema normalization ran without altering the original mapping reference
    assert s.schema is inline_schema
