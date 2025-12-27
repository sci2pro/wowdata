import pytest

from wowdata import WowDataUserError, Source


def _write_csv(path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_source_missing_file_fails_fast(tmp_path):
    """Construction fails fast with E_SOURCE_NOT_FOUND when the CSV is absent."""
    missing = tmp_path / "missing.csv"
    with pytest.raises(WowDataUserError) as ex:
        Source(str(missing))
    err = ex.value
    assert getattr(err, "code", None) == "E_SOURCE_NOT_FOUND"
    # The message should help users locate the problem quickly
    assert "not found" in str(err).lower() or "missing" in str(err).lower()


def test_source_existing_csv_can_read_header(tmp_path):
    """Source.table reads a simple CSV header successfully."""
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
    """Pathlib.Path URIs are coerced to strings and type inferred."""
    p = tmp_path / "data.csv"
    _write_csv(p, "a\n1\n")

    s = Source(p)  # pass a pathlib.Path

    assert isinstance(s.uri, str)
    assert s.uri == str(p)
    assert s.type == "csv"


def test_source_rejects_non_string_uri_type():
    """Non-string/path-like URIs raise E_SOURCE_URI_TYPE."""
    with pytest.raises(WowDataUserError) as ex:
        Source(123)  # type: ignore[arg-type]
    assert getattr(ex.value, "code", None) == "E_SOURCE_URI_TYPE"


def test_source_infer_type_failure_raises(tmp_path):
    """URIs without an inferable type raise E_SOURCE_TYPE_INFER."""
    p = tmp_path / "data"  # no extension => cannot infer type
    p.write_text("a\n1\n", encoding="utf-8")

    with pytest.raises(WowDataUserError) as ex:
        Source(str(p))
    assert getattr(ex.value, "code", None) == "E_SOURCE_TYPE_INFER"


def test_source_rejects_unsupported_explicit_type(tmp_path):
    """Explicit unsupported types raise E_SOURCE_TYPE_UNSUPPORTED."""
    p = tmp_path / "data.csv"
    _write_csv(p, "a\n1\n")

    with pytest.raises(WowDataUserError) as ex:
        Source(str(p), type="json")
    assert getattr(ex.value, "code", None) == "E_SOURCE_TYPE_UNSUPPORTED"


def test_source_normalizes_inline_schema(tmp_path):
    """Inline schema passes through normalization without mutating the mapping."""
    p = tmp_path / "data.csv"
    _write_csv(p, "a\n1\n")
    inline_schema = {"fields": [{"name": "a", "type": "integer"}]}

    s = Source(str(p), schema=inline_schema)

    # Ensure inline schema normalization ran without altering the original mapping reference
    assert s.schema is inline_schema


def test_table_wraps_filenotfound_from_petl(monkeypatch, tmp_path):
    """FileNotFoundError from petl is wrapped as E_SOURCE_NOT_FOUND."""
    p = tmp_path / "data.csv"
    _write_csv(p, "a\n1\n")
    s = Source(str(p))

    def raise_filenotfound(*args, **kwargs):
        raise FileNotFoundError("gone")

    monkeypatch.setattr("wowdata.models.sources.etl.fromcsv", raise_filenotfound)

    with pytest.raises(WowDataUserError) as ex:
        s.table()
    assert getattr(ex.value, "code", None) == "E_SOURCE_NOT_FOUND"


def test_table_wraps_other_errors(monkeypatch, tmp_path):
    """Non-ENOENT errors from petl are wrapped as E_SOURCE_READ."""
    p = tmp_path / "data.csv"
    _write_csv(p, "a\n1\n")
    s = Source(str(p))

    def raise_other(*args, **kwargs):
        raise PermissionError("nope")

    monkeypatch.setattr("wowdata.models.sources.etl.fromcsv", raise_other)

    with pytest.raises(WowDataUserError) as ex:
        s.table()
    assert getattr(ex.value, "code", None) == "E_SOURCE_READ"
    assert "PermissionError" in str(ex.value)


def test_head_returns_preview_rows(tmp_path):
    """head() returns a bounded table with header and data rows."""
    p = tmp_path / "data.csv"
    _write_csv(p, "a\n1\n2\n3\n")
    s = Source(str(p))

    tbl = s.head()  # default preview_rows=5
    rows = list(tbl)
    assert len(rows) == 4  # header + 3 data rows; bounded by file size here
    assert rows[0] == ("a",)


def test_preview_str_truncates(tmp_path, monkeypatch):
    """_preview_str enforces preview_max_chars and marks truncation."""
    p = tmp_path / "data.csv"
    # Build a long CSV to exceed preview_max_chars
    rows = ["col\n"] + [f"{i}\n" for i in range(1000)]
    _write_csv(p, "".join(rows))
    s = Source(str(p))

    # Force a small preview_max_chars to trigger truncation deterministically
    object.__setattr__(s, "preview_max_chars", 40)

    preview = s._preview_str()
    assert "truncated" in preview
    assert len(preview) <= 60  # small slack for table borders + suffix


def test_peek_schema_uses_cached_result(tmp_path):
    """peek_schema returns cached results and leaves warnings empty for inline schema."""
    p = tmp_path / "data.csv"
    _write_csv(p, "a\n1\n")
    inline_schema = {"fields": [{"name": "a", "type": "integer"}]}
    s = Source(str(p), schema=inline_schema)

    sch1 = s.peek_schema()
    sch2 = s.peek_schema()

    assert sch1 is sch2  # cached path
    assert s.schema_warnings() == []


def test_peek_schema_without_frictionless(monkeypatch, tmp_path):
    """When frictionless is unavailable, peek_schema returns empty fields and emits a warning."""
    p = tmp_path / "data.csv"
    _write_csv(p, "a\n1\n")
    s = Source(str(p))

    monkeypatch.setattr("wowdata.models.sources.Resource", None)
    monkeypatch.setattr("wowdata.models.sources.Detector", None)

    sch = s.peek_schema()
    assert sch == {"fields": []}
    warns = s.schema_warnings()
    assert warns and "Frictionless not available" in warns[0]


def test_peek_schema_warns_on_numeric_strings(monkeypatch, tmp_path):
    """Numeric-looking strings trigger a heuristic warning about explicit casts."""
    p = tmp_path / "data.csv"
    _write_csv(p, "age\n1\n2\n3\n4\n5\n")
    s = Source(str(p))

    class StubDetector:
        def __init__(self, sample_size: int):
            self.sample_size = sample_size

    class StubResource:
        def __init__(self, path, detector):
            self.path = path
            self.detector = detector

        def infer(self):
            return None

        def to_descriptor(self):
            return {"schema": {"fields": [{"name": "age", "type": "string"}]}}

    monkeypatch.setattr("wowdata.models.sources.Detector", StubDetector)
    monkeypatch.setattr("wowdata.models.sources.Resource", StubResource)
    monkeypatch.setattr(
        "wowdata.models.sources.etl.head",
        lambda table, n: [("age",), ("1",), ("2",), ("3",), ("4",), ("5",)],
    )
    monkeypatch.setattr("wowdata.models.sources.etl.data", lambda t: t)

    sch = s.peek_schema(sample_rows=10)
    assert sch.get("fields")
    warns = s.schema_warnings()
    assert any("look numeric" in w for w in warns)
