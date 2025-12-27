import pytest

from wowdata import Sink, WowDataUserError


def test_sink_infers_csv_type(tmp_path):
    """CSV extension is inferred as sink type."""
    p = tmp_path / "out.csv"
    s = Sink(str(p))

    assert s.type == "csv"


def test_sink_infer_type_failure():
    """Missing/unknown extension raises E_SINK_TYPE_INFER."""
    with pytest.raises(WowDataUserError) as ex:
        Sink("out")  # no extension
    assert getattr(ex.value, "code", None) == "E_SINK_TYPE_INFER"


def test_sink_rejects_unsupported_type(tmp_path):
    """Explicit unsupported type raises E_SINK_TYPE_UNSUPPORTED."""
    p = tmp_path / "out.csv"
    with pytest.raises(WowDataUserError) as ex:
        Sink(str(p), type="json")
    assert getattr(ex.value, "code", None) == "E_SINK_TYPE_UNSUPPORTED"


def test_sink_requires_existing_directory(tmp_path):
    """Nonexistent output directory raises E_SINK_DIR_NOT_FOUND."""
    p = tmp_path / "missing" / "out.csv"
    with pytest.raises(WowDataUserError) as ex:
        Sink(str(p))
    assert getattr(ex.value, "code", None) == "E_SINK_DIR_NOT_FOUND"


def test_sink_requires_writable_directory(monkeypatch, tmp_path):
    """Unwritable directory raises E_SINK_NOT_WRITABLE."""
    d = tmp_path / "dir"
    d.mkdir()
    monkeypatch.setattr("wowdata.models.sinks.os.access", lambda path, mode: False)

    with pytest.raises(WowDataUserError) as ex:
        Sink(str(d / "out.csv"))
    assert getattr(ex.value, "code", None) == "E_SINK_NOT_WRITABLE"


def test_sink_write_success(monkeypatch, tmp_path):
    """write() delegates to petl.tocsv with provided options."""
    p = tmp_path / "out.csv"
    s = Sink(str(p), options={"delimiter": ";"})

    calls = []

    def fake_tocsv(table, uri, **opts):
        calls.append((table, uri, opts))

    monkeypatch.setattr("wowdata.models.sinks.etl.tocsv", fake_tocsv)

    s.write([("a", 1)])
    assert calls and calls[0][1] == str(p)
    assert calls[0][2]["delimiter"] == ";"


def test_sink_write_wraps_filenotfound(monkeypatch, tmp_path):
    """FileNotFoundError is wrapped as E_SINK_DIR_NOT_FOUND."""
    s = Sink.__new__(Sink)
    object.__setattr__(s, "uri", str(tmp_path / "missing" / "out.csv"))
    object.__setattr__(s, "type", "csv")
    object.__setattr__(s, "options", {})

    def raise_fnf(*args, **kwargs):
        raise FileNotFoundError("missing dir")

    monkeypatch.setattr("wowdata.models.sinks.etl.tocsv", raise_fnf)

    with pytest.raises(WowDataUserError) as ex:
        s.write([("a",)])
    assert getattr(ex.value, "code", None) == "E_SINK_DIR_NOT_FOUND"


def test_sink_write_wraps_permissionerror(monkeypatch, tmp_path):
    """PermissionError is wrapped as E_SINK_NOT_WRITABLE."""
    s = Sink.__new__(Sink)
    object.__setattr__(s, "uri", str(tmp_path / "out.csv"))
    object.__setattr__(s, "type", "csv")
    object.__setattr__(s, "options", {})

    def raise_perm(*args, **kwargs):
        raise PermissionError("nope")

    monkeypatch.setattr("wowdata.models.sinks.etl.tocsv", raise_perm)

    with pytest.raises(WowDataUserError) as ex:
        s.write([("a",)])
    assert getattr(ex.value, "code", None) == "E_SINK_NOT_WRITABLE"


def test_sink_write_wraps_other_errors(monkeypatch, tmp_path):
    """Other errors are wrapped as E_SINK_WRITE."""
    s = Sink.__new__(Sink)
    object.__setattr__(s, "uri", str(tmp_path / "out.csv"))
    object.__setattr__(s, "type", "csv")
    object.__setattr__(s, "options", {})

    def raise_other(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("wowdata.models.sinks.etl.tocsv", raise_other)

    with pytest.raises(WowDataUserError) as ex:
        s.write([("a",)])
    assert getattr(ex.value, "code", None) == "E_SINK_WRITE"


def test_sink_write_unsupported_type():
    """write() rejects unsupported sink types."""
    s = Sink.__new__(Sink)
    object.__setattr__(s, "uri", "out.json")
    object.__setattr__(s, "type", "json")
    object.__setattr__(s, "options", {})

    with pytest.raises(WowDataUserError) as ex:
        s.write([("a",)])
    assert getattr(ex.value, "code", None) == "E_SINK_WRITE_UNSUPPORTED"


def test_sink_str_formats_kind():
    """__str__ includes uri and kind."""
    s = Sink.__new__(Sink)
    object.__setattr__(s, "uri", "out.csv")
    object.__setattr__(s, "type", "csv")
    object.__setattr__(s, "options", {})

    msg = str(s)
    assert 'Sink("out.csv")' in msg
    assert "kind=csv" in msg
