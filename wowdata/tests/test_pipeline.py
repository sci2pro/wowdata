import json

import petl as etl
import pytest

import wowdata.models.pipeline as mp
from wowdata import Pipeline, Sink, Source, Transform, WowDataUserError
from wowdata.models.transforms import register_transform, TransformImpl


@register_transform("test_addcol")
class _AddCol(TransformImpl):
    @classmethod
    def apply(cls, table, *, params, context):
        return etl.addfield(table, "b", lambda row: params.get("value", 0))

    @classmethod
    def output_schema(cls, input_schema, params):
        fields = input_schema.get("fields", []) if isinstance(input_schema, dict) else []
        return {"fields": [*fields, {"name": "b", "type": "any"}]}


def _make_source(tmp_path, name="in.csv"):
    p = tmp_path / name
    p.write_text("a\n1\n2\n", encoding="utf-8")
    return Source(str(p))


def _make_sink(tmp_path, name="out.csv"):
    return Sink(str(tmp_path / name))


def test_pipeline_then_validates_step_type(tmp_path):
    src = _make_source(tmp_path)
    pipe = Pipeline(src)

    with pytest.raises(WowDataUserError) as ex:
        pipe.then("not-a-step")  # type: ignore[arg-type]
    assert getattr(ex.value, "code", None) == "E_PIPELINE_STEP"


def test_pipeline_then_rejects_transform_after_sink(tmp_path):
    src = _make_source(tmp_path)
    sink = _make_sink(tmp_path)
    pipe = Pipeline(src).then(sink)

    with pytest.raises(WowDataUserError) as ex:
        pipe.then(Transform("test_addcol", params={"value": 1}))
    assert getattr(ex.value, "code", None) == "E_PIPELINE_ORDER"


def test_pipeline_preflight_rejects_unknown_step_type(tmp_path):
    src = _make_source(tmp_path)
    pipe = Pipeline(src, steps=[object()])

    with pytest.raises(WowDataUserError) as ex:
        pipe.preflight()
    assert getattr(ex.value, "code", None) == "E_PIPELINE_STEP_TYPE"


def test_pipeline_run_records_checkpoints_and_schema(tmp_path, monkeypatch):
    src = _make_source(tmp_path)
    sink = _make_sink(tmp_path)
    sink_calls = []
    monkeypatch.setattr(
        Sink,
        "write",
        lambda self, table: sink_calls.append(list(etl.data(etl.head(table, 10)))),
    )

    pipe = Pipeline(src).then(Transform("test_addcol", params={"value": 7})).then(sink)
    ctx = pipe.run()

    assert ctx.schema and any(f.get("name") == "b" for f in ctx.schema.get("fields", []))
    assert len(ctx.checkpoints) == 2
    kinds = [c[1]["kind"] for c in ctx.checkpoints]
    assert kinds == ["transform", "sink"]
    assert sink_calls and sink_calls[0][0] == ("1", 7)


def test_pipeline_schema_applies_transform_output_schema(monkeypatch):
    class FakeSource:
        uri = "fake.csv"

        def peek_schema(self, *_, **__):
            return {"fields": [{"name": "a", "type": "string"}]}

    src = FakeSource()
    pipe = Pipeline(src).then(Transform("test_addcol", params={"value": 1}))

    sch = pipe.schema()
    assert sch and any(f.get("name") == "b" for f in sch.get("fields", []))


def test_pipeline_to_ir_and_from_ir(tmp_path):
    src = _make_source(tmp_path)
    sink = _make_sink(tmp_path)
    pipe = Pipeline(src).then(Transform("test_addcol", params={"value": 2})).then(sink)

    ir = pipe.to_ir()
    assert ir["pipeline"]["start"]["uri"].endswith("in.csv")
    assert ir["pipeline"]["steps"][0]["transform"]["op"] == "test_addcol"

    pipe2 = Pipeline.from_ir(ir)
    assert isinstance(pipe2.start, Source)
    assert isinstance(pipe2.steps[0], Transform)
    assert isinstance(pipe2.steps[1], Sink)


def test_pipeline_yaml_roundtrip(tmp_path, monkeypatch):
    if mp.yaml is None:
        monkeypatch.setattr(
            mp,
            "yaml",
            type(
                "Y",
                (),
                {
                    "safe_dump": lambda obj, sort_keys=False: json.dumps(obj),
                    "safe_load": lambda text: json.loads(text),
                },
            ),
        )

    src = _make_source(tmp_path)
    sink = _make_sink(tmp_path)
    pipe = Pipeline(src).then(Transform("test_addcol", params={"value": 3})).then(sink)

    text = pipe.to_yaml()
    pipe2 = Pipeline.from_yaml(text)
    assert isinstance(pipe2.start, Source)
    assert isinstance(pipe2.steps[-1], Sink)


def test_to_ir_coerces_pathlike_uris(tmp_path):
    from pathlib import Path

    src_path = Path(tmp_path / "in.csv")
    src_path.write_text("a\n1\n", encoding="utf-8")
    sink_path = Path(tmp_path / "out.csv")
    pipe = Pipeline(Source(src_path)).then(Sink(str(sink_path)))

    ir = pipe.to_ir()
    assert isinstance(ir["pipeline"]["start"]["uri"], str)
    assert isinstance(ir["pipeline"]["steps"][0]["sink"]["uri"], str)


def test_pipeline_lock_schema_sets_overrides(monkeypatch):
    class FakeSource:
        uri = "fake.csv"

        def peek_schema(self, *_, **__):
            return {"fields": [{"name": "a", "type": "string"}]}

    src = FakeSource()
    pipe = Pipeline(src).then(Transform("test_addcol", params={"value": 1}))

    locked = pipe.lock_schema()
    assert isinstance(locked.steps[0], Transform)
    assert locked.steps[0].output_schema_override is not None


def test_pipeline_str_includes_steps(tmp_path):
    src = _make_source(tmp_path)
    sink = _make_sink(tmp_path)
    pipe = Pipeline(src).then(Transform("test_addcol")).then(sink)

    msg = str(pipe)
    assert "Pipeline(start=" in msg
    assert "Transform(" in msg
    assert "Sink(" in msg


def test_pipeline_context_defaults():
    ctx = mp.PipelineContext()
    assert ctx.checkpoints == []
    assert ctx.schema is None
    assert ctx.validations == []


def test_preflight_rejects_transform_after_sink(tmp_path):
    src = _make_source(tmp_path)
    sink = _make_sink(tmp_path)
    pipe = Pipeline(src, steps=[sink, Transform("test_addcol")])

    with pytest.raises(WowDataUserError) as ex:
        pipe.preflight()
    assert getattr(ex.value, "code", None) == "E_PIPELINE_ORDER"


def test_run_rejects_transform_after_sink(tmp_path, monkeypatch):
    src = _make_source(tmp_path)
    sink = Sink.__new__(Sink)
    object.__setattr__(sink, "uri", str(tmp_path / "out.csv"))
    object.__setattr__(sink, "type", "csv")
    object.__setattr__(sink, "options", {})
    object.__setattr__(sink, "write", lambda table: None)

    # bypass preflight to hit runtime guard
    monkeypatch.setattr(Pipeline, "preflight", lambda self: None)

    pipe = Pipeline(src, steps=[sink, Transform("test_addcol")])
    with pytest.raises(WowDataUserError) as ex:
        pipe.run()
    assert getattr(ex.value, "code", None) == "E_PIPELINE_ORDER"


def test_run_checkpoint_on_header_failure(tmp_path, monkeypatch):
    src = _make_source(tmp_path)
    pipe = Pipeline(src).then(Transform("test_addcol", params={"value": 1}))

    monkeypatch.setattr("wowdata.models.pipeline.etl.header", lambda table: (_ for _ in ()).throw(RuntimeError()))
    monkeypatch.setattr("wowdata.models.pipeline.etl.head", lambda table, n: [("a",), ("1",)])
    monkeypatch.setattr("wowdata.models.pipeline.etl.data", lambda t: t)

    ctx = pipe.run()
    # header should be [] due to failure; preview present
    assert ctx.checkpoints[0][1]["header"] == []
    assert ctx.checkpoints[0][1]["preview"]


def test_run_checkpoint_on_preview_failure(tmp_path, monkeypatch):
    src = _make_source(tmp_path)
    pipe = Pipeline(src).then(Transform("test_addcol", params={"value": 1}))

    monkeypatch.setattr("wowdata.models.pipeline.etl.header", lambda table: ["a", "b"])
    monkeypatch.setattr("wowdata.models.pipeline.etl.head", lambda table, n: (_ for _ in ()).throw(RuntimeError()))

    ctx = pipe.run()
    assert ctx.checkpoints[0][1]["preview"] == []


def test_run_unknown_step_type(tmp_path, monkeypatch):
    class FakeSource:
        uri = "fake.csv"

        def peek_schema(self):
            return {}

        def table(self):
            return []

    pipe = Pipeline(FakeSource(), steps=[object()])
    monkeypatch.setattr(Pipeline, "preflight", lambda self: None)

    with pytest.raises(WowDataUserError) as ex:
        pipe.run()
    assert getattr(ex.value, "code", None) == "E_PIPELINE_STEP_TYPE"


def test_pipeline_schema_returns_none_on_unknown_step():
    class FakeSource:
        def peek_schema(self, *_, **__):
            return {}

    pipe = Pipeline(FakeSource(), steps=[object()])
    assert pipe.schema() is None


def test_pipeline_schema_with_sink_passes_through(tmp_path):
    src = _make_source(tmp_path)
    sink = _make_sink(tmp_path)
    pipe = Pipeline(src).then(sink)

    sch = pipe.schema()
    assert sch == src.peek_schema()


def test_to_ir_rejects_unknown_step(tmp_path):
    src = _make_source(tmp_path)
    pipe = Pipeline(src, steps=[object()])

    with pytest.raises(WowDataUserError) as ex:
        pipe.to_ir()
    assert getattr(ex.value, "code", None) == "E_IR_STEP"


def test_from_ir_validations(tmp_path):
    src_file = tmp_path / "x.csv"
    src_file.write_text("a\n1\n", encoding="utf-8")
    base_ir = {
        "wowdata": 0,
        "pipeline": {"start": {"uri": str(src_file), "type": "csv"}, "steps": []},
    }

    bad_steps_type = {"wowdata": 0, "pipeline": {"start": {"uri": str(src_file)}, "steps": "oops"}}
    with pytest.raises(WowDataUserError) as ex:
        Pipeline.from_ir(bad_steps_type)
    assert getattr(ex.value, "code", None) == "E_IR_STEPS"

    bad_step_shape = {
        "wowdata": 0,
        "pipeline": {"start": {"uri": str(src_file), "type": "csv"}, "steps": [{}]},
    }
    with pytest.raises(WowDataUserError) as ex:
        Pipeline.from_ir(bad_step_shape)
    assert getattr(ex.value, "code", None) == "E_IR_STEP"

    bad_step_key = {
        "wowdata": 0,
        "pipeline": {"start": {"uri": str(src_file), "type": "csv"}, "steps": [{"unknown": {}}]},
    }
    with pytest.raises(WowDataUserError) as ex:
        Pipeline.from_ir(bad_step_key)
    assert getattr(ex.value, "code", None) == "E_IR_STEP"


def test_from_ir_validations_in_pipeline_module(monkeypatch):
    class DummySource:
        uri = "dummy"

    monkeypatch.setattr(mp, "_normalize_ir", lambda ir, base_dir=None: ir)
    monkeypatch.setattr(mp, "_source_from_ir", lambda d: DummySource())
    monkeypatch.setattr(mp, "_transform_from_ir", lambda d: "T")
    monkeypatch.setattr(mp, "_sink_from_ir", lambda d: "S")

    with pytest.raises(WowDataUserError) as ex:
        Pipeline.from_ir({"pipeline": {"start": {}, "steps": "oops"}, "wowdata": 0})
    assert getattr(ex.value, "code", None) == "E_IR_STEPS"

    with pytest.raises(WowDataUserError) as ex:
        Pipeline.from_ir({"pipeline": {"start": {}, "steps": [{}]}, "wowdata": 0})
    assert getattr(ex.value, "code", None) == "E_IR_STEP"

    with pytest.raises(WowDataUserError) as ex:
        Pipeline.from_ir({"pipeline": {"start": {}, "steps": [{"unknown": {}}]}, "wowdata": 0})
    assert getattr(ex.value, "code", None) == "E_IR_STEP"


def test_to_yaml_errors_when_yaml_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(mp, "yaml", None)
    src = _make_source(tmp_path)
    pipe = Pipeline(src)

    with pytest.raises(WowDataUserError) as ex:
        pipe.to_yaml()
    assert getattr(ex.value, "code", None) == "E_YAML_IMPORT"


def test_from_yaml_parse_errors(monkeypatch):
    class DummyYAML:
        @staticmethod
        def safe_load(text):
            raise ValueError("bad")

    monkeypatch.setattr(mp, "yaml", DummyYAML)

    with pytest.raises(WowDataUserError) as ex:
        Pipeline.from_yaml("not yaml")
    assert getattr(ex.value, "code", None) == "E_YAML_PARSE"


def test_from_yaml_errors_when_yaml_missing(monkeypatch):
    monkeypatch.setattr(mp, "yaml", None)
    with pytest.raises(WowDataUserError) as ex:
        Pipeline.from_yaml("text")
    assert getattr(ex.value, "code", None) == "E_YAML_IMPORT"


def test_save_and_load_yaml(tmp_path, monkeypatch):
    class DummyYAML:
        @staticmethod
        def safe_dump(obj, sort_keys=False):
            return json.dumps(obj)

        @staticmethod
        def safe_load(text):
            return json.loads(text)

    monkeypatch.setattr(mp, "yaml", DummyYAML)

    src = _make_source(tmp_path)
    sink = _make_sink(tmp_path)
    pipe = Pipeline(src).then(sink)

    out = tmp_path / "pipe.yaml"
    pipe.save_yaml(out)
    loaded = Pipeline.load_yaml(out)
    assert isinstance(loaded.start, Source)


def test_to_yaml_writes_path(tmp_path, monkeypatch):
    class DummyYAML:
        @staticmethod
        def safe_dump(obj, sort_keys=False):
            return json.dumps(obj)

    monkeypatch.setattr(mp, "yaml", DummyYAML)

    src = _make_source(tmp_path)
    pipe = Pipeline(src)
    out = tmp_path / "pipe.yaml"

    text = pipe.to_yaml(out)
    assert out.read_text(encoding="utf-8") == text


def test_from_yaml_reads_path(tmp_path, monkeypatch):
    class DummyYAML:
        @staticmethod
        def safe_dump(obj, sort_keys=False):
            return json.dumps(obj)

        @staticmethod
        def safe_load(text):
            return json.loads(text)

    monkeypatch.setattr(mp, "yaml", DummyYAML)

    src = _make_source(tmp_path)
    pipe = Pipeline(src)
    out = tmp_path / "pipe.yaml"
    out.write_text(DummyYAML.safe_dump(pipe.to_ir()), encoding="utf-8")

    loaded = Pipeline.from_yaml(out)
    assert isinstance(loaded.start, Source)


def test_from_yaml_accepts_non_path_objects(tmp_path, monkeypatch):
    class DummyObj:
        pass

    class DummyYAML:
        @staticmethod
        def safe_load(text):
            assert isinstance(text, DummyObj)
            return {
                "wowdata": 0,
                "pipeline": {"start": {"uri": str(tmp_path / "in.csv"), "type": "csv"}, "steps": []},
            }

    (tmp_path / "in.csv").write_text("a\n1\n", encoding="utf-8")
    monkeypatch.setattr(mp, "yaml", DummyYAML)

    pipe = Pipeline.from_yaml(DummyObj())
    assert isinstance(pipe.start, Source)


def test_save_yaml_writes_file(tmp_path, monkeypatch):
    class DummyYAML:
        @staticmethod
        def safe_dump(obj, sort_keys=False):
            return json.dumps(obj)

        @staticmethod
        def safe_load(text):
            return json.loads(text)

    monkeypatch.setattr(mp, "yaml", DummyYAML)

    src = _make_source(tmp_path)
    pipe = Pipeline(src)
    path = tmp_path / "out.yaml"

    pipe.save_yaml(path)
    assert path.exists()
    loaded = Pipeline.load_yaml(path)
    assert isinstance(loaded.start, Source)


def test_lock_schema_preserves_sinks(tmp_path):
    src = _make_source(tmp_path)
    sink = _make_sink(tmp_path)
    pipe = Pipeline(src).then(Transform("test_addcol")).then(sink)

    locked = pipe.lock_schema()
    assert isinstance(locked.steps[-1], Sink)
