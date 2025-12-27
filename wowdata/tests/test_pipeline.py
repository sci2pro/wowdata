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
