from wowdata import Transform, Source, Sink, Pipeline

from wowdata.tests import DATA_DIR

s = Source(DATA_DIR / "people.csv")
print(s)  # bounded preview
print("Inferred schema:", s.peek_schema(sample_rows=200))
if getattr(s, "_schema_warnings", None):
    print("Schema warnings:", s._schema_warnings)

# demonstrate errors
# pipe = s > Transform("select", params={"columns": ["person_id", "age", "country"]})
# pipe = pipe.then(
#     Transform("cast", params={"types": {"age": "integer", "income": "number"}, "on_error": "null"})
# )
pipe = Pipeline(s).then(Transform("cast", params={"types": {"age": "integer", "income": "number"}, "on_error": "null"}))
pipe = pipe.then(
    Transform("select", params={"columns": ["person_id", "age", "country"]})
)
pipe = pipe.then(Sink(DATA_DIR / "out.csv"))
ctx = pipe.run()
print(f"checkpoints: {ctx.checkpoints}")
