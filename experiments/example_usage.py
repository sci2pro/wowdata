from core import Source, Transform, Sink

s = Source("people.csv")
print(s)  # bounded preview
print("Inferred schema:", s.peek_schema(sample_rows=200))
if getattr(s, "_schema_warnings", None):
    print("Schema warnings:", s._schema_warnings)

pipe = s > Transform("cast", params={"types": {"age": "integer", "income": "number"}, "on_error": "null"})
pipe = pipe.then(
    Transform("select", params={"columns": ["person_id", "age", "country"]})
)
pipe = pipe.then(Sink("out.csv"))
ctx = pipe.run()
print(ctx.checkpoints)
