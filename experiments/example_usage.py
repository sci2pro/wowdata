from core import Source, Transform, Sink

s = Source("people.csv")
print(s)  # bounded preview
print("Inferred schema:", s.peek_schema(sample_rows=200))
if getattr(s, "_schema_warnings", None):
    print("Schema warnings:", s._schema_warnings)

pipe = s > Transform("select", params={"columns": ["person_id", "age", "country"]})
# pipe = pipe.then(Transform("filter", where="age >= 30")) # not supported
pipe = pipe.then(Sink("out.csv"))
ctx = pipe.run()
print(ctx.checkpoints)
