from core import Transform
from wowdata.models.sinks import Sink
from wowdata.models.sources import Source

s = Source("people.csv")
print(s)  # bounded preview
print("Inferred schema:", s.peek_schema(sample_rows=200))
if getattr(s, "_schema_warnings", None):
    print("Schema warnings:", s._schema_warnings)

# demonstrate errors
# pipe = s > Transform("select", params={"columns": ["person_id", "age", "country"]})
# pipe = pipe.then(
#     Transform("cast", params={"types": {"age": "integer", "income": "number"}, "on_error": "null"})
# )
pipe = s > Transform("cast", params={"types": {"age": "integer", "income": "number"}, "on_error": "null"})
pipe = pipe.then(
    Transform("select", params={"columns": ["person_id", "age", "country"]})
)
pipe = pipe.then(Sink("out.csv"))
ctx = pipe.run()
print(f"checkpoints: {ctx.checkpoints}")
