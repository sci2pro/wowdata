from core import Source, Transform, Sink, Pipeline

s = Source("people.csv")

pipe = (
    Pipeline(s)
    .then(
        Transform("cast", params={"types": {"age": "integer", "income": "number"}, "on_error": "null"})
    )
    .then(
        Transform("select", params={"columns": ["person_id", "age", "income", "country"]})
    )
    .then(
        Sink("out_cast.csv")
    )
)

print(pipe)
pipe.run()