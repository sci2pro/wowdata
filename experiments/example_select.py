from core import Source, Transform, Sink, Pipeline

s = Source("people.csv")

pipe = Pipeline(s).then(
    Transform("select", params={"columns": ["person_id", "age", "country"]})
).then(
    Sink("out_select.csv")
)

print(pipe)
pipe.run()