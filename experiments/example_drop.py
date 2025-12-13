from core import Source, Transform, Sink, Pipeline


s = Source("people.csv")

pipe = Pipeline(s).then(
    Transform("drop", params={"columns": ["occupation"]})
).then(
    Sink("out_drop.csv")
)

print(pipe)
pipe.run()