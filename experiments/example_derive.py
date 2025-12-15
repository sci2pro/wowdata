from core import Source, Transform, Sink, Pipeline

s = Source("people.csv")
pipe = (
    Pipeline(Source("people.csv"))
    .then(Transform("derive", params={"new": "is_adult", "expr": "age >= 18"}))
    .then(Transform("derive", params={"new": "income_k", "expr": "income / 1000"}))
    .then(Sink("out_derive.csv"))
)

pipe.run()

pipe = (
    Pipeline(Source("people.csv"))
    .then(Transform("derive", params={"new": "age", "expr": "age + 1", "overwrite": True}))
    .then(Sink("out_derive2.csv"))
)
pipe.run()