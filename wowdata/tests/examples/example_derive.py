from wowdata import Transform, Source, Sink, Pipeline

from wowdata.tests import DATA_DIR

s = Source(DATA_DIR / "people.csv")
pipe = (
    Pipeline(Source(DATA_DIR / "people.csv"))
    .then(Transform("derive", params={"new": "is_adult", "expr": "age >= 18"}))
    .then(Transform("derive", params={"new": "income_k", "expr": "income / 1000"}))
    .then(Sink(DATA_DIR / "out_derive.csv"))
)

pipe.run()

pipe = (
    Pipeline(Source(DATA_DIR / "people.csv"))
    .then(Transform("derive", params={"new": "age", "expr": "age + 1", "overwrite": True}))
    .then(Sink(DATA_DIR / "out_derive2.csv"))
)
print(pipe)
pipe.run()
