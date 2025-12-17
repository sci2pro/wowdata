from wowdata import Transform, Source, Sink, Pipeline

from wowdata.tests import DATA_DIR

source = Source(DATA_DIR / "people.csv")
print(source)

pipe = (
    Pipeline(source)
    .then(Transform("cast", params={"types": {"age": "integer"}, "on_error": "null"}))
    .then(Transform("filter", params={"where": "age >= 30 and country == 'KE'", "strict": True}))
    .then(Sink(DATA_DIR / "out_filtered.csv"))
)
pipe.run()

pipe = (
    Pipeline(Source("people.csv"))
    .then(Transform("filter", params={"where": "country == 'KE'"}))
    .then(Sink(DATA_DIR / "out_ke.csv"))
)
pipe.run()
