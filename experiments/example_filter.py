from core import Source, Transform, Sink, Pipeline

source = Source("people.csv")
print(source)

pipe = (
    Pipeline(source)
    .then(Transform("cast", params={"types": {"age": "integer"}, "on_error": "null"}))
    .then(Transform("filter", params={"where": "age >= 30 and country == 'KE'", "strict": True}))
    .then(Sink("out_filtered.csv"))
)
pipe.run()

pipe = (
    Pipeline(Source("people.csv"))
    .then(Transform("filter", params={"where": "country == 'KE'"}))
    .then(Sink("out_ke.csv"))
)
pipe.run()
