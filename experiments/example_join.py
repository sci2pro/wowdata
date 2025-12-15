from core import Source, Transform, Sink, Pipeline

pipe = (
    Pipeline(Source("people.csv"))
    .then(Transform("join", params={
        "right": "countries.csv",
        "left_on": ["country"],
        "right_on": ["country_code"],
        "how": "left",
    }))
    .then(Sink("people_enriched.csv"))
)

print(pipe.schema())  # will now include right-side fields (with suffix if needed)
pipe.run()
