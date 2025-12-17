from wowdata import Transform, Source, Sink, Pipeline

from wowdata.tests import DATA_DIR

pipe = (
    Pipeline(Source(DATA_DIR / "people.csv"))
    .then(Transform("join", params={
        "right": str(DATA_DIR / "countries.csv"),
        "left_on": ["country"],
        "right_on": ["country_code"],
        "how": "left",
    }))
    .then(Sink(DATA_DIR / "people_enriched.csv"))
)

print(pipe.schema())  # will now include right-side fields (with suffix if needed)
pipe.run()
