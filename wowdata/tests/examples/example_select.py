from wowdata import Transform, Source, Sink, Pipeline

from wowdata.tests import DATA_DIR

s = Source(DATA_DIR / "people.csv")

pipe = Pipeline(s).then(
    Transform("select", params={"columns": ["person_id", "age", "country"]})
).then(
    Sink(DATA_DIR / "out_select.csv")
)

print(pipe.schema())
pipe.run()
