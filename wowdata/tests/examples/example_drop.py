from wowdata import Transform, Source, Sink, Pipeline

from wowdata.tests import DATA_DIR

s = Source(DATA_DIR / "people.csv")

pipe = Pipeline(s).then(
    Transform("drop", params={"columns": ["occupation"]})
).then(
    Sink(DATA_DIR / "out_drop.csv")
)

print(pipe)
pipe.run()
