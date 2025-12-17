from wowdata import Transform, Source, Sink, Pipeline

from wowdata.tests import DATA_DIR

s = Source(DATA_DIR / "people.csv")

pipe = (
    Pipeline(s)
    .then(
        Transform("cast", params={"types": {"age": "integer", "income": "number"}, "on_error": "null"})
    )
    .then(
        Transform("select", params={"columns": ["person_id", "age", "income", "country"]})
    )
    .then(
        Sink(DATA_DIR / "out_cast.csv")
    )
)

print(pipe)
pipe.run()
