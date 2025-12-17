from wowdata import Transform, Source, Sink, Pipeline
from wowdata.errors import WowDataUserError
from wowdata.tests import DATA_DIR

s = Source(DATA_DIR / "people.csv")

pipe = (
    Pipeline(s)
    .then(Transform("cast", params={"types": {"age": "integer"}, "on_error": "null"}))
    .then(Transform("filter", params={
        # placeholder until the expression compiler exists
        "where": "age >= 30 and country == 'KE'"
    }))
    .then(Sink(DATA_DIR / "out_filter.csv"))
)

print(pipe)

try:
    pipe.run()
except WowDataUserError as e:
    print(e)
