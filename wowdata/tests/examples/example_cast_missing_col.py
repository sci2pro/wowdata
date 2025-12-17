from wowdata.errors import WowDataUserError
from wowdata import Transform, Source, Sink, Pipeline

from wowdata.tests import DATA_DIR

s = Source(DATA_DIR / "people.csv")

pipe = (
    Pipeline(s)
    # Mistake: select away 'income' first
    .then(Transform("select", params={"columns": ["person_id", "age", "country"]}))
    # Then try to cast 'income' (which no longer exists)
    .then(Transform("cast", params={
        "types": {"age": "integer", "income": "number"},
        "on_error": "null",
    }))
    .then(Sink(DATA_DIR / "out_should_not_write.csv"))
)

print(pipe)

try:
    pipe.run()
except WowDataUserError as e:
    print(e)
