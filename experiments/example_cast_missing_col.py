from wowdata.models.transforms import Transform
from wowdata.models.pipeline import Pipeline
from wowdata.models.sinks import Sink
from wowdata.models.sources import Source
from wowdata.errors import WowDataUserError

s = Source("people.csv")

pipe = (
    Pipeline(s)
    # Mistake: select away 'income' first
    .then(Transform("select", params={"columns": ["person_id", "age", "country"]}))
    # Then try to cast 'income' (which no longer exists)
    .then(Transform("cast", params={
        "types": {"age": "integer", "income": "number"},
        "on_error": "null",
    }))
    .then(Sink("out_should_not_write.csv"))
)

print(pipe)

try:
    pipe.run()
except WowDataUserError as e:
    print(e)
