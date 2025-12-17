from core import Transform
from wowdata.models.pipeline import Pipeline
from wowdata.models.sinks import Sink
from wowdata.models.sources import Source
from wowdata.errors import WowDataUserError

s = Source("people.csv")

pipe = (
    Pipeline(s)
    .then(Transform("cast", params={"types": {"age": "integer"}, "on_error": "null"}))
    .then(Transform("filter", params={
        # placeholder until the expression compiler exists
        "where": "age >= 30 and country == 'KE'"
    }))
    .then(Sink("out_filter.csv"))
)

print(pipe)

try:
    pipe.run()
except WowDataUserError as e:
    print(e)
