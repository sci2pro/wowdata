from core import Transform
from wowdata.models.pipeline import Pipeline
from wowdata.models.sinks import Sink
from wowdata.models.sources import Source

s = Source("people.csv")

pipe = (
    Pipeline(s)
    .then(
        Transform("cast", params={"types": {"age": "integer", "income": "number"}, "on_error": "null"})
    )
    .then(
        Transform("select", params={"columns": ["person_id", "age", "income", "country"]})
    )
    .then(
        Sink("out_cast.csv")
    )
)

print(pipe)
pipe.run()