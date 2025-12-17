from wowdata.models.transforms import Transform
from wowdata.models.pipeline import Pipeline
from wowdata.models.sinks import Sink
from wowdata.models.sources import Source

s = Source("people.csv")

pipe = Pipeline(s).then(
    Transform("select", params={"columns": ["person_id", "age", "country"]})
).then(
    Sink("out_select.csv")
)

print(pipe.schema())
pipe.run()