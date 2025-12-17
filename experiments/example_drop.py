from wowdata.models.transforms import Transform
from wowdata.models.pipeline import Pipeline
from wowdata.models.sinks import Sink
from wowdata.models.sources import Source

s = Source("people.csv")

pipe = Pipeline(s).then(
    Transform("drop", params={"columns": ["occupation"]})
).then(
    Sink("out_drop.csv")
)

print(pipe)
pipe.run()