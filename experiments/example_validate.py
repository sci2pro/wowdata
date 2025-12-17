from wowdata.models.transforms import Transform
from wowdata.models.pipeline import Pipeline
from wowdata.models.sinks import Sink
from wowdata.models.sources import Source

s = Source("people.csv")

pipe = (
    Pipeline(s)
    .then(Transform("cast", params={
        "types": {"age": "integer", "income": "number"},
        "on_error": "null",
    }))
    .then(Transform("validate", params={
        # v0: stored as a checkpoint; later becomes actual frictionless validation
        "name": "after_cast",
        "schema": "infer",  # later: allow schema refs/inline schemas
        "sample_rows": 200,  # later: used by frictionless detector/validator
    }))
    .then(Transform("select", params={"columns": ["person_id", "age", "income", "country"]}))
    .then(Sink("out_validate.csv"))
)

print(pipe)
ctx = pipe.run()
print("Checkpoints:", ctx.checkpoints)
