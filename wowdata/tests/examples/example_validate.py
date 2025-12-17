from wowdata import Transform, Source, Sink, Pipeline

from wowdata.tests import DATA_DIR

s = Source(DATA_DIR / "people.csv")

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
    .then(Sink(DATA_DIR / "out_validate.csv"))
)

print(pipe)
ctx = pipe.run()
print("Checkpoints:", ctx.checkpoints)
