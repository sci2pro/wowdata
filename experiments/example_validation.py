from wowdata.models.transforms import Transform
from wowdata.models.pipeline import Pipeline
from wowdata.models.sources import Source

# Inline schema so validate has something authoritative to check
schema = {
    "fields": [
        {"name": "id", "type": "integer"},
        {"name": "age", "type": "integer"},
        {"name": "country", "type": "string"},
    ]
}


def run(path, fail=True, strict_schema=True):
    pipe = (
        Pipeline(Source(path, schema=schema))
        .then(Transform("validate", params={"sample_rows": 50, "fail": fail, "strict_schema": strict_schema}))
    )
    ctx = pipe.run()
    print("checkpoints:", ctx.checkpoints[-1])
    print("validations:", ctx.validations[-1])


print("=== OK ===")
run("../wowdata/tests/data/people_ok.csv")

print("\n=== BAD (fail=True) ===")
try:
    run("../wowdata/tests/data/people_bad.csv", fail=True)
except Exception as e:
    print("Raised:", type(e).__name__, e)

print("\n=== BAD (fail=False) ===")
run("../wowdata/tests/data/people_bad.csv", fail=False)
