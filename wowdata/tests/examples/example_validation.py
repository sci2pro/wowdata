from wowdata import Transform, Source, Sink, Pipeline

from wowdata.tests import DATA_DIR
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
run(DATA_DIR / "people_ok.csv")

print("\n=== BAD (fail=True) ===")
try:
    run(DATA_DIR / "people_bad.csv", fail=True)
except Exception as e:
    print("Raised:", type(e).__name__, e)

print("\n=== BAD (fail=False) ===")
run(DATA_DIR / "people_bad.csv", fail=False)
