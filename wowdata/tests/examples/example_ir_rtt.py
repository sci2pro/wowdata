from wowdata import Transform, Source, Sink, Pipeline

from wowdata.tests import DATA_DIR

pipe = (
    Pipeline(Source(str(DATA_DIR / "people.csv")))
    .then(Transform("filter", params={"where": "age >= 30"}))
    .then(Transform("join", params={"right": "countries.csv", "left_on": ["country"], "right_on": ["country_code"],
                                    "how": "left"}))
    .then(Sink(str(DATA_DIR / "out_ir_rtt.csv")))
)

y = pipe.to_yaml()
pipe2 = Pipeline.from_yaml(y)

assert pipe2.to_ir() == pipe.to_ir()
print(pipe2.to_yaml())
print(pipe2.schema())

pipe.save_yaml(str(DATA_DIR / "join.yaml"))  # default: NOT schema-locked
pipe2 = Pipeline.load_yaml(str(DATA_DIR / "join.yaml"))  # resolves paths relative to pipelines/

pipe.save_yaml(str(DATA_DIR / "join.locked.yaml"), lock_schema=True, sample_rows=200)
pipe3 = Pipeline.load_yaml(str(DATA_DIR / "join.locked.yaml"))
