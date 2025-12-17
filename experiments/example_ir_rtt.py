from wowdata.models.transforms import Transform
from wowdata.models.pipeline import Pipeline
from wowdata.models.sinks import Sink
from wowdata.models.sources import Source

pipe = (
    Pipeline(Source("people.csv"))
    .then(Transform("filter", params={"where": "age >= 30"}))
    .then(Transform("join", params={"right": "countries.csv", "left_on": ["country"], "right_on": ["country_code"],
                                    "how": "left"}))
    .then(Sink("out_ir_rtt.csv"))
)

y = pipe.to_yaml()
pipe2 = Pipeline.from_yaml(y)

assert pipe2.to_ir() == pipe.to_ir()
print(pipe2.to_yaml())
print(pipe2.schema())

pipe.save_yaml("join.yaml")  # default: NOT schema-locked
pipe2 = Pipeline.load_yaml("join.yaml")  # resolves paths relative to pipelines/

pipe.save_yaml("join.locked.yaml", lock_schema=True, sample_rows=200)
pipe3 = Pipeline.load_yaml("join.locked.yaml")