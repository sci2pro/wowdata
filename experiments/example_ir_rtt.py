from core import Source, Transform, Sink, Pipeline

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
