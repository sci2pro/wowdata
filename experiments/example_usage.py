from core import Source, Transform, Sink

s = Source("people.csv")
print(s)  # bounded preview

pipe = s > Transform("select", params={"columns": ["person_id", "age", "country"]})
# pipe = pipe.then(Transform("filter", where="age >= 30")) # not supported
pipe = pipe.then(Sink("out.csv"))
ctx = pipe.run()
print(ctx.checkpoints)
