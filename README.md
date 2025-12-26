# **WowData™**

**WowData™ makes complex data engineering tasks easy for ordinary users.**

WowData™ is a human-centred data wrangling and pipeline framework designed to make advanced data preparation *understandable*, *teachable*, and *inspectable*—without sacrificing power.

It is built for people who need to work with real data, not just programmers.

---

## **Why WowData™?**

Most data tools assume that if you are touching data, you are already an expert.

WowData™ rejects this assumption.

We believe that:

* data engineering is a foundational skill,

* learning should be fast and durable,

* and tools should teach rather than intimidate.

WowData™ is designed so that **anyone can build, read, and reason about a data pipeline**—even when that pipeline performs non-trivial work.

---

## **Core Concepts**

WowData™ is built on four stable, universal concepts:

* **Source** — where data comes from

* **Transform** — how data changes

* **Sink** — where data goes

* **Pipeline** — how everything fits together

These concepts are deliberately persistent and reused everywhere.

We avoid hidden helpers, magical shortcuts, and proliferating abstractions.

If you understand these four ideas, you understand WowData™.

---

## **Example**

```python
from core import Source, Transform, Sink, Pipeline

pipe \= (  
    Pipeline(Source("people.csv"))  
    .then(Transform("cast", params={"types": {"age": "integer"}, "on\_error": "null"}))  
    .then(Transform("filter", params={"where": "age \>= 30 and country \== 'KE'"}))  
    .then(Sink("out\_filtered.csv"))  
)

pipe.run()
```

This pipeline:

1. reads a CSV file,

2. explicitly casts a column,

3. filters rows using a small, teachable expression language,

4. and writes the result to disk.

Nothing is hidden. Nothing is inferred without consent.

---

## **Learning-First by Design**

WowData™ makes deliberate trade-offs to accelerate learning:

* A **small, closed vocabulary** of concepts

* A **restricted expression language** that can be mastered quickly

* **Explicit transforms** instead of silent automation

* **Deterministic checkpoints** for inspection

* A serializable pipeline that can be read like a recipe

If a feature makes learning harder—even if it saves time—we reject it.

---

## **Serialization That Humans Can Read**

Every WowData™ pipeline can be serialized into a human-readable form.

Serialization is not configuration for machines—it is a **cognitive artifact** for people.

Our goal is that an ordinary user can:

* read a serialized pipeline,

* understand what it does,

* explain it to someone else,

* and safely modify it.

---

## **Errors That Teach**

In WowData™, error messages are part of the interface.

Every user-facing error:

1. explains what went wrong,

2. explains why it happened,

3. suggests what to do next.

Errors are designed to teach correct mental models, not expose internal stack traces.

---

## **Built on the Best**

WowData™ does not reinvent proven tools.

Instead, it **piggybacks on best-in-class ecosystems**:

* mature ETL engines,

* established data modelling and validation frameworks,

* battle-tested execution backends.

Our contribution is an opinionated, human-centred layer that makes these tools usable by more people.

---

## **Open Source, Unapologetically**

WowData™ is open source by principle, not convenience.

We believe that tools shaping how people think about data must be:

* transparent,

* inspectable,

* extensible,

* and owned by the community.

---

## **A Graphical Future**

WowData™ is pipeline-first, not code-first or GUI-first.

We plan to build a **world-class graphical application** that:

* visualizes the same pipelines,

* manipulates the same underlying representation,

* and never diverges from the conceptual model.

The graphical interface will not be a toy—it will be another way of seeing the same truth.

---

## **What WowData™ Is** 

## **Not**

WowData™ is not:

* a low-code gimmick,

* a black-box automation tool,

* a thin wrapper around someone else’s API,

* or a system that only experts can use safely.

If forced to choose between power and clarity, **we choose clarity**.

---

## **Status**

WowData™ is under active development.

The API is opinionated and evolving, but the core principles are stable.

If these ideas resonate with you, contributions and discussion are welcome.

---

**WowData™ is not trying to make data engineering smaller.**

**It is trying to make it thinkable.**

