import json
import sys

import jsonschema
from yaml import safe_load

import semantic_checker
import static_checker
from experiments.schema_inferencer import IRSchemaInferencerV0


def load_yaml(fn: str):
    with open(fn) as f:
        return safe_load(f)


def main():
    ir = load_yaml("./example.pipeline.yaml")
    print(json.dumps(ir, indent=2))

    with open("./wowdata.pipeline.schema.json") as f:
        schema = json.load(f)

    issues = []

    # JSON Schema validation (collect errors; jsonschema.validate returns None on success)
    validator = jsonschema.Draft202012Validator(schema)
    for err in sorted(validator.iter_errors(ir), key=lambda e: list(e.path)):
        issues.append({
            "code": "E_SCHEMA",
            "message": err.message,
            "path": "/" + "/".join(str(p) for p in err.path),
        })
    # static checking
    issues += static_checker.IRStaticCheckerV0().check(ir)
    issues += semantic_checker.IRSemanticCheckerV0().check(ir)

    infer = IRSchemaInferencerV0()
    schemas_by_node, infer_issues = infer.infer(ir)

    # attach to node.meta for explain/GUI
    for node in ir["nodes"]:
        nid = node["id"]
        if nid in schemas_by_node:
            node.setdefault("meta", {})["inferred_schema"] = schemas_by_node[nid]

    # show warnings
    for w in infer_issues:
        print(w)

    # Pretty-print issues
    print(json.dumps(issues, indent=2))

    return 0


if __name__ == '__main__':
    sys.exit(main())
