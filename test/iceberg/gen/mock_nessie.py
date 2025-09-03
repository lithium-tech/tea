from flask import Flask, request, jsonify

app = Flask(__name__)

state = {
    "branches": {
        "main": {
            "name": "main",
            "type": "BRANCH",
            "hash": "abcdef1234567890"
        }
    },
    "tables": {}
}

@app.route("/api/v1/references/<name>", methods=["GET"])
def get_reference(name):
    branch = state["branches"].get(name)
    if branch:
        return jsonify(branch)
    return jsonify({"error": f"Branch '{name}' not found"}), 404

@app.route("/api/v1/trees/<branch>", methods=["POST"])
def commit(branch):
    expected_hash = request.args.get("expectedHash")
    data = request.get_json()

    operations = data.get("operations", [])
    for op in operations:
        key = tuple(op.get("key", []))
        if op["type"] == "DELETE":
            state["tables"].pop(key, None)
        elif op["type"] == "PUT":
            content = op.get("content", {})
            state["tables"][key] = content

    new_hash = f"hash_{len(state['tables'])}"
    state["branches"][branch]["hash"] = new_hash

    return jsonify({
        "targetBranch": {
            "name": branch,
            "hash": new_hash
        }
    })

@app.route("/api/v1/debug", methods=["GET"])
def debug_state():
    return jsonify({
        "branches": state["branches"],
        "tables": state["tables"]
    })

@app.route("/api/v1/trees/tree/<name>", methods=["GET"])
def get_tree_by_name(name):
    branch = state["branches"].get(name)
    if branch:
        return jsonify(branch)
    return jsonify({"error": f"Branch '{name}' not found"}), 404

@app.route("/api/v1/trees/branch/<branch>/commit", methods=["POST"])
def commit_branch(branch):
    expected_hash = request.args.get("expectedHash")
    data = request.get_json()

    operations = data.get("operations", [])
    for op in operations:
        key = op["key"]['elements'][0]
        if op["type"] == "DELETE":
            state["tables"].pop(key, None)
        elif op["type"] == "PUT":
            content = op.get("content", {})
            state["tables"][key] = content

    new_hash = f"hash_{len(state['tables'])}"
    state["branches"][branch]["hash"] = new_hash

    return jsonify({
        "name": branch,
        "type": "BRANCH",
        "hash": new_hash
    })

@app.route("/api/v2/trees/<ref>/contents/<path:key>", methods=["GET"])
def get_table_content_v2(ref, key):

    content = state["tables"].get(key)
    print(key, ref)
    if not content:
        return jsonify({"message": f"Table '{key}' not found"}), 404

    return jsonify({
        "content": {
            "type": "ICEBERG_TABLE",
            "metadataLocation": content.get("metadataLocation"),
            "snapshotId": content.get("snapshotId"),
            "schemaId": content.get("schemaId"),
            "specId": content.get("specId"),
            "sortOrderId": content.get("sortOrderId"),
        }
    })

if __name__ == "__main__":
    app.run(port=19120)
