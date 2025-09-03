import argparse
import requests
import json
import sys

API_URL = "http://0.0.0.0:19120/api/v1"
BRANCH = "main"

def get_branch_hash(branch):
    response = requests.get(f"{API_URL}/trees/tree/{branch}")
    response.raise_for_status()
    data = response.json()
    print("Branch info:")
    print(json.dumps(data, indent=2))
    return data["hash"]

def delete_old_table(branch, hash_, table_key):
    payload = {
        "branch": branch,
        "hash": hash_,
        "operations": [
            {
                "type": "DELETE",
                "key": table_key
            }
        ]
    }

    response = requests.post(
        f"{API_URL}/trees/branch/{branch}/commit",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )

    print(f"DELETE status code: {response.status_code}")
    if response.status_code >= 400:
        print("Warning: DELETE failed or table did not exist.")

def commit_new_table(branch, table_key, metadata_location):
    payload = {
        "operations": [
            {
                "type": "DELETE",
                "key": table_key,
                "content": {
                    "id": None,
                    "type": "ICEBERG_TABLE",
                    "metadataLocation": metadata_location,
                    "snapshotId": 1,
                    "schemaId": 1,
                    "specId": 1,
                    "sortOrderId": 0
                }
            }
        ]
    }

    response = requests.post(
        f"{API_URL}/trees/branch/{branch}/commit",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )
    response.raise_for_status()
    print("Commit successful.")

def main():
    parser = argparse.ArgumentParser(description="Register or update an Iceberg table in Nessie.")
    parser.add_argument("namespace_name", help="Namespace of the Iceberg table, e.g. db")
    parser.add_argument("table_name", help="Name of the Iceberg table, e.g. table")
    parser.add_argument("metadata_location", help="Metadata location URI, e.g. s3://bucket/path/metadata.json")

    args = parser.parse_args()

    full_table_name = args.namespace_name + "." + args.table_name
    table_key = {"elements": full_table_name.split(".")}

    try:
        hash_ = get_branch_hash(BRANCH)
        delete_old_table(BRANCH, hash_, table_key)

        hash_ = get_branch_hash(BRANCH)
        commit_new_table(BRANCH, table_key, args.metadata_location)

        print("Done.")
    except requests.RequestException as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
