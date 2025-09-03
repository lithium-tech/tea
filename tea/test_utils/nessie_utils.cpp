#include "tea/test_utils/nessie_utils.h"

#include <stdexcept>

namespace tea {

static constexpr const char* upload_table_script = R"(
import argparse
import pynessie

def delete_table(client, table_name, metadata_location):
    main_branch = client.get_reference("main")
    main_hash = main_branch.hash_

    table_key = pynessie.model.ContentKey.from_path_string(table_name)

    put_operation = pynessie.model.Delete(key=table_key)
    client.commit("main", main_hash, None, None, put_operation)

def add_table(table_name, metadata_location):
    print(table_name, metadata_location)
    client = pynessie.init(config_dict={
        'endpoint': 'http://0.0.0.0:19120/api/v1',
        'verify': True,
        'default_branch': 'main',
    })
    
    try:
        delete_table(client, table_name, metadata_location)
    except:
        pass

    main_branch = client.get_reference("main")
    main_hash = main_branch.hash_
    
    table_key = pynessie.model.ContentKey.from_path_string(table_name)
    
    iceberg_table = pynessie.model.IcebergTable(
        id=None,
        metadata_location=metadata_location,
        snapshot_id=1,
        schema_id=1,
        spec_id=1,
        sort_order_id=0
    )
    
    put_operation = pynessie.model.Put(key=table_key, content=iceberg_table)
    client.commit("main", main_hash, None, None, put_operation)
    
def create_namespace_if_not_exists(namespace_name):
    client = pynessie.init(config_dict={
        'endpoint': 'http://0.0.0.0:19120/api/v1',
        'verify': True,
        'default_branch': 'main',
    })

    main_branch = client.get_reference("main")
    main_hash = main_branch.hash_

    namespace = pynessie.model.Namespace(id=None, elements=[namespace_name])
    namespace_key = pynessie.model.ContentKey.from_path_string(namespace_name)
    put_operation = pynessie.model.Put(key=namespace_key, content=namespace)
    try:
        client.commit(
            "main",
            main_hash,
            None,
            None,
            put_operation
        )
    except:
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add a table to Nessie.")
    parser.add_argument("namespace_name", type=str, help="Namespace of the table to add.")
    parser.add_argument("table_name", type=str, help="Name of the table to add.")
    parser.add_argument("metadata_location", type=str, help="Metadata location for the table.")
    
    args = parser.parse_args()
    create_namespace_if_not_exists(args.namespace_name)
    add_table(args.namespace_name + "." + args.table_name, args.metadata_location)
    
)";

void UploadNessieCatalog([[maybe_unused]] const std::string& db_name, [[maybe_unused]] const std::string& table_name,
                         [[maybe_unused]] const std::string& location) {
#if USE_REST
  std::ofstream file("script.py");
  if (!file) {
    throw std::runtime_error("Can not open script file");
  }
  file << upload_table_script;
  file.close();

  std::string run_cmd = "python3 script.py " + db_name + " " + table_name + " " + location;
  [[maybe_unused]] auto return_code = std::system(run_cmd.c_str());
#endif
}

}  // namespace tea
