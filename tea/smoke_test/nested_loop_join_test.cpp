#include <string>

#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"

namespace tea {
namespace {

#if PG_VERSION_MAJOR >= 9
class NestedLoopJoinTest : public TeaTest {};

TEST_F(NestedLoopJoinTest, NestedLoopJoinTest) {
  std::string table_name = "i_am_test_table";

  ASSIGN_OR_FAIL(auto native_table_defer,
                 pq::CreateNativeTableQuery({GreenplumColumnInfo{.name = "id", .type = "int"},
                                             GreenplumColumnInfo{.name = "elems", .type = "json"}},
                                            "i_am_native_table")
                     .Run(*conn_));

  for (int id : {1, 1}) {
    std::string elems = "[{\"id\": 2}]";

    std::string query = "INSERT INTO i_am_native_table VALUES (" + std::to_string(id) + ", '" + elems + "');";

    ASSERT_OK(pq::Command(query).Run(*conn_));
  }

  OptionalVector<int32_t> col1 = {0};

  auto column1 = MakeInt32Column("col1", 1, col1);

  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1}));
  ASSERT_OK(state_->AddDataFiles({file_path}, table_name));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}, table_name));

  {
    std::string query_allow_to_modify_catalog = "SET allow_system_table_mods = ON";
    ASSERT_OK(pq::Command(query_allow_to_modify_catalog).Run(*conn_));

    ASSERT_OK(pq::Command("UPDATE pg_class SET reltuples = 1000 WHERE relname = '" + std::string(table_name) + "'")
                  .Run(*conn_));
  }

  std::string query =
      "SELECT id FROM i_am_native_table "
      " t1, json_array_elements(t1.elems) AS elems, " +
      table_name + " WHERE (elems.value->>'id')::int = col1 GROUP BY id";

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query(query).Run(*conn_));

  auto expected = pq::ScanResult({"id"}, {});
  ASSERT_EQ(result, expected);
}
#endif

}  // namespace

}  // namespace tea
