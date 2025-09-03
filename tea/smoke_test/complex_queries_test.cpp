#include <iceberg/result.h>
#include <iceberg/test_utils/column.h>

#include <stdexcept>
#include <string>

#include "gtest/gtest.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"
#include "tea/test_utils/common.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class ComplexQueryTest : public TeaTest {
 public:
  ParquetColumn MakeColumn(const std::string& name, const std::string& type) {
    ++field_id_;
    if (type == "int4") {
      return iceberg::MakeInt32Column(name, field_id_, {});
    } else if (type == "int8") {
      return iceberg::MakeInt64Column(name, field_id_, {});
    } else if (type == "text") {
      return iceberg::MakeStringColumn(name, field_id_, {});
    } else if (type == "decimal(12, 2)") {
      return iceberg::MakeNumericColumn(name, field_id_, OptionalVector<int64_t>{}, 12, 2);
    } else if (type == "date") {
      return iceberg::MakeDateColumn(name, field_id_, {});
    }

    throw std::runtime_error("Internal error in ComplexQueryTest. Unexpected type " + type + " for column '" + name +
                             "'");
  }

  void PrepareData(const std::vector<GreenplumColumnInfo>& schema, TableName table_name) {
    std::vector<ParquetColumn> columns;
    for (const auto& [name, type] : schema) {
      columns.emplace_back(MakeColumn(name, type));
    }

    auto path = iceberg::ValueSafe(state_->WriteFile(columns));
    iceberg::Ensure(state_->AddDataFiles({path}, table_name));

    auto defer = iceberg::ValueSafe(state_->CreateTable(schema, table_name));
    table_defers_.emplace_back(std::move(defer));
  }

 private:
  int32_t field_id_ = 1;
  std::vector<pq::DropTableDefer> table_defers_;
};

TEST_F(ComplexQueryTest, Trivial) {
  if (Environment::GetMetadataType() == MetadataType::kIceberg) {
    GTEST_SKIP() << "Skip test (metadata is iceberg)";
  }

  constexpr std::string_view query = R"(
select
	s_name,
	count(*) as numwait
from
	supplier,
	lineitem l1,
	orders,
	nation
where
	s_suppkey = l1.l_suppkey
	and o_orderkey = l1.l_orderkey
	and o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and exists (
		select
			*
		from
			lineitem l2
		where
			l2.l_orderkey = l1.l_orderkey
			and l2.l_suppkey <> l1.l_suppkey
	)
	and not exists (
		select
			*
		from
			lineitem l3
		where
			l3.l_orderkey = l1.l_orderkey
			and l3.l_suppkey <> l1.l_suppkey
			and l3.l_receiptdate > l3.l_commitdate
	)
	and s_nationkey = n_nationkey
	and n_name = 'SAUDI ARABIA'
group by
	s_name
order by
	numwait desc,
	s_name
limit 100
;
  )";

  {
    auto schema = {GreenplumColumnInfo{.name = "s_suppkey", .type = "int4"},
                   GreenplumColumnInfo{.name = "s_name", .type = "text"},
                   GreenplumColumnInfo{.name = "s_address", .type = "text"},
                   GreenplumColumnInfo{.name = "s_nationkey", .type = "int4"},
                   GreenplumColumnInfo{.name = "s_phone", .type = "text"},
                   GreenplumColumnInfo{.name = "s_acctbal", .type = "decimal(12, 2)"},
                   GreenplumColumnInfo{.name = "s_comment", .type = "text"}};

    PrepareData(schema, "supplier");
  }
  {
    auto schema = {GreenplumColumnInfo{.name = "l_orderkey", .type = "int8"},
                   GreenplumColumnInfo{.name = "l_partkey", .type = "int4"},
                   GreenplumColumnInfo{.name = "l_suppkey", .type = "int4"},
                   GreenplumColumnInfo{.name = "l_linenumber", .type = "int4"},
                   GreenplumColumnInfo{.name = "l_quantity", .type = "decimal(12, 2)"},
                   GreenplumColumnInfo{.name = "l_extendedprice", .type = "decimal(12, 2)"},
                   GreenplumColumnInfo{.name = "l_discount", .type = "decimal(12, 2)"},
                   GreenplumColumnInfo{.name = "l_tax", .type = "decimal(12, 2)"},
                   GreenplumColumnInfo{.name = "l_returnflag", .type = "text"},
                   GreenplumColumnInfo{.name = "l_shipdate", .type = "date"},
                   GreenplumColumnInfo{.name = "l_commitdate", .type = "date"},
                   GreenplumColumnInfo{.name = "l_receiptdate", .type = "date"},
                   GreenplumColumnInfo{.name = "l_shipinstruct", .type = "text"},
                   GreenplumColumnInfo{.name = "l_shipmode", .type = "text"},
                   GreenplumColumnInfo{.name = "l_comment", .type = "text"}};

    PrepareData(schema, "lineitem");
  }
  {
    auto schema = {GreenplumColumnInfo{.name = "o_orderkey", .type = "int8"},
                   GreenplumColumnInfo{.name = "o_custkey", .type = "int4"},
                   GreenplumColumnInfo{.name = "o_orderstatus", .type = "text"},
                   GreenplumColumnInfo{.name = "o_totalprice", .type = "decimal(12, 2)"},
                   GreenplumColumnInfo{.name = "o_orderdate", .type = "date"},
                   GreenplumColumnInfo{.name = "o_orderpriority", .type = "text"},
                   GreenplumColumnInfo{.name = "o_clerk", .type = "text"},
                   GreenplumColumnInfo{.name = "o_shippriority", .type = "int4"},
                   GreenplumColumnInfo{.name = "o_comment", .type = "text"}};

    PrepareData(schema, "orders");
  }
  {
    auto schema = {GreenplumColumnInfo{.name = "n_nationkey", .type = "int4"},
                   GreenplumColumnInfo{.name = "n_name", .type = "text"},
                   GreenplumColumnInfo{.name = "n_regionkey", .type = "int4"},
                   GreenplumColumnInfo{.name = "n_comment", .type = "text"}};

    PrepareData(schema, "nation");
  }

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query(std::string(query)).Run(*conn_));
  auto expected = pq::ScanResult({"s_name", "numwait"}, {});
  ASSERT_EQ(result, expected);
}

}  // namespace
}  // namespace tea
