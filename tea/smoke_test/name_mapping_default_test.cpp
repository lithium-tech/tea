#include <map>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "iceberg/schema.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/type.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

constexpr int kNoFieldId = -1;

class NameMappingDefaultTest : public TeaTest {
 public:
  void SetUp() override {
    if (Environment::GetMetadataType() != MetadataType::kIceberg) {
      GTEST_SKIP() << "schema.name-mapping.default only applies to the Iceberg metadata path";
    }
    TeaTest::SetUp();
  }

 protected:
  static std::shared_ptr<iceberg::Schema> MakeIcebergSchema() {
    using iceberg::types::NestedField;
    using iceberg::types::PrimitiveType;

    std::vector<NestedField> fields;
    fields.push_back(NestedField{.name = "col1",
                                 .field_id = 1,
                                 .is_required = false,
                                 .type = std::make_shared<PrimitiveType>(iceberg::TypeID::kString)});
    fields.push_back(NestedField{.name = "col2",
                                 .field_id = 2,
                                 .is_required = false,
                                 .type = std::make_shared<PrimitiveType>(iceberg::TypeID::kInt)});
    fields.push_back(NestedField{.name = "col3",
                                 .field_id = 3,
                                 .is_required = false,
                                 .type = std::make_shared<PrimitiveType>(iceberg::TypeID::kLong)});
    return std::make_shared<iceberg::Schema>(0, std::move(fields));
  }
};

TEST_F(NameMappingDefaultTest, ResolvesFieldIdsByName) {
  std::string s0 = "a";
  std::string s1 = "b";

  auto col1 = MakeStringColumn("col1", kNoFieldId, std::vector<std::string*>{&s0, &s1});
  auto col2 = MakeInt32Column("col2", kNoFieldId, OptionalVector<int32_t>{10, 20});
  auto col3 = MakeInt64Column("col3", kNoFieldId, OptionalVector<int64_t>{100, 200});

  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({col1, col2, col3}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  state_->SetSchema(MakeIcebergSchema());
  state_->SetProperties({{"schema.name-mapping.default", R"([{"field-id":1,"names":["col1"]},)"
                                                         R"({"field-id":2,"names":["col2"]},)"
                                                         R"({"field-id":3,"names":["col3"]}])"}});

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "text"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "int8"}}));

  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName, "col1, col2, col3").Run(*conn_));

  pq::ScanResult expected_result({"col1", "col2", "col3"}, {{"a", "10", "100"}, {"b", "20", "200"}});
  EXPECT_EQ(result, expected_result);
}

TEST_F(NameMappingDefaultTest, ParquetNameDiffersFromIcebergName) {
  std::string s0 = "a";
  std::string s1 = "b";

  auto col1 = MakeStringColumn("old_col1", kNoFieldId, std::vector<std::string*>{&s0, &s1});
  auto col2 = MakeInt32Column("old_col2", kNoFieldId, OptionalVector<int32_t>{10, 20});
  auto col3 = MakeInt64Column("old_col3", kNoFieldId, OptionalVector<int64_t>{100, 200});

  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({col1, col2, col3}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  state_->SetSchema(MakeIcebergSchema());
  state_->SetProperties({{"schema.name-mapping.default", R"([{"field-id":1,"names":["old_col1"]},)"
                                                         R"({"field-id":2,"names":["old_col2"]},)"
                                                         R"({"field-id":3,"names":["old_col3"]}])"}});

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "text"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "int8"}}));

  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName, "col1, col2, col3").Run(*conn_));

  pq::ScanResult expected_result({"col1", "col2", "col3"}, {{"a", "10", "100"}, {"b", "20", "200"}});
  EXPECT_EQ(result, expected_result);
}

TEST_F(NameMappingDefaultTest, NoFieldIdNoSchemaNameMapping) {
  std::string s0 = "a";
  std::string s1 = "b";

  auto col1 = MakeStringColumn("col1", kNoFieldId, std::vector<std::string*>{&s0, &s1});
  auto col2 = MakeInt32Column("col2", kNoFieldId, OptionalVector<int32_t>{10, 20});
  auto col3 = MakeInt64Column("col3", kNoFieldId, OptionalVector<int64_t>{100, 200});

  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({col1, col2, col3}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  state_->SetSchema(MakeIcebergSchema());

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "text"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "int8"}}));

  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName, "col1, col2, col3").Run(*conn_));

  pq::ScanResult expected_result({"col1", "col2", "col3"}, {{"", "", ""}, {"", "", ""}});
  EXPECT_EQ(result, expected_result);
}

}  // namespace
}  // namespace tea
