#include "tea/smoke_test/fragment_info.h"

#include <memory>
#include <string>
#include <vector>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/s3fs.h"
#include "parquet/file_reader.h"
#include "parquet/schema.h"

#include "tea/smoke_test/environment.h"

namespace tea {

// copypasted from tea/teapot/mock_server.cpp
static teapot::FieldType GetIcebergFieldType(parquet::schema::NodePtr file_field) {
  auto logical_type = file_field->logical_type();
  if (file_field->is_primitive()) {
    auto primitive_node = std::static_pointer_cast<parquet::schema::PrimitiveNode>(file_field);
    if (primitive_node->physical_type() == parquet::Type::BOOLEAN) {
      return teapot::FieldType::TYPE_BOOLEAN;
    } else if (primitive_node->physical_type() == parquet::Type::FLOAT) {
      return teapot::FieldType::TYPE_FLOAT;
    } else if (primitive_node->physical_type() == parquet::Type::DOUBLE) {
      return teapot::FieldType::TYPE_DOUBLE;
    } else {
      if (primitive_node->logical_type()->is_int()) {
        auto int_type = std::static_pointer_cast<const parquet::IntLogicalType>(primitive_node->logical_type());
        if (int_type->bit_width() == 64) {
          return teapot::FieldType::TYPE_LONG;
        } else {
          return teapot::FieldType::TYPE_INTEGER;
        }
      }
      if (primitive_node->logical_type()->is_date()) {
        return teapot::FieldType::TYPE_DATE;
      }
      if (primitive_node->logical_type()->is_time()) {
        return teapot::FieldType::TYPE_TIME;
      }
      if (primitive_node->logical_type()->is_UUID()) {
        return teapot::FieldType::TYPE_UUID;
      }
      if (primitive_node->logical_type()->is_string()) {
        return teapot::FieldType::TYPE_STRING;
      }
      if (primitive_node->logical_type()->is_decimal()) {
        return teapot::FieldType::TYPE_DECIMAL;
      }
      if (primitive_node->logical_type()->is_timestamp()) {
        auto timestamp_type =
            std::static_pointer_cast<const parquet::TimestampLogicalType>(primitive_node->logical_type());
        if (timestamp_type->is_adjusted_to_utc()) {
          return teapot::FieldType::TYPE_TIMESTAMPTZ;
        } else {
          return teapot::FieldType::TYPE_TIMESTAMP;
        }
      }
      if (primitive_node->logical_type()->is_JSON()) {
        return teapot::FieldType::TYPE_STRING;
      }
      if (primitive_node->physical_type() == parquet::Type::INT32) {
        return teapot::FieldType::TYPE_INTEGER;
      }
      if (primitive_node->physical_type() == parquet::Type::INT64) {
        return teapot::FieldType::TYPE_LONG;
      }
      // maybe it is undefined
      return teapot::FieldType::TYPE_BINARY;
    }
  } else {
    return teapot::FieldType::TYPE_UNDEFINED;
  }
}

teapot::MetadataResponse TeapotExpectedResponse(const std::vector<FragmentInfo>& fragments_info) {
  teapot::MetadataResponse response;
  teapot::MetadataResponseResult& result = *response.mutable_result();
  for (auto& fragment_info : fragments_info) {
    auto fragment = result.add_fragments();
    fragment->set_path(fragment_info.data_path);
    {
      std::string path;
      std::shared_ptr<arrow::fs::FileSystem> fs = [&]() -> std::shared_ptr<arrow::fs::FileSystem> {
        if (fragment_info.data_path.starts_with("s3://")) {
          return Environment::GetS3Filesystem();
        }
        return std::make_shared<arrow::fs::LocalFileSystem>();
      }();

      auto input_file = fs->OpenInputFile(fs->PathFromUri(fragment_info.data_path).ValueOrDie()).ValueOrDie();
      auto file_reader = parquet::ParquetFileReader::Open(input_file);
      auto metadata = file_reader->metadata();
      if (metadata->num_row_groups() == 0) {
        fragment->set_length(1);
        fragment->set_position(4);
        continue;
      }
      if (!result.has_schema()) {
        // copypasted from tea/teapot/mock_server.cpp
        const parquet::SchemaDescriptor* schema = metadata->schema();
        auto num_columns = schema->num_columns();
        auto* result_schema = result.mutable_schema();
        for (int col = 0; col < num_columns; ++col) {
          const parquet::ColumnDescriptor* column = schema->Column(col);
          const parquet::schema::Node* node = schema->GetColumnRoot(col);
          const std::shared_ptr<const parquet::LogicalType> logical_type = node->logical_type();

          auto* result_schema_field = result_schema->add_fields();
          result_schema_field->set_id(node->field_id());
          result_schema_field->set_name(node->name());
          result_schema_field->set_repeated(logical_type && logical_type->is_list());

          teapot::FieldType field_type = GetIcebergFieldType(column->schema_node());
          result_schema_field->set_type(field_type);
          if (field_type == teapot::FieldType::TYPE_DECIMAL) {
            auto decimal_type =
                std::static_pointer_cast<const parquet::DecimalLogicalType>(column->schema_node()->logical_type());
            result_schema_field->set_precision(decimal_type->precision());
            result_schema_field->set_scale(decimal_type->scale());
          }
        }
      }
      if (fragment_info.position.has_value()) {
        fragment->set_position(*fragment_info.position);
      } else {
        fragment->set_position(4);
      }
      int32_t num_rg = metadata->num_row_groups();
      int64_t total_size = metadata->RowGroup(num_rg - 1)->total_byte_size() +
                           metadata->RowGroup(num_rg - 1)->file_offset() - metadata->RowGroup(0)->file_offset();
      if (fragment_info.length.has_value()) {
        fragment->set_length(*fragment_info.length);
      } else {
        fragment->set_length(total_size);
      }
    }
    for (auto& pos_del_info : fragment_info.pos_deletes) {
      auto del = fragment->add_positional_deletes();
      del->set_path(pos_del_info.path);
    }
    for (auto& eq_del_info : fragment_info.eq_deletes_info) {
      auto del = fragment->add_equality_deletes();
      del->set_path(eq_del_info.path);
      for (auto& id : eq_del_info.field_ids) {
        del->add_delete_field_ids(id);
      }
    }
  }
  return response;
}

}  // namespace tea
