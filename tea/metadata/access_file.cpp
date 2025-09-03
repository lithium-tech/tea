#include "tea/metadata/access_file.h"

#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include "iceberg/common/error.h"
#include "iceberg/common/fs/file_reader_provider_impl.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "parquet/schema.h"

namespace tea::meta::access {
namespace {

// TODO(gmusya): consider moving this code to iceberg-cpp
std::shared_ptr<const iceberg::types::Type> ConvertPhysicalType(const parquet::ColumnDescriptor& column) {
  using PrimitiveType = iceberg::types::PrimitiveType;

  auto physical_type = column.physical_type();

  switch (physical_type) {
    case parquet::Type::BOOLEAN:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kBoolean);
    case parquet::Type::INT32:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kInt);
    case parquet::Type::INT64:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kLong);
    case parquet::Type::FLOAT:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kFloat);
    case parquet::Type::DOUBLE:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kDouble);
    case parquet::Type::BYTE_ARRAY:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kBinary);
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kFixed);
    default:
      break;
  }
  throw std::runtime_error("Not supported physical type " + std::to_string(physical_type) + " for column " +
                           column.name());
}

static std::shared_ptr<const iceberg::types::Type> IceTypeFromColumn(const parquet::ColumnDescriptor& column) {
  using ParquetLogicalType = parquet::LogicalType::Type;
  using PrimitiveType = iceberg::types::PrimitiveType;

  auto& parquet_logical_type = column.logical_type();

  switch (parquet_logical_type->type()) {
    case ParquetLogicalType::STRING:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kString);
    case ParquetLogicalType::DATE:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kDate);
    case ParquetLogicalType::TIME:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kTime);
    case ParquetLogicalType::TIMESTAMP: {
      auto timestamp_type = std::static_pointer_cast<const parquet::TimestampLogicalType>(parquet_logical_type);
      if (timestamp_type->is_adjusted_to_utc()) {
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kTimestamptz);
      } else {
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kTimestamp);
      }
    }
    case ParquetLogicalType::INT: {
      auto int_logical_type = std::static_pointer_cast<const parquet::IntLogicalType>(parquet_logical_type);
      if (!int_logical_type->is_signed()) {
        throw std::runtime_error("Only signed integers are supported");
      }
      const auto bit_width = int_logical_type->bit_width();
      if (bit_width == 16 || bit_width == 32) {
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kInt);
      }
      if (bit_width == 64) {
        return std::make_shared<const PrimitiveType>(iceberg::TypeID::kLong);
      }
      throw std::runtime_error("Integer with bit width " + std::to_string(bit_width) + " is not supported");
    }
    case ParquetLogicalType::JSON:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kString);
    case ParquetLogicalType::UUID:
      return std::make_shared<const PrimitiveType>(iceberg::TypeID::kUuid);
    case ParquetLogicalType::DECIMAL: {
      auto decimal_logical_type = std::static_pointer_cast<const parquet::DecimalLogicalType>(parquet_logical_type);
      const auto scale = decimal_logical_type->scale();
      const auto precision = decimal_logical_type->precision();
      return std::make_shared<iceberg::types::DecimalType>(precision, scale);
    }
    case ParquetLogicalType::NONE:
      return ConvertPhysicalType(column);
    default:
      break;
  }

  throw std::runtime_error("Not supported logical type " + parquet_logical_type->ToString() + " for column " +
                           column.name());
}

std::shared_ptr<iceberg::Schema> ParquetSchemaToIcebergSchema(const parquet::SchemaDescriptor& parquet_schema) {
  int num_columns = parquet_schema.num_columns();

  std::vector<iceberg::types::NestedField> fields;
  fields.reserve(num_columns);

  for (int i = 0; i < num_columns; ++i) {
    const parquet::ColumnDescriptor* column = parquet_schema.Column(i);
    if (!column) {
      throw std::runtime_error("No column with number " + std::to_string(i));
    }

    auto type = IceTypeFromColumn(*column);

    const parquet::schema::Node* node = parquet_schema.GetColumnRoot(i);
    iceberg::Ensure(node != nullptr, "Node for column " + std::to_string(i) + " is nullptr");

    bool is_repeated = node->logical_type() && node->logical_type()->is_list();

    if (is_repeated) {
      type = std::make_shared<iceberg::types::ListType>(node->field_id(), false, type);
    }

    iceberg::Ensure(node->field_id() != -1,
                    std::string(__PRETTY_FUNCTION__) + ": field id is not set in column '" + node->name() + "'");

    iceberg::types::NestedField ice_field{
        .name = node->name(), .field_id = node->field_id(), .is_required = node->is_required(), .type = type};
    fields.emplace_back(std::move(ice_field));
  }

  return std::make_shared<iceberg::Schema>(0, std::move(fields));
}

}  // namespace

iceberg::ice_tea::ScanMetadata FromFileUrl(const std::string& file_url,
                                           std::shared_ptr<iceberg::IFileSystemProvider> fs_provider) {
  iceberg::ice_tea::ScanMetadata meta;

  meta.schema = [&]() {
    iceberg::FileReaderProvider file_reader_provider(fs_provider);
    auto file_reader = iceberg::ValueSafe(file_reader_provider.Open(file_url));
    iceberg::Ensure(file_reader.get(), std::string(__PRETTY_FUNCTION__) + ": file_reader is nullptr");

    auto parquet_reader = file_reader->parquet_reader();
    iceberg::Ensure(parquet_reader, std::string(__PRETTY_FUNCTION__) + ": parquet_reader is nullptr");

    auto parquet_metadata = parquet_reader->metadata();
    iceberg::Ensure(parquet_metadata.get(), std::string(__PRETTY_FUNCTION__) + ": parquet_metadata is nullptr");

    auto parquet_schema = parquet_metadata->schema();
    iceberg::Ensure(parquet_schema, std::string(__PRETTY_FUNCTION__) + ": parquet_schema is nullptr");

    return ParquetSchemaToIcebergSchema(*parquet_schema);
  }();

  iceberg::ice_tea::DataEntry::Segment segment(4, 0);
  iceberg::ice_tea::DataEntry result(file_url, {segment});

  meta.partitions.emplace_back();
  meta.partitions.back().emplace_back();
  meta.partitions.back().back().data_entries_.emplace_back(std::move(result));
  return meta;
}

}  // namespace tea::meta::access
