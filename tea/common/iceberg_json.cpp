#include "tea/common/iceberg_json.h"

#include <cassert>
#include <cstdint>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/manifest_entry.h"
#include "iceberg/tea_scan.h"
#include "iceberg/type.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace tea {

namespace {

constexpr std::string_view kFilePathField = "file_path";
constexpr std::string_view kContentField = "content";
constexpr std::string_view kEqualityIdsField = "equality_ids";
constexpr std::string_view kIdField = "id";
constexpr std::string_view kIsRequiredField = "is_required";
constexpr std::string_view kNameField = "name";
constexpr std::string_view kTypeField = "type";
constexpr std::string_view kFieldsField = "type";
constexpr std::string_view kSchemaField = "schema";
constexpr std::string_view kScanMetadata = "scan_metadata";
constexpr std::string_view kScanMetadataIdentifier = "scan_metadata_identifier";
constexpr std::string_view kPartitionsField = "partitions";
constexpr std::string_view kDataEntriesField = "data_entries";
constexpr std::string_view kPosDelEntriesField = "posdel_entries";
constexpr std::string_view kEqDelEntriesField = "eqdel_entries";
constexpr std::string_view kElementIdField = "element_id";
constexpr std::string_view kElementRequiredField = "element_required";
constexpr std::string_view kElementField = "element";
constexpr std::string_view kManifestEntry = "manifest_entry";
constexpr std::string_view kTaskParts = "task_parts";
constexpr std::string_view kSegmentOffset = "offset";
constexpr std::string_view kSegmentLength = "length";
constexpr std::string_view kList = "list";

class Serializer {
 public:
  using Allocator = rapidjson::MemoryPoolAllocator<rapidjson::CrtAllocator>;

  rapidjson::Value Serialize(ScanMetadataMessage&& scan_metadata);

  rapidjson::Value Serialize(iceberg::ice_tea::ScanMetadata&& scan_metadata);

  rapidjson::Value Serialize(iceberg::Schema&& schema);

  rapidjson::Value Serialize(iceberg::types::NestedField&& field);

  rapidjson::Value Serialize(std::shared_ptr<const iceberg::types::Type> data_type);

  rapidjson::Value Serialize(iceberg::ice_tea::ScanMetadata::Layer&& layer);

  rapidjson::Value Serialize(iceberg::ice_tea::DataEntry::Segment&& segment);

  rapidjson::Value Serialize(iceberg::ice_tea::DataEntry&& task);

  rapidjson::Value Serialize(iceberg::ice_tea::PositionalDeleteInfo&& entry);

  rapidjson::Value Serialize(iceberg::ice_tea::EqualityDeleteInfo&& entry);

  template <typename T>
  rapidjson::Value Serialize(std::vector<T>&& values);

  rapidjson::Value Serialize(std::string&& str) {
    rapidjson::Value result(str.c_str(), allocator_);
    str.clear();
    return result;
  }

  rapidjson::Value Serialize(int64_t value) { return rapidjson::Value(value); }

  rapidjson::Value Serialize(int32_t value) { return rapidjson::Value(value); }

  rapidjson::Value Serialize(bool value) { return rapidjson::Value(value); }

  template <typename T>
  void AddMember(rapidjson::Value& result, std::string_view name, T&& value) {
    result.AddMember(rapidjson::Value(name.data(), allocator_), Serialize(std::forward<T>(value)), allocator_);
  }

 private:
  Allocator allocator_;
};

template <typename T>
rapidjson::Value Serializer::Serialize(std::vector<T>&& values) {
  rapidjson::Value result(rapidjson::kArrayType);
  for (auto&& value : values) {
    result.PushBack(Serialize(std::move(value)), allocator_);
  }
  return result;
}

rapidjson::Value Serializer::Serialize(iceberg::ice_tea::DataEntry::Segment&& segment) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddMember(result, kSegmentOffset, segment.offset);
  AddMember(result, kSegmentLength, segment.length);
  return result;
}

rapidjson::Value Serializer::Serialize(iceberg::ice_tea::DataEntry&& task) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddMember(result, kFilePathField, std::move(task.path));
  AddMember(result, kTaskParts, std::move(task.parts));
  return result;
}

rapidjson::Value Serializer::Serialize(iceberg::ice_tea::PositionalDeleteInfo&& task) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddMember(result, kFilePathField, std::move(task.path));
  return result;
}

rapidjson::Value Serializer::Serialize(iceberg::ice_tea::EqualityDeleteInfo&& task) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddMember(result, kFilePathField, std::move(task.path));
  AddMember(result, kEqualityIdsField, std::move(task.field_ids));
  return result;
}

rapidjson::Value Serializer::Serialize(std::shared_ptr<const iceberg::types::Type> data_type) {
  auto typeId = data_type->TypeId();

  if (iceberg::types::IsDecimalType(typeId)) {
    auto decimal_type = std::static_pointer_cast<const iceberg::types::DecimalType>(data_type);
    std::string str =
        "decimal(" + std::to_string(decimal_type->Precision()) + ", " + std::to_string(decimal_type->Scale()) + ")";
    return rapidjson::Value(str.c_str(), allocator_);
  } else if (data_type->IsPrimitiveType()) {
    std::optional<std::string> maybe_str = iceberg::types::PrimitiveTypeToName(typeId);
    if (!maybe_str.has_value()) {
      throw std::runtime_error("SerializeDataType: unknown type '" + data_type->ToString() + "'");
    }
    return rapidjson::Value(maybe_str->c_str(), allocator_);
  } else if (data_type->IsListType()) {
    auto list_type = std::static_pointer_cast<const iceberg::types::ListType>(data_type);
    rapidjson::Value result(rapidjson::kObjectType);
    AddMember(result, kElementIdField, list_type->ElementId());
    AddMember(result, kElementRequiredField, list_type->ElementRequired());
    AddMember(result, kElementField, list_type->ElementType());
    AddMember(result, kTypeField, std::string(kList));
    return result;
  }
  throw std::runtime_error("SerializeDataType: unknown type");
}

rapidjson::Value Serializer::Serialize(iceberg::types::NestedField&& field) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddMember(result, kIdField, field.field_id);
  AddMember(result, kIsRequiredField, field.is_required);
  AddMember(result, kNameField, std::move(field.name));
  AddMember(result, kTypeField, field.type);
  return result;
}

rapidjson::Value Serializer::Serialize(iceberg::Schema&& schema) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddMember(result, kIdField, schema.SchemaId());
  auto schema_columns = schema.Columns();
  AddMember(result, kFieldsField, std::move(schema_columns));
  return result;
}

rapidjson::Value Serializer::Serialize(iceberg::ice_tea::ScanMetadata::Layer&& layer) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddMember(result, kDataEntriesField, std::move(layer.data_entries_));
  AddMember(result, kPosDelEntriesField, std::move(layer.positional_delete_entries_));
  AddMember(result, kEqDelEntriesField, std::move(layer.equality_delete_entries_));
  return result;
}

rapidjson::Value Serializer::Serialize(iceberg::ice_tea::ScanMetadata&& scan_metadata) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddMember(result, kSchemaField, std::move(*scan_metadata.schema));
  AddMember(result, kPartitionsField, std::move(scan_metadata.partitions));
  return result;
}

rapidjson::Value Serializer::Serialize(ScanMetadataMessage&& scan_metadata) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddMember(result, kScanMetadata, std::move(scan_metadata.scan_metadata));
  AddMember(result, kScanMetadataIdentifier, std::move(scan_metadata.scan_metadata_identifier));
  return result;
}

void Ensure(bool condition, const std::string& error_message) {
  if (!condition) {
    throw std::runtime_error(error_message);
  }
}

// TODO(gmusya): consider using __PRETTY_FUNCTION__ for error messages
template <typename T>
T Deserialize(const rapidjson::Value& document) {
  Ensure(document.Is<T>(), "Deserialize (primitive): wrong type");
  return document.Get<T>();
}

// clang-format off
template <typename T>
T Deserialize(const rapidjson::Value& document)
requires(std::is_same_v<std::vector<typename T::value_type>, T>) {
  Ensure(document.IsArray(), "Deserialize (array): wrong type");

  T result;
  for (const auto& element : document.GetArray()) {
    result.emplace_back(Deserialize<typename T::value_type>(element));
  }
  return result;
}
// clang-format on

template <>
std::string Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsString(), "Deserialize (string): wrong type");
  return std::string(document.GetString(), document.GetStringLength());
}

template <typename T>
T Extract(const rapidjson::Value& document, std::string_view field_name) {
  const char* c_str = field_name.data();
  Ensure(document.HasMember(c_str), "ExtractPrimitive: !document.HasMember(" + std::string(field_name) + ")");
  return Deserialize<T>(document[c_str]);
}

template <>
std::shared_ptr<const iceberg::types::Type> Deserialize(const rapidjson::Value& value) {
  if (value.IsString()) {
    std::string str = value.GetString();
    if (auto maybe_value = iceberg::types::NameToPrimitiveType(str); maybe_value.has_value()) {
      return std::make_shared<iceberg::types::PrimitiveType>(maybe_value.value());
    }
    if (str.starts_with("decimal")) {
      // decimal(P, S)
      std::stringstream ss(str);
      ss.ignore(std::string("decimal(").size());
      int32_t precision = -1;
      int32_t scale = -1;
      ss >> precision;
      ss.ignore(1);  // skip comma
      ss >> scale;
      return std::make_shared<iceberg::types::DecimalType>(precision, scale);
    }
    throw std::runtime_error("JsonToDataType: unknown type '" + str + "'");
  }
  if (value.IsObject()) {
    std::string type = Extract<std::string>(value, kTypeField);
    if (type == kList) {
      int32_t element_field_id = Extract<int32_t>(value, kElementIdField);
      bool element_required = Extract<bool>(value, kElementRequiredField);
      auto element_type = Extract<std::shared_ptr<const iceberg::types::Type>>(value, kElementField);
      return std::make_shared<iceberg::types::ListType>(element_field_id, element_required, element_type);
    }
  }
  throw std::runtime_error("Unknown type");
}

template <>
iceberg::ice_tea::DataEntry::Segment Deserialize(const rapidjson::Value& value) {
  Ensure(value.IsObject(), "Deserialize (Task::Segment): wrong type");

  int64_t offset = Extract<int64_t>(value, kSegmentOffset);
  int64_t length = Extract<int64_t>(value, kSegmentLength);
  return iceberg::ice_tea::DataEntry::Segment(offset, length);
}

template <>
iceberg::ice_tea::DataEntry Deserialize(const rapidjson::Value& value) {
  Ensure(value.IsObject(), "Deserialize (Task): wrong type");

  auto path = Extract<std::string>(value, kFilePathField);
  auto parts = Extract<std::vector<iceberg::ice_tea::DataEntry::Segment>>(value, kTaskParts);
  return iceberg::ice_tea::DataEntry(std::move(path), std::move(parts));
}

template <>
iceberg::ice_tea::ScanMetadata::Layer Deserialize(const rapidjson::Value& value) {
  Ensure(value.IsObject(), "Deserialize (Layer): wrong type");

  iceberg::ice_tea::ScanMetadata::Layer result;
  result.data_entries_ = Extract<decltype(result.data_entries_)>(value, kDataEntriesField);
  result.positional_delete_entries_ = Extract<decltype(result.positional_delete_entries_)>(value, kPosDelEntriesField);
  result.equality_delete_entries_ = Extract<decltype(result.equality_delete_entries_)>(value, kEqDelEntriesField);

  return result;
}

template <>
iceberg::ice_tea::PositionalDeleteInfo Deserialize(const rapidjson::Value& value) {
  Ensure(value.IsObject(), "Deserialize (PositionalDeleteInfo): wrong type");

  auto path = Extract<std::string>(value, kFilePathField);
  return iceberg::ice_tea::PositionalDeleteInfo(std::move(path));
}

template <>
iceberg::ice_tea::EqualityDeleteInfo Deserialize(const rapidjson::Value& value) {
  Ensure(value.IsObject(), "Deserialize (EqualityDeleteInfo): wrong type");

  auto path = Extract<std::string>(value, kFilePathField);
  auto field_ids = Extract<std::vector<int32_t>>(value, kEqualityIdsField);
  return iceberg::ice_tea::EqualityDeleteInfo(std::move(path), std::move(field_ids));
}

template <>
iceberg::types::NestedField Deserialize(const rapidjson::Value& value) {
  Ensure(value.IsObject(), "Deserialize (Field): wrong type");

  iceberg::types::NestedField result;
  result.field_id = Extract<int32_t>(value, kIdField);
  result.name = Extract<std::string>(value, kNameField);
  result.is_required = Extract<bool>(value, kIsRequiredField);
  result.type = Extract<std::shared_ptr<const iceberg::types::Type>>(value, kTypeField);
  return result;
}

template <>
iceberg::Schema Deserialize(const rapidjson::Value& value) {
  Ensure(value.IsObject(), "Deserialize (Schema): wrong type");

  int32_t schema_id = Extract<int32_t>(value, kIdField);
  auto fields = Extract<std::vector<iceberg::types::NestedField>>(value, kFieldsField);
  return iceberg::Schema(schema_id, std::move(fields));
}

template <>
iceberg::ice_tea::ScanMetadata Deserialize(const rapidjson::Value& value) {
  Ensure(value.IsObject(), "Deserialize (ScanMetadata): wrong type");

  auto schema = std::make_shared<iceberg::Schema>(Extract<iceberg::Schema>(value, kSchemaField));
  auto partitions = Extract<std::vector<iceberg::ice_tea::ScanMetadata::Partition>>(value, kPartitionsField);
  return iceberg::ice_tea::ScanMetadata{.schema = std::move(schema), .partitions = std::move(partitions)};
}

template <>
ScanMetadataMessage Deserialize(const rapidjson::Value& value) {
  Ensure(value.IsObject(), "Deserialize (ScanMetadataMessage): wrong type");

  auto scan_metadata = Extract<iceberg::ice_tea::ScanMetadata>(value, kScanMetadata);
  auto scan_metadata_identifier = Extract<std::string>(value, kScanMetadataIdentifier);
  return ScanMetadataMessage{.scan_metadata = std::move(scan_metadata),
                             .scan_metadata_identifier = std::move(scan_metadata_identifier)};
}

}  // namespace

std::string ScanMetadataToJSONString(ScanMetadataMessage&& scan_metadata) {
  Serializer serializer;
  auto json_result = serializer.Serialize(std::move(scan_metadata));
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  json_result.Accept(writer);
  return std::string(buffer.GetString(), buffer.GetLength());
}

ScanMetadataMessage JSONStringToScanMetadata(const std::string& data) {
  rapidjson::Document document;
  document.Parse(data.c_str(), data.length());

  return Deserialize<ScanMetadataMessage>(document);
}

}  // namespace tea
