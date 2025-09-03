#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/absl_log.h"
#include "absl/log/initialize.h"
#include "arrow/filesystem/localfs.h"
#include "grpcpp/grpcpp.h"
#include "grpcpp/security/server_credentials.h"
#include "grpcpp/server.h"
#include "grpcpp/server_builder.h"
#include "grpcpp/server_context.h"
#include "parquet/arrow/reader.h"

#include "teapot/teapot.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

struct EqualityDeleteInfo {
  std::string path;
  std::vector<int32_t> delete_field_ids;
};

class FileMatcher {
 public:
  bool Match(const std::filesystem::directory_entry& dir_entry) {
    return dir_entry.is_regular_file() && dir_entry.path().extension() == ".parquet";
  }
};

enum class SourceType { kProto, kDir };

template <typename Sink>
void AbslStringify(Sink& sink, SourceType type) {
  switch (type) {
    case SourceType::kProto:
      sink.Append(std::string_view("proto"));
      break;
    case SourceType::kDir:
      sink.Append(std::string_view("dir"));
      break;
  }
}

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
      // maybe it is undefined
      return teapot::FieldType::TYPE_BINARY;
    }
  } else {
    return teapot::FieldType::TYPE_UNDEFINED;
  }
}

static void FilterSegments(const int segment_id, const int segment_count, teapot::MetadataResponseResult& result) {
  std::vector<teapot::Fragment> fragments;

  for (int i = 0, end = result.fragments_size(); i != end; ++i) {
    if (segment_id == -1 || (i % segment_count) == segment_id) {
      fragments.push_back(result.fragments(i));
    }
  }
  result.clear_fragments();
  result.mutable_fragments()->Assign(fragments.begin(), fragments.end());
}

class TeapotImpl final : public teapot::Teapot::Service {
 public:
  TeapotImpl(SourceType source_type, const std::string& src_string)
      : source_type_(source_type), root_path_(src_string) {
    ABSL_LOG(INFO) << "Teapot(" << source_type << "): " << src_string;
  }

  Status GetMetadata(ServerContext* context, const teapot::MetadataRequest* request,
                     teapot::MetadataResponse* response) override {
    std::lock_guard lg(mutex_);
    ABSL_LOG(INFO) << "GetMetadata for table '" << request->table_id() << "' segment " << request->segment_id() << "/"
                   << request->segment_count();
    auto* result = response->mutable_result();

    if (auto it = cache_.find(request->table_id()); it != cache_.end()) {
      ABSL_LOG(INFO) << "Return cached result";
      result->CopyFrom(it->second);
      FilterSegments(request->segment_id(), request->segment_count(), *response->mutable_result());
      return Status::OK;
    }

    switch (source_type_) {
      case SourceType::kProto:
        MakeProtoResult(result);
        break;
      case SourceType::kDir:
        MakeDirResult(*request, result);
        break;
    }

    cache_[request->table_id()] = response->result();
    FilterSegments(request->segment_id(), request->segment_count(), *response->mutable_result());
    return Status::OK;
  }

  Status DeleteMetadata(ServerContext* context, const teapot::MetadataRequest* request,
                        teapot::MetadataResponse* response) override {
    std::lock_guard lg(mutex_);
    ABSL_LOG(INFO) << "DeleteMetadata for table '" << request->table_id() << "' segment " << request->segment_id()
                   << "/" << request->segment_count();

    cache_.erase(request->table_id());

    response->clear_result();
    return Status::OK;
  }

 private:
  std::mutex mutex_;
  SourceType source_type_;
  std::filesystem::path root_path_;
  std::unordered_map<std::string, teapot::MetadataResponseResult> cache_;

  bool MakeProtoResult(teapot::MetadataResponseResult* result) const {
    std::fstream is(std::string(root_path_.c_str()), std::ios::binary | std::ios::in);
    if (!result->ParseFromIstream(&is)) {
      ABSL_LOG(ERROR) << "Cannot parse proto from file '" << root_path_ << "'";
      return false;
    }

    return true;
  }

  // root_dir/<table_id>/*.parquet
  // root_dir/<table_id>/equality_deletes/*.parquet
  void MakeDirResult(const teapot::MetadataRequest& request, teapot::MetadataResponseResult* result) const {
    std::shared_ptr<arrow::fs::FileSystem> fs = std::make_shared<arrow::fs::LocalFileSystem>();

    std::filesystem::path base_path = root_path_ / request.table_id();

    if (!std::filesystem::exists(base_path)) {
      ABSL_LOG(ERROR) << "Directory " << base_path.string() << " does not exist";
      return;
    }
    if (!std::filesystem::is_directory(base_path)) {
      ABSL_LOG(ERROR) << base_path.string() << " expected to be directory";
      return;
    }

    std::vector<EqualityDeleteInfo> equality_deletes;
    std::filesystem::path eqdel_base_path = base_path / "equality_deletes";
    if (std::filesystem::exists(eqdel_base_path) && std::filesystem::is_directory(eqdel_base_path)) {
      for (const auto& entry : std::filesystem::directory_iterator{eqdel_base_path}) {
        std::error_code ec;
        auto path = std::filesystem::absolute(entry.path(), ec);
        if (ec) {
          ABSL_LOG(ERROR) << "Cannot get path for entry: " << entry << ". " << ec.message();
          return;
        }

        auto input_file = fs->OpenInputFile(path.string());
        if (!input_file.ok()) {
          ABSL_LOG(ERROR) << "Cannot open input file: " << path.string();
          return;
        }
        std::optional<EqualityDeleteInfo> eq_del_info = GetEqualityDeleteInfo(path.string(), *input_file, "file://");
        if (eq_del_info.has_value()) {
          equality_deletes.push_back(eq_del_info.value());
        }
      }
    }

    FileMatcher matcher;
    for (const auto& entry : std::filesystem::directory_iterator{base_path}) {
      if (matcher.Match(entry)) {
        ABSL_LOG(INFO) << "Matching entry: " << entry;

        std::error_code ec;
        auto path = std::filesystem::absolute(entry.path(), ec);
        if (ec) {
          ABSL_LOG(ERROR) << "Cannot get path for entry: " << entry << ". " << ec.message();
          return;
        }

        auto input_file = fs->OpenInputFile(path.string());
        if (!input_file.ok()) {
          ABSL_LOG(ERROR) << "Cannot open input file: " << path.string();
          continue;
        }
        AddFileFragments(path.string(), *input_file, result, "file://", equality_deletes);
      } else {
        ABSL_LOG(INFO) << "Not matching entry: " << entry;
      }
    }
  }

  static std::optional<EqualityDeleteInfo> GetEqualityDeleteInfo(const std::string& path,
                                                                 std::shared_ptr<arrow::io::RandomAccessFile> file,
                                                                 const std::string& uri) {
    parquet::arrow::FileReaderBuilder reader_builder;
    if (!reader_builder.Open(file, parquet::default_reader_properties()).ok()) {
      ABSL_LOG(ERROR) << "Cannot open parquet file: " << path;
      return std::nullopt;
    }
    reader_builder.memory_pool(arrow::default_memory_pool());
    reader_builder.properties(parquet::default_arrow_reader_properties());

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader = *reader_builder.Build();

    auto metadata = arrow_reader->parquet_reader()->metadata();
    EqualityDeleteInfo result;
    result.path = uri + path;
    auto num_columns = metadata->num_columns();
    for (int i = 0; i < num_columns; ++i) {
      result.delete_field_ids.push_back(metadata->schema()->group_node()->field(i)->field_id());
    }
    return result;
  }

  static bool AddFileFragments(const std::string& path, std::shared_ptr<arrow::io::RandomAccessFile> file,
                               teapot::MetadataResponseResult* result, const std::string& uri,
                               const std::vector<EqualityDeleteInfo>& equality_deletes = {}) {
    parquet::arrow::FileReaderBuilder reader_builder;
    if (!reader_builder.Open(file, parquet::default_reader_properties()).ok()) {
      ABSL_LOG(ERROR) << "Cannot open parquet file: " << path;
      return false;
    }
    reader_builder.memory_pool(arrow::default_memory_pool());
    reader_builder.properties(parquet::default_arrow_reader_properties());

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader = *reader_builder.Build();

    auto metadata = arrow_reader->parquet_reader()->metadata();
    auto num_row_groups = metadata->num_row_groups();
    ::teapot::Fragment* prev_fragment = nullptr;
    for (int i = 0; i < num_row_groups; ++i) {
      auto row_group = metadata->RowGroup(i);
      auto offset = row_group->file_offset();
      // row_group->num_rows();

      auto fragment = result->add_fragments();
      fragment->set_path(uri + path);
      fragment->set_position(offset);
      if (prev_fragment) {
        prev_fragment->set_length(offset - prev_fragment->position());
      }
      prev_fragment = fragment;
      for (const auto& eq_del : equality_deletes) {
        auto new_equality_delete = fragment->add_equality_deletes();
        new_equality_delete->set_path(eq_del.path);
        for (auto field_id : eq_del.delete_field_ids) {
          new_equality_delete->add_delete_field_ids(field_id);
        }
      }
#if 0
      if (i < (num_row_groups - 1)) {
        auto next_row_group = metadata->RowGroup(i + 1);
        fragment->set_length(next_row_group->file_offset() - offset);
      }
#endif
    }

    const parquet::SchemaDescriptor* schema = metadata->schema();
    auto num_columns = schema->num_columns();
    auto* result_schema = result->mutable_schema();
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

    return true;
  }
};

ABSL_FLAG(std::string, host, "0.0.0.0:50051", "host:port to connect to");
ABSL_FLAG(std::optional<std::string>, dir, std::nullopt, "directory with files");
ABSL_FLAG(std::optional<std::string>, proto, std::nullopt, "proto with response");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  absl::InitializeLog();

  std::string server_address = absl::GetFlag(FLAGS_host);
  auto dir = absl::GetFlag(FLAGS_dir);
  auto proto = absl::GetFlag(FLAGS_proto);

  int count = int(dir.has_value()) + int(proto.has_value());
  if (count != 1) {
    ABSL_LOG(ERROR) << "One of dir/uri/proto must be set. Actual args count: " << count;
    return 1;
  }

  std::unique_ptr<TeapotImpl> service;
  if (dir.has_value()) {
    service = std::make_unique<TeapotImpl>(SourceType::kDir, *dir);
  } else if (proto.has_value()) {
    service = std::make_unique<TeapotImpl>(SourceType::kProto, *proto);
  }

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(service.get());
  std::unique_ptr<Server> server(builder.BuildAndStart());
  ABSL_LOG(INFO) << "Server listening on " << server_address;
  server->Wait();
  return 0;
}
