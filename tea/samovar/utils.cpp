#include "tea/samovar/utils.h"

#include <arrow/result.h>
#include <arrow/status.h>
#include <iceberg/common/error.h>
#include <iceberg/common/rg_metadata.h>
#include <iceberg/streams/iceberg/data_entries_meta_stream.h>
#include <iceberg/streams/iceberg/plan.h>
#include <parquet/metadata.h>

#include <algorithm>
#include <cstdint>
#include <deque>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/common/fs/filesystem_provider.h"
#include "iceberg/common/fs/url.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/tea_scan.h"
#include "parquet/arrow/reader.h"

#include "tea/common/utils.h"
#include "tea/observability/return_fl.h"
#include "tea/observability/tea_log.h"
#include "tea/samovar/network_layer/samovar_client.h"
#include "tea/samovar/proto/samovar.pb.h"

namespace tea::samovar {
using std::literals::string_view_literals::operator""sv;

samovar::ContentType ConvertContentTypeToSamovar(iceberg::ContentFile::FileContent content) {
  switch (content) {
    case iceberg::ContentFile::FileContent::kData: {
      return samovar::ContentType::kData;
    }
    case iceberg::ContentFile::FileContent::kEqualityDeletes: {
      return samovar::ContentType::kEqualityDeletes;
    }
    case iceberg::ContentFile::FileContent::kPositionDeletes: {
      return samovar::ContentType::kPositionDeletes;
    }
    default:
      throw std::runtime_error("Incorrect file content");
  }
}

iceberg::ContentFile::FileContent ConvertSamovarContentTypeToContentType(samovar::ContentType content) {
  switch (content) {
    case samovar::ContentType::kData: {
      return iceberg::ContentFile::FileContent::kData;
    }
    case samovar::ContentType::kEqualityDeletes: {
      return iceberg::ContentFile::FileContent::kEqualityDeletes;
    }
    case samovar::ContentType::kPositionDeletes: {
      return iceberg::ContentFile::FileContent::kPositionDeletes;
    }
    default:
      throw std::runtime_error("Incorrect file content");
  }
}

samovar::ScanMetadata ConvertIcebergScanMetaToSamovarRepresentation(iceberg::ice_tea::ScanMetadata&& scan_metadata,
                                                                    const samovar::FileList& file_list) {
  auto search_file = [&](const std::string& file) -> size_t {
    return std::lower_bound(file_list.filenames().begin(), file_list.filenames().end(), file) -
           file_list.filenames().begin();
  };

  samovar::ScanMetadata result;
  *result.mutable_schema() = IcebergSchemaToTeapotSchema(scan_metadata.schema);
  for (const auto& partition : scan_metadata.partitions) {
    auto* samovar_partition = result.add_partitions();
    for (const auto& layer : partition) {
      auto* samovar_layer = samovar_partition->add_layers();
#if 0
      for (const auto& entry : layer.data_entries_) {
        auto* samovar_entry = samovar_layer->add_data_entries();
        for (const auto& part : entry.parts) {
          auto* segment = samovar_entry->add_segments();
          segment->set_length(part.length);
          segment->set_offset(part.offset);
        }
        auto file_index = search_file(entry.path);
        samovar_entry->mutable_entry()->set_file_index(file_index);
        samovar_entry->mutable_entry()->set_content_type(
            ConvertContentTypeToSamovar(iceberg::ContentFile::FileContent::kData));
      }
#endif
      for (const auto& equality_delete : layer.equality_delete_entries_) {
        auto* samovar_equality_delete = samovar_layer->add_equality_delete_entries();
        auto file_index = search_file(equality_delete.path);
        samovar_equality_delete->set_file_index(file_index);
        samovar_equality_delete->set_content_type(
            ConvertContentTypeToSamovar(iceberg::ContentFile::FileContent::kEqualityDeletes));
        for (auto field_id : equality_delete.field_ids) {
          samovar_equality_delete->add_equality_deletes_fields_ids(field_id);
        }
      }

      for (const auto& positional_delete : layer.positional_delete_entries_) {
        auto* samovar_positional_delete = samovar_layer->add_positional_delete_entries();
        auto file_index = search_file(positional_delete.path);
        samovar_positional_delete->set_file_index(file_index);
        samovar_positional_delete->set_content_type(
            ConvertContentTypeToSamovar(iceberg::ContentFile::FileContent::kPositionDeletes));
      }
    }
  }
  return result;
}

iceberg::ice_tea::ScanMetadata ConvertSamovarRepresentationToScanMeta(const samovar::ScanMetadata& scan_metadata,
                                                                      const samovar::FileList& file_list) {
  iceberg::ice_tea::ScanMetadata result;

  for (const auto& partition : scan_metadata.partitions()) {
    iceberg::ice_tea::ScanMetadata::Partition ice_partition;
    for (const auto& layer : partition.layers()) {
      iceberg::ice_tea::ScanMetadata::Layer ice_layer;
      for (const auto& equality_delete : layer.equality_delete_entries()) {
        std::string path = file_list.filenames()[equality_delete.file_index()];
        std::vector<int32_t> field_ids;
        for (auto field_id : equality_delete.equality_deletes_fields_ids()) {
          field_ids.push_back(field_id);
        }
        ice_layer.equality_delete_entries_.emplace_back(std::move(path), std::move(field_ids));
      }

      for (const auto& positional_delete : layer.positional_delete_entries()) {
        std::string path = file_list.filenames()[positional_delete.file_index()];
        ice_layer.positional_delete_entries_.emplace_back(std::move(path));
      }
      ice_partition.push_back(std::move(ice_layer));
    }
    result.partitions.push_back(std::move(ice_partition));
  }

  result.schema = TeapotSchemaToIcebergSchema(scan_metadata.schema());

  return result;
}

std::string SerializeCompactDataEntry(const iceberg::ice_tea::DataEntry& initial_entry) {
  std::string result = "[";
  for (auto segment : initial_entry.parts) {
    result += "(" + std::to_string(segment.offset) + "," + std::to_string(segment.length) + "),";
  }
  result.pop_back();
  result += "]";
  return result;
}

void LogSplitValues(const iceberg::ice_tea::DataEntry& initial_entry, const std::vector<int64_t>& row_group_offsets,
                    const std::vector<iceberg::ice_tea::DataEntry>& result) {
  std::string serialized_offsets = "[";
  for (auto offset : row_group_offsets) {
    serialized_offsets += std::to_string(offset) + ",";
  }
  serialized_offsets.pop_back();
  serialized_offsets += "]";

  auto serialized_initial_entry = SerializeCompactDataEntry(initial_entry);
  std::string serialized_result_entries = "[";
  for (const auto& entry : result) {
    serialized_result_entries += SerializeCompactDataEntry(entry) + ",";
  }
  serialized_result_entries.pop_back();
  serialized_result_entries += "]";
  TEA_LOG("Row group offsets: " + serialized_offsets + ", initial data entry " + serialized_initial_entry +
          ", result representation " + serialized_result_entries);
}

std::vector<iceberg::ice_tea::DataEntry> SplitFileBySplitOffsets(iceberg::ice_tea::DataEntry initial_entry,
                                                                 const std::vector<int64_t>& row_group_offsets) {
  std::sort(initial_entry.parts.begin(), initial_entry.parts.end(), [](const auto& lhs, const auto& rhs) {
    return std::tie(lhs.offset, lhs.length) < std::tie(rhs.offset, rhs.length);
  });
  std::vector<iceberg::ice_tea::DataEntry> result;

  int iter_offsets = 0;
  for (auto segment : initial_entry.parts) {
    std::vector<int64_t> offsets_by_segment;
    while (iter_offsets < static_cast<int>(row_group_offsets.size()) &&
           (segment.length == 0 || row_group_offsets[iter_offsets] < segment.offset + segment.length)) {
      if (row_group_offsets[iter_offsets] > segment.offset) {
        offsets_by_segment.push_back(row_group_offsets[iter_offsets]);
      }
      iter_offsets++;
    }

    if (offsets_by_segment.empty()) {
      iceberg::ice_tea::DataEntry current_entry = initial_entry;
      current_entry.parts = {segment};
      result.push_back(current_entry);
      continue;
    }

    for (size_t i = 0; i < offsets_by_segment.size(); ++i) {
      iceberg::ice_tea::DataEntry current_entry = initial_entry;
      current_entry.parts.clear();
      if (i == 0) {
        current_entry.parts.emplace_back(segment.offset, offsets_by_segment[i] - segment.offset);
      } else {
        current_entry.parts.emplace_back(offsets_by_segment[i - 1], offsets_by_segment[i] - offsets_by_segment[i - 1]);
      }
      result.push_back(current_entry);
    }

    iceberg::ice_tea::DataEntry final_entry = initial_entry;
    final_entry.parts.clear();
    if (segment.length == 0) {
      final_entry.parts.emplace_back(offsets_by_segment.back(), 0);
    } else {
      final_entry.parts.emplace_back(offsets_by_segment.back(),
                                     segment.offset + segment.length - offsets_by_segment.back());
    }
    result.push_back(final_entry);
  }
  LogSplitValues(initial_entry, row_group_offsets, result);
  return result;
}

arrow::Result<std::vector<iceberg::ice_tea::DataEntry>> SplitFile(
    const iceberg::ice_tea::DataEntry& initial_entry, const Config& config,
    std::shared_ptr<iceberg::IFileSystemProvider> fs_provider) {
  const auto& url = initial_entry.path;
  auto components = iceberg::SplitUrl(url);
  std::string path;
  ARROW_ASSIGN_OR_RAISE(auto fs, fs_provider->GetFileSystem(url));

  if (components.schema == "s3a"sv || components.schema == "s3"sv) {
    path = absl::StrCat(std::string(components.location), std::string(components.path));
  } else if (components.schema == "file"sv) {
    path = components.path;
  } else {
    return ::arrow::Status::ExecutionError("unknown fs prefix for file: ", url);
  }

  ARROW_ASSIGN_OR_RAISE(auto input_file, fs->OpenInputFile(path));

  auto props = parquet::default_reader_properties();
  auto arrow_props = parquet::default_arrow_reader_properties();

  parquet::arrow::FileReaderBuilder reader_builder;
  RETURN_FL_NOT_OK_MSG(reader_builder.Open(input_file, props), url);
  reader_builder.memory_pool(arrow::default_memory_pool());
  reader_builder.properties(arrow_props);

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  ARROW_ASSIGN_OR_RAISE(arrow_reader, reader_builder.Build());
  auto parquet_metadata = arrow_reader->parquet_reader()->metadata();

  std::vector<int64_t> row_group_offsets;
  row_group_offsets.reserve(arrow_reader->num_row_groups());
  for (int i = 0; i < arrow_reader->num_row_groups(); ++i) {
    auto row_group = parquet_metadata->RowGroup(i);
    iceberg::Ensure(row_group.get(), std::string(__PRETTY_FUNCTION__) + ": no RowGroup '" + std::to_string(i) + "'");
    auto row_group_offset = iceberg::RowGroupMetaToFileOffset(*row_group);

    row_group_offsets.push_back(row_group_offset);
  }

  std::sort(row_group_offsets.begin(), row_group_offsets.end());

  return SplitFileBySplitOffsets(initial_entry, row_group_offsets);
}

arrow::Result<SplitResult> SplitPartitions(const std::vector<iceberg::ice_tea::ScanMetadata::Partition>& partitions,
                                           const Config& config) {
  SplitResult result;
  std::vector<std::string> all_files;

  for (size_t partition_id = 0; partition_id < partitions.size(); ++partition_id) {
    for (size_t layer_id = 0; layer_id < partitions[partition_id].size(); ++layer_id) {
      for (const auto& data_file : partitions[partition_id][layer_id].data_entries_) {
        all_files.push_back(data_file.path);
      }

      for (const auto& delete_file : partitions[partition_id][layer_id].equality_delete_entries_) {
        all_files.push_back(delete_file.path);
      }

      for (const auto& delete_file : partitions[partition_id][layer_id].positional_delete_entries_) {
        all_files.push_back(delete_file.path);
      }
    }
  }

  std::sort(all_files.begin(), all_files.end());
  all_files.erase(std::unique(all_files.begin(), all_files.end()), all_files.end());

  for (const auto& data_filename : all_files) {
    result.file_list.add_filenames(data_filename);
  }

  auto search_file_index = [&](const std::string& file_name) -> size_t {
    return std::lower_bound(all_files.begin(), all_files.end(), file_name) - all_files.begin();
  };

  for (size_t partition_id = 0; partition_id < partitions.size(); ++partition_id) {
    for (size_t layer_id = 0; layer_id < partitions[partition_id].size(); ++layer_id) {
      auto layer = partitions[partition_id][layer_id];

      std::sort(layer.data_entries_.begin(), layer.data_entries_.end(),
                [&](const auto& lhs, const auto& rhs) { return lhs.path < rhs.path; });

      for (size_t data_entry_id = 0; data_entry_id < layer.data_entries_.size(); ++data_entry_id) {
        const auto& data_entry = layer.data_entries_[data_entry_id];
        switch (config.samovar_config.split_type) {
          case SplitType::kOffsets: {
            for (auto entry_segment : data_entry.parts) {
              samovar::AnnotatedDataEntry additional_entry;
              additional_entry.set_layer_id(layer_id);
              additional_entry.set_partition_id(partition_id);
              size_t data_file_index = search_file_index(data_entry.path);
              additional_entry.mutable_data_entry()->mutable_entry()->set_file_index(data_file_index + 1);

              auto segment = additional_entry.mutable_data_entry()->add_segments();
              segment->set_length(entry_segment.length);
              segment->set_offset(entry_segment.offset);
              result.data_entries.push_back(std::move(additional_entry));
            }
            break;
          }
          case SplitType::kWholeDataEntry: {
            samovar::AnnotatedDataEntry additional_entry;
            additional_entry.set_layer_id(layer_id);
            additional_entry.set_partition_id(partition_id);
            size_t data_file_index = search_file_index(data_entry.path);
            additional_entry.mutable_data_entry()->mutable_entry()->set_file_index(data_file_index + 1);
            for (auto entry_segment : data_entry.parts) {
              auto segment = additional_entry.mutable_data_entry()->add_segments();
              segment->set_length(entry_segment.length);
              segment->set_offset(entry_segment.offset);
            }
            result.data_entries.push_back(std::move(additional_entry));
            break;
          }
        }
      }
    }
  }
  return result;
}

iceberg::AnnotatedDataPath ConvertSamovarAnnotatedDataEntryToAnnotatedDataEntry(
    const samovar::AnnotatedDataEntry& additional_data_entry, const samovar::FileList& file_list) {
  int partition_id = additional_data_entry.partition_id();
  int layer_id = additional_data_entry.layer_id();
  std::string path;
  if (additional_data_entry.data_entry().entry().file_index() > 0) {
    path = file_list.filenames().at(additional_data_entry.data_entry().entry().file_index() - 1);
  } else {
    path = additional_data_entry.data_entry().entry().file_path();
  }
  std::vector<iceberg::AnnotatedDataPath::Segment> segments;
  for (auto segment : additional_data_entry.data_entry().segments()) {
    segments.emplace_back(segment.offset(), segment.length());
  }
  return iceberg::AnnotatedDataPath(
      iceberg::PartitionLayerFile(iceberg::PartitionLayer(partition_id, layer_id), std::move(path)),
      std::move(segments));
}

std::string MakeSessionIdentifier(const TableSource& source, const std::string& cluster_id,
                                  const std::string& session_id, const std::string& uuid, int slice_id,
                                  int scan_identifier, bool fdw_mode) {
  std::string table_id;
  {
    if (auto teapot_table = std::get_if<TeapotTable>(&source)) {
      table_id = teapot_table->table_id.ToString();
    } else if (auto file_table = std::get_if<FileTable>(&source)) {
      table_id = file_table->url;
    } else if (auto iceberg_table = std::get_if<IcebergTable>(&source)) {
      table_id = iceberg_table->table_id.ToString();
    }
  }
  std::string query_id = uuid;
  if (!fdw_mode) {
    query_id = "0";
  }
  static constexpr const std::string_view protocol_version = TEA_VERSION;
  return "/tea/" + std::string(protocol_version) + "/" + query_id + "/" + cluster_id + "/" + table_id + "/" +
         session_id + "/" + std::to_string(slice_id) + "/" + std::to_string(scan_identifier);
}

bool ContainsPositionalDeletes(const samovar::ScanMetadata& scan_metadata) {
  for (const auto& delete_metadata : scan_metadata.partitions()) {
    for (const auto& layer : delete_metadata.layers()) {
      if (layer.positional_delete_entries_size() > 0) {
        return true;
      }
    }
  }
  return false;
}

template <typename Element>
void SendArray(const std::shared_ptr<ISamovarClient> client, const std::vector<Element>& elements,
               const std::string& queue_id, std::chrono::seconds ttl_seconds, uint32_t batch_size) {
  batch_size = std::max(batch_size, 1u);

  auto push = [&client, &queue_id, &ttl_seconds, ttl_updated = false](std::vector<std::string>&& entries) mutable {
    if (entries.empty()) {
      return;
    }

    client->PushQueue(queue_id, entries);

    if (!ttl_updated) {
      ttl_updated = true;
      client->UpdateTTL(queue_id, ttl_seconds);
    }
  };

  std::vector<std::string> serialized_entries;
  for (const auto& elem : elements) {
    auto serialized = elem.SerializeAsString();
    serialized_entries.emplace_back(std::move(serialized));

    if (serialized_entries.size() == batch_size) {
      push(std::move(serialized_entries));
      serialized_entries.clear();
    }
  }

  push(std::move(serialized_entries));
}

void SendDataEntries(const std::shared_ptr<ISamovarClient> client,
                     const std::vector<samovar::AnnotatedDataEntry>& additional_data_entries,
                     const std::string& queue_id, std::chrono::seconds ttl_seconds, uint32_t batch_size) {
  SendArray(client, additional_data_entries, queue_id, ttl_seconds, batch_size);
}

void SendManifestLists(const std::shared_ptr<ISamovarClient> client,
                       const std::vector<samovar::ManifestList>& manifests, const std::string& queue_id,
                       std::chrono::seconds ttl_seconds, uint32_t batch_size) {
  SendArray(client, manifests, queue_id, ttl_seconds, batch_size);
}

samovar::ScanMetadata ClearDataEntries(const samovar::ScanMetadata& scan_metadata) {
  samovar::ScanMetadata result;
  *result.mutable_schema() = scan_metadata.schema();
  result.set_use_distributed_metadata_processing(scan_metadata.use_distributed_metadata_processing());
  for (int i = 0; i < scan_metadata.partitions_size(); ++i) {
    const auto& partition = scan_metadata.partitions()[i];
    auto* new_partititon = result.add_partitions();
    for (int j = 0; j < partition.layers_size(); ++j) {
      const auto& layer = partition.layers()[j];
      auto* new_layer = new_partititon->add_layers();

      for (const auto& positional_delete : layer.positional_delete_entries()) {
        new_layer->add_positional_delete_entries()->CopyFrom(positional_delete);
      }

      for (const auto& equality_delete : layer.equality_delete_entries()) {
        new_layer->add_equality_delete_entries()->CopyFrom(equality_delete);
      }
    }
  }
  return result;
}

std::vector<samovar::ManifestList> ConvertToSamovarManifestLists(const std::deque<iceberg::ManifestFile>& manifests) {
  std::vector<samovar::ManifestList> result;
  result.reserve(manifests.size());

  for (const auto& elem : manifests) {
    samovar::ManifestList new_entry;
    new_entry.set_file_path(elem.path);

    result.emplace_back(std::move(new_entry));
  }

  return result;
}

}  // namespace tea::samovar
