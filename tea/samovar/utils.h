#pragma once

#include <iceberg/streams/iceberg/data_entries_meta_stream.h>

#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "iceberg/common/fs/filesystem_provider.h"
#include "iceberg/tea_scan.h"

#include "tea/common/config.h"
#include "tea/metadata/metadata.h"
#include "tea/samovar/network_layer/samovar_client.h"
#include "tea/samovar/proto/samovar.pb.h"

namespace tea::samovar {

samovar::ScanMetadata ConvertIcebergScanMetaToSamovarRepresentation(iceberg::ice_tea::ScanMetadata&& scan_metadata,
                                                                    const samovar::FileList& file_list);

iceberg::ice_tea::ScanMetadata ConvertSamovarRepresentationToScanMeta(const samovar::ScanMetadata& scan_metadata,
                                                                      const samovar::FileList& file_list);

struct SplitResult {
  std::vector<samovar::AnnotatedDataEntry> data_entries;
  samovar::FileList file_list;
};

std::vector<iceberg::ice_tea::DataEntry> SplitFileBySplitOffsets(iceberg::ice_tea::DataEntry initial_entry,
                                                                 const std::vector<int64_t>& row_group_offsets);

arrow::Result<SplitResult> SplitPartitions(const std::vector<iceberg::ice_tea::ScanMetadata::Partition>& partitions,
                                           const Config& config);

std::vector<samovar::ManifestList> ConvertToSamovarManifestLists(const std::deque<iceberg::ManifestFile>& manifest);

void SendManifestLists(const std::shared_ptr<ISamovarClient> client,
                       const std::vector<samovar::ManifestList>& manifests, const std::string& queue_id,
                       std::chrono::seconds ttl_seconds, uint32_t batch_size);

iceberg::AnnotatedDataPath ConvertSamovarAnnotatedDataEntryToAnnotatedDataEntry(
    const samovar::AnnotatedDataEntry& additional_data_entry, const samovar::FileList& file_list);

std::string MakeSessionIdentifier(const TableSource& source, const std::string& cluster_id,
                                  const std::string& session_id, const std::string& uuid, int slice_id,
                                  int scan_identifier, bool fdw_mode);

bool ContainsPositionalDeletes(const samovar::ScanMetadata& scan_metadata);

void SendDataEntries(const std::shared_ptr<ISamovarClient> client,
                     const std::vector<samovar::AnnotatedDataEntry>& additional_data_entries,
                     const std::string& queue_id, std::chrono::seconds ttl_seconds, uint32_t batch_size);

samovar::ScanMetadata ClearDataEntries(const samovar::ScanMetadata& scan_metadata);

}  // namespace tea::samovar
