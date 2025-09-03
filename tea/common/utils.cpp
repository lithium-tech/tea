#include "tea/common/utils.h"

#include <arrow/result.h>
#include <arrow/status.h>
#include <iceberg/streams/iceberg/plan.h>

#include <algorithm>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "iceberg/manifest_entry.h"
#include "iceberg/nested_field.h"
#include "iceberg/tea_scan.h"
#include "iceberg/type.h"

#include "tea/smoke_test/fragment_info.h"
#include "teapot/teapot.pb.h"

namespace tea {
namespace {
void IcebergTypeToTeapotType(std::shared_ptr<const iceberg::types::Type> type, teapot::Field* field) {
  using IceTypeId = iceberg::TypeID;
  using iceberg::types::PrimitiveType;
  using teapot::FieldType;
  switch (type->TypeId()) {
    case IceTypeId::kBoolean: {
      field->set_type(FieldType::TYPE_BOOLEAN);
      break;
    }
    case IceTypeId::kInt: {
      field->set_type(FieldType::TYPE_INTEGER);
      break;
    }
    case IceTypeId::kLong: {
      field->set_type(FieldType::TYPE_LONG);
      break;
    }
    case IceTypeId::kFloat: {
      field->set_type(FieldType::TYPE_FLOAT);
      break;
    }
    case IceTypeId::kDouble: {
      field->set_type(FieldType::TYPE_DOUBLE);
      break;
    }
    case IceTypeId::kDate: {
      field->set_type(FieldType::TYPE_DATE);
      break;
    }
    case IceTypeId::kTime: {
      field->set_type(FieldType::TYPE_TIME);
      break;
    }
    case IceTypeId::kTimestamp: {
      field->set_type(FieldType::TYPE_TIMESTAMP);
      break;
    }
    case IceTypeId::kString: {
      field->set_type(FieldType::TYPE_STRING);
      break;
    }
    case IceTypeId::kUuid: {
      field->set_type(FieldType::TYPE_UUID);
      break;
    }
    case IceTypeId::kBinary: {
      field->set_type(FieldType::TYPE_BINARY);
      break;
    }
    case IceTypeId::kTimestamptz: {
      field->set_type(FieldType::TYPE_TIMESTAMPTZ);
      break;
    }
    case IceTypeId::kDecimal: {
      field->set_type(FieldType::TYPE_DECIMAL);
      auto primitive_ptr =
          std::dynamic_pointer_cast<iceberg::types::PrimitiveType>(std::const_pointer_cast<iceberg::types::Type>(type));
      field->set_precision(std::dynamic_pointer_cast<iceberg::types::DecimalType>(primitive_ptr)->Precision());
      field->set_scale(std::dynamic_pointer_cast<iceberg::types::DecimalType>(primitive_ptr)->Scale());
      break;
    }
    case IceTypeId::kList: {
      field->set_repeated(true);
      auto list_ptr =
          std::dynamic_pointer_cast<iceberg::types::ListType>(std::const_pointer_cast<iceberg::types::Type>(type));

      IcebergTypeToTeapotType(list_ptr->ElementType(), field);
      break;
    }
    default:
      throw std::runtime_error("Unexpected type " + std::to_string(static_cast<int>(type->TypeId())));
  }
}
}  // namespace

teapot::Schema IcebergSchemaToTeapotSchema(const std::shared_ptr<iceberg::Schema>& schema) {
  teapot::Schema result;
  for (const auto& column : schema->Columns()) {
    auto* added_column = result.add_fields();
    added_column->set_id(column.field_id);
    added_column->set_name(column.name);
    IcebergTypeToTeapotType(column.type, added_column);
  }
  return result;
}

std::shared_ptr<const iceberg::types::Type> TeapotTypeToIcebergType(teapot::FieldType type) {
  using IceTypeId = iceberg::TypeID;
  using iceberg::types::PrimitiveType;
  using teapot::FieldType;
  switch (type) {
    case FieldType::TYPE_BOOLEAN:
      return std::make_shared<PrimitiveType>(IceTypeId::kBoolean);
    case FieldType::TYPE_INTEGER:
      return std::make_shared<PrimitiveType>(IceTypeId::kInt);
    case FieldType::TYPE_LONG:
      return std::make_shared<PrimitiveType>(IceTypeId::kLong);
    case FieldType::TYPE_FLOAT:
      return std::make_shared<PrimitiveType>(IceTypeId::kFloat);
    case FieldType::TYPE_DOUBLE:
      return std::make_shared<PrimitiveType>(IceTypeId::kDouble);
    case FieldType::TYPE_DATE:
      return std::make_shared<PrimitiveType>(IceTypeId::kDate);
    case FieldType::TYPE_TIME:
      return std::make_shared<PrimitiveType>(IceTypeId::kTime);
    case FieldType::TYPE_TIMESTAMP:
      return std::make_shared<PrimitiveType>(IceTypeId::kTimestamp);
    case FieldType::TYPE_STRING:
      return std::make_shared<PrimitiveType>(IceTypeId::kString);
    case FieldType::TYPE_UUID:
      return std::make_shared<PrimitiveType>(IceTypeId::kUuid);
    case FieldType::TYPE_BINARY:
      return std::make_shared<PrimitiveType>(IceTypeId::kBinary);
    case FieldType::TYPE_TIMESTAMPTZ:
      return std::make_shared<PrimitiveType>(IceTypeId::kTimestamptz);
    case FieldType::TYPE_UNDEFINED:
    case FieldType::TYPE_DECIMAL:
    default:
      throw std::runtime_error("Unexpected type");
  }
}

namespace {

using FileContent = iceberg::DataFile::FileContent;

struct EntryId {
  FileContent type;
  uint32_t id;

  std::strong_ordering operator<=>(const EntryId& other) const {
    return std::make_tuple(static_cast<uint32_t>(type), id) <=>
           std::make_tuple(static_cast<uint32_t>(other.type), other.id);
  }

  bool operator==(const EntryId& other) const = default;

  bool operator<(const EntryId& other) const = default;
};

struct EntryIdHasher {
  static constexpr uint32_t TotalEntryTypes = 3;

  std::size_t operator()(const EntryId& entry_id) const {
    return TotalEntryTypes * entry_id.id + static_cast<uint32_t>(entry_id.type);
  }
};

// used for converting teapot result
// TODO(gmusya): consider using proper structures for this
struct CompactManifestEntry {
  iceberg::ContentFile::FileContent content;
  std::string path;
  std::vector<int32_t> equality_ids;
};

struct CompactDataEntry {
  CompactDataEntry() = default;
  explicit CompactDataEntry(CompactManifestEntry e) : compact_manifest_entry(std::move(e)) {}
  explicit CompactDataEntry(CompactManifestEntry e, std::vector<iceberg::ice_tea::DataEntry::Segment> p)
      : compact_manifest_entry(std::move(e)), parts(std::move(p)) {}

  using Segment = iceberg::ice_tea::DataEntry::Segment;

  inline void SortParts() {
    std::sort(parts.begin(), parts.end(), [&](const auto& lhs, const auto& rhs) { return lhs.offset < rhs.offset; });
  }

  CompactManifestEntry compact_manifest_entry;
  std::vector<Segment> parts;
};

using DataEntry = CompactDataEntry;
using DeletesInfo = std::unordered_map<EntryId, std::set<EntryId>, EntryIdHasher>;

template <FileContent ContentType>
struct TaskContainer {
 public:
  bool Contains(const std::string& str) const { return name_to_id_.contains(str); }

  DataEntry* GetEntry(const std::string& str) {
    auto it = name_to_id_.find(str);
    if (it == name_to_id_.end()) {
      return nullptr;
    }
    return &(tasks_[it->second]);
  }

  DataEntry& AppendEntry(std::string str) {
    name_to_id_.emplace(std::move(str), tasks_.size());
    tasks_.emplace_back(DataEntry{});
    tasks_.back().compact_manifest_entry.content = ContentType;
    return tasks_.back();
  }

  DataEntry& operator[](uint32_t id) { return tasks_.at(id); }

  EntryId GetId(const std::string& str) const { return EntryId{.type = ContentType, .id = name_to_id_.at(str)}; }

  size_t Size() const { return tasks_.size(); }

  std::vector<DataEntry>& GetEntries() { return tasks_; }

 private:
  std::unordered_map<std::string, uint32_t> name_to_id_;
  std::vector<DataEntry> tasks_;
};

struct Entries {
  TaskContainer<FileContent::kData> data;
  TaskContainer<FileContent::kPositionDeletes> position_deletes;
  TaskContainer<FileContent::kEqualityDeletes> equality_deletes;

  DataEntry& GetDataEntry(uint32_t id) { return data.GetEntries().at(id); }
  DataEntry& GetPositionDeleteEntry(uint32_t id) { return position_deletes.GetEntries().at(id); }
  DataEntry& GetEqualityDeleteEntry(uint32_t id) { return equality_deletes.GetEntries().at(id); }
};

Entries GetEntries(const teapot::MetadataResponseResult& meta) {
  using iceberg::DataFile;
  TaskContainer<DataFile::FileContent::kData> data;
  TaskContainer<DataFile::FileContent::kPositionDeletes> position_deletes;
  TaskContainer<DataFile::FileContent::kEqualityDeletes> equality_deletes;

  for (const auto& fragment : meta.fragments()) {
    if (auto data_ptr = data.GetEntry(fragment.path()); data_ptr != nullptr) {
      DataEntry& task = *data_ptr;
      task.parts.emplace_back(DataEntry::Segment(fragment.position(), fragment.length()));
      continue;
    }

    DataEntry& data_task = data.AppendEntry(fragment.path());
    data_task.parts.emplace_back(DataEntry::Segment{fragment.position(), fragment.length()});
    data_task.compact_manifest_entry.path = fragment.path();
    data_task.compact_manifest_entry.content = FileContent::kData;

    for (const auto& position_delete : fragment.positional_deletes()) {
      if (position_deletes.Contains(position_delete.path())) {
        continue;
      }

      DataEntry& task = position_deletes.AppendEntry(position_delete.path());
      task.compact_manifest_entry.path = position_delete.path();
      task.compact_manifest_entry.content = FileContent::kPositionDeletes;
    }

    for (const auto& equality_delete : fragment.equality_deletes()) {
      if (equality_deletes.Contains(equality_delete.path())) {
        continue;
      }

      DataEntry& task = equality_deletes.AppendEntry(equality_delete.path());
      task.compact_manifest_entry.path = equality_delete.path();
      task.compact_manifest_entry.content = FileContent::kEqualityDeletes;

      const auto& ids = equality_delete.delete_field_ids();
      task.compact_manifest_entry.equality_ids = std::vector<int32_t>(ids.begin(), ids.end());
    }
  }

  for (auto& task : data.GetEntries()) {
    task.SortParts();
  }

  return Entries{.data = std::move(data),
                 .position_deletes = std::move(position_deletes),
                 .equality_deletes = std::move(equality_deletes)};
}

DeletesInfo GetDataIdToDeleteIds(const teapot::MetadataResponseResult& meta, const Entries& entries) {
  DeletesInfo data_id_to_deletes_id;

  for (const auto& fragment : meta.fragments()) {
    std::set<EntryId> delete_ids;
    for (const auto& position_delete : fragment.positional_deletes()) {
      if (!entries.position_deletes.Contains(position_delete.path())) {
        throw std::runtime_error(
            "Different fragments of same file have different position "
            "deletes");
      }
      delete_ids.insert(entries.position_deletes.GetId(position_delete.path()));
    }

    for (const auto& equality_delete : fragment.equality_deletes()) {
      if (!entries.equality_deletes.Contains(equality_delete.path())) {
        throw std::runtime_error(
            "Different fragments of same file have different equality "
            "deletes");
      }
      delete_ids.insert(entries.equality_deletes.GetId(equality_delete.path()));
    }

    auto global_data_id = entries.data.GetId(fragment.path());

    if (auto it = data_id_to_deletes_id.find(global_data_id); it != data_id_to_deletes_id.end()) {
      if (it->second != delete_ids) {
        throw std::runtime_error("Different fragments of same file have different deletes");
      }
    } else {
      data_id_to_deletes_id.emplace(global_data_id, std::move(delete_ids));
    }
  }

  return data_id_to_deletes_id;
}

}  // namespace

iceberg::types::NestedField TeapotFieldToIcebergField(const teapot::Field& field) {
  iceberg::types::NestedField result;
  result.field_id = field.id();
  result.name = field.name();
  result.is_required = false;
  if (field.type() == teapot::FieldType::TYPE_DECIMAL) {
    result.type = std::make_shared<iceberg::types::DecimalType>(field.precision(), field.scale());
  } else {
    result.type = TeapotTypeToIcebergType(field.type());
  }
  if (field.repeated()) {
    result.type = std::make_shared<iceberg::types::ListType>(0, false, std::move(result.type));
  }
  return result;
}

std::shared_ptr<iceberg::Schema> TeapotSchemaToIcebergSchema(const teapot::Schema& schema) {
  std::vector<iceberg::types::NestedField> fields;
  for (const auto& field : schema.fields()) {
    fields.emplace_back(TeapotFieldToIcebergField(field));
  }
  return std::make_shared<iceberg::Schema>(0, std::move(fields));
}

std::vector<std::vector<DeletePlanner::SetId>> GroupBySize(
    const std::vector<std::set<DeletePlanner::DeleteId>>& delete_sets) {
  size_t max_delete_set_size = [&delete_sets]() {
    size_t result = 0;
    for (const auto& delete_set : delete_sets) {
      result = std::max(delete_set.size(), result);
    }
    return result;
  }();

  std::vector<std::vector<DeletePlanner::SetId>> delete_sets_grouped_by_size(max_delete_set_size + 1);
  for (DeletePlanner::SetId set_id = 0; set_id < delete_sets.size(); ++set_id) {
    delete_sets_grouped_by_size[delete_sets[set_id].size()].emplace_back(set_id);
  }

  return delete_sets_grouped_by_size;
}

class ChainBuilder {
 public:
  using SetId = DeletePlanner::SetId;
  using DeleteId = DeletePlanner::DeleteId;
  using DeleteSet = DeletePlanner::DeleteSet;

  using Chain = DeletePlanner::Chain;
  using ChainId = DeletePlanner::ChainId;

  explicit ChainBuilder(const std::vector<DeleteSet>& delete_sets) : delete_sets_(delete_sets) {}

  void Append(DeletePlanner::SetId set_id) {
    bool continues_some_chain = TryToContinueChain(set_id);

    if (!continues_some_chain) {
      AddNewChain(set_id);
    }
  }

  std::vector<DeletePlanner::Chain> GetChains() && { return std::move(chains_); }

 private:
  bool TryToContinueChain(DeletePlanner::SetId set_id) {
    const std::set<DeletePlanner::DeleteId>& set_to_add = delete_sets_.at(set_id);

    for (DeleteId delete_id : set_to_add) {
      if (!delete_id_to_chain_id_.contains(delete_id)) {
        continue;
      }

      ChainId chain_id = delete_id_to_chain_id_.at(delete_id);
      Chain& chain = chains_.at(chain_id);
      const DeleteSet& chain_last_set = delete_sets_[chain.back()];

      if (std::includes(set_to_add.begin(), set_to_add.end(), chain_last_set.begin(), chain_last_set.end())) {
        chain.emplace_back(set_id);
        UpdateDeleteToChainMapping(set_to_add, chain_id);
        return true;
      }
    }

    return false;
  }

  void AddNewChain(DeletePlanner::SetId set_id) {
    const std::set<DeletePlanner::DeleteId>& set_to_add = delete_sets_.at(set_id);

    ChainId chain_id = chains_.size();
    chains_.emplace_back(std::vector<SetId>{set_id});
    UpdateDeleteToChainMapping(set_to_add, chain_id);
  }

  void UpdateDeleteToChainMapping(const std::set<DeletePlanner::DeleteId>& set_to_add, ChainId chain_id) {
    for (DeleteId d : set_to_add) {
      delete_id_to_chain_id_[d] = chain_id;
    }
  }

  const std::vector<DeleteSet>& delete_sets_;

  std::vector<DeletePlanner::Chain> chains_;
  std::map<DeletePlanner::DeleteId, DeletePlanner::ChainId> delete_id_to_chain_id_;
};

std::vector<std::vector<DeletePlanner::SetId>> DeletePlanner::GroupNestedDeletes(
    const std::vector<DeleteSet>& delete_sets) {
  std::vector<std::vector<SetId>> delete_sets_grouped_by_size = GroupBySize(delete_sets);

  ChainBuilder chain_builder(delete_sets);

  for (size_t set_size = 0; set_size < delete_sets_grouped_by_size.size(); ++set_size) {
    for (SetId set_id : delete_sets_grouped_by_size[set_size]) {
      chain_builder.Append(set_id);
    }
  }

  return std::move(chain_builder).GetChains();
}

struct ComparablePositionalDeleteInfo {
  iceberg::ice_tea::PositionalDeleteInfo value;

  bool operator<(const ComparablePositionalDeleteInfo& other) const { return value.path < other.value.path; }
};

struct ComparableEqualityDeleteInfo {
  iceberg::ice_tea::EqualityDeleteInfo value;

  bool operator<(const ComparableEqualityDeleteInfo& other) const {
    return std::tie(value.path, value.field_ids) < std::tie(other.value.path, other.value.field_ids);
  }
};

template <typename Key>
struct IndexedSet {
 public:
  static IndexedSet Make(std::set<Key>&& keys) {
    std::map<Key, uint32_t> key_to_index;
    std::vector<Key> all_keys;
    for (auto& key : keys) {
      key_to_index.emplace(key, all_keys.size());
      all_keys.emplace_back(std::move(key));
    }

    return IndexedSet(std::move(all_keys), std::move(key_to_index));
  }

  const Key& IndexToKey(uint32_t id) const { return all_keys_.at(id); }

  uint32_t KeyToIndex(const Key& key) const { return key_to_index_.at(key); }

  const std::vector<Key>& GetKeys() const { return all_keys_; }

  IndexedSet(IndexedSet&& other) = default;

 private:
  IndexedSet(std::vector<Key>&& keys, std::map<Key, uint32_t>&& key_to_index)
      : all_keys_(std::move(keys)), key_to_index_(std::move(key_to_index)) {}

  const std::vector<Key> all_keys_;
  const std::map<Key, uint32_t> key_to_index_;
};

static DeletePlanner::DeleteId PositionalToDeleteId(DeletePlanner::DeleteId positional_delete_id) {
  return positional_delete_id * 2;
}
static DeletePlanner::DeleteId DeleteIdToPositional(DeletePlanner::DeleteId delete_id) { return (delete_id) / 2; }

static DeletePlanner::DeleteId EqualityToDeleteId(DeletePlanner::DeleteId equality_delete_id) {
  return equality_delete_id * 2 + 1;
}
static DeletePlanner::DeleteId DeleteIdToEquality(DeletePlanner::DeleteId delete_id) { return (delete_id - 1) / 2; }

static bool IsPositionalDeleteId(DeletePlanner::DeleteId delete_id) { return delete_id % 2 == 0; }

#if 0  // unused
static bool IsEqualityDeleteId(DeletePlanner::DeleteId delete_id) { return delete_id % 2 == 1; }
#endif

static bool ContainsDeletes(const iceberg::ice_tea::ScanMetadata& meta) {
  for (const auto& partition : meta.partitions) {
    for (const auto& layer : partition) {
      if (!layer.equality_delete_entries_.empty() || !layer.positional_delete_entries_.empty()) {
        return true;
      }
    }
  }

  return false;
}

class OptimizeScanMetadataImpl {
 public:
  using SetId = DeletePlanner::SetId;
  using DeleteId = DeletePlanner::DeleteId;
  using DeleteSet = DeletePlanner::DeleteSet;

  using Chain = DeletePlanner::Chain;
  using ChainId = DeletePlanner::ChainId;

  explicit OptimizeScanMetadataImpl(iceberg::ice_tea::ScanMetadata&& meta) : meta_(std::move(meta)) {}

  iceberg::ice_tea::ScanMetadata Optimize() && {
    const IndexedSet<ComparablePositionalDeleteInfo> positional_deletes = PreparePositionalDeletes();
    const IndexedSet<ComparableEqualityDeleteInfo> equality_deletes = PrepareEqualityDeletes();

    const IndexedSet<DeleteSet> delete_sets = PrepareDeleteSets(positional_deletes, equality_deletes);

    std::vector<std::vector<iceberg::ice_tea::DataEntry>> delete_set_to_data_entries =
        PrepareDeleteSetToDataEntries(positional_deletes, equality_deletes, delete_sets);

    std::vector<Chain> optimized_result = DeletePlanner::GroupNestedDeletes(delete_sets.GetKeys());

    // meta was already optimal
    if (optimized_result.size() >= meta_.partitions.size()) {
      return std::move(meta_);
    }

    return ConvertOptimizedResultToScanMetadata(optimized_result, positional_deletes, equality_deletes, delete_sets,
                                                delete_set_to_data_entries);
  }

 private:
  IndexedSet<ComparablePositionalDeleteInfo> PreparePositionalDeletes() {
    std::set<ComparablePositionalDeleteInfo> unique_positional_deletes;
    for (const auto& partition : meta_.partitions) {
      for (const auto& layer : partition) {
        for (const auto& entry : layer.positional_delete_entries_) {
          unique_positional_deletes.insert(ComparablePositionalDeleteInfo{.value = entry});
        }
      }
    }

    return IndexedSet<ComparablePositionalDeleteInfo>::Make(std::move(unique_positional_deletes));
  }

  IndexedSet<ComparableEqualityDeleteInfo> PrepareEqualityDeletes() {
    std::set<ComparableEqualityDeleteInfo> unique_equality_deletes;
    for (const auto& partition : meta_.partitions) {
      for (const auto& layer : partition) {
        for (const auto& entry : layer.equality_delete_entries_) {
          unique_equality_deletes.insert(ComparableEqualityDeleteInfo{.value = entry});
        }
      }
    }

    return IndexedSet<ComparableEqualityDeleteInfo>::Make(std::move(unique_equality_deletes));
  }

  IndexedSet<DeleteSet> PrepareDeleteSets(const IndexedSet<ComparablePositionalDeleteInfo>& positional_deletes,
                                          const IndexedSet<ComparableEqualityDeleteInfo>& equality_deletes) {
    std::set<DeleteSet> unique_delete_sets;

    InvokeForEachLayerWithDeleteSet(positional_deletes, equality_deletes,
                                    [&unique_delete_sets](const iceberg::ice_tea::ScanMetadata::Layer& layer,
                                                          const DeleteSet& delete_ids_for_layer) {
                                      unique_delete_sets.insert(delete_ids_for_layer);
                                    });

    return IndexedSet<DeleteSet>::Make(std::move(unique_delete_sets));
  }

  std::vector<std::vector<iceberg::ice_tea::DataEntry>> PrepareDeleteSetToDataEntries(
      const IndexedSet<ComparablePositionalDeleteInfo>& positional_deletes,
      const IndexedSet<ComparableEqualityDeleteInfo>& equality_deletes, const IndexedSet<DeleteSet>& delete_sets) {
    std::vector<std::vector<iceberg::ice_tea::DataEntry>> delete_set_to_data_entries_(delete_sets.GetKeys().size());

    InvokeForEachLayerWithDeleteSet(
        positional_deletes, equality_deletes,
        [&](const iceberg::ice_tea::ScanMetadata::Layer& layer, const DeleteSet& delete_ids_for_layer) {
          SetId set_id = delete_sets.KeyToIndex(delete_ids_for_layer);
          auto& data_entries_with_this_set = delete_set_to_data_entries_.at(set_id);

          for (const auto& entry : layer.data_entries_) {
            data_entries_with_this_set.emplace_back(entry);
          }
        });

    return delete_set_to_data_entries_;
  }

  iceberg::ice_tea::ScanMetadata ConvertOptimizedResultToScanMetadata(
      const std::vector<Chain>& chains, const IndexedSet<ComparablePositionalDeleteInfo>& positional_deletes,
      const IndexedSet<ComparableEqualityDeleteInfo>& equality_deletes, const IndexedSet<DeleteSet>& delete_sets,
      const std::vector<std::vector<iceberg::ice_tea::DataEntry>>& delete_set_to_data_entries) {
    iceberg::ice_tea::ScanMetadata result_meta;
    result_meta.schema = meta_.schema;

    for (const auto& chain : chains) {
      std::optional<SetId> old_delete_set_id;
      iceberg::ice_tea::ScanMetadata::Partition partition;

      for (const SetId set_id : chain) {
        const auto& delete_set = delete_sets.IndexToKey(set_id);

        std::vector<DeleteId> new_deletes;
        if (!old_delete_set_id.has_value()) {
          new_deletes = std::vector<DeleteId>(delete_set.begin(), delete_set.end());
        } else {
          const auto& old_delete_set = delete_sets.IndexToKey(old_delete_set_id.value());
          std::set_difference(delete_set.begin(), delete_set.end(), old_delete_set.begin(), old_delete_set.end(),
                              std::back_inserter(new_deletes));
        }

        old_delete_set_id = set_id;

        iceberg::ice_tea::ScanMetadata::Layer layer;
        layer.data_entries_ = std::move(delete_set_to_data_entries.at(set_id));
        for (const DeleteId delete_id : new_deletes) {
          if (IsPositionalDeleteId(delete_id)) {
            layer.positional_delete_entries_.emplace_back(
                positional_deletes.IndexToKey(DeleteIdToPositional(delete_id)).value);
          } else {
            layer.equality_delete_entries_.emplace_back(
                equality_deletes.IndexToKey(DeleteIdToEquality(delete_id)).value);
          }
        }
        partition.emplace_back(std::move(layer));
      }

      std::reverse(partition.begin(), partition.end());

      result_meta.partitions.emplace_back(std::move(partition));
    }

    return result_meta;
  }

  void InvokeForEachLayerWithDeleteSet(
      const IndexedSet<ComparablePositionalDeleteInfo>& positional_deletes,
      const IndexedSet<ComparableEqualityDeleteInfo>& equality_deletes,
      std::function<void(const iceberg::ice_tea::ScanMetadata::Layer&, const DeletePlanner::DeleteSet&)> cb) {
    for (const auto& partition : meta_.partitions) {
      DeletePlanner::DeleteSet current_delete_set;

      for (size_t layer_no_p1 = partition.size(); layer_no_p1 >= 1; --layer_no_p1) {
        const auto& layer = partition[layer_no_p1 - 1];

        for (const auto& entry : layer.positional_delete_entries_) {
          ComparablePositionalDeleteInfo comp_entry{.value = entry};
          current_delete_set.insert(PositionalToDeleteId(positional_deletes.KeyToIndex(comp_entry)));
        }
        for (const auto& entry : layer.equality_delete_entries_) {
          ComparableEqualityDeleteInfo comp_entry{.value = entry};
          current_delete_set.insert(EqualityToDeleteId(equality_deletes.KeyToIndex(comp_entry)));
        }

        cb(layer, current_delete_set);
      }
    }
  }

  const iceberg::ice_tea::ScanMetadata meta_;
};

iceberg::ice_tea::ScanMetadata DeletePlanner::OptimizeScanMetadata(iceberg::ice_tea::ScanMetadata&& meta) {
  if (!ContainsDeletes(meta)) {
    return meta;
  }

  OptimizeScanMetadataImpl impl(std::move(meta));

  return std::move(impl).Optimize();
}

iceberg::ice_tea::ScanMetadata MetadataResponseResultToScanMetadata(const teapot::MetadataResponseResult& meta) {
  iceberg::ice_tea::ScanMetadata result;

  result.schema = meta.has_schema() ? TeapotSchemaToIcebergSchema(meta.schema())
                                    : std::make_shared<iceberg::Schema>(iceberg::Schema(0, {}));

  auto entries = GetEntries(meta);

  auto data_id_to_delete_ids = GetDataIdToDeleteIds(meta, entries);

  // group data files with same deletes to same partition
  std::map<std::set<EntryId>, std::vector<EntryId>> deletes_to_data_with_this_deletes;
  for (auto& [data_id, delete_ids] : data_id_to_delete_ids) {
    deletes_to_data_with_this_deletes[delete_ids].emplace_back(data_id);
  }

  std::vector<iceberg::ice_tea::ScanMetadata::Partition> partitions;
  // one trivial partition per same delete group
  for (const auto& [deletes, data_with_this_deletes] : deletes_to_data_with_this_deletes) {
    iceberg::ice_tea::ScanMetadata::Layer layer;
    for (const auto& data_id : data_with_this_deletes) {
      if (data_id.type != FileContent::kData) {
        throw std::runtime_error("MetadataResponseResultToScanMetadata: data expected");
      }
      DataEntry entry = std::move(entries.GetDataEntry(data_id.id));
      layer.data_entries_.emplace_back(iceberg::ice_tea::DataEntry(entry.compact_manifest_entry.path, entry.parts));
    }

    for (const auto& del : deletes) {
      switch (del.type) {
        case FileContent::kPositionDeletes: {
          iceberg::ice_tea::PositionalDeleteInfo info(
              entries.GetPositionDeleteEntry(del.id).compact_manifest_entry.path);
          layer.positional_delete_entries_.emplace_back(std::move(info));
          break;
        }
        case FileContent::kEqualityDeletes: {
          const auto& delete_entry = entries.GetEqualityDeleteEntry(del.id).compact_manifest_entry;
          layer.equality_delete_entries_.emplace_back(delete_entry.path, delete_entry.equality_ids);
          break;
        }
        default:
          throw std::runtime_error("MetadataResponseResultToScanMetadata: delete expected");
      }
    }
    auto partition = iceberg::ice_tea::ScanMetadata::Partition{std::move(layer)};
    partitions.push_back(std::move(partition));
  }

  result.partitions = std::move(partitions);
  return result;
}

void SortDataEntries(std::vector<iceberg::ice_tea::DataEntry>& data_entries) {
  std::sort(data_entries.begin(), data_entries.end(),
            [](const auto& lhs, const auto& rhs) { return lhs.path < rhs.path; });
}

void FixDataOrder(iceberg::ice_tea::ScanMetadata& meta) {
  for (auto& partition : meta.partitions) {
    for (auto& layer : partition) {
      SortDataEntries(layer.data_entries_);
    }
  }

  auto partition_to_value =
      [](const iceberg::ice_tea::ScanMetadata::Partition& partition) -> std::optional<std::string> {
    for (const auto& layer : partition) {
      if (!layer.data_entries_.empty()) {
        return layer.data_entries_[0].path;
      }
    }
    return std::nullopt;
  };

  std::sort(meta.partitions.begin(), meta.partitions.end(), [partition_to_value](const auto& lhs, const auto& rhs) {
    auto lhs_val = partition_to_value(lhs);
    auto rhs_val = partition_to_value(rhs);
    if (!lhs_val.has_value() || rhs_val.has_value()) {
      return lhs_val.has_value() < rhs_val.has_value();
    }
    return lhs_val.value() < rhs_val.value();
  });
}

iceberg::ice_tea::ScanMetadata SplitPartitionsAndFilter(iceberg::ice_tea::ScanMetadata&& scan_metadata,
                                                        const int segment_id, const int segment_count) {
  const auto& partitions = scan_metadata.partitions;
  iceberg::ice_tea::ScanMetadata result;
  result.schema = scan_metadata.schema;

  int iterator = 0;
  for (const auto& partition : partitions) {
    iceberg::ice_tea::ScanMetadata::Partition new_partition;
    for (const auto& layer : partition) {
      iceberg::ice_tea::ScanMetadata::Layer new_layer;
      new_layer.equality_delete_entries_ = std::move(layer.equality_delete_entries_);
      new_layer.positional_delete_entries_ = std::move(layer.positional_delete_entries_);
      for (const auto& data_entry : layer.data_entries_) {
        std::vector<iceberg::ice_tea::DataEntry::Segment> segments;
        for (auto segment : data_entry.parts) {
          if ((iterator++ % segment_count) != segment_id) {
            continue;
          }
          segments.emplace_back(segment);
        }
        if (segments.empty()) {
          continue;
        }
        new_layer.data_entries_.emplace_back(data_entry.path, std::move(segments));
      }
      new_partition.emplace_back(std::move(new_layer));
    }
    result.partitions.emplace_back(std::move(new_partition));
  }
  return result;
}

}  // namespace tea
