#pragma once

#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "tea/samovar/network_layer/samovar_client.h"
#include "tea/samovar/proto/samovar.pb.h"
#include "tea/samovar/samovar_data_client.h"

namespace tea::samovar {

struct IBatchSizeScheduler {
  // return batch size of request in current iteration
  virtual int GetNextBatchSize() = 0;

  virtual ~IBatchSizeScheduler() = default;
};

// TODO(hvintus): maybe do other types of batch scheduler (for example with linear decreasing)
class ConstantBatchSizeScheduler : public IBatchSizeScheduler {
 public:
  explicit ConstantBatchSizeScheduler(int batch_size) : batch_size_(batch_size) {}

  int GetNextBatchSize() override { return batch_size_; }

 private:
  int batch_size_;
};

class Batcher {
 public:
  explicit Batcher(std::shared_ptr<ISamovarClient> client, std::shared_ptr<IBatchSizeScheduler> batch_size_scheduler);

  std::optional<samovar::AnnotatedDataEntry> GetNextDataEntry(const std::string& queue_name);

 private:
  std::shared_ptr<ISamovarClient> client_;
  std::shared_ptr<IBatchSizeScheduler> batch_size_scheduler_;

  std::queue<samovar::AnnotatedDataEntry> task_queue_;
};

}  // namespace tea::samovar
