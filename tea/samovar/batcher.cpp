#include "tea/samovar/batcher.h"

#include <cstdlib>
#include <optional>

#include "tea/observability/tea_log.h"
#include "tea/samovar/network_layer/samovar_client.h"
#include "tea/samovar/proto/samovar.pb.h"
#include "tea/samovar/samovar_data_client.h"

namespace tea::samovar {

namespace {

std::vector<samovar::AnnotatedDataEntry> MakeBatchRequest(std::shared_ptr<ISamovarClient> client,
                                                          const std::string queue_name, int batch_size) {
  auto responses = client->PopQueue(queue_name, batch_size);
  std::vector<samovar::AnnotatedDataEntry> result;
  for (const auto& response : responses) {
    samovar::AnnotatedDataEntry entry;
    entry.ParseFromString(response);
    result.push_back(entry);
  }
  return result;
}

}  // namespace

Batcher::Batcher(std::shared_ptr<ISamovarClient> client, std::shared_ptr<IBatchSizeScheduler> batch_size_scheduler)
    : client_(client), batch_size_scheduler_(batch_size_scheduler) {}

std::optional<samovar::AnnotatedDataEntry> Batcher::GetNextDataEntry(const std::string& queue_name) {
  if (task_queue_.empty()) {
    int batch_size = batch_size_scheduler_->GetNextBatchSize();
    auto response = MakeBatchRequest(client_, queue_name, batch_size);
    if (response.empty()) {
      return std::nullopt;
    }
    for (auto elem : response) {
      task_queue_.push(std::move(elem));
    }
  }
  auto elem = std::move(task_queue_.front());
  task_queue_.pop();
  return elem;
}

}  // namespace tea::samovar
