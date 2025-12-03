#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <limits>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>

#include "gtest/gtest.h"

#include "tea/common/config.h"
#include "tea/compression/identity.h"
#include "tea/samovar/batcher.h"
#include "tea/samovar/network_layer/backoff.h"
#include "tea/samovar/network_layer/redis_client.h"
#include "tea/samovar/planner.h"
#include "tea/samovar/proto/samovar.pb.h"
#include "tea/samovar/single_queue_client.h"
#include "tea/samovar/utils.h"
#include "tea/util/cancel.h"
#include "teapot/teapot.pb.h"

namespace tea::samovar {

namespace {

std::string GetQueueName(int test_iter = 0) {
  return MakeSessionIdentifier(EmptyTable{}, "cluster", "session_id", "1234-5678", 0, test_iter, false);
}

static constexpr const char* metadata_prefix = "/samovar_meta";
static constexpr const char* file_list_prefix = "/file_list";
static constexpr const char* checkpoint_prefix = "/checkpoint";
static constexpr const char* processing_queue_prefix = "/processing";
static constexpr const char* done_queue_prefix = "/done";
static constexpr const char* init_scan_prefix = "/init_scan";

std::vector<std::string> SplitKey(const std::string& key, char delim = '/') {
  std::vector<std::string> result = {""};

  for (size_t i = 1; i < key.size(); ++i) {
    if (key[i] == '/') {
      result.push_back("");
    } else {
      result.back().push_back(key[i]);
    }
  }
  if (result.back().empty()) {
    result.pop_back();
  }
  return result;
}

bool CheckRedisKey(const std::string& key) {
  if (key.substr(0, std::strlen(metadata_prefix)) == metadata_prefix) {
    return CheckRedisKey(key.substr(std::strlen(metadata_prefix)));
  }

  if (key.substr(0, std::strlen(file_list_prefix)) == file_list_prefix) {
    return CheckRedisKey(key.substr(std::strlen(file_list_prefix)));
  }

  if (key.substr(0, std::strlen(checkpoint_prefix)) == checkpoint_prefix) {
    return CheckRedisKey(key.substr(std::strlen(checkpoint_prefix)));
  }

  if (key.substr(0, std::strlen(processing_queue_prefix)) == processing_queue_prefix) {
    return CheckRedisKey(key.substr(std::strlen(processing_queue_prefix)));
  }

  if (key.substr(0, std::strlen(done_queue_prefix)) == done_queue_prefix) {
    return CheckRedisKey(key.substr(std::strlen(done_queue_prefix)));
  }

  if (key.substr(0, std::strlen(init_scan_prefix)) == init_scan_prefix) {
    return CheckRedisKey(key.substr(std::strlen(init_scan_prefix)));
  }

  auto parts = SplitKey(key);
  if (parts.size() != 8) {
    return false;
  }

  if (parts[0] != "ws" && parts[0] != "tea") {
    return false;
  }
  std::vector<size_t> integer_keys = {1, parts.size() - 1, parts.size() - 2};
  for (auto index : integer_keys) {
    std::stoi(parts[index]);
  }

  if (parts[5] != "session_id") {
    return false;
  }

  if (parts[3] != "cluster") {
    return false;
  }

  if (parts[4] != "") {
    return false;
  }
  return true;
}

void CheckQueueIdentifiers() {
  auto client = RedisClient(std::vector<Endpoint>{Endpoint{.host = "0.0.0.0", .port = kDefaultPort}},
                            std::chrono::milliseconds(30000), std::chrono::milliseconds(30000));

  auto all_keys = client.SendRequest({"keys", "*"});
  for (size_t i = 0; i < all_keys.Get()->elements; ++i) {
    if (!CheckRedisKey(all_keys.Get()->element[i]->str)) {
      throw std::runtime_error(std::string("Incorrect part ") + all_keys.Get()->element[i]->str);
    }
  }
}  // namespace

void FlushServer() {
  auto simple_client = RedisClient(std::vector<Endpoint>{Endpoint{.host = "0.0.0.0", .port = kDefaultPort}},
                                   std::chrono::milliseconds(30000), std::chrono::milliseconds(30000));
  simple_client.SendRequest({"FLUSHALL"});
}

void KillRedis() {
  [[maybe_unused]] int result = std::system("pkill redis-server");

  while (true) {
    try {
      auto simple_client = RedisClient(std::vector<Endpoint>{Endpoint{.host = "0.0.0.0", .port = kDefaultPort}},
                                       std::chrono::milliseconds(30000), std::chrono::milliseconds(30000));
    } catch (const std::runtime_error& ex) {
      break;
    }
  }
}

void StartRedis() {
  [[maybe_unused]] int result = std::system("redis-server &");

  while (true) {
    try {
      auto simple_client = RedisClient(std::vector<Endpoint>{Endpoint{.host = "0.0.0.0", .port = kDefaultPort}},
                                       std::chrono::milliseconds(30000), std::chrono::milliseconds(3000));
      break;
    } catch (const std::runtime_error& ex) {
    }
  }
}

TEST(RedisClient, NoRedis) {
  KillRedis();

  auto backoff = std::make_shared<NoBackoff>(30);
  try {
    auto redis_client =
        std::make_shared<SamovarRedisClient>(std::vector<Endpoint>{Endpoint{.host = "0.0.0.0", .port = kDefaultPort}},
                                             std::chrono::milliseconds(30000), std::chrono::milliseconds(3000));
    EXPECT_FALSE(true);
  } catch (const std::exception& ex) {
    EXPECT_EQ(std::string(ex.what()), "No available server redis");
  }
}

TEST(RedisClient, Test1) {
  StartRedis();
  FlushServer();

  auto backoff = std::make_shared<NoBackoff>(30);
  auto batch_size_scheduler = std::make_shared<ConstantBatchSizeScheduler>(1);
  auto redis_client =
      std::make_shared<SamovarRedisClient>(std::vector<Endpoint>{Endpoint{.host = "0.0.0.0", .port = kDefaultPort}},
                                           std::chrono::milliseconds(30000), std::chrono::milliseconds(3000));
  auto batcher = std::make_shared<Batcher>(redis_client, batch_size_scheduler);
  auto client = SingleQueueClient(redis_client, batcher, std::chrono::seconds(std::numeric_limits<int32_t>::max()),
                                  GetQueueName(), 1, std::string(compression::kIdentityCompressorName),
                                  SamovarRole::kCoordinator, backoff, backoff, true, 1);

  client.FillFilesQueue({}, {}, {});
  EXPECT_FALSE(client.GetNextDataEntry());
  KillRedis();
}

TEST(RedisClient, MultiThreading) {
  StartRedis();
  FlushServer();

  unsigned int seed = 123;
  const size_t num_segments = 3;
  const size_t num_tests = 10;
  const size_t num_fragments = 10;

  std::mt19937 generator(seed);
  std::uniform_int_distribution<int> distribution(0, 100000);
  CancelToken cancel_token;

  for (size_t test_iter = 0; test_iter < num_tests; ++test_iter) {
    std::vector<std::thread> workers;
    for (size_t i = 0; i < num_segments; ++i) {
      auto task_worker = [segment_id = i, num_fragments, test_iter, &cancel_token]() {
        auto backoff = std::make_shared<LinearBackoff>(30, std::chrono::seconds(1), cancel_token);
        auto batch_size_scheduler = std::make_shared<ConstantBatchSizeScheduler>(1);
        auto redis_client = std::make_shared<SamovarRedisClient>(
            std::vector<Endpoint>{Endpoint{.host = "0.0.0.0", .port = kDefaultPort}}, std::chrono::milliseconds(3000),
            std::chrono::milliseconds(3000));
        auto batcher = std::make_shared<Batcher>(redis_client, batch_size_scheduler);
        auto client =
            SingleQueueClient(redis_client, batcher, std::chrono::seconds(std::numeric_limits<int32_t>::max()),
                              GetQueueName(test_iter), num_segments, std::string(compression::kIdentityCompressorName),
                              SamovarRole::kCoordinator, backoff, backoff, true, 1);

        if (segment_id == 0) {
          samovar::ScanMetadata scan_metadata;
          std::vector<samovar::AnnotatedDataEntry> data_entries;
          for (size_t fragment_id = 0; fragment_id < num_fragments; ++fragment_id) {
            auto* partition = scan_metadata.add_partitions();
            partition->add_layers();
            samovar::AnnotatedDataEntry data_entry;
            data_entry.set_layer_id(0);
            data_entry.set_partition_id(fragment_id);
            auto* segment = data_entry.mutable_data_entry()->add_segments();
            segment->set_length(fragment_id);
            segment->set_offset(fragment_id);
            *data_entry.mutable_data_entry()->mutable_entry()->mutable_file_path() = "aaaaaa";
            data_entries.push_back(data_entry);
          }
          samovar::FileList file_list;
          file_list.add_filenames("aaaaaa");
          client.FillFilesQueue(std::move(scan_metadata), std::move(file_list), std::move(data_entries));
          CheckQueueIdentifiers();
        }

        client.GetPlannedMetadata();

        std::vector<int> worker_fragments;
        while (true) {
          auto entry = client.GetNextDataEntry();

          if (!entry) {
            break;
          }
          worker_fragments.push_back(entry->data_entry().segments()[0].length());
        }

        {
          int prev_fragment_id = -1e9;
          for (auto fragment_id : worker_fragments) {
            EXPECT_LT(prev_fragment_id, fragment_id);
            EXPECT_LE(0, fragment_id);
            EXPECT_LT(fragment_id, num_fragments);
            prev_fragment_id = fragment_id;
          }
        }
      };

      workers.emplace_back(task_worker);
    }

    for (auto& worker : workers) {
      worker.join();
    }
  }
  KillRedis();
}

#if 0

TEST(RedisClient, Cache) {
  unsigned int seed = 42;

  std::mt19937 generator(seed);
  std::uniform_int_distribution<int> distribution(3, 100000);

  teapot::MetadataRequest request;
  request.set_segment_count(distribution(generator));
  request.set_segment_id(2);
  request.set_table_id("table_" + std::to_string(distribution(generator)));

  auto client = RedisFragmentsClient("0.0.0.0", std::chrono::seconds(std::numeric_limits<int32_t>::max()));
  client.CleanCache(request);

  {
    auto response = client.GetCache(request);
    EXPECT_FALSE(response);
  }
  {
    teapot::MetadataResponse response;

    teapot::Fragment fragment;
    *fragment.mutable_path() = "aaaaaa";
    fragment.set_length(23);
    fragment.set_position(14);
    response.mutable_result()->add_fragments()->CopyFrom(std::move(fragment));

    client.SetCache(request, response);
  }

  {
    auto response = client.GetCache(request);
    EXPECT_TRUE(response);
    EXPECT_EQ(response.value().result().fragments().size(), 1);
    EXPECT_EQ(response.value().result().fragments()[0].path(), "aaaaaa");
    EXPECT_EQ(response.value().result().fragments()[0].position(), 14);
    EXPECT_EQ(response.value().result().fragments()[0].length(), 23);
  }
}

#endif

TEST(RedisClient, NoServer) {
  uint16_t some_incorrect_port = 4242;
  auto backoff = std::make_shared<NoBackoff>(30);
  auto batch_size_scheduler = std::make_shared<ConstantBatchSizeScheduler>(1);
  EXPECT_THROW(std::make_shared<SamovarRedisClient>(
                   std::vector<Endpoint>{Endpoint{.host = "0.0.0.0", .port = some_incorrect_port}},
                   std::chrono::milliseconds(30000), std::chrono::milliseconds(3000)),
               std::runtime_error);
}

TEST(RedisClient, FailServer) {
  StartRedis();
  FlushServer();

  unsigned int seed = 123;
  const size_t num_segments = 3;
  const size_t num_tests = 2;
  const size_t num_fragments = 10;

  std::mt19937 generator(seed);
  std::uniform_int_distribution<int> distribution(0, 100000);

  std::mutex kill_mutex;
  for (size_t test_iter = 0; test_iter < num_tests; ++test_iter) {
    std::vector<std::thread> workers;
    bool was_killed = false;

    auto do_with_kill_check = [&](const std::function<void()>& operation) -> bool {
      std::lock_guard lock(kill_mutex);
      if (was_killed) {
        EXPECT_THROW(operation(), std::runtime_error);
        return true;
      } else {
        operation();
        return false;
      }
    };

    CancelToken cancel_token;

    for (size_t i = 0; i < num_segments; ++i) {
      auto task_worker = [segment_id = i, num_fragments, &do_with_kill_check, &kill_mutex, &was_killed, &cancel_token,
                          num_segments]() {
        auto backoff = std::make_shared<LinearBackoff>(30, std::chrono::milliseconds(300), cancel_token);
        auto batch_size_scheduler = std::make_shared<ConstantBatchSizeScheduler>(1);
        std::shared_ptr<SamovarRedisClient> redis_client;
        if (do_with_kill_check([&]() {
              redis_client = std::make_shared<SamovarRedisClient>(
                  std::vector<Endpoint>{Endpoint{.host = "0.0.0.0", .port = kDefaultPort}},
                  std::chrono::milliseconds(30000), std::chrono::milliseconds(3000));
            })) {
          return;
        }
        auto batcher = std::make_shared<Batcher>(redis_client, batch_size_scheduler);
        std::shared_ptr<SingleQueueClient> client;

        try {
          client = std::make_shared<SingleQueueClient>(
              redis_client, batcher, std::chrono::seconds(std::numeric_limits<int32_t>::max()), GetQueueName(),
              num_segments, std::string(compression::kIdentityCompressorName), SamovarRole::kCoordinator, backoff,
              backoff, true, 1);
        } catch (const std::runtime_error& ex) {
          std::lock_guard lock(kill_mutex);
          EXPECT_TRUE(was_killed);
          return;
        }

        if (segment_id == 0) {
          samovar::ScanMetadata scan_metadata;
          std::vector<samovar::AnnotatedDataEntry> data_entries;
          for (size_t fragment_id = 0; fragment_id < num_fragments; ++fragment_id) {
            auto* partition = scan_metadata.add_partitions();
            partition->add_layers();
            samovar::AnnotatedDataEntry data_entry;
            data_entry.set_layer_id(0);
            data_entry.set_partition_id(fragment_id);
            auto* segment = data_entry.mutable_data_entry()->add_segments();
            segment->set_length(fragment_id);
            segment->set_offset(fragment_id);
            *data_entry.mutable_data_entry()->mutable_entry()->mutable_file_path() = "aaaaaa";
            data_entries.push_back(data_entry);
          }

          samovar::FileList file_list;
          file_list.add_filenames("aaaaaa");
          if (do_with_kill_check([&]() {
                client->FillFilesQueue(std::move(scan_metadata), std::move(file_list), std::move(data_entries));
              })) {
            return;
          }
        }

        try {
          client->GetPlannedMetadata();
        } catch (const std::runtime_error& ex) {
          std::lock_guard lock(kill_mutex);
          EXPECT_TRUE(was_killed);
        }

        std::vector<int> worker_fragments;
        while (true) {
          std::optional<samovar::AnnotatedDataEntry> entry;
          {
            std::lock_guard lock(kill_mutex);
            if (was_killed) {
              return;
            }
            try {
              entry = client->GetNextDataEntry();
            } catch (const std::runtime_error& ex) {
              EXPECT_TRUE(was_killed);
            }
          }

          if (!entry) {
            break;
          }
          worker_fragments.push_back(entry->data_entry().segments()[0].length());
        }

        {
          int prev_fragment_id = -1e9;
          for (auto fragment_id : worker_fragments) {
            EXPECT_LT(prev_fragment_id, fragment_id);
            EXPECT_LE(0, fragment_id);
            EXPECT_LT(fragment_id, num_fragments);
            prev_fragment_id = fragment_id;
          }
        }
      };

      workers.emplace_back(task_worker);
    }
    workers.emplace_back([&]() {
      std::lock_guard lock(kill_mutex);

      was_killed = true;

      KillRedis();
    });
    for (auto& worker : workers) {
      worker.join();
    }
    StartRedis();
    FlushServer();
  }
  KillRedis();
}

}  // namespace

}  // namespace tea::samovar
