#include <string>
#include <thread>

#include "arrow/status.h"
#include "gtest/gtest.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"

namespace tea {

namespace {

class SessionTest : public TeaTest {};

TEST_F(SessionTest, Trivial) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  constexpr int32_t kOpenSessions = 2;
  constexpr int32_t kQueriesPerSession = 5;
  std::vector<arrow::Status> status(kOpenSessions, arrow::Status::OK());

  auto do_some_job = [&status](int id) {
    pq::PGconnWrapper conn(PQconnectdb("dbname = postgres"));
    for (int i = 0; i < kQueriesPerSession; ++i) {
      arrow::Result<pq::ScanResult> result =
          pq::TableScanQuery(kDefaultTableName, "count(*)").SetWhere("col2 = 2").Run(conn);
      if (!result.ok()) {
        status[id] = result.status();
      }
    }
  };

  std::vector<std::thread> workers;
  for (int i = 0; i < kOpenSessions; ++i) {
    workers.emplace_back(do_some_job, i);
  }

  for (int i = 0; i < kOpenSessions; ++i) {
    workers[i].join();
  }

  for (int i = 0; i < kOpenSessions; ++i) {
    ASSERT_OK(status[i]);
  }

  auto stats = this->stats_state_->GetStats(false);
  EXPECT_EQ(stats.size(), kOpenSessions * kQueriesPerSession * pq::GetSegmentsCount(*conn_));
}

TEST_F(SessionTest, TrivialWithErrors) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{0});
  std::vector<ParquetColumn> columns = {column1, column2};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  constexpr int32_t kAttempts = 200;
  for (int attempt = 0; attempt < kAttempts; ++attempt) {
    if (attempt % 20 == 0) {
      std::cerr << "attempt " << attempt + 1 << "/" << kAttempts << "\n";
    }
    constexpr int32_t kOpenSessions = 2;
    constexpr int32_t kQueriesPerSession = 5;
    std::vector<arrow::Status> status(kOpenSessions, arrow::Status::OK());

    auto do_some_job = [&status](int id) {
      pq::PGconnWrapper conn(PQconnectdb("dbname = postgres"));
      for (int i = 0; i < kQueriesPerSession; ++i) {
        arrow::Result<pq::ScanResult> result =
            pq::TableScanQuery(kDefaultTableName, "count(*)").SetWhere("col1 / col2 > 2").Run(conn);
        if (!result.ok()) {
          auto message = result.status().message();
          std::transform(message.begin(), message.end(), message.begin(), [](char ch) { return std::tolower(ch); });
          if (message.find("division by zero") == std::string::npos) {
            status[id] = result.status();
            return;
          }
        }
      }
    };

    std::vector<std::thread> workers;
    for (int i = 0; i < kOpenSessions; ++i) {
      workers.emplace_back(do_some_job, i);
    }

    for (int i = 0; i < kOpenSessions; ++i) {
      workers[i].join();
    }

    for (int i = 0; i < kOpenSessions; ++i) {
      ASSERT_OK(status[i]);
    }

    auto stats = this->stats_state_->GetStats(false);
    EXPECT_GE(static_cast<int32_t>(stats.size()),
              kOpenSessions * kQueriesPerSession * pq::GetSegmentsCount(*conn_) - 2);
  }
}

TEST_F(SessionTest, LongSession) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{0});
  std::vector<ParquetColumn> columns = {column1, column2};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  constexpr int32_t kOpenSessions = 2;
  constexpr int32_t kQueriesPerSession = 1000;
  std::vector<arrow::Status> status(kOpenSessions, arrow::Status::OK());

  auto do_some_job = [&status](int id) {
    pq::PGconnWrapper conn(PQconnectdb("dbname = postgres"));
    for (int i = 0; i < kQueriesPerSession; ++i) {
      if (i % 20 == 0) {
        std::stringstream ss;
        ss << "session " << id << ", query " << i + 1 << "/" << kQueriesPerSession << "\n";
        std::cerr << ss.str();
        ss.flush();
      }
      arrow::Result<pq::ScanResult> result =
          pq::TableScanQuery(kDefaultTableName, "count(*)").SetWhere("col1 / col2 > 2").Run(conn);
      if (!result.ok()) {
        auto message = result.status().message();
        std::transform(message.begin(), message.end(), message.begin(), [](char ch) { return std::tolower(ch); });
        if (message.find("division by zero") == std::string::npos) {
          status[id] = result.status();
          return;
        }
      }
    }
  };

  std::vector<std::thread> workers;
  for (int i = 0; i < kOpenSessions; ++i) {
    workers.emplace_back(do_some_job, i);
  }

  for (int i = 0; i < kOpenSessions; ++i) {
    workers[i].join();
  }

  for (int i = 0; i < kOpenSessions; ++i) {
    ASSERT_OK(status[i]);
  }
}

}  // namespace
}  // namespace tea
