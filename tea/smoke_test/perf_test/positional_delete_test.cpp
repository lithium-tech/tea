#include "gen/src/positional_delete.h"

#include <sstream>
#include <thread>

#include "arrow/filesystem/s3fs.h"
#include "gen/src/batch.h"
#include "gen/src/generators.h"
#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/scoped_temp_dir.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/types.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/perf_test/positional_delete_table.h"
#include "tea/smoke_test/test_base.h"
#include "tea/util/thread_pool.h"

namespace tea {
namespace {

arrow::Result<std::shared_ptr<gen::IProcessor>> MakePositionalDeleteProcessor(
    const std::string& output_file_name, const std::string& data_file_name, gen::RandomDevice& random_device,
    double ratio, std::shared_ptr<parquet::WriterProperties> writer_properties) {
  gen::BernouliDistribution positional_delete_gen(ratio);
  auto positional_delete_processor = std::make_shared<gen::PositionalDeleteProcessor<gen::BernouliDistribution>>(
      data_file_name, std::move(positional_delete_gen), random_device);

  arrow::FieldVector positional_delete_fields;
  positional_delete_fields.emplace_back(arrow::field("file", arrow::utf8()));
  positional_delete_fields.emplace_back(arrow::field("row", arrow::int64()));
  auto positional_delete_schema = std::make_shared<arrow::Schema>(positional_delete_fields);

  auto s3 = Environment::GetS3Filesystem();
  ARROW_ASSIGN_OR_RAISE(auto outfile, s3->OpenOutputStream(output_file_name));

  auto positional_delete_writer = std::make_shared<gen::ParquetWriter>(
      outfile, gen::ParquetSchemaFromArrowSchema(positional_delete_schema), writer_properties);

  positional_delete_processor->SetDeleteProcessor(std::make_shared<gen::WriterProcessor>(positional_delete_writer));

  return positional_delete_processor;
}

struct Configuration {
  int64_t rows_in_data_file = 10'000'000;
  int64_t rows_in_positional_delete_file = 0;
  int64_t positional_delete_files = 0;
  int64_t max_rows_in_row_group = 1'000'000;
  int64_t max_page_size = 1024 * 1024;
  bool use_delta_packed = false;
  bool use_compression = false;

  Configuration SetRowsInPositionalDelete(int64_t rows) {
    Configuration result = *this;
    result.rows_in_positional_delete_file = rows;
    return result;
  }

  Configuration SetPositionalDeleteFiles(int64_t files) {
    Configuration result = *this;
    result.positional_delete_files = files;
    return result;
  }

  Configuration SetMaxRowsInRowGroup(int64_t rows_in_rg) {
    Configuration result = *this;
    result.max_rows_in_row_group = rows_in_rg;
    return result;
  }

  Configuration SetMaxPageSize(int64_t sz) {
    Configuration result = *this;
    result.max_page_size = sz;
    return result;
  }

  Configuration SetUseDeltaPacked(bool use) {
    Configuration result = *this;
    result.use_delta_packed = use;
    return result;
  }

  Configuration SetUseCompression(bool use) {
    Configuration result = *this;
    result.use_compression = use;
    return result;
  }

  std::string ToString() const {
    std::stringstream ss;
    ss << "(";
    ss << "rows_in_data_file = " << rows_in_data_file << ", ";
    ss << "rows_in_positional_delete_file = " << rows_in_positional_delete_file << ", ";
    ss << "positional_delete_files = " << positional_delete_files << ", ";
    ss << "max_rows_in_row_group = " << max_rows_in_row_group << ", ";
    ss << "max_page_size = " << max_page_size << ", ";
    ss << "use_delta_packed = " << use_delta_packed << ", ";
    ss << "use_compression = " << use_compression;
    ss << ")";
    return ss.str();
  }

  Configuration() = default;
};

std::string Exec(const char* cmd) {
  std::array<char, 128> buffer;
  std::string result;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
  if (!pipe) {
    throw std::runtime_error("popen() failed!");
  }
  while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

class PositionalDeleteTable : public TeaTest {
 private:
  std::optional<MetadataType> old_metadata_type_;
};

std::vector<Configuration> AddChoice(std::vector<Configuration> x,
                                     std::function<std::vector<Configuration>(Configuration)> transform) {
  std::vector<Configuration> result;
  for (const auto& elem : x) {
    auto res = transform(elem);
    result.insert(result.end(), res.begin(), res.end());
  }

  return result;
}

TEST_F(PositionalDeleteTable, RowsReadSkipped) {
  auto s3fs = Environment::GetS3Filesystem();

  ScopedS3TempDir data_dir_(s3fs);

  int table_num = 0;
  auto program = gen::MakePositionalDeleteProgram();

  std::vector<Configuration> configurations = {Configuration()};

  configurations = AddChoice(std::move(configurations), [](Configuration input) {
    std::vector<Configuration> result;
    // result.emplace_back(input.SetPositionalDeleteFiles(10).SetRowsInPositionalDelete(9'000'000));
    // result.emplace_back(input.SetPositionalDeleteFiles(100).SetRowsInPositionalDelete(900'000));
    result.emplace_back(input.SetPositionalDeleteFiles(1000).SetRowsInPositionalDelete(90'000));
    return result;
  });

  configurations = AddChoice(std::move(configurations), [](Configuration input) {
    std::vector<Configuration> result;
    result.emplace_back(input.SetUseDeltaPacked(false).SetMaxPageSize(1024 * 1024));
    result.emplace_back(input.SetUseDeltaPacked(false).SetMaxPageSize(128 * 1024));
    result.emplace_back(input.SetUseDeltaPacked(false).SetMaxPageSize(16 * 1024));
    result.emplace_back(input.SetUseDeltaPacked(true).SetMaxPageSize(128 * 1024));
    result.emplace_back(input.SetUseDeltaPacked(true).SetMaxPageSize(1024 * 1024));
    result.emplace_back(input.SetUseDeltaPacked(true).SetMaxPageSize(16 * 1024));
    return result;
  });

  configurations = AddChoice(std::move(configurations), [](Configuration input) {
    std::vector<Configuration> result;
    result.emplace_back(input.SetUseCompression(true));
    result.emplace_back(input.SetUseCompression(false));
    return result;
  });

  configurations = AddChoice(std::move(configurations), [](Configuration input) {
    std::vector<Configuration> result;
    result.emplace_back(input.SetMaxRowsInRowGroup(10'000));
    result.emplace_back(input.SetMaxRowsInRowGroup(1'000'000));
    return result;
  });

  std::cerr << "Configurations to test count: " << configurations.size() << std::endl;

  constexpr int64_t kBatchSize = 1'000'000;

  for (const auto& configuration : configurations) {
    std::string data_path = "s3://" + data_dir_.path().string() + "/data" + std::to_string(table_num) + ".parquet";
    const std::string table_name = "positional_delete_table" + std::to_string(table_num);
    ++table_num;

    std::string local_file_path = data_path.substr(std::string("s3://").size());

    ASSERT_OK(state_->AddDataFiles({data_path}, table_name));
    std::cerr << "data file path: " << local_file_path << std::endl;

    {
      auto table = std::make_shared<gen::PositionalDeleteTable>();

      ASSIGN_OR_FAIL(auto outfile, s3fs->OpenOutputStream(local_file_path));

      auto writer = std::make_shared<gen::WriterProcessor>(
          std::make_shared<gen::ParquetWriter>(outfile, table->MakeParquetSchema()));

      gen::BatchSizeMaker batch_size_maker(kBatchSize, configuration.rows_in_data_file);

      for (int64_t rows_in_next_batch = batch_size_maker.NextBatchSize(), batch_no = 0; rows_in_next_batch != 0;
           rows_in_next_batch = batch_size_maker.NextBatchSize(), ++batch_no) {
        ASSIGN_OR_FAIL(auto batch, program.Generate(rows_in_next_batch));
        auto column_names = table->MakeColumnNames();
        ASSIGN_OR_FAIL(auto proj, batch->GetProjection(column_names));
        ASSIGN_OR_FAIL(auto arrow_batch, batch->GetArrowBatch(column_names));

        ASSERT_OK(writer->Process(proj));
      }
    }
    auto positional_delete_properties = [&]() {
      parquet::WriterProperties::Builder properties;
      if (configuration.use_delta_packed) {
        properties.disable_dictionary("row");
        auto path_row = std::make_shared<parquet::schema::ColumnPath>(std::vector<std::string>{"row"});
        properties.encoding(path_row, parquet::Encoding::DELTA_BINARY_PACKED);
      }
      properties.data_pagesize(configuration.max_page_size);
      properties.max_row_group_length(configuration.max_rows_in_row_group);
      if (configuration.use_compression) {
        properties.compression(parquet::Compression::ZSTD);
      }
      return properties.build();
    }();

    std::atomic<int> generated = 0;
    auto generate_delete = [&](int i) {
      gen::RandomDevice random_device(i);

      double deleted_ratio_per_file =
          configuration.rows_in_positional_delete_file * 1.0 / configuration.rows_in_data_file;
      std::string positional_delete_path = "s3://" + data_dir_.path().string() + "/delete" + std::to_string(table_num) +
                                           "_" + std::to_string(i) + ".parquet";
      std::string local_positional_delete_path = positional_delete_path.substr(std::string("s3://").size());

      ASSIGN_OR_FAIL(auto positional_delete_processor,
                     MakePositionalDeleteProcessor(local_positional_delete_path, data_path, random_device,
                                                   deleted_ratio_per_file, positional_delete_properties));
      gen::BatchSizeMaker batch_size_maker(kBatchSize, configuration.rows_in_data_file);

      for (int64_t rows_in_next_batch = batch_size_maker.NextBatchSize(), batch_no = 0; rows_in_next_batch != 0;
           rows_in_next_batch = batch_size_maker.NextBatchSize(), ++batch_no) {
        gen::BatchPtr x = std::make_shared<gen::Batch>(rows_in_next_batch);
        ASSIGN_OR_FAIL(auto batch, program.Generate(rows_in_next_batch));
        ASSERT_OK(positional_delete_processor->Process(x));
      }

      auto val = ++generated;

      if (val * 10 / configuration.positional_delete_files != (val + 1) * 10 / configuration.positional_delete_files) {
        std::cerr << "Generated " << val * 100 / configuration.positional_delete_files << "% of positional delete files"
                  << std::endl;
      }
    };

    ThreadPool pool(10);
    for (int i = 0; i < configuration.positional_delete_files; ++i) {
      pool.Submit([=]() { generate_delete(i); });
    }

    pool.Stop(true);

    for (int i = 0; i < configuration.positional_delete_files; ++i) {
      std::string positional_delete_path = "s3://" + data_dir_.path().string() + "/delete" + std::to_string(table_num) +
                                           "_" + std::to_string(i) + ".parquet";
      std::string local_positional_delete_path = positional_delete_path.substr(std::string("s3://").size());

      ASSERT_OK(state_->AddPositionalDeleteFiles({positional_delete_path}, table_name));
    }

    ASSIGN_OR_FAIL(
        auto defer1,
        state_->CreateTable(
            {GreenplumColumnInfo{.name = std::string(gen::PositionalDeleteTable::kInt4Columns), .type = "int4"}},
            table_name));

    std::vector<std::string> projections{"count(c_int4)"};

    for (const auto& projection : projections) {
      int64_t max_memory_consumed = 0;
      std::atomic<bool> is_stopped = false;

      auto measure_memory = [&]() {
        while (!is_stopped.load()) {
          std::string value_str = Exec("ps x -o rss,command | grep postgres | awk '{s += $1} END {print s}'");
          double value = std::stold(value_str);
          max_memory_consumed = std::max(max_memory_consumed, static_cast<int64_t>(value));
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
      };

      std::thread measuring_worker(measure_memory);

      std::cerr << "Selecting " << projection << " from table '" << table_name
                << "', configuration: " << configuration.ToString() << std::endl;

      pq::PGconnWrapper wrapper(PQconnectdb("dbname = postgres"));
      ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(table_name, projection).Run(wrapper));

      is_stopped.store(true);
      measuring_worker.join();

      auto stats = stats_state_->GetStats(false);
      for (const auto& stat : stats) {
        auto duration_to_string = [](const ::google::protobuf::Duration& duration) {
          std::string milli = std::to_string(duration.nanos() / 1'000'000);
          while (milli.size() < 3) {
            milli = std::string("0") + milli;
          }
          return std::to_string(duration.seconds()) + "." + milli;
        };
        std::cerr << "total duration: " << duration_to_string(stat.durations().total()) << std::endl;
        std::cerr << "positional delete duration: " << duration_to_string(stat.durations().positional()) << std::endl;
        std::cerr << "read duration: " << duration_to_string(stat.durations().read()) << std::endl;
        std::cerr << "positional delete files read: " << stat.positional_delete().files_read() << std::endl;
        std::cerr << "positional delete rows read: " << stat.positional_delete().rows_read() << std::endl;
        std::cerr << "max memory consumed: " << max_memory_consumed / 1024 << " MB" << std::endl;
        std::cerr << "s3 requests: " << stat.s3().s3_requests() << std::endl;
        std::cerr << "bytes read from s3: " << stat.s3().bytes_read_from_s3() / 1024 / 1024 << " MB" << std::endl;
      }
      std::cerr << std::string(80, '-') << std::endl;
    }
  }
}

}  // namespace
}  // namespace tea
