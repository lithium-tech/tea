#include "tea/test_utils/metadata.h"

#include <memory>

#include "arrow/filesystem/filesystem.h"
#include "parquet/arrow/reader.h"

namespace tea {

std::vector<int64_t> GetParquetRowGroupOffsets(const std::string& data_path) {
  std::string path;
  auto fs = arrow::fs::FileSystemFromUri(data_path, &path).ValueOrDie();

  auto input_file = fs->OpenInputFile(path).ValueOrDie();
  parquet::arrow::FileReaderBuilder reader_builder;
  if (!reader_builder.Open(input_file, parquet::default_reader_properties()).ok()) {
    throw std::runtime_error("Cannot open file " + data_path);
  }
  reader_builder.memory_pool(arrow::default_memory_pool());
  reader_builder.properties(parquet::default_arrow_reader_properties());

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader = *reader_builder.Build();

  std::vector<int64_t> offsets;

  auto metadata = arrow_reader->parquet_reader()->metadata();
  auto num_row_groups = metadata->num_row_groups();
  for (int i = 0; i < num_row_groups; ++i) {
    auto row_group = metadata->RowGroup(i);
    auto offset = row_group->file_offset();
    offsets.push_back(offset);
  }

  return offsets;
}

}  // namespace tea
