#pragma once

#include <iceberg/common/error.h>

#include <cstdint>
#include <memory>
#include <string>

#include "iceberg/common/fs/filesystem_wrapper.h"

#include "tea/util/measure.h"

namespace tea {

struct IcebergMetrics {
  uint64_t requests = 0;
  uint64_t bytes_read = 0;
  uint64_t files_opened = 0;

  DurationTicks filesystem_duration = 0;
};

class LoggingInputFile : public iceberg::InputFileWrapper {
 public:
  LoggingInputFile(std::shared_ptr<arrow::io::RandomAccessFile> file, std::shared_ptr<IcebergMetrics> metrics)
      : InputFileWrapper(file), metrics_(metrics) {
    iceberg::Ensure(file != nullptr, std::string(__PRETTY_FUNCTION__) + ": fs is nullptr");
    iceberg::Ensure(metrics != nullptr, std::string(__PRETTY_FUNCTION__) + ": metrics is nullptr");
  }

  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    ScopedTimerTicks timer(metrics_->filesystem_duration);
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::ReadAt(position, nbytes, out);
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    ScopedTimerTicks timer(metrics_->filesystem_duration);
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::Read(nbytes, out);
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    ScopedTimerTicks timer(metrics_->filesystem_duration);
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::Read(nbytes);
  }

 private:
  void TakeRequestIntoAccount(int64_t bytes) {
    ++metrics_->requests;
    metrics_->bytes_read += bytes;
  }

  std::shared_ptr<IcebergMetrics> metrics_;
};

class IcebergLoggingFileSystem : public iceberg::FileSystemWrapper {
 public:
  IcebergLoggingFileSystem(std::shared_ptr<arrow::fs::FileSystem> fs, std::shared_ptr<IcebergMetrics> metrics)
      : FileSystemWrapper(fs), metrics_(metrics) {
    iceberg::Ensure(fs != nullptr, std::string(__PRETTY_FUNCTION__) + ": fs is nullptr");
    iceberg::Ensure(metrics != nullptr, std::string(__PRETTY_FUNCTION__) + ": metrics is nullptr");
  }

  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(const std::string& path) override {
    ++metrics_->files_opened;
    ARROW_ASSIGN_OR_RAISE(auto file, FileSystemWrapper::OpenInputFile(path));
    return std::make_shared<LoggingInputFile>(file, metrics_);
  }

 private:
  std::shared_ptr<IcebergMetrics> metrics_;
};

}  // namespace tea
