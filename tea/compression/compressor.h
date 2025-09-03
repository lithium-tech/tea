#pragma once

#include <memory>
#include <string>

namespace tea::compression {

class ICompressor {
 public:
  virtual std::string GetName() const = 0;
  virtual void Compress(std::string& data) = 0;
  virtual void Decompress(std::string& data) = 0;

  virtual ~ICompressor() = default;
};

using CompressorPtr = std::shared_ptr<ICompressor>;

}  // namespace tea::compression
