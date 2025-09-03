#include "tea/table/json_substitutor.h"

#include <array>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>

namespace tea {

namespace {
uint16_t HexToValue(char ch) {
  if (ch >= '0' && ch <= '9') {
    return ch - '0';
  } else if (ch >= 'a' && ch <= 'f') {
    return ch - 'a' + 10;
  } else if (ch >= 'A' && ch <= 'F') {
    return ch - 'A' + 10;
  } else {
    throw std::runtime_error(std::string("JSON substitute: \"\\u\" must be followed by four hexadecimal digits (") +
                             ch + " found)");
  }
}

// https://www.unicode.org/Public/MAPPINGS/VENDORS/MICSFT/WINDOWS/CP1251.TXT
class Cp1251Table {
 public:
  consteval Cp1251Table() : is_cp1251_() {
    for (auto code : kCp1251Codes) {
      is_cp1251_[code] = true;
    }
  }

  bool IsAscii(uint16_t value) const { return value <= kAsciiMaxUnicodeValue; }
  bool IsCp1251(uint16_t value) const { return is_cp1251_[value]; }

  std::string GetUtf8Representation(uint16_t value) const {
    if (value <= 0x007F) {
      uint8_t bytes[1];
      bytes[0] = value;

      std::string result(1, 0);
      std::memcpy(result.data(), bytes, sizeof(bytes));
      return result;
    } else if (value <= 0x07FF) {
      uint8_t bytes[2];
      bytes[1] = 0b10000000 + (value & 0b00111111);
      bytes[0] = 0b11000000 + ((value >> 6) & (0b00011111));

      std::string result(2, 0);
      std::memcpy(result.data(), bytes, sizeof(bytes));
      return result;
    } else /* if (value <= 0xFFFF) is always true */ {
      uint8_t bytes[3];
      bytes[2] = 0b10000000 + (value & 0b00111111);
      bytes[1] = 0b10000000 + ((value >> 6) & 0b00111111);
      bytes[0] = 0b11100000 + ((value >> 12) & (0b00001111));

      std::string result(3, 0);
      std::memcpy(result.data(), bytes, sizeof(bytes));
      return result;
    }
  }

 private:
  static constexpr uint16_t kAsciiMaxUnicodeValue = 127;

  // Command to get this list: awk '{print $2 ","}' tea/table/cp1251_to_unicode.txt | grep 0x
  // 0x98 is undefined, so list contains 127 characters instead of 128
  static constexpr std::array<uint16_t, 127> kCp1251Codes = {
      0x0402, 0x0403, 0x201A, 0x0453, 0x201E, 0x2026, 0x2020, 0x2021, 0x20AC, 0x2030, 0x0409, 0x2039, 0x040A,
      0x040C, 0x040B, 0x040F, 0x0452, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014, 0x2122, 0x0459,
      0x203A, 0x045A, 0x045C, 0x045B, 0x045F, 0x00A0, 0x040E, 0x045E, 0x0408, 0x00A4, 0x0490, 0x00A6, 0x00A7,
      0x0401, 0x00A9, 0x0404, 0x00AB, 0x00AC, 0x00AD, 0x00AE, 0x0407, 0x00B0, 0x00B1, 0x0406, 0x0456, 0x0491,
      0x00B5, 0x00B6, 0x00B7, 0x0451, 0x2116, 0x0454, 0x00BB, 0x0458, 0x0405, 0x0455, 0x0457, 0x0410, 0x0411,
      0x0412, 0x0413, 0x0414, 0x0415, 0x0416, 0x0417, 0x0418, 0x0419, 0x041A, 0x041B, 0x041C, 0x041D, 0x041E,
      0x041F, 0x0420, 0x0421, 0x0422, 0x0423, 0x0424, 0x0425, 0x0426, 0x0427, 0x0428, 0x0429, 0x042A, 0x042B,
      0x042C, 0x042D, 0x042E, 0x042F, 0x0430, 0x0431, 0x0432, 0x0433, 0x0434, 0x0435, 0x0436, 0x0437, 0x0438,
      0x0439, 0x043A, 0x043B, 0x043C, 0x043D, 0x043E, 0x043F, 0x0440, 0x0441, 0x0442, 0x0443, 0x0444, 0x0445,
      0x0446, 0x0447, 0x0448, 0x0449, 0x044A, 0x044B, 0x044C, 0x044D, 0x044E, 0x044F};

  std::array<bool, 256 * 256> is_cp1251_;
};

static constexpr Cp1251Table table_;

}  // namespace

bool NeedToSubstituteAsciiCp1251(std::string_view str) {
  for (size_t i = 0; i + 1 < str.size(); ++i) {
    if (str[i] == '\\' && str[i + 1] == 'u') {
      return true;
    }
  }
  return false;
}

// for each '\u0123' pattern:
// * skip if ascii
// * replace with utf-8 representation if cp1251
// * replace with '?' otherwise
std::string SubstituteAsciiCp1251(std::string data) {
  char* output = data.data();
  const char* end = data.data() + data.size();
  for (const char* input = data.data(); input < end;) {
    if (*input != '\\') {
      *output = *input;
      ++output;
      ++input;
      continue;
    }
    if (input + 1 < end && *(input + 1) == 'u') {
      if (input + 5 >= end) {
        throw std::runtime_error(
            std::string("JSON substitute: \"\\u\" must be followed by four hexadecimal digits, found only ") +
            std::to_string(end - (input + 2)) + " characters");
      }
      uint16_t value = (HexToValue(*(input + 2)) << 12) + (HexToValue(*(input + 3)) << 8) +
                       (HexToValue(*(input + 4)) << 4) + HexToValue(*(input + 5));
      if (!table_.IsAscii(value)) {
        if (!table_.IsCp1251(value)) {
          // non-cp1251 symbol, replace with ?
          *output = '?';
          ++output;
          input += 6;
          continue;
        }
        auto representation = table_.GetUtf8Representation(value);
        std::memcpy(output, representation.data(), representation.size());
        output += representation.size();
        input += 6;
        continue;
      }

      // do not unescape ascii value
      for (int it = 0; it < 6; ++it) {
        *output = *input;
        ++output;
        ++input;
      }
      continue;
    }

    // next symbol is escaped, just copy (do not try to fund '\u0123' pattern starting with this symbol)
    *output = *input;
    ++output;
    ++input;
    if (input < end) {
      *output = *input;
      ++output;
      ++input;
    }
  }

  data.resize(output - data.data());
  return data;
}

}  // namespace tea
