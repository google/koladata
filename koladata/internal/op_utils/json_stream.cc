// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#include "koladata/internal/op_utils/json_stream.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "unicode/uchar.h"
#include "unicode/unistr.h"
#include "unicode/utf16.h"
#include "unicode/utf8.h"
#include "unicode/utypes.h"

namespace koladata::internal {
namespace {

constexpr char32_t kReplacementCharacter = U'\ufffd';
constexpr size_t kMaxBufferSize = 100;

bool IsEncodableCodePoint(char32_t c) {
  return c < 0x110000 && !U16_IS_SURROGATE(c);
}

// UTF-16 helper functions.

std::pair<char16_t, char16_t> ToUtf16SurrogatePair(char32_t c) {
  return std::make_pair(U16_LEAD(c), U16_TRAIL(c));
}

// UTF-8 helper functions.

int Utf8MultibyteSequenceLength(uint8_t start_byte) {
  if (start_byte < 0xC2) {
    return 1;
  } else if (start_byte < 0xE0) {
    return 2;
  } else if (start_byte < 0xF0) {
    return 3;
  } else if (start_byte < 0xF8) {
    return 4;
  } else {
    return 1;  // Invalid start byte.
  }
}

void AppendUtf8CodePoint(char32_t value, std::string& output) {
  icu::UnicodeString s;
  s.append(static_cast<UChar32>(value));
  s.toUTF8String(output);  // Appended.
};

// Number parsing helper functions.

bool IsHexDigit(std::optional<char32_t> c) {
  return c.has_value() && c < 0x80 && absl::ascii_isxdigit(*c);
}

bool IsOctalDigit(std::optional<char32_t> c) { return c >= '0' && c <= '7'; }

bool IsBinaryDigit(std::optional<char32_t> c) { return c == '0' || c == '1'; }

uint32_t DigitValue(std::optional<char32_t> c) {
  if (c >= '0' && c <= '9') {
    return *c - '0';
  } else if (c >= 'a' && c <= 'f') {
    return 10 + (*c - 'a');
  } else if (c >= 'A' && c <= 'F') {
    return 10 + (*c - 'A');
  } else {
    return 0;
  }
}

template <typename Container>
uint64_t ConvertDigitSpan(const Container& data, size_t begin, size_t end,
                          uint64_t base) {
  DCHECK_LE(begin, end);
  DCHECK_LE(end, data.size());
  uint64_t value = 0;
  for (size_t i = begin; i < end && i < data.size(); ++i) {
    value = value * base + DigitValue(data[i]);
  }
  return value;
}

JsonStreamProcessResult FromSingleStringOutput(std::string output, bool end) {
  absl::InlinedVector<std::string, 1> chunks;
  if (!output.empty()) {
    chunks.push_back(std::move(output));
  }
  return {std::move(chunks), end};
}

}  // namespace

JsonStreamProcessResult JsonSalvageStreamProcessor::Process(
    std::string_view input_chunk, bool end_of_input) {
  DCHECK(output_.empty());
  for (char c : input_chunk) {
    ProcessInputByte(static_cast<uint8_t>(c));
  }
  if (end_of_input) {
    ProcessInputEndInternal();
  }
  return FromSingleStringOutput(ConsumeOutput(), end_of_input);
}

void JsonSalvageStreamProcessor::ProcessInputByte(uint8_t c) {
  if ((U8_IS_SINGLE(c) || U8_IS_LEAD(c)) && !utf8_buffer_.empty()) {
    utf8_buffer_.clear();
    ProcessInputCodePoint(kReplacementCharacter);
  } else if (!U8_IS_TRAIL(c) && !utf8_buffer_.empty()) {
    utf8_buffer_.clear();
    ProcessInputCodePoint(kReplacementCharacter);
  }

  utf8_buffer_.push_back(c);
  if (Utf8MultibyteSequenceLength(utf8_buffer_[0]) > utf8_buffer_.size()) {
    return;  // Wait for more of the UTF-8 multibyte sequence.
  }

  UChar32 code_point;
  int32_t offset = 0;
  U8_NEXT_OR_FFFD(utf8_buffer_.data(), offset, utf8_buffer_.size(), code_point);
  utf8_buffer_.clear();
  ProcessInputCodePoint(static_cast<char32_t>(code_point));
}

void JsonSalvageStreamProcessor::ProcessInputCodePoint(
    std::optional<char32_t> c) {
  for (size_t attempt = 0; attempt < 3; ++attempt) {
    // ProcessAllRules may return false to say that it has updated state but not
    // consumed the input `c` and needs to be called again. It should only need
    // to be called twice consecutively, However, if there is some bug, we call
    // it up to three times consecutively in non-debug mode.
    if (ProcessAllRules(c)) {
      break;  // Input consumed.
    }
    DCHECK_LT(attempt, 2);
  }
}

void JsonSalvageStreamProcessor::ProcessInputEndInternal() {
  ProcessInputCodePoint(std::nullopt);

  while (!container_stack_.empty()) {
    if (container_stack_.back() == ContainerType::kObject) {
      if (!is_object_key_) {
        if (needs_leading_separator_) {
          output_.push_back(':');
          needs_leading_separator_ = false;
        }
        output_.append("null");
      }
      output_.push_back('}');
    } else {
      output_.push_back(']');
    }
    container_stack_.pop_back();
  }
}

// https://spec.json5.org/#white-space
bool IsJson5Whitespace(char32_t c) {
  return u_isUWhiteSpace(c) || c == U'\ufeff';
}

bool IsJson5LineTerminator(std::optional<char32_t> c) {
  return c == '\n' || c == '\r' || c == U'\u2028' || c == U'\u2029';
}

bool JsonSalvageStreamProcessor::IsValidUnquotedStringNextCodePoint(
    std::optional<char32_t> c) {
  return !(c == std::nullopt || IsJson5Whitespace(*c) || c == ',' || c == ':' ||
           c == ']' || c == '}' || c == ')' ||
           (container_stack_.size() < options_.max_depth &&
            (c == '[' || c == '{' || c == '(')));
}

bool JsonSalvageStreamProcessor::ProcessAllRules(std::optional<char32_t> c) {
  switch (state_) {
    case State::kBase:
      return ProcessBaseRules(c);
    case State::kStringDoubleQuote:
    case State::kStringSingleQuote:
    case State::kStringBacktick:
    case State::kStringTripleDoubleQuote:
    case State::kStringTripleSingleQuote:
    case State::kStringUnquoted:
      return ProcessStringRules(c);
    case State::kCommentSingleLine:
    case State::kCommentMultiLine:
      return ProcessCommentRules(c);
    case State::kNumberBuffered:
    case State::kNumberDecimal:
      return ProcessNumberRules(c);
  }
  return true;
}

bool JsonSalvageStreamProcessor::ProcessBaseRules(std::optional<char32_t> c) {
  DCHECK_EQ(state_, State::kBase);

  bool consumed_c = true;
  if (BufferEquals("")) {
    if (c == std::nullopt || IsJson5Whitespace(*c) || c == ',' || c == ':') {
      // Skip whitespace and separators before leaf.
    } else if (c == '#') {
      SetStateWithBuffer(State::kCommentSingleLine, "");
    } else if (c == '/') {
      AppendToBuffer(c);
    } else if (c == ']' || c == '}' || c == ')') {
      EndCurrentContainer();
    } else if (c == '{' && container_stack_.size() < options_.max_depth) {
      StartObject();
    } else if ((c == '[' || c == '(') &&
               container_stack_.size() < options_.max_depth) {
      StartArray();
    } else if (c == '"' || c == '\'') {
      StartString();
      AppendToBuffer(c);
    } else if (c == '`') {
      StartValue();
      AppendToBuffer('`');
    } else if (!is_object_key_ && (c == '-' || c == '+')) {
      StartValue();
      AppendToBuffer(c);
    } else if (!is_object_key_ && c == '0') {
      StartValue();
      SetStateWithBuffer(State::kNumberBuffered, "0");
    } else if (!is_object_key_ && c == '.') {
      StartValue();
      EmitRawOutput("0.");
      SetStateWithBuffer(State::kNumberDecimal, ".");
    } else if (!is_object_key_ && (c >= '1' && c <= '9')) {
      StartValue();
      EmitRawOutput(c);
      SetStateWithBuffer(State::kNumberDecimal, "");
    } else {
      AppendToBuffer(c);
    }
  } else if (BufferEquals("/") && c == '/') {
    SetStateWithBuffer(State::kCommentSingleLine, "");
  } else if (BufferEquals("/") && c == '*') {
    SetStateWithBuffer(State::kCommentMultiLine, "");
  } else if (BufferEquals("\"")) {
    if (c == '"') {
      AppendToBuffer('"');
    } else {
      SetStateWithBuffer(State::kStringDoubleQuote, "");
      consumed_c = false;
    }
  } else if (BufferEquals("'")) {
    if (c == '\'') {
      AppendToBuffer('\'');
    } else {
      SetStateWithBuffer(State::kStringSingleQuote, "");
      consumed_c = false;
    }
  } else if (BufferEquals("`")) {
    if (c == '`') {
      AppendToBuffer('`');
    } else {
      EmitRawOutput('"');
      SetStateWithBuffer(State::kStringBacktick, "");
      consumed_c = false;
    }
  } else if (BufferEquals("\"\"")) {
    if (c == '"') {
      SetStateWithBuffer(State::kStringTripleDoubleQuote, "");
    } else {
      EmitRawOutput('"');
      EndValue();
      consumed_c = false;
    }
  } else if (BufferEquals("''")) {
    if (c == '\'') {
      SetStateWithBuffer(State::kStringTripleSingleQuote, "");
    } else {
      EmitRawOutput('"');
      EndValue();
      consumed_c = false;
    }
  } else if (BufferEquals("``") && c == std::nullopt) {
    EmitRawOutput("\"\"");
  } else if (BufferEquals("-") && c == '0') {
    EmitRawOutput('-');
    SetStateWithBuffer(State::kNumberBuffered, "0");
  } else if (BufferEquals("-") && (c >= '1' && c <= '9')) {
    EmitRawOutput('-');
    EmitRawOutput(c);
    SetStateWithBuffer(State::kNumberDecimal, "");
  } else if (BufferEquals("-") && c == '.') {
    EmitRawOutput("-0.");
    SetStateWithBuffer(State::kNumberDecimal, ".");
  } else if (BufferEquals("+") && c == '0') {
    SetStateWithBuffer(State::kNumberBuffered, "0");
  } else if (BufferEquals("+") && (c >= '1' && c <= '9')) {
    EmitRawOutput(c);
    SetStateWithBuffer(State::kNumberDecimal, "");
  } else if (BufferEquals("+") && c == '.') {
    EmitRawOutput("0.");
    SetStateWithBuffer(State::kNumberDecimal, ".");
  } else if (!IsValidUnquotedStringNextCodePoint(c)) {
    // Treat buffer as complete unquoted string.
    StartString();
    for (char32_t value : buffer_) {
      EmitJsonStringCodePoint(value);
    }
    EndString();
    consumed_c = false;
  } else {
    // Append to buffer and match against atoms (true, false, null, NaN, etc.).
    AppendToBuffer(c);

    bool fully_matched_atom = false;
    bool partially_matched_atom = false;
    if (!is_object_key_) {
      auto emit_buffered_atom_if_matched = [&](std::string_view input_atom,
                                               std::string_view output_atom) {
        if (fully_matched_atom) {
          return;
        }
        if (BufferContainsAnycasePrefixOf(input_atom)) {
          partially_matched_atom = true;
          if (buffer_.size() == input_atom.size()) {
            fully_matched_atom = true;
            EmitAtom(output_atom);
          }
        }
      };
      emit_buffered_atom_if_matched("true", "true");
      emit_buffered_atom_if_matched("false", "false");
      emit_buffered_atom_if_matched("null", "null");
      emit_buffered_atom_if_matched("none", "null");
      if (options_.allow_nan) {
        emit_buffered_atom_if_matched("nan", "NaN");
        emit_buffered_atom_if_matched("-nan", "NaN");
        emit_buffered_atom_if_matched("+nan", "NaN");
        emit_buffered_atom_if_matched("infinity", "Infinity");
        emit_buffered_atom_if_matched("-infinity", "-Infinity");
        emit_buffered_atom_if_matched("+infinity", "Infinity");
      }
    }

    if (fully_matched_atom) {
      ClearBuffer();
    } else if (partially_matched_atom) {
      // Already appended to buffer, will continue accumulating.
    } else if (BufferEquals("b\"") || BufferEquals("b'") ||
               BufferEquals("u\"") || BufferEquals("u'")) {
      // Ignore various python-style string prefixes, then retry.
      SetStateWithBuffer(State::kBase, "");
      consumed_c = false;
    } else {
      // Treat buffer as beginning of unquoted string.
      StartString();
      for (char32_t value : buffer_) {
        EmitJsonStringCodePoint(value);
      }
      SetStateWithBuffer(State::kStringUnquoted, "");
    }
  }

  return consumed_c;
}

bool JsonSalvageStreamProcessor::ProcessStringRules(std::optional<char32_t> c) {
  bool consumed_c = true;
  if (state_ == State::kStringDoubleQuote ||
      state_ == State::kStringSingleQuote ||
      state_ == State::kStringTripleDoubleQuote ||
      state_ == State::kStringTripleSingleQuote ||
      state_ == State::kStringBacktick) {
    // Expected buffer states:
    // ""       // Default state.
    // "\"      // Started escape.
    // "\\r"    // Start of ignored end-of-line for \r and \r\n line endings.
    // "\""     // Triple-double-quote only: saw one double quote
    // "\"\""   // Triple-double-quote only: saw two double quotes
    // "'"      // Triple-single-quote only: saw one single quote
    // "''"     // Triple-single-quote only: saw two single quotes
    // "\u"...  // Buffered \uXXXX escape.
    // "\o"...  // Buffered 1-3 digit octal escape.
    // "\x"...  // Buffered \xXX escape.
    // "\U"...  // Buffered \UXXXXXXXX escape.
    // "\{"...  // Buffered \u{...} or \U{...} escape.

    if (BufferEquals("")) {
      if (c == std::nullopt) {
        EndString();
      } else if (c == '\\') {
        AppendToBuffer('\\');
      } else {
        if ((state_ == State::kStringDoubleQuote && c == '"') ||
            (state_ == State::kStringSingleQuote && c == '\'') ||
            (state_ == State::kStringBacktick && c == '`')) {
          EndString();
        } else if ((state_ == State::kStringTripleDoubleQuote && c == '"') ||
                   (state_ == State::kStringTripleSingleQuote && c == '\'')) {
          AppendToBuffer(c);
        } else {
          EmitJsonStringCodePoint(*c);
        }
      }
    } else if (BufferEquals("\\")) {
      // First character of escape sequence.
      if (c == '"' || c == '\\' || c == '/' || c == 'b' || c == 'f' ||
          c == 'n' || c == 'r' || c == 't') {
        EmitRawOutput('\\');
        EmitRawOutput(c);
        ClearBuffer();
      } else if (c == 'a') {
        EmitJsonStringCodePoint('\a');
        ClearBuffer();
      } else if (c == 'v') {
        EmitJsonStringCodePoint('\v');
        ClearBuffer();
      } else if (c == '`' || c == '\'') {
        EmitRawOutput(c);
        ClearBuffer();
      } else if (c == 'u' || c == 'U') {
        AppendToBuffer(c);
      } else if (c == 'x' || c == 'X') {
        AppendToBuffer('x');
      } else if (c.has_value() && IsOctalDigit(*c)) {
        AppendToBuffer('o');
        AppendToBuffer(c);
      } else if (c != '\r' && IsJson5LineTerminator(c)) {
        // Skipped end-of-line (not \r or \r\n line ending).
        ClearBuffer();
      } else if (c == '\r') {
        // Skipped end-of-line (\r or \r\n line ending).
        AppendToBuffer('\r');
      } else if (c == std::nullopt) {
        EndString();
      } else {
        // Invalid escape "\?" -> "\\?".
        EmitRawOutput("\\\\");
        EmitRawOutput(c);
        ClearBuffer();
      }
    } else if (BufferEquals("\\\r")) {
      if (c == '\n') {
        // Skipped end-of-line (\r\n line ending).
        ClearBuffer();  // NOMUTANTS -- redundant; kept for readability
      } else {
        // Skipped end-of-line (\r line ending).
        ClearBuffer();
        consumed_c = false;
      }
    } else if (BufferEquals("\"")) {
      // Maybe end of triple double quoted string.
      DCHECK_EQ(state_, State::kStringTripleDoubleQuote);
      if (c == '"') {
        AppendToBuffer('"');
      } else if (c.has_value()) {
        EmitJsonStringCodePoint('"');
        EmitJsonStringCodePoint(*c);
        ClearBuffer();
      } else {
        EndString();
      }
    } else if (BufferEquals("\"\"")) {
      // Maybe end of triple double quoted string.
      DCHECK_EQ(state_, State::kStringTripleDoubleQuote);
      if (c == '"' || c == std::nullopt) {
        EndString();
      } else if (c.has_value()) {
        EmitJsonStringCodePoint('"');
        EmitJsonStringCodePoint('"');
        EmitJsonStringCodePoint(*c);
        ClearBuffer();
      }
    } else if (BufferEquals("'")) {
      // Maybe end of triple single quoted string.
      DCHECK_EQ(state_, State::kStringTripleSingleQuote);
      if (c == '\'') {
        AppendToBuffer('\'');
      } else if (c.has_value()) {
        EmitJsonStringCodePoint('\'');
        EmitJsonStringCodePoint(*c);
        ClearBuffer();
      } else {
        EndString();
      }
    } else if (BufferEquals("''")) {
      // Maybe end of triple single quoted string.
      DCHECK_EQ(state_, State::kStringTripleSingleQuote);
      if (c == '\'' || c == std::nullopt) {
        EndString();
      } else {
        EmitJsonStringCodePoint('\'');
        EmitJsonStringCodePoint('\'');
        EmitJsonStringCodePoint(*c);
        ClearBuffer();
      }
    } else if (BufferStartsWith("\\u")) {
      if (BufferEquals("\\u") && c == '{') {
        ClearBuffer();
        AppendToBuffer('\\');
        AppendToBuffer('{');
      } else if (IsHexDigit(c)) {
        AppendToBuffer(c);
        if (buffer_.size() == 6) {
          EmitJsonStringCodePoint(ConvertBufferValueAt(2, 16));
          ClearBuffer();
        }
      } else {
        EmitJsonStringCodePoint(ConvertBufferValueAt(2, 16));
        ClearBuffer();
        consumed_c = false;
      }
    } else if (BufferStartsWith("\\o")) {
      if (c.has_value() && IsOctalDigit(*c)) {
        AppendToBuffer(c);
        if (buffer_.size() == 5) {
          EmitJsonStringCodePoint(ConvertBufferValueAt(2, 8));
          ClearBuffer();
        }
      } else {
        EmitJsonStringCodePoint(ConvertBufferValueAt(2, 8));
        ClearBuffer();
        consumed_c = false;
      }
    } else if (BufferStartsWith("\\x")) {
      if (IsHexDigit(c)) {
        AppendToBuffer(c);
        if (buffer_.size() == 4) {
          EmitJsonStringCodePoint(ConvertBufferValueAt(2, 16));
          ClearBuffer();
        }
      } else {
        EmitJsonStringCodePoint(ConvertBufferValueAt(2, 16));
        ClearBuffer();
        consumed_c = false;
      }
    } else if (BufferStartsWith("\\U")) {
      if (IsHexDigit(c)) {
        AppendToBuffer(c);
        if (buffer_.size() == 10) {
          EmitJsonStringCodePoint(ConvertBufferValueAt(2, 16));
          ClearBuffer();
        }
      } else if (BufferEquals("\\U") && c == '{') {
        ClearBuffer();
        AppendToBuffer('\\');
        AppendToBuffer('{');
      } else {
        EmitJsonStringCodePoint(ConvertBufferValueAt(2, 16));
        ClearBuffer();
        consumed_c = false;
      }
    } else if (BufferStartsWith("\\{")) {
      if (c.has_value() && IsHexDigit(*c) && buffer_.size() < kMaxBufferSize) {
        AppendToBuffer(c);
      } else {
        EmitJsonStringCodePoint(ConvertBufferValueAt(2, 16));
        ClearBuffer();
        consumed_c = (c == '}');
      }
    } else {
      // Unexpected buffer state.
      SetStateWithBuffer(State::kBase, "");
      DCHECK(false);
    }
  } else if (state_ == State::kStringUnquoted) {  // NOMUTANTS -- Always true.
    // Buffer expected to be empty.

    if (!IsValidUnquotedStringNextCodePoint(c)) {
      EndString();
      consumed_c = false;
    } else {
      EmitJsonStringCodePoint(*c);
    }
  } else {  // NOMUTANTS -- Statically unreachable.
    // Unexpected state_.
    SetStateWithBuffer(State::kBase, "");
    DCHECK(false);
  }
  return consumed_c;
}

bool JsonSalvageStreamProcessor::ProcessCommentRules(
    std::optional<char32_t> c) {
  bool consumed_c = true;
  if (state_ == State::kCommentSingleLine) {
    // Buffer expected to be empty.

    if (IsJson5LineTerminator(c)) {
      SetStateWithBuffer(State::kBase, "");
    }
  } else if (state_ == State::kCommentMultiLine) {  // NOMUTANTS -- Always true.
    // Expected buffer states:
    // ""    // Waiting for '*'.
    // "*".  // Waiting for '/'.

    if (buffer_.empty()) {
      if (c == '*') {
        AppendToBuffer(c);
      }
    } else {
      if (c == '/') {
        SetStateWithBuffer(State::kBase, "");
      } else {
        ClearBuffer();
      }
    }
  } else {  // NOMUTANTS -- Statically unreachable.
    // Unexpected state_.
    SetStateWithBuffer(State::kBase, "");
    DCHECK(false);
  }
  return consumed_c;
}

bool JsonSalvageStreamProcessor::ProcessNumberRules(std::optional<char32_t> c) {
  if (c == '_' || c == '\'') {
    // Ignore _ and ' digit separators anywhere they appear.
    return true;  // consumed_c = true
  }

  bool consumed_c = true;
  if (state_ == State::kNumberBuffered) {
    // Expected buffer states:
    // "0"      // One or more leading zeroes.
    // "0x"...  // Buffered hexadecimal literal.
    // "0o"...  // Buffered octal literal.
    // "0b"...  // Buffered binary literal.

    if (BufferEquals("0") && c >= '1' && c <= '9') {
      EmitRawOutput(c);
      SetStateWithBuffer(State::kNumberDecimal, "");
    } else if (BufferEquals("0") && c == '0') {
      // Accumulate any number of leading zeros.
    } else if ((BufferEquals("") || BufferEquals("0")) && c == '.') {
      EmitRawOutput("0.");
      SetStateWithBuffer(State::kNumberDecimal, ".");
    } else if (BufferEquals("0") && (c == 'x' || c == 'X')) {
      AppendToBuffer('x');
    } else if (BufferEquals("0") && (c == 'o' || c == 'O')) {
      AppendToBuffer('o');
    } else if (BufferEquals("0") && (c == 'b' || c == 'B')) {
      AppendToBuffer('b');
    } else if (BufferStartsWith("0x") && IsHexDigit(c)) {
      if (buffer_.size() < kMaxBufferSize) {
        AppendToBuffer(c);
      }
    } else if (BufferStartsWith("0o") && IsOctalDigit(c)) {
      if (buffer_.size() < kMaxBufferSize) {
        AppendToBuffer(c);
      }
    } else if (BufferStartsWith("0b") && IsBinaryDigit(c)) {
      if (buffer_.size() < kMaxBufferSize) {
        AppendToBuffer(c);
      }
    } else {
      if (BufferEquals("") || BufferEquals("0")) {
        EmitRawOutput('0');
      } else if (BufferStartsWith("0x")) {
        EmitRawOutput(absl::StrCat(ConvertBufferValueAt(2, 16)));
      } else if (BufferStartsWith("0o")) {
        EmitRawOutput(absl::StrCat(ConvertBufferValueAt(2, 8)));
      } else if (BufferStartsWith("0b")) {
        EmitRawOutput(absl::StrCat(ConvertBufferValueAt(2, 2)));
      }

      if (c == 'e' || c == 'E') {
        EmitRawOutput('e');
        SetStateWithBuffer(State::kNumberDecimal, ".0e");
      } else {
        EndValue();
        if (c == 'n' || c == 'l' || c == 'L') {
          // Allow trailing `n` like in JS, and `l` and `L` like in Python 2.
        } else {
          consumed_c = false;
        }
      }
    }
  } else if (state_ == State::kNumberDecimal) {  // NOMUTANTS -- Always true.
    // Expected buffer states:
    // ""       // Integer part, at least one digit already.
    // "."      // Decimal point, no fractional digits yet.
    // ".0"     // At least one fractional digit.
    // ".0e"    // Exponent separator, no exponent sign or digits yet.
    // ".0e+"   // Exponent separator and sign, no digits yet.
    // ".0e+0"  // At least one exponent digit.

    if (c >= '0' && c <= '9') {
      if (BufferEquals(".0e")) {
        AppendToBuffer('+');
      }
      if (BufferEquals(".") || BufferEquals(".0e+")) {
        AppendToBuffer('0');
      }
      EmitRawOutput(c);
    } else if (BufferEquals("") && c == '.') {
      AppendToBuffer('.');
      EmitRawOutput('.');
    } else if (BufferEquals("") && (c == 'e' || c == 'E')) {
      AppendToBuffer('.');
      AppendToBuffer('0');
      AppendToBuffer('e');
      EmitRawOutput('e');
    } else if (BufferEquals(".") && (c == 'e' || c == 'E')) {
      AppendToBuffer('0');
      AppendToBuffer('e');
      EmitRawOutput("0e");
    } else if (BufferEquals(".0") && (c == 'e' || c == 'E')) {
      AppendToBuffer('e');
      EmitRawOutput("e");
    } else if (BufferEquals(".0e") && (c == '+' || c == '-')) {
      AppendToBuffer('+');
      EmitRawOutput(c);
    } else {
      if (BufferEquals(".") || BufferEquals(".0e") || BufferEquals(".0e+")) {
        EmitRawOutput('0');
      }
      EndValue();
      if (c == 'n' || c == 'l' || c == 'L') {
        // Allow trailing `n` like in JS, and `l` and `L` like in Python 2.
      } else {
        consumed_c = false;
      }
    }
  } else {  // NOMUTANTS -- Statically unreachable.
    SetStateWithBuffer(State::kBase, "");
    DCHECK(false);
  }
  return consumed_c;
}

void JsonSalvageStreamProcessor::SetStateWithBuffer(State state,
                                                    std::string_view s) {
  SetBuffer(s);
  state_ = state;
}

void JsonSalvageStreamProcessor::ClearBuffer() { buffer_.clear(); }

void JsonSalvageStreamProcessor::SetBuffer(std::string_view s) {
  ClearBuffer();
  AppendToBuffer(s);
}

void JsonSalvageStreamProcessor::AppendToBuffer(std::optional<char32_t> c) {
  if (c.has_value()) {
    buffer_.push_back(*c);
  }
}

void JsonSalvageStreamProcessor::AppendToBuffer(std::string_view s) {
  for (const char c : s) {
    buffer_.push_back(static_cast<char32_t>(c));
  }
}

bool JsonSalvageStreamProcessor::BufferEquals(std::string_view s) const {
  if (s.size() != buffer_.size()) {
    return false;
  }
  for (size_t i = 0; i < s.size(); ++i) {
    if (s[i] != buffer_[i]) {
      return false;
    }
  }
  return true;
}

bool JsonSalvageStreamProcessor::BufferStartsWith(std::string_view s) const {
  if (buffer_.size() < s.size()) {
    return false;
  }
  for (size_t i = 0; i < s.size(); ++i) {
    if (buffer_[i] != s[i]) {
      return false;
    }
  }
  return true;
}

bool JsonSalvageStreamProcessor::BufferContainsAnycasePrefixOf(
    std::string_view s) const {
  if (buffer_.size() > s.size()) {
    return false;
  }
  for (size_t i = 0; i < buffer_.size(); ++i) {
    if (buffer_[i] >= 0x80 ||
        absl::ascii_tolower(buffer_[i]) != absl::ascii_tolower(s[i])) {
      return false;
    }
  }
  return true;
}

uint64_t JsonSalvageStreamProcessor::ConvertBufferValueAt(size_t offset,
                                                          uint64_t base) const {
  return ConvertDigitSpan(buffer_, offset, buffer_.size(), base);
}

void JsonSalvageStreamProcessor::StartValue() {
  if (needs_leading_separator_) {
    if (GetCurrentContainer() == ContainerType::kTopLevel) {
      EmitRawOutput('\n');
    } else if (GetCurrentContainer() == ContainerType::kObject &&
               !is_object_key_) {
      EmitRawOutput(':');
    } else {
      EmitRawOutput(',');
    }
  }
  needs_leading_separator_ = false;
}

void JsonSalvageStreamProcessor::StartNonObjectKeyValue() {
  StartValue();
  if (is_object_key_) {
    EmitRawOutput("\"\":");
    is_object_key_ = false;
  }
}

void JsonSalvageStreamProcessor::EndValue() {
  SetStateWithBuffer(State::kBase, "");
  needs_leading_separator_ = true;
  if (GetCurrentContainer() == ContainerType::kObject) {
    is_object_key_ = !is_object_key_;
  }
}

JsonSalvageStreamProcessor::ContainerType
JsonSalvageStreamProcessor::GetCurrentContainer() const {
  return container_stack_.empty() ? ContainerType::kTopLevel
                                  : container_stack_.back();
}

void JsonSalvageStreamProcessor::StartArray() {
  StartNonObjectKeyValue();
  EmitRawOutput('[');
  container_stack_.push_back(ContainerType::kArray);
  SetStateWithBuffer(State::kBase, "");
  needs_leading_separator_ = false;
  is_object_key_ = false;
}

void JsonSalvageStreamProcessor::StartObject() {
  StartNonObjectKeyValue();
  EmitRawOutput('{');
  container_stack_.push_back(ContainerType::kObject);
  SetStateWithBuffer(State::kBase, "");
  needs_leading_separator_ = false;
  is_object_key_ = true;
}

void JsonSalvageStreamProcessor::EndCurrentContainer() {
  if (!container_stack_.empty()) {
    switch (container_stack_.back()) {
      case ContainerType::kArray:
        EmitRawOutput(']');
        break;
      case ContainerType::kObject:
        if (!is_object_key_) {
          EmitRawOutput(":null");
        }
        EmitRawOutput('}');
        break;
      default:
        // Should be unreachable.
        DCHECK(false);
        break;
    }
    container_stack_.pop_back();
  }
  is_object_key_ = false;
  EndValue();
}

void JsonSalvageStreamProcessor::StartString() {
  StartValue();
  EmitJsonStringStart();
}

void JsonSalvageStreamProcessor::EndString() {
  EmitJsonStringEnd();
  EndValue();
}

void JsonSalvageStreamProcessor::EmitAtom(std::string_view s) {
  StartValue();
  EmitRawOutput(s);
  EndValue();
}

void JsonSalvageStreamProcessor::EmitJsonStringStart() { EmitRawOutput('"'); }

void JsonSalvageStreamProcessor::EmitJsonStringCodePoint(
    std::optional<char32_t> c) {
  if (!c.has_value()) {
    return;
  }

  if (json_string_utf16_first_surrogate_.has_value() && !U16_IS_TRAIL(*c)) {
    json_string_utf16_first_surrogate_.reset();
    EmitJsonStringCodePoint(kReplacementCharacter);
  }

  if (c == '\b') {
    EmitRawOutput("\\b");
  } else if (c == '\f') {
    EmitRawOutput("\\f");
  } else if (c == '\n') {
    EmitRawOutput("\\n");
  } else if (c == '\r') {
    EmitRawOutput("\\r");
  } else if (c == '\t') {
    EmitRawOutput("\\t");
  } else if (c == '\\') {
    EmitRawOutput("\\\\");
  } else if (c == '"') {
    EmitRawOutput("\\\"");
  } else if (c < 0x20) {
    EmitRawOutput(absl::StrFormat("\\u%04x", *c));
  } else if (U16_IS_LEAD(*c)) {
    // `c` is not a scalar code point that we can represent normally, but rather
    // the first code point in a UTF-16 surrogate pair. We don't know whether
    // the second half of the pair exists or is valid yet, so we buffer it for
    // now.
    json_string_utf16_first_surrogate_ = c;
  } else if (U16_IS_TRAIL(*c)) {
    // `c` is the second code point in a UTF-16 surrogate pair (or should be).
    // We match it with the first half and emit a full code point, or if there
    // is no first half, we emit REPLACEMENT CHARACTER.
    if (json_string_utf16_first_surrogate_.has_value()) {
      const char32_t c32 =
          U16_GET_SUPPLEMENTARY(json_string_utf16_first_surrogate_.value(), *c);
      if (options_.ensure_ascii) {
        auto [a, b] = ToUtf16SurrogatePair(c32);
        EmitRawOutput(absl::StrFormat("\\u%04x\\u%04x",
                                      static_cast<uint32_t>(a),
                                      static_cast<uint32_t>(b)));
      } else {
        EmitRawOutput(c32);
      }
      json_string_utf16_first_surrogate_.reset();
    } else {
      EmitJsonStringCodePoint(kReplacementCharacter);
    }
  } else if (c.has_value() && !IsEncodableCodePoint(*c)) {
    EmitJsonStringCodePoint(kReplacementCharacter);
  } else if (c >= 0x80 && options_.ensure_ascii) {
    if (c < 0x10000) {
      EmitRawOutput(absl::StrFormat("\\u%04x", static_cast<uint32_t>(*c)));
    } else {
      auto [a, b] = ToUtf16SurrogatePair(*c);
      EmitRawOutput(absl::StrFormat("\\u%04x\\u%04x", static_cast<uint32_t>(a),
                                    static_cast<uint32_t>(b)));
    }
  } else {
    EmitRawOutput(c);
  }
}

void JsonSalvageStreamProcessor::EmitJsonStringEnd() {
  if (json_string_utf16_first_surrogate_.has_value()) {
    json_string_utf16_first_surrogate_.reset();
    EmitJsonStringCodePoint(kReplacementCharacter);
  }
  EmitRawOutput('"');
}

void JsonSalvageStreamProcessor::EmitRawOutput(std::string_view s) {
  output_.append(s);
}

void JsonSalvageStreamProcessor::EmitRawOutput(char c) { output_.push_back(c); }

void JsonSalvageStreamProcessor::EmitRawOutput(char32_t c) {
  AppendUtf8CodePoint(c, output_);
}

void JsonSalvageStreamProcessor::EmitRawOutput(std::optional<char32_t> c) {
  if (c.has_value()) {
    EmitRawOutput(*c);
  }
}

std::string JsonSalvageStreamProcessor::ConsumeOutput() {
  std::string output = std::move(output_);
  output_.clear();  // NOMUTANTS -- to avoid implementation-defined state
  return output;
}

JsonStreamProcessResult JsonHeadStreamProcessor::Process(
    std::string_view input_chunk, bool end_of_input) {
  std::string output;
  if (line_number_ >= options_.n) {
    return FromSingleStringOutput(std::move(output), true);
  }
  for (char c : input_chunk) {
    output.push_back(c);
    if (c == '\n') {
      ++line_number_;
      if (line_number_ >= options_.n) {
        return FromSingleStringOutput(std::move(output), true);
      }
    }
  }
  return FromSingleStringOutput(std::move(output), end_of_input);
}

JsonStreamProcessResult JsonPrettifyStreamProcessor::Process(
    std::string_view input_chunk, bool end_of_input) {
  std::string output;

  auto emit_newline_and_indent_if_needed = [&]() {
    if (needs_newline_and_indent_) {
      output.push_back('\n');
      for (int64_t i = 0; i < container_depth_; ++i) {
        output.append(options_.indent_string);
      }
      needs_newline_and_indent_ = false;
    }
  };

  for (char c : input_chunk) {
    if (is_in_string_) {
      output.push_back(c);
      if (is_in_escape_) {
        is_in_escape_ = false;
      } else if (c == '"') {
        is_in_string_ = false;
        if (container_depth_ == 0) {
          needs_newline_and_indent_ = true;
        }
      } else if (c == '\\') {
        is_in_escape_ = true;
      }
    } else {
      if (absl::ascii_isspace(c)) {
        // Strip original whitespace and normalize whitespace between top-level
        // values.
        if (container_depth_ == 0) {
          needs_newline_and_indent_ = true;
        }
      } else if (c == '"') {
        emit_newline_and_indent_if_needed();
        output.push_back('"');
        is_in_string_ = true;
        has_contents_ = true;
      } else if (c == '[' || c == '{') {
        emit_newline_and_indent_if_needed();
        ++container_depth_;
        output.push_back(c);
        needs_newline_and_indent_ = true;
        has_contents_ = false;
      } else if (c == ']' || c == '}') {
        --container_depth_;
        if (has_contents_) {
          needs_newline_and_indent_ = true;
          emit_newline_and_indent_if_needed();
        }
        output.push_back(c);
        needs_newline_and_indent_ = true;
        has_contents_ = true;
      } else if (c == ',') {
        output.push_back(c);
        needs_newline_and_indent_ = true;
        has_contents_ = true;
      } else if (c == ':') {
        output.push_back(c);
        output.push_back(' ');
        has_contents_ = true;
      } else {
        emit_newline_and_indent_if_needed();
        output.push_back(c);
        has_contents_ = true;
      }
    }
  }

  return FromSingleStringOutput(std::move(output), end_of_input);
}

JsonStreamProcessResult JsonCompactifyStreamProcessor::Process(
    std::string_view input_chunk, bool end_of_input) {
  std::string output;

  auto emit_newline_if_needed = [&]() {
    if (container_depth_ == 0 && is_in_value_) {
      output.push_back('\n');
      is_in_value_ = false;
    }
  };

  for (char c : input_chunk) {
    if (is_in_string_) {
      output.push_back(c);
      if (is_in_escape_) {
        is_in_escape_ = false;
      } else if (c == '"') {
        is_in_string_ = false;
        emit_newline_if_needed();
      } else if (c == '\\') {
        is_in_escape_ = true;
      }
    } else {
      if (absl::ascii_isspace(c)) {
        emit_newline_if_needed();
      } else if (c == '"') {
        output.push_back(c);
        is_in_value_ = true;
        is_in_string_ = true;
      } else if (c == '[' || c == '{') {
        emit_newline_if_needed();
        output.push_back(c);
        ++container_depth_;
        is_in_value_ = true;
      } else if (c == ']' || c == '}') {
        output.push_back(c);
        --container_depth_;
        emit_newline_if_needed();
      } else {
        output.push_back(c);
        is_in_value_ = true;
      }
    }
  }
  if (end_of_input && is_in_value_) {
    output.push_back('\n');
  }
  return FromSingleStringOutput(std::move(output), end_of_input);
}

JsonStreamProcessResult JsonSelectNonemptyObjectsStreamProcessor::Process(
    std::string_view input_chunk, bool end_of_input) {
  std::string output;
  for (char c : input_chunk) {
    switch (state_) {
      case State::kStart:
        if (c == '{') {
          state_ = State::kOpenBrace;
        } else {
          state_ = State::kDropLine;
        }
        break;
      case State::kOpenBrace:
        if (c == '}') {
          state_ = State::kDropLine;
        } else {
          output.push_back('{');
          output.push_back(c);
          state_ = State::kKeepLine;
        }
        break;
      case State::kDropLine:
        if (c == '\n') {
          state_ = State::kStart;
        }
        break;
      case State::kKeepLine:
        output.push_back(c);
        if (c == '\n') {
          state_ = State::kStart;
        }
        break;
    }
  }
  return FromSingleStringOutput(std::move(output), end_of_input);
}

JsonStreamProcessResult JsonSelectNonemptyArraysStreamProcessor::Process(
    std::string_view input_chunk, bool end_of_input) {
  std::string output;
  for (char c : input_chunk) {
    switch (state_) {
      case State::kStart:
        if (c == '[') {
          state_ = State::kOpenSquareBracket;
        } else {
          state_ = State::kDropLine;
        }
        break;
      case State::kOpenSquareBracket:
        if (c == ']') {
          state_ = State::kDropLine;
        } else {
          output.push_back('[');
          output.push_back(c);
          state_ = State::kKeepLine;
        }
        break;
      case State::kDropLine:
        if (c == '\n') {
          state_ = State::kStart;
        }
        break;
      case State::kKeepLine:
        output.push_back(c);
        if (c == '\n') {
          state_ = State::kStart;
        }
        break;
    }
  }
  return FromSingleStringOutput(std::move(output), end_of_input);
}

JsonStreamProcessResult JsonSelectNonnullStreamProcessor::Process(
    std::string_view input_chunk, bool end_of_input) {
  std::string output;
  for (char c : input_chunk) {
    switch (state_) {
      case State::kStart:
        if (c == 'n') {
          state_ = State::kDropLine;
        } else {
          output.push_back(c);
          state_ = State::kKeepLine;
        }
        break;
      case State::kDropLine:
        if (c == '\n') {
          state_ = State::kStart;
        }
        break;
      case State::kKeepLine:
        output.push_back(c);
        if (c == '\n') {
          state_ = State::kStart;
        }
        break;
    }
  }
  return FromSingleStringOutput(std::move(output), end_of_input);
}

JsonStreamProcessResult JsonExtractValuesStreamProcessor::Process(
    std::string_view input_chunk, bool end_of_input) {
  std::string output;

  auto run_processor = [&](auto processor, std::string input) -> std::string {
    auto [chunks, end] = processor.Process(std::move(input), true);
    return absl::StrJoin(chunks, "");
  };

  auto emit_path_array = [&]() {
    // Emit `[[path...],` part of [path, value] pair.
    output.append("[[");
    for (size_t i = 0; i < container_path_stack_.size(); ++i) {
      auto path_part = container_path_stack_[i];
      if (int64_t* index = std::get_if<int64_t>(&path_part)) {
        absl::StrAppend(&output, *index);
      } else if (std::string* key = std::get_if<std::string>(&path_part)) {
        output.append(run_processor(JsonQuoteStreamProcessor{}, *key));
      }
      if (i < container_path_stack_.size() - 1) {
        output.push_back(',');
      }
    }
    output.append("],");
  };

  auto try_start_match_at_current_path = [&]() {
    DCHECK(!is_in_match_);
    is_in_match_ = options_.path_match_fn(container_path_stack_);
    if (is_in_match_) {
      match_depth_ = 0;
      output.push_back(has_any_matches_ ? ',' : '[');
      has_any_matches_ = true;
      if (options_.with_path) {
        emit_path_array();
      }
    }
  };

  auto is_in_array = [&]() {
    return !container_path_stack_.empty() &&
           std::holds_alternative<int64_t>(container_path_stack_.back());
  };

  auto is_in_object = [&]() {
    return !container_path_stack_.empty() &&
           std::holds_alternative<std::string>(container_path_stack_.back());
  };

  for (char c : input_chunk) {
    if (container_path_stack_.empty() && !is_in_match_ && c != '\n') {
      try_start_match_at_current_path();
    }

    // Handle deferred processing of first array element.
    if (!is_in_match_ && is_in_array() &&
        std::get<int64_t>(container_path_stack_.back()) == -1 && c != ']') {
      container_path_stack_.back() = 0;
      try_start_match_at_current_path();
    }

    if (is_in_match_) {
      // May be popped back off, but only on this iteration.
      output.push_back(c);
    }

    if (is_in_string_) {
      if (!is_in_match_ && is_object_key_) {
        key_literal_buffer_.push_back(c);
      }
      if (is_in_escape_) {
        is_in_escape_ = false;
      } else if (c == '"') {
        is_in_string_ = false;
      } else if (c == '\\') {
        is_in_escape_ = true;
      }
    } else {
      if (c == '"') {
        is_in_string_ = true;
        if (!is_in_match_ && is_object_key_) {
          key_literal_buffer_.push_back('"');
        }
      } else if (c == '[') {
        if (!is_in_match_) {
          // Defer until we know the array is non-empty (see above).
          container_path_stack_.push_back(-1);  // Placeholder.
          is_object_key_ = false;
        } else {
          ++match_depth_;
        }
      } else if (c == ']' || c == '}') {
        if (is_in_match_) {
          if (match_depth_ == 0) {
            is_in_match_ = false;
            output.pop_back();  // Don't include trailing ',' in match.
            if (options_.with_path) {
              output.push_back(']');  // End [path, value] pair.
            }
          } else {
            --match_depth_;
          }
        }

        if (!is_in_match_) {
          container_path_stack_.pop_back();
        }
      } else if (c == ',') {
        if (is_in_match_) {
          if (match_depth_ == 0) {
            is_in_match_ = false;
            output.pop_back();  // Don't include trailing ',' in match.
            if (options_.with_path) {
              output.push_back(']');  // End [path, value] pair.
            }
          }
        }

        if (!is_in_match_) {
          if (is_in_array()) {
            ++*std::get_if<int64_t>(&container_path_stack_.back());
            try_start_match_at_current_path();
          } else if (is_in_object()) {
            is_object_key_ = true;
          }
        }
      } else if (c == '{') {
        if (!is_in_match_) {
          container_path_stack_.push_back("");  // Placeholder.
          is_object_key_ = true;
        } else {
          ++match_depth_;
        }
      } else if (c == ':' && is_in_object() && !is_in_match_) {
        container_path_stack_.back() = run_processor(
            JsonUnquoteStreamProcessor{}, std::move(key_literal_buffer_));
        key_literal_buffer_.clear();
        is_object_key_ = false;
        try_start_match_at_current_path();
      } else if (container_path_stack_.empty() && c == '\n') {
        if (!is_in_match_) {
          output.append(has_any_matches_ ? "]\n" : "[]\n");
        } else {
          is_in_match_ = false;
          output.pop_back();  // Don't include trailing '\n' in match.
          if (options_.with_path) {
            output.push_back(']');  // End [path, value] pair.
          }
          output.append("]\n");
        }
        has_any_matches_ = false;
      }
    }
  }
  return FromSingleStringOutput(std::move(output), end_of_input);
}

JsonStreamProcessResult JsonImplodeArrayStreamProcessor::Process(
    std::string_view input, bool end_of_input) {
  std::string output;
  if (!emitted_opening_square_bracket_) {
    output.push_back('[');
    emitted_opening_square_bracket_ = true;
  }
  for (char c : input) {
    if (c == '\n') {
      needs_leading_comma_ = true;
    } else {
      if (needs_leading_comma_) {
        output.push_back(',');
        needs_leading_comma_ = false;
      }
      output.push_back(c);
    }
  }
  if (end_of_input) {
    if (emitted_opening_square_bracket_) {
      output.append("]\n");
    } else {
      output.append("[]\n");
    }
  }
  return FromSingleStringOutput(std::move(output), end_of_input);
}

JsonStreamProcessResult JsonExplodeArrayStreamProcessor::Process(
    std::string_view input_chunk, bool end_of_input) {
  std::string output;
  for (char c : input_chunk) {
    auto emit_c_if_matched = [&]() {
      if (is_top_level_array_) {
        output.push_back(c);
      }
    };

    if (is_in_string_) {
      emit_c_if_matched();
      if (is_in_escape_) {
        is_in_escape_ = false;
      } else if (c == '"') {
        is_in_string_ = false;
      } else if (c == '\\') {
        is_in_escape_ = true;
      }
    } else {
      if (c == '[') {
        ++container_depth_;
        if (container_depth_ == 1) {
          is_top_level_array_ = true;
          has_contents_ = false;
        } else {
          emit_c_if_matched();
          has_contents_ = true;
        }
      } else if (c == '{') {
        ++container_depth_;
        emit_c_if_matched();
        has_contents_ = true;
      } else if (c == ',' && is_top_level_array_ && container_depth_ == 1) {
        output.push_back('\n');
      } else if (c == ']') {
        --container_depth_;
        if (container_depth_ == 0) {
          if (has_contents_) {
            output.push_back('\n');
            has_contents_ = false;
          }
          is_top_level_array_ = false;
        } else {
          emit_c_if_matched();
        }
      } else if (c == '}') {
        --container_depth_;
        emit_c_if_matched();
      } else if (c == '"') {
        is_in_string_ = true;
        emit_c_if_matched();
        has_contents_ = true;
      } else if (!absl::ascii_isspace(c)) {
        emit_c_if_matched();
        has_contents_ = true;
      }
    }
  }
  return FromSingleStringOutput(std::move(output), end_of_input);
}

JsonStreamProcessResult JsonGetArrayNthValueStreamProcessor::Process(
    std::string_view input_chunk, bool end_of_input) {
  std::string output;
  for (char c : input_chunk) {
    auto emit_c_if_matched = [&]() {
      if (is_top_level_array_ && top_level_array_value_index_ == options_.n) {
        output.push_back(c);
      }
    };

    auto emit_top_level_null_if_not_already_done = [&]() {
      if (!emitted_value_) {
        output.append("null\n");
        emitted_value_ = true;
      }
    };

    auto end_top_level_value = [&]() {
      has_contents_ = false;
      is_top_level_array_ = false;
      emitted_value_ = false;
      top_level_array_value_index_ = 0;
    };

    if (is_in_string_) {
      emit_c_if_matched();
      if (is_in_escape_) {
        is_in_escape_ = false;
      } else if (c == '"') {
        is_in_string_ = false;
      } else if (c == '\\') {
        is_in_escape_ = true;
      }
    } else {
      if (c == '[') {
        ++container_depth_;
        if (container_depth_ == 1) {
          is_top_level_array_ = true;
          top_level_array_value_index_ = 0;
        } else {
          emit_c_if_matched();
          has_contents_ = true;
        }
      } else if (c == '{') {
        if (container_depth_ == 0) {
          emit_top_level_null_if_not_already_done();
        }
        ++container_depth_;
        emit_c_if_matched();
        has_contents_ = true;
      } else if (c == ',' && is_top_level_array_ && container_depth_ == 1) {
        if (top_level_array_value_index_ == options_.n) {
          output.push_back('\n');
          emitted_value_ = true;
        }
        ++top_level_array_value_index_;
        has_contents_ = true;
      } else if (c == ']') {
        --container_depth_;
        if (container_depth_ == 0) {
          if (top_level_array_value_index_ == options_.n) {
            if (has_contents_) {
              output.push_back('\n');
            } else {
              output.append("null\n");
            }
          } else if (top_level_array_value_index_ < options_.n) {
            emit_top_level_null_if_not_already_done();
          }
          end_top_level_value();
        } else {
          emit_c_if_matched();
        }
      } else if (c == '}') {
        --container_depth_;
        emit_c_if_matched();
        if (container_depth_ == 0) {
          end_top_level_value();
        }
      } else if (c == '"') {
        if (container_depth_ == 0) {
          emit_top_level_null_if_not_already_done();
        }
        is_in_string_ = true;
        emit_c_if_matched();
        has_contents_ = true;
      } else if (is_top_level_array_ && !absl::ascii_isspace(c)) {
        emit_c_if_matched();
        has_contents_ = true;
      } else if (!absl::ascii_isspace(c)) {
        if (container_depth_ == 0) {
          emit_top_level_null_if_not_already_done();
        }
      }
    }
  }
  return FromSingleStringOutput(std::move(output), end_of_input);
}

JsonStreamProcessResult JsonUnquoteStreamProcessor::Process(
    std::string_view input_chunk, bool end_of_input) {
  std::string output;
  for (char c : input_chunk) {
    if (is_in_string_) {
      if (escape_buffer_.empty()) {
        if (c == '"') {
          is_in_string_ = false;
        } else if (c == '\\') {
          escape_buffer_.push_back(c);
        } else {
          output.push_back(c);
        }
      } else if (escape_buffer_ == "\\") {
        if (c == '"' || c == '\\' || c == '/') {
          output.push_back(c);
          escape_buffer_.clear();
        } else if (c == 'b') {
          output.push_back('\b');
          escape_buffer_.clear();
        } else if (c == 'f') {
          output.push_back('\f');
          escape_buffer_.clear();
        } else if (c == 'n') {
          output.push_back('\n');
          escape_buffer_.clear();
        } else if (c == 'r') {
          output.push_back('\r');
          escape_buffer_.clear();
        } else if (c == 't') {
          output.push_back('\t');
          escape_buffer_.clear();
        } else if (c == 'u') {
          escape_buffer_.push_back('u');
        }
      } else if (c == '"') {
        // Purely defensive, not required by contract. Prevent truncated \u
        // escapes from swallowing end quotes and causing non-local chaos.
        is_in_string_ = false;
        AppendUtf8CodePoint(kReplacementCharacter, output);
        escape_buffer_.clear();
      } else if (escape_buffer_.size() == 5 && IsHexDigit(c)) {
        escape_buffer_.push_back(c);  // \uXXX + X
        uint16_t value = ConvertDigitSpan(escape_buffer_, 2, 6, 16);
        if (!U16_IS_LEAD(value)) {
          AppendUtf8CodePoint(value, output);
          escape_buffer_.clear();
        }
      } else if (escape_buffer_.size() == 11 && IsHexDigit(c)) {
        escape_buffer_.push_back(c);  // \uXXXX\uXXX + X
        uint16_t a = ConvertDigitSpan(escape_buffer_, 2, 6, 16);
        uint16_t b = ConvertDigitSpan(escape_buffer_, 8, 12, 16);
        if (U16_IS_TRAIL(b)) {
          AppendUtf8CodePoint(U16_GET_SUPPLEMENTARY(a, b), output);
        } else {
          AppendUtf8CodePoint(kReplacementCharacter, output);
        }
        escape_buffer_.clear();
      } else if (escape_buffer_.size() == 6 && c == '\\') {
        escape_buffer_.push_back(c);  // \uXXXX + \ .
      } else if (escape_buffer_.size() == 7 && c == 'u') {
        escape_buffer_.push_back(c);  // \uXXXX\ + u
      } else if ((escape_buffer_.size() == 2 || escape_buffer_.size() == 3 ||
                  escape_buffer_.size() == 4 || escape_buffer_.size() == 8 ||
                  escape_buffer_.size() == 9 || escape_buffer_.size() == 10) &&
                 IsHexDigit(c)) {
        // \uXXXX\uXXXX has Xs at indices {2, 3, 4, 5, 8, 9, 10, 11}. 5 and 11
        // are handled above.
        escape_buffer_.push_back(c);
      } else {
        // Broken escape.
        AppendUtf8CodePoint(kReplacementCharacter, output);
        escape_buffer_.clear();
      }
    } else if (c == '"') {
      is_in_string_ = true;
    }
  }
  return FromSingleStringOutput(std::move(output), end_of_input);
}

JsonStreamProcessResult JsonQuoteStreamProcessor::Process(
    std::string_view input_chunk, bool end_of_input) {
  std::string output;
  if (!emitted_opening_quote_) {
    output.push_back('"');
    emitted_opening_quote_ = true;
  }
  for (char c : input_chunk) {
    if (c == '\\') {
      output.append("\\\\");
    } else if (c == '"') {
      output.append("\\\"");
    } else if (c == '\b') {
      output.append("\\b");
    } else if (c == '\f') {
      output.append("\\f");
    } else if (c == '\n') {
      output.append("\\n");
    } else if (c == '\r') {
      output.append("\\r");
    } else if (c == '\t') {
      output.append("\\t");
    } else if (static_cast<uint8_t>(c) < 0x20) {
      output.append(absl::StrFormat("\\u%04x", static_cast<uint8_t>(c)));
    } else {
      output.push_back(c);
    }
  }
  if (end_of_input) {
    if (!emitted_opening_quote_) {
      output.append("\"\"");
    } else {
      output.push_back('"');
    }
  }
  return FromSingleStringOutput(std::move(output), end_of_input);
}

JsonStreamProcessResult JsonChunkValuesStreamProcessor::Process(
    std::string_view input_chunk, bool end_of_input) {
  auto [compactify_chunks, end_of_compactify] =
      compactify_.Process(input_chunk, end_of_input);
  absl::InlinedVector<std::string, 1> output_chunks;
  for (auto& compactify_chunk : compactify_chunks) {
    for (char c : compactify_chunk) {
      if (c == '\n') {
        DCHECK(!buffer_.empty());
        buffer_.push_back(c);
        output_chunks.push_back(std::move(buffer_));
        buffer_.clear();
      } else {
        buffer_.push_back(c);
      }
    }
  }
  if (end_of_compactify) {
    DCHECK(buffer_.empty());
  }
  return {std::move(output_chunks), end_of_compactify};
}

}  // namespace koladata::internal
