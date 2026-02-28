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

#include "absl/cleanup/cleanup.h"
#include "absl/log/check.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "unicode/uchar.h"
#include "unicode/unistr.h"
#include "unicode/utf16.h"
#include "unicode/utf8.h"
#include "unicode/utypes.h"
#include "koladata/internal/op_utils/stream_processor_state.pb.h"

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

}  // namespace

void JsonSalvageStreamProcessor::Reset() {
  utf8_buffer_.clear();
  container_stack_.clear();
  needs_leading_separator_ = false;
  is_object_key_ = false;
  buffer_.clear();
  state_ = State::kBase;
  json_string_utf16_first_surrogate_.reset();
  output_.clear();
}

bool JsonSalvageStreamProcessor::LoadState(std::string_view state) {
  Reset();
  auto reset_on_failure = absl::MakeCleanup([this] { Reset(); });

  JsonSalvageStateProto proto;
  if (!proto.ParseFromString(state)) {
    return false;
  }
  if (options_.allow_nan != proto.allow_nan() ||
      options_.ensure_ascii != proto.ensure_ascii() ||
      options_.max_depth != proto.max_depth()) {
    return false;
  }
  if (proto.utf8_buffer().size() > 3 ||
      proto.container_stack_size() > options_.max_depth ||
      proto.buffer_size() > kMaxBufferSize) {
    return false;
  }
  for (char c : proto.utf8_buffer()) {
    utf8_buffer_.push_back(static_cast<uint8_t>(c));
  }
  for (int32_t x : proto.container_stack()) {
    switch (static_cast<ContainerType>(x)) {
      case ContainerType::kArray:
      case ContainerType::kObject:
        container_stack_.push_back(static_cast<ContainerType>(x));
        break;
      default:
        return false;
    }
  }
  needs_leading_separator_ = proto.needs_leading_separator();
  is_object_key_ = proto.is_object_key();
  for (int32_t c : proto.buffer()) {
    if (c < 0 || !IsEncodableCodePoint(c)) {
      return false;
    }
    buffer_.push_back(c);
  }
  switch (static_cast<State>(proto.state())) {
    case State::kBase:
    case State::kStringDoubleQuote:
    case State::kStringSingleQuote:
    case State::kStringBacktick:
    case State::kStringTripleDoubleQuote:
    case State::kStringTripleSingleQuote:
    case State::kStringUnquoted:
    case State::kCommentSingleLine:
    case State::kCommentMultiLine:
    case State::kNumberBuffered:
    case State::kNumberDecimal:
      state_ = static_cast<State>(proto.state());
      break;
    default:
      return false;
  }

  if (proto.has_json_string_utf16_first_surrogate()) {
    json_string_utf16_first_surrogate_ =
        proto.json_string_utf16_first_surrogate();
  }

  std::move(reset_on_failure).Cancel();
  return true;
}

std::string JsonSalvageStreamProcessor::ToState() const {
  JsonSalvageStateProto proto;
  proto.set_allow_nan(options_.allow_nan);
  proto.set_ensure_ascii(options_.ensure_ascii);
  proto.set_max_depth(options_.max_depth);
  for (auto c : utf8_buffer_) {
    proto.mutable_utf8_buffer()->push_back(static_cast<char>(c));
  }
  for (auto container_type : container_stack_) {
    proto.add_container_stack(static_cast<int32_t>(container_type));
  }
  proto.set_needs_leading_separator(needs_leading_separator_);
  proto.set_is_object_key(is_object_key_);
  for (auto c : buffer_) {
    proto.add_buffer(c);
  }
  proto.set_state(static_cast<int32_t>(state_));
  if (json_string_utf16_first_surrogate_.has_value()) {
    proto.set_json_string_utf16_first_surrogate(
        json_string_utf16_first_surrogate_.value());
  }
  return proto.SerializeAsString();
}

std::string JsonSalvageStreamProcessor::ProcessInputChunk(
    std::string_view input_chunk) {
  for (char c : input_chunk) {
    ProcessInputByte(static_cast<uint8_t>(c));
  }
  return ConsumeOutput();
}

std::string JsonSalvageStreamProcessor::ProcessEnd() {
  ProcessInputEndInternal();
  auto output = ConsumeOutput();
  Reset();
  return output;
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
  uint64_t value = 0;
  for (size_t i = offset; i < buffer_.size(); ++i) {
    value = value * base + DigitValue(buffer_[i]);
  }
  return value;
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

}  // namespace koladata::internal
