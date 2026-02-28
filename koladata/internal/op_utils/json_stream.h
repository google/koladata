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
#ifndef KOLADATA_INTERNAL_OP_UTILS_JSON_STREAM_H_
#define KOLADATA_INTERNAL_OP_UTILS_JSON_STREAM_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace koladata::internal {

struct JsonSalvageOptions {
  // If true, like in python `json.dumps`, the non-standard non-finite number
  // literals `NaN` and `Infinity` are allowed in the output. If false, these
  // literals are quoted as strings in the output if they are found in the
  // input.
  bool allow_nan = false;

  // If true, the output will only contain ASCII-range characters. If false,
  // multi-byte UTF-8 encodings of non-ASCII code points will be preferred.
  bool ensure_ascii = true;

  // The maximum container (object or array) nesting depth that we will handle
  // in the input.
  int64_t max_depth = 100;
};

// Fixes syntax issues in a chunked string stream of JSON-like values, while
// adding as little delay as possible.
//
// Basic properties:
// - The output is always a sequence of zero or more newline-separated valid
//   JSON values.
// - If the input is a sequence of whitespace-separated valid JSON values,
//   the output is an equivalent sequence of values, with strings compared by
//   sequence of represented code points, and numbers compared by numeric value
//   with unlimited precision.
//   - If the input nesting depth exceeds `max_depth` in the options, this
//     property is no longer guaranteed.
//
// Supports the following additional syntax, to tolerate "variant" JSON:
// - All of JSON5 according to https://spec.json5.org/
//   - Non-decimal integer literal magnitudes (ignoring sign) are 64 bit.
//   - Decimal number literals use unlimited precision.
// - Additional syntax from Python:
//   - Line comments starting with a single hash character `#`.
//   - False, True, and None (as true, false, and null).
//   - Tuple literals (interpreted as arrays).
//   - '''...''' and """...""" triple-quoted strings.
//   - \a string escape interpreted as U+0007.
//   - \o \oo \ooo octal string escapes.
//   - \UXXXXXXXX 32-bit hexadecimal string escapes.
//   - u"" and b"" string prefixes (accepted and ignored).
//   - Underscores in numeric literals (like 123_456).
//   - Octal (0o) and binary (0b) integer literals.
//     - Magnitudes (ignoring sign) are 64 bit.
//   - l and L integer suffixes (accepted and ignored).
// - Additional syntax from JavaScript:
//   - \u{...} variable-length hexadecimal string escapes.
//   - n integer suffix (accepted and ignored).
//
// All other input is handled in an implementation-defined way and is subject
// to change in future versions.
class JsonSalvageStreamProcessor {
 public:
  explicit JsonSalvageStreamProcessor(const JsonSalvageOptions& options)
      : options_(options) {}

  // Resets to the initial state.
  void Reset();

  // Initializes from a state string. Returns true if successful, or false if
  // the state string is invalid or does not match the options passed to the
  // constructor.
  [[nodiscard]] bool LoadState(std::string_view state);

  // Returns a state string that can be used to recreate the state of the
  // processor.
  std::string ToState() const;

  // Processes a chunk of input bytes, returning a chunk of output bytes.
  std::string ProcessInputChunk(std::string_view input_chunk);

  // Ends the input, returning a chunk of output bytes.
  std::string ProcessEnd();

 private:
  enum class ContainerType : uint8_t {
    kTopLevel = 0,
    kArray = 1,
    kObject = 2,
  };

  // The state of the (non-container) value we are in the middle of parsing.
  //
  // The overall state machine operates on (state, buffer) pairs, where buffer
  // is an arbitrary utf-32 string, so these "states" are relatively coarse.
  enum class State : uint8_t {
    kBase = 0,                     // Start state.
    kStringDoubleQuote = 1,        // "..."
    kStringSingleQuote = 2,        // '...'
    kStringBacktick = 3,           // `...`
    kStringTripleDoubleQuote = 4,  // """..."""
    kStringTripleSingleQuote = 5,  // '''...'''
    kStringUnquoted = 6,           // ... (no whitespace, commas, or colons)
    kCommentSingleLine = 7,        // //...\n or #...\n
    kCommentMultiLine = 8,         // /* ... */
    kNumberBuffered = 9,           // Buffered maybe-non-decimal number.
    kNumberDecimal = 10,           // Streamed decimal number.
  };

  void ProcessInputByte(uint8_t c);
  void ProcessInputCodePoint(std::optional<char32_t> c);
  void ProcessInputEndInternal();

  bool IsValidUnquotedStringNextCodePoint(std::optional<char32_t> c);

  // Returns true if some rule matched. Otherwise, the input `c` was not
  // consumed and all rules should be retried from the top.
  bool ProcessAllRules(std::optional<char32_t> c);

  // Broken up for readability.
  bool ProcessBaseRules(std::optional<char32_t> c);
  bool ProcessStringRules(std::optional<char32_t> c);
  bool ProcessCommentRules(std::optional<char32_t> c);
  bool ProcessNumberRules(std::optional<char32_t> c);

  void SetStateWithBuffer(State state, std::string_view s);

  void ClearBuffer();
  void SetBuffer(std::string_view s);
  void AppendToBuffer(std::optional<char32_t> c);
  void AppendToBuffer(std::string_view s);

  bool BufferEquals(std::string_view s) const;
  bool BufferStartsWith(std::string_view s) const;
  bool BufferContainsAnycasePrefixOf(std::string_view s) const;

  uint64_t ConvertBufferValueAt(size_t offset, uint64_t base) const;

  void StartValue();
  void StartNonObjectKeyValue();
  void EndValue();

  ContainerType GetCurrentContainer() const;
  void StartArray();
  void StartObject();
  void EndCurrentContainer();

  void StartString();
  void EndString();

  void EmitAtom(std::string_view s);

  void EmitJsonStringStart();
  void EmitJsonStringCodePoint(std::optional<char32_t> c);
  void EmitJsonStringEnd();

  void EmitRawOutput(std::string_view s);
  void EmitRawOutput(char c);
  void EmitRawOutput(char32_t c);
  void EmitRawOutput(std::optional<char32_t> c);

  std::string ConsumeOutput();

  const JsonSalvageOptions options_;

  // Contains a partial utf-8 multibyte sequence if we are in the middle of one.
  // The size of this vector is always between 0 and 4.
  std::vector<uint8_t> utf8_buffer_;

  // A stack of nested containers (arrays and objects) at the current point in
  // parsing.
  std::vector<ContainerType> container_stack_;

  // Whether the current value needs a leading separator (comma or colon) that
  // hasn't been emitted yet. We have to defer emitting separators until we know
  // that there won't be an end-of-container, so it's simpler to have each value
  // own its leading separator instead of its trailing separator.
  bool needs_leading_separator_ = false;

  // For the topmost object context on the stack, whether we are scanning for an
  // object key instead of an object value. All object contexts deeper in the
  // stack must (for JSON structural reasons) be in the object-value-parsing
  // state, so we don't need a stack for this.
  bool is_object_key_ = false;

  // Contains a buffer of input codepoints that, due to contextual ambiguity,
  // haven't been fully processed yet. We try to keep this empty or small to
  // minimize delay, and its size is bounded at 100 elements. Some usages
  // include waiting for a third quote to detect triple-quoted strings,
  // accumulating non-decimal number digits before doing a conversion to
  // decimal, and accumulating escape sequences in strings. It is also used in
  // some places to augment the state machine, where additional full states
  // would be unwieldy.
  std::vector<char32_t> buffer_;

  // Leaf-level value parsing state machine.
  State state_ = State::kBase;

  // If ensure_ascii = true, we may need to emit a UTF-16 surrogate pair as a
  // pair of JSON \uXXXX string escapes, and for practical reasons this is done
  // using two separate emit method calls. This buffer is used to delay the
  // first surrogate so that if the second surrogate isn't emitted, we can
  // prevent ourselves from emitting an invalid surrogate pair escape sequence
  // to the output.
  std::optional<char32_t> json_string_utf16_first_surrogate_;

  // Temporary output buffer. Should be empty except during method invocations.
  // Having this be an instance member makes it possible for private methods to
  // operate on it implicitly.
  std::string output_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_JSON_STREAM_H_
