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
#ifndef KOLADATA_FUNCTOR_PARALLEL_STREAM_FILTER_JSON_H_
#define KOLADATA_FUNCTOR_PARALLEL_STREAM_FILTER_JSON_H_

#include <cstddef>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace koladata::functor::parallel {

/* JsonStreamParser usage examples.
Example JSON: { "query": { "docs": [ { "id" : 1 }, { "id" : 2 } ] } }
Let `get_next_chunk()` return a few next characters from the JSON on each call.
***
  // This example extracts $.query.docs[*]. Each doc is received in
  // ValueEndCallback in one piece even if get_next_chunk gets it by parts.
  // Output:
  //   { "id" : 1 }
  //   { "id" : 2 }
  absl::Status example_extract_docs() {
    JsonStreamParser parser(JsonStreamParser::SeparatorPolicy::KEEP);
    parser.SetValueEndCallback(
        [](absl::string_view path, absl::string_view json) {
          if (path == "$.query.docs[*]") { std::cout << json << std::endl; }
        });
    while (has_data()) {
      RETURN_IF_ERROR(parser.AddChunk(get_next_chunk()));
    }
    RETURN_IF_ERROR(parser.Finalize());
  }
***
  // This example extracts $.query.docs[*] and streams the result. Each doc
  // is printed in parts as soon as we get new data. Also removes all spaces
  // from the result.
  // Output:
  //   {"id":1}
  //   {"id":2}
  absl::Status example_stream_docs() {
    JsonStreamParser parser(JsonStreamParser::SeparatorPolicy::REMOVE);
    std::string filter = "$.query.docs[*]";
    bool streaming = false;
    parser.SetValueBeginCallback(
        [&](absl::string_view path, JsonStreamParser::ValueType type) {
          if (path == filter && type == JsonStreamParser::ValueType::DICT) {
            parser.ConsumeChunk();  // flush previous values
            streaming = true;
          }
        });
    parser.SetValueEndCallback(
        [&](absl::string_view path, absl::string_view json) {
          if (path == filter && streaming) {
            std::cout << parser.ConsumeChunk() << std::endl;
            streaming = false;
          }
        });
    while (has_data()) {
      RETURN_IF_ERROR(parser.AddChunk(get_next_chunk()));
      if (streaming) {
        std::cout << parser.ConsumeChunk();  // no endl
      }
    }
    RETURN_IF_ERROR(parser.Finalize());
  }
***
*/

class JsonStreamParser {
 public:
  enum class SeparatorPolicy { KEEP, REMOVE };

  explicit JsonStreamParser(SeparatorPolicy p = SeparatorPolicy::KEEP);

  void SetSeparatorPolicy(SeparatorPolicy p) { separator_policy_ = p; }

  // Compatibility settings for parsing invalid json.
  void AllowSingleQuotes(bool v) { allow_single_quotes_ = v; }
  void AllowUnquotedKeys(bool v) { allow_unquoted_keys_ = v; }
  void AllowLinebreakInStrings(bool v) { allow_linebreak_in_strings_ = v; }
  void EnableQuotingInvalidTokens(bool v) { quote_invalid_tokens_ = v; }

  enum class ValueType {
    DICT,    // starts with '{'
    LIST,    // starts with '['
    STRING,  // starts with '"'
    TOKEN    // everything else: number, boolean, or null
  };

  using ValueBeginCallback =
      std::function<void(absl::string_view path, ValueType type)>;
  using ValueEndCallback =
      std::function<void(absl::string_view path, absl::string_view json)>;

  // ValueBeginCallback is called on the beginning of each new dict, list, or
  // literal value.
  // `ConsumeChunk()`, if called in the callback, will return all prior
  // characters NOT including the starting character of the new value.
  // Callback arguments:
  //   path - JSONPath of the new value, e.g. "$.query.docs[*]". Always starts
  // with "$". All list items have "[*]" in the path (rather than actual index).
  //   type - one of DICT/LIST/STRING/TOKEN (see ValueType).
  void SetValueBeginCallback(ValueBeginCallback fn) {
    vbegin_fn_ = std::move(fn);
  }

  // ValueEndCallback is called at the end of each value.
  // Callback arguments:
  //   path - same JSONPath as was in the corresponding ValueBeginCallback.
  //   json - JSON string for this value (including subvalues) with all
  // compatibility fixes and separator policy applied.
  void SetValueEndCallback(ValueEndCallback fn) {
    vend_fn_ = std::move(fn);
  }

  // Adds new data to parse. The parser will call
  // ValueBeginCallback/ValueEndCallback during processing.
  // If the output needs to be streamed, partial values can be accessed by
  // `ConsumeChunk()` (can be used either in callbacks during AddChunk, or at
  // any moment before or after AddChunk as well).
  absl::Status AddChunk(absl::string_view chunk);

  // Use at the end of JSON stream. It returns error status if there are
  // unclosed brackets. It can call ValueEndCallback if the stream ended with
  // a token like true/false/null without any separator after.
  absl::Status Finalize();

  // Returns processed characters since the previous call of `ConsumeChunk()`.
  // The result will have separator policy and compatibility fixes applied
  // (e.g. no unnecessary spaces if SeparatorPolicy is REMOVE, and with
  // inserted missing quotes if AllowUnquotedKeys is used).
  // Note that value ValueEndCallback will receive string_view of the entire
  // value regardless of ConsumeChunk calls.
  absl::string_view ConsumeChunk() {
    auto res = absl::string_view(data_).substr(
        data_consumed_pos_, data_available_pos_ - data_consumed_pos_);
    data_consumed_pos_ = data_available_pos_;
    return res;
  }

 private:
  struct ValueInfo {
    size_t path_size;
    size_t data_begin;
    bool is_list;
  };

  enum {
    EXPECT_VALUE,  // expect value (or ']' in case of empty list)
    EXPECT_NAME,   // expect field name (or '}' in case of empty dict)
    NAME,
    NAME_ESCAPE,
    UNQUOTED_NAME,
    NAME_END,
    VALUE_TOKEN,
    VALUE_STRING,
    VALUE_STRING_ESCAPE,
    VALUE_END
  } state_ = EXPECT_VALUE;

  absl::Status ProcessExpectValue(char c);
  absl::Status ProcessExpectName(char c);
  absl::Status ProcessName(char c);
  absl::Status ProcessNameEscape(char c);
  absl::Status ProcessUnquotedName(char c);
  absl::Status ProcessNameEnd(char c);
  absl::Status ProcessValueToken(char c);
  absl::Status ProcessValueString(char c);
  absl::Status ProcessValueStringEscape(char c);
  absl::Status ProcessValueEnd(char c);

  absl::Status HandleValueEnd();
  char PreprocessSpace(char c);

  std::string data_;
  size_t data_available_pos_ = 0;
  size_t data_consumed_pos_ = 0;
  std::string path_ = "$";
  std::vector<ValueInfo> stack_;
  ValueBeginCallback vbegin_fn_;
  ValueEndCallback vend_fn_;
  SeparatorPolicy separator_policy_ = SeparatorPolicy::KEEP;
  char last_opening_quote_;
  bool allow_single_quotes_ = false;
  bool allow_unquoted_keys_ = false;
  bool allow_linebreak_in_strings_ = false;
  bool quote_invalid_tokens_ = false;
};

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_STREAM_FILTER_JSON_H_
