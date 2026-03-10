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
#include "koladata/functor/parallel/stream_filter_json.h"

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/basic_routine.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {

namespace {
constexpr absl::string_view kListPath = "[*]";

// For allowed number formats see
// https://en.cppreference.com/w/c/string/byte/strtof
bool IsNumber(absl::string_view s) {
  double d;
  return absl::SimpleAtod(s, &d);
}

// Valid unquoted literals are only "true", "false", "null", or numbers.
// See https://www.json.org/json-en.html
// This function is tested in JsonStreamParserTest.TokenParsing.
bool IsValidToken(absl::string_view s) {
  return s == "true" || s == "false" || s == "null" || IsNumber(s);
}

}  // namespace

JsonStreamParser::JsonStreamParser(SeparatorPolicy p) : separator_policy_(p) {
  // add a dummy value, so `stack_.back()` always exists
  stack_.push_back(ValueInfo{path_.size(), 0, false});
}

absl::Status JsonStreamParser::HandleValueEnd() {
  const ValueInfo& vinfo = stack_.back();
  size_t data_begin = vinfo.data_begin;
  absl::string_view data = absl::string_view(data_).substr(
      data_begin, data_available_pos_ - data_begin);
  if (data.empty()) {
    return absl::FailedPreconditionError("value is empty");
  }
  char last_char = data.back();
  if (last_char == ']' && !vinfo.is_list) {
    return absl::InvalidArgumentError(
        absl::StrFormat("unexpected ']' at pos %d", data_available_pos_ - 1));
  }
  if (vinfo.is_list) {
    if (last_char != ']') {
      return absl::InvalidArgumentError(
          absl::StrFormat("']' expected at pos %d", data_available_pos_ - 1));
    }
    if (!path_.ends_with(kListPath)) {
      return absl::FailedPreconditionError("path_ is invalid");
    }
    path_.resize(path_.size() - kListPath.size());
  }
  if (vend_fn_) { vend_fn_(path_, data); }
  if (stack_.size() > 1) {
    stack_.pop_back();
  } else {
    return absl::InvalidArgumentError(absl::StrFormat(
        "unexpected '%c' at pos %d", last_char, data_available_pos_ - 1));
  }
  path_.resize(stack_.back().path_size);
  return absl::OkStatus();
}

char JsonStreamParser::PreprocessSpace(char c) {
  if (state_ == NAME || state_ == VALUE_STRING) {
    if (c == '\n' && allow_linebreak_in_strings_) {
      data_.append("\\n");
      return 'n';
    } else if (c == '\r' && allow_linebreak_in_strings_) {
      data_.append("\\r");
      return 'r';
    } else {
      data_.push_back(c);
    }
  }
  if (separator_policy_ == SeparatorPolicy::KEEP) {
    data_.push_back(c);
  }
  return c;
}

absl::Status JsonStreamParser::ProcessExpectValue(char c) {
  if (c == '{') {
    if (vbegin_fn_) {
      vbegin_fn_(path_, ValueType::DICT);
    }
    stack_.push_back({path_.size(), data_available_pos_, /*is_list=*/false});
    state_ = EXPECT_NAME;
  } else if (c == '[') {
    if (vbegin_fn_) {
      vbegin_fn_(path_, ValueType::LIST);
    }
    path_.append(kListPath);
    stack_.push_back({path_.size(), data_available_pos_, /*is_list=*/true});
    state_ = EXPECT_VALUE;
  } else if (c == ']') {
    data_available_pos_ = data_.size();
    RETURN_IF_ERROR(HandleValueEnd());
    state_ = VALUE_END;
  } else if (c == '}') {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected value begin, got '}' at pos %d", data_available_pos_));
  } else if (!absl::ascii_isspace(c)) {
    stack_.push_back({path_.size(), data_available_pos_, /*is_list=*/false});
    if (c != '"' && c != '\'') {
      state_ = VALUE_TOKEN;
      if (quote_invalid_tokens_) {
        // skip vbegin_fn_ callback; it will be called later
        return absl::OkStatus();
      }
    } else {
      state_ = VALUE_STRING;
      if (c != '"') {
        data_.back() = '"';
        if (!allow_single_quotes_) {
          return absl::InvalidArgumentError(
              absl::StrFormat("single quote instead of double quote at pos %d",
                              data_available_pos_));
        }
      }
      last_opening_quote_ = c;
    }
    if (vbegin_fn_) {
      vbegin_fn_(path_,
                 state_ == VALUE_STRING ? ValueType::STRING : ValueType::TOKEN);
    }
  }
  return absl::OkStatus();
}

absl::Status JsonStreamParser::ProcessExpectName(char c) {
  if (absl::ascii_isspace(c)) {
    return absl::OkStatus();
  }
  if (c == '}' || c == ']') {
    data_available_pos_ = data_.size();
    RETURN_IF_ERROR(HandleValueEnd());
    state_ = VALUE_END;
    return absl::OkStatus();
  }
  path_.push_back('.');
  if (c == '"' || c == '\'') {
    state_ = NAME;
    last_opening_quote_ = c;
    if (c != '"') {
      data_.back() = '"';
      if (!allow_single_quotes_) {
        return absl::InvalidArgumentError(
            absl::StrFormat("single quote instead of double quote at pos %d",
                            data_available_pos_));
      }
    }
  } else {
    data_.back() = '"';
    data_.push_back(c);
    path_.push_back(c);
    state_ = UNQUOTED_NAME;
    if (!allow_unquoted_keys_) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected '\"' at pos %d, got '%c'", data_available_pos_, c));
    }
  }
  return absl::OkStatus();
}

absl::Status JsonStreamParser::ProcessName(char c) {
  if (c == '\r' || c == '\n') {
    return absl::InvalidArgumentError(absl::StrFormat(
        "linebreak inside of string literal at pos %d", data_available_pos_));
  }
  if (c == last_opening_quote_) {
    data_.back() = '"';
    state_ = NAME_END;
  } else if (c == '\\') {
    state_ = NAME_ESCAPE;
  } else {
    path_.push_back(c);
  }
  return absl::OkStatus();
}

absl::Status JsonStreamParser::ProcessNameEscape(char c) {
  path_.push_back(c);
  state_ = NAME;
  return absl::OkStatus();
}

absl::Status JsonStreamParser::ProcessUnquotedName(char c) {
  if (!absl::ascii_isspace(c) && c != ':') {
    path_.push_back(c);
    return absl::OkStatus();
  }
  data_.back() = '"';
  data_.push_back(c);
  state_ = c == ':' ? EXPECT_VALUE : NAME_END;
  return absl::OkStatus();
}

absl::Status JsonStreamParser::ProcessNameEnd(char c) {
  if (c == ':') {
    state_ = EXPECT_VALUE;
  } else if (!absl::ascii_isspace(c)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected ':' at pos %d, got '%c'", data_available_pos_, c));
  }
  return absl::OkStatus();
}

absl::Status JsonStreamParser::ProcessValueToken(char c) {
  auto handle_token_end = [&]() -> absl::Status {
    if (quote_invalid_tokens_) {
      absl::string_view token =
          absl::string_view(data_).substr(data_available_pos_);
      bool ends_with_c = false;
      if (!token.empty() && token.back() == c) {
        token = token.substr(0, token.size() - 1);
        ends_with_c = true;
      }
      if (IsValidToken(token)) {
        if (vbegin_fn_) {
          vbegin_fn_(path_, ValueType::TOKEN);
        }
        data_available_pos_ += token.size();
      } else {
        if (vbegin_fn_) {
          vbegin_fn_(path_, ValueType::STRING);
        }
        std::string quoted = absl::StrCat("\"", token, "\"");
        data_.resize(data_available_pos_);
        data_.append(quoted);
        data_available_pos_ = data_.size();
        if (ends_with_c) {
          data_.push_back(c);
        }
      }
    }
    return HandleValueEnd();
  };
  if (absl::ascii_isspace(c)) {
    RETURN_IF_ERROR(handle_token_end());
    state_ = VALUE_END;
  } else if (c == ',') {
    RETURN_IF_ERROR(handle_token_end());
    state_ = stack_.back().is_list ? EXPECT_VALUE : EXPECT_NAME;
  } else if (c == ']' || c == '}') {
    RETURN_IF_ERROR(handle_token_end());
    data_available_pos_ = data_.size();
    RETURN_IF_ERROR(HandleValueEnd());
    state_ = VALUE_END;
  }
  return absl::OkStatus();
}

absl::Status JsonStreamParser::ProcessValueString(char c) {
  if (c == '\r' || c == '\n') {
    return absl::InvalidArgumentError(absl::StrFormat(
        "linebreak inside of string literal at pos %d", data_available_pos_));
  }
  if (c == last_opening_quote_) {
    data_.back() = '"';
    data_available_pos_ = data_.size();
    RETURN_IF_ERROR(HandleValueEnd());
    state_ = VALUE_END;
  } else if (c == '\\') {
    state_ = VALUE_STRING_ESCAPE;
  }
  return absl::OkStatus();
}

absl::Status JsonStreamParser::ProcessValueStringEscape(char c) {
  state_ = VALUE_STRING;
  return absl::OkStatus();
}

absl::Status JsonStreamParser::ProcessValueEnd(char c) {
  if (c == ',') {
    state_ = stack_.back().is_list ? EXPECT_VALUE : EXPECT_NAME;
  } else if (c == ']' || c == '}') {
    data_available_pos_ = data_.size();
    RETURN_IF_ERROR(HandleValueEnd());
  } else if (!absl::ascii_isspace(c)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected ',', ']', or '}' at pos %d, got '%c'", data_.size() - 1, c));
  }
  return absl::OkStatus();
}

absl::Status JsonStreamParser::AddChunk(absl::string_view chunk) {
  for (char c : chunk) {
    if (state_ == VALUE_END && stack_.size() == 1) {
      // Note: we support parsing several concatenated JSONs, so on top level
      // after VALUE_END we immediately switch to EXPECT_VALUE.
      state_ = EXPECT_VALUE;
    }
    if (absl::ascii_isspace(c)) {
      c = PreprocessSpace(c);
    } else {
      data_.push_back(c);
    }
    switch (state_) {
      case EXPECT_VALUE:
        RETURN_IF_ERROR(ProcessExpectValue(c));
        break;
      case EXPECT_NAME:
        RETURN_IF_ERROR(ProcessExpectName(c));
        break;
      case NAME:
        RETURN_IF_ERROR(ProcessName(c));
        break;
      case NAME_ESCAPE:
        RETURN_IF_ERROR(ProcessNameEscape(c));
        break;
      case UNQUOTED_NAME:
        RETURN_IF_ERROR(ProcessUnquotedName(c));
        break;
      case NAME_END:
        RETURN_IF_ERROR(ProcessNameEnd(c));
        break;
      case VALUE_TOKEN:
        RETURN_IF_ERROR(ProcessValueToken(c));
        break;
      case VALUE_STRING:
        RETURN_IF_ERROR(ProcessValueString(c));
        break;
      case VALUE_STRING_ESCAPE:
        RETURN_IF_ERROR(ProcessValueStringEscape(c));
        break;
      case VALUE_END:
        RETURN_IF_ERROR(ProcessValueEnd(c));
        break;
    }
    if (state_ != VALUE_TOKEN || !quote_invalid_tokens_) {
      data_available_pos_ = data_.size();
    }
  }
  return absl::OkStatus();
}

absl::Status JsonStreamParser::Finalize() {
  if (state_ == VALUE_TOKEN) {
    RETURN_IF_ERROR(ProcessValueToken(' '));
  }
  if (stack_.size() != 1) {
    return absl::InvalidArgumentError("unexpected end of json stream");
  }
  return absl::OkStatus();
}

namespace {

class StreamFilterJsonHooks final : public BasicRoutineHooks {
 public:
  StreamFilterJsonHooks(StreamPtr absl_nonnull input_stream,
                        StreamWriterPtr absl_nonnull writer,
                        std::string field_to_extract)
      : input_stream_(std::move(input_stream)),
        writer_(std::move(writer)),
        field_to_extract_(std::move(field_to_extract)) {
    parser_.SetSeparatorPolicy(JsonStreamParser::SeparatorPolicy::REMOVE);
    parser_.AllowLinebreakInStrings(true);
    parser_.AllowSingleQuotes(true);
    parser_.AllowUnquotedKeys(true);
    parser_.EnableQuotingInvalidTokens(true);
    parser_.SetValueEndCallback(
        [this](absl::string_view path, absl::string_view json) {
          if (path == field_to_extract_) {
            DataSlice ds_out = DataSlice::CreatePrimitive(arolla::Text(json));
            writer_->Write(arolla::TypedRef::FromValue(ds_out));
          }
        });
  }

  bool Interrupted() const final { return writer_->Orphaned(); }

  void OnCancel(absl::Status&& status) final {
    writer_->TryClose(std::move(status));
  }

  StreamReaderPtr absl_nullable Start() final {
    return Resume(input_stream_->MakeReader());
  }

  StreamReaderPtr absl_nullable Resume(StreamReaderPtr
                                       absl_nonnull reader) final {
    auto try_read_result = reader->TryRead();
    while (arolla::TypedRef* item = try_read_result.item()) {
      if (Interrupted()) {
        return nullptr;
      }
      const auto& ds = item->UnsafeAs<DataSlice>();
      if (!ds.is_item() || !ds.item().holds_value<arolla::Text>()) {
        writer_->TryClose(absl::InvalidArgumentError("string expected"));
        return nullptr;
      }
      const arolla::Text& data = ds.item().value<arolla::Text>();
      if (absl::Status status = parser_.AddChunk(data); !status.ok()) {
        writer_->TryClose(std::move(status));
        return nullptr;
      }
      try_read_result = reader->TryRead();
    }
    if (absl::Status* status = try_read_result.close_status()) {
      if (status->ok()) {
        writer_->TryClose(parser_.Finalize());
      } else {
        writer_->TryClose(*status);
      }
      return nullptr;
    }
    return reader;
  }

 private:
  const StreamPtr absl_nonnull input_stream_;
  const StreamWriterPtr absl_nonnull writer_;
  std::string field_to_extract_;
  JsonStreamParser parser_;
};

}  // namespace

absl::StatusOr<StreamPtr> StreamFilterJson(ExecutorPtr executor,
                                           StreamPtr input,
                                           const DataSlice& field_to_extract) {
  std::string filter;
  if (!field_to_extract.is_item() ||
      !field_to_extract.item().holds_value<arolla::Text>()) {
    return absl::InvalidArgumentError("string expected");
  }
  filter = field_to_extract.item().value<arolla::Text>().view();
  auto [output, writer] = MakeStream(arolla::GetQType<DataSlice>());
  StartBasicRoutine(
      std::move(executor),
      std::make_unique<StreamFilterJsonHooks>(
          std::move(input), std::move(writer), std::move(filter)));
  return std::move(output);
}

}  // namespace koladata::functor::parallel
