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

#include <memory>
#include <string>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/basic_routine.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/operators/json_stream_parser.h"

namespace koladata::functor::parallel {

namespace {

void ConfigureParser(ops::JsonStreamParser& parser) {
  parser.SetSeparatorPolicy(ops::JsonStreamParser::SeparatorPolicy::REMOVE);
  parser.AllowLinebreakInStrings(true);
  parser.AllowSingleQuotes(true);
  parser.AllowUnquotedKeys(true);
  parser.EnableQuotingInvalidTokens(true);
}

class StreamFilterJsonHooks final : public BasicRoutineHooks {
 public:
  StreamFilterJsonHooks(StreamPtr absl_nonnull input_stream,
                        StreamWriterPtr absl_nonnull writer,
                        std::string field_to_extract)
      : input_stream_(std::move(input_stream)),
        writer_(std::move(writer)),
        field_to_extract_(std::move(field_to_extract)) {
    ConfigureParser(parser_);
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
  ops::JsonStreamParser parser_;
};

class StreamStringFromJsonHooks final : public BasicRoutineHooks {
 public:
  StreamStringFromJsonHooks(StreamPtr absl_nonnull input_stream,
                            StreamWriterPtr absl_nonnull writer,
                            std::string field_to_extract)
      : input_stream_(std::move(input_stream)),
        writer_(std::move(writer)),
        field_to_extract_(std::move(field_to_extract)) {
    ConfigureParser(parser_);
    parser_.SetValueBeginCallback(
        [&](absl::string_view path, ops::JsonStreamParser::ValueType type) {
          if (path == field_to_extract_ &&
              type == ops::JsonStreamParser::ValueType::STRING &&
              state_ == FILTERING) {
            parser_.ConsumeChunk();  // flush previous values
            state_ = START_STREAMING;
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

  absl::Status ProcessChunk(absl::string_view chunk) {
    RETURN_IF_ERROR(parser_.AddChunk(chunk));
    chunk = parser_.ConsumeChunk();
    if (chunk.empty() || state_ == FILTERING) {
      return absl::OkStatus();
    }
    std::string res;
    for (char c : chunk) {
      if (state_ == ESCAPE_UTF && !absl::ascii_isxdigit(c)) {
        // Note: we ignore invalid \uXXXX sequence and continue streaming.
        utf_code_.clear();
        state_ = STREAMING;
        break;
      }
      switch (state_) {
        case START_STREAMING:
          if (c != '"') {
            return absl::FailedPreconditionError(
                absl::StrFormat("expected '\"', got '%c'", c));
          }
          state_ = STREAMING;
          break;
        case STREAMING:
          if (c == '"') {
            state_ = FINISHED;
          } else if (c == '\\') {
            state_ = ESCAPE;
          } else {
            res.push_back(c);
          }
          break;
        case ESCAPE:
          if (c == 'u') {
            state_ = ESCAPE_UTF;
            utf_code_.clear();
            break;
          }
          res.push_back(GetEscapedCharacter(c));
          state_ = STREAMING;
          break;
        case ESCAPE_UTF:
          utf_code_.push_back(c);
          if (utf_code_.size() >= 4) {
            int code = std::stoi(utf_code_, nullptr, 16);
            utf_code_.clear();
            if (code >= 0xd800 && code < 0xdc00) {  // high surrogate
              utf16_high_surrogate_ = code - 0xd800;
              state_ = STREAMING;
              break;
            }
            if (code >= 0xdc00 && code < 0xe000) {  // low surrogate
              code = utf16_high_surrogate_ * 0x400 + (code - 0xdc00) + 0x10000;
            }
            EncodeUtf8(code, res);
            state_ = STREAMING;
          }
          break;
        default: {
        }
      }
    }
    if (!res.empty()) {
      DataSlice ds_out =
          DataSlice::CreatePrimitive(arolla::Text(std::move(res)));
      writer_->Write(arolla::TypedRef::FromValue(ds_out));
    }
    return absl::OkStatus();
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
      if (absl::Status status = ProcessChunk(data);
          !status.ok() || state_ == FINISHED) {
        writer_->TryClose(std::move(status));
        return nullptr;
      }
      try_read_result = reader->TryRead();
    }
    if (absl::Status* status = try_read_result.close_status()) {
      if (status->ok()) {
        absl::Status st = ProcessChunk(" ");
        if (st.ok() && state_ == FILTERING) {
          st = absl::InvalidArgumentError(absl::StrFormat(
              "field '%s' is not found in JSON stream", field_to_extract_));
        }
        writer_->TryClose(std::move(st));
      } else {
        writer_->TryClose(*status);
      }
      return nullptr;
    }
    return reader;
  }

 private:
  static char GetEscapedCharacter(char c) {
    switch (c) {
      case 'b':
        return '\b';
      case 'f':
        return '\f';
      case 'n':
        return '\n';
      case 'r':
        return '\r';
      case 't':
        return '\t';
      default:
        return c;
    }
  }

  static void EncodeUtf8(int code, std::string& res) {
    if (code < 0x80) {
      res.push_back(code);
    } else if (code < 0x800) {
      res.push_back(0xc0 | ((code >> 6) & 0x1f));
      res.push_back(0x80 | (code & 0x3f));
    } else if (code < 0x10000) {
      res.push_back(0xe0 | ((code >> 12) & 0xf));
      res.push_back(0x80 | ((code >> 6) & 0x3f));
      res.push_back(0x80 | (code & 0x3f));
    } else {
      res.push_back(0xf0 | ((code >> 18) & 0x7));
      res.push_back(0x80 | ((code >> 12) & 0x3f));
      res.push_back(0x80 | ((code >> 6) & 0x3f));
      res.push_back(0x80 | (code & 0x3f));
    }
  }

  const StreamPtr absl_nonnull input_stream_;
  const StreamWriterPtr absl_nonnull writer_;
  std::string field_to_extract_;
  ops::JsonStreamParser parser_;
  enum {
    FILTERING,
    START_STREAMING,
    STREAMING,
    ESCAPE,
    ESCAPE_UTF,
    FINISHED
  } state_ = FILTERING;
  std::string utf_code_;
  int utf16_high_surrogate_;
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

absl::StatusOr<StreamPtr> StreamStringFromJson(
    ExecutorPtr absl_nonnull executor, StreamPtr absl_nonnull input,
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
      std::make_unique<StreamStringFromJsonHooks>(
          std::move(input), std::move(writer), std::move(filter)));
  return std::move(output);
}

}  // namespace koladata::functor::parallel
