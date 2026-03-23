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
#include "absl/strings/string_view.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/basic_routine.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/operators/json_stream_parser.h"

namespace koladata::functor::parallel {

namespace {

class StreamFilterJsonHooks final : public BasicRoutineHooks {
 public:
  StreamFilterJsonHooks(StreamPtr absl_nonnull input_stream,
                        StreamWriterPtr absl_nonnull writer,
                        std::string field_to_extract)
      : input_stream_(std::move(input_stream)),
        writer_(std::move(writer)),
        field_to_extract_(std::move(field_to_extract)) {
    parser_.SetSeparatorPolicy(ops::JsonStreamParser::SeparatorPolicy::REMOVE);
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
  ops::JsonStreamParser parser_;
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
