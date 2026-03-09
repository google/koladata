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
#include "koladata/operators/json_stream.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/basic_routine.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/json_stream.h"
#include "koladata/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

using ::koladata::functor::parallel::BasicRoutineHooks;
using ::koladata::functor::parallel::MakeStream;
using ::koladata::functor::parallel::StartBasicRoutine;
using ::koladata::functor::parallel::StreamPtr;
using ::koladata::functor::parallel::StreamReaderPtr;
using ::koladata::functor::parallel::StreamWriterPtr;

template <typename ProcessorType>
class StreamProcessorHooks final : public BasicRoutineHooks {
 public:
  StreamProcessorHooks(StreamWriterPtr absl_nonnull writer,
                       StreamPtr absl_nonnull input_stream,
                       std::unique_ptr<ProcessorType> processor)
      : writer_(std::move(writer)),
        input_stream_(std::move(input_stream)),
        processor_(std::move(processor)) {}

  bool Interrupted() const final { return writer_->Orphaned(); }

  void OnCancel(absl::Status&& status) final { OnError(std::move(status)); }

  StreamReaderPtr absl_nullable Start() final {
    return Resume(input_stream_->MakeReader());
  }

  StreamReaderPtr absl_nullable Resume(StreamReaderPtr
                                       absl_nonnull reader) final {
    const auto write_output = [&](std::string output) {
      if (!output.empty()) {
        (void)writer_->TryWrite(arolla::TypedRef::FromValue(
            DataSlice::CreatePrimitive(arolla::Text(std::move(output)))));
      }
    };
    auto try_read_result = reader->TryRead();
    while (arolla::TypedRef* item = try_read_result.item()) {
      if (Interrupted()) {
        return nullptr;
      }

      ASSIGN_OR_RETURN(auto item_slice, item->As<DataSlice>(), OnError(_));
      if (!item_slice.is_item() ||
          item_slice.GetSchemaImpl() != schema::kString) {
        return OnError(absl::InvalidArgumentError(
            absl::StrFormat("expected STRING DataItem from input stream, got "
                            "slice with schema %s and ndim=%d",
                            arolla::Repr(item_slice.GetSchemaImpl()),
                            item_slice.GetShape().rank())));
      }
      if (!item_slice.IsEmpty()) {
        auto [output, is_end_of_output] =
            processor_->Process(item_slice.item().value<arolla::Text>(), false);
        write_output(std::move(output));
        if (is_end_of_output) {
          writer_->TryClose(absl::OkStatus());
          return nullptr;
        }
      }

      try_read_result = reader->TryRead();
    }
    if (absl::Status* status = try_read_result.close_status()) {
      if (status->ok()) {
        auto [output, is_end_of_output] = processor_->Process("", true);
        DCHECK(is_end_of_output);
        write_output(std::move(output));
      }
      writer_->TryClose(std::move(*status));
      return nullptr;
    }
    return reader;
  }

 private:
  std::nullptr_t OnError(absl::Status&& status) {
    writer_->TryClose(std::move(status));
    return nullptr;
  }

  const StreamWriterPtr absl_nonnull writer_;
  const StreamPtr absl_nonnull input_stream_;
  const std::unique_ptr<ProcessorType> processor_;
};

absl::StatusOr<koladata::functor::parallel::StreamPtr> JsonStreamSalvageStream(
    functor::parallel::ExecutorPtr absl_nonnull executor,
    const koladata::functor::parallel::StreamPtr absl_nonnull& input_stream,
    const DataSlice& allow_nan, const DataSlice& ensure_ascii,
    const DataSlice& max_depth) {
  RETURN_IF_ERROR(ExpectPresentScalar("allow_nan", allow_nan, schema::kBool));
  RETURN_IF_ERROR(
      ExpectPresentScalar("ensure_ascii", ensure_ascii, schema::kBool));
  RETURN_IF_ERROR(ExpectPresentScalar("max_depth", max_depth, schema::kInt32));
  internal::JsonSalvageOptions options = {
      .allow_nan = allow_nan.item().value<bool>(),
      .ensure_ascii = ensure_ascii.item().value<bool>(),
      .max_depth = max_depth.item().value<int32_t>(),
  };

  auto [output_stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  StartBasicRoutine(
      std::move(executor),
      std::make_unique<
          StreamProcessorHooks<internal::JsonSalvageStreamProcessor>>(
          std::move(writer), input_stream,
          std::make_unique<internal::JsonSalvageStreamProcessor>(options)));

  return std::move(output_stream);
}

}  // namespace koladata::ops
