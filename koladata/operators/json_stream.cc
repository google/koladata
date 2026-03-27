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
#include <string_view>
#include <utility>
#include <variant>

#include "absl/base/nullability.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "koladata/arolla_utils.h"
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
using ::koladata::functor::parallel::ExecutorPtr;
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
                       ProcessorType processor)
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
    const auto write_output = [&](absl::InlinedVector<std::string, 1> output) {
      for (auto& output_chunk : output) {
        (void)writer_->TryWrite(arolla::TypedRef::FromValue(
            DataSlice::CreatePrimitive(arolla::Text(std::move(output_chunk)))));
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
            processor_.Process(item_slice.item().value<arolla::Text>(), false);
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
        auto [output, is_end_of_output] = processor_.Process("", true);
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
  ProcessorType processor_;
};

namespace {
template <typename... Processors>
class CompositeStreamProcessor {
 public:
  template <typename... Args>
  explicit CompositeStreamProcessor(Args&&... args)
      : processors_(std::forward<Args>(args)...) {}

  internal::JsonStreamProcessResult Process(std::string_view input_chunk,
                                            bool is_end_of_input) {
    absl::InlinedVector<std::string, 1> chunks = {std::string(input_chunk)};
    bool is_end = is_end_of_input;
    std::apply(
        [&](auto&... ps) {
          (([&]() {
             absl::InlinedVector<std::string, 1> next_chunks;
             bool next_end = false;
             for (auto& chunk : chunks) {
               if (next_end) break;
               auto [out_chunks, out_end] = ps.Process(chunk, false);
               for (auto& c : out_chunks) {
                 next_chunks.push_back(std::move(c));
               }
               if (out_end) {
                 next_end = true;
               }
             }
             if (is_end && !next_end) {
               auto [out_chunks, out_end] = ps.Process("", true);
               DCHECK(out_end);
               for (auto& c : out_chunks) {
                 next_chunks.push_back(std::move(c));
               }
               next_end = true;
             }
             chunks = std::move(next_chunks);
             is_end = next_end;
           }()),
           ...);
        },
        processors_);
    return {std::move(chunks), is_end};
  }

  std::tuple<Processors...> processors_;
};

template <typename Processor>
StreamPtr RunJsonStreamProcessor(ExecutorPtr absl_nonnull executor,
                                 StreamPtr absl_nonnull input_stream,
                                 Processor processor) {
  auto [output_stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  StartBasicRoutine(
      std::move(executor),
      std::make_unique<StreamProcessorHooks<Processor>>(
          std::move(writer), std::move(input_stream), std::move(processor)));
  return std::move(output_stream);
}

template <typename... Processors>
StreamPtr RunCompositeJsonStreamProcessor(ExecutorPtr absl_nonnull executor,
                                          StreamPtr absl_nonnull input_stream,
                                          Processors... processors) {
  return RunJsonStreamProcessor(
      std::move(executor), std::move(input_stream),
      CompositeStreamProcessor<Processors...>(std::move(processors)...));
}

}  // namespace

absl::StatusOr<StreamPtr> JsonStreamSalvageStream(
    ExecutorPtr absl_nonnull executor, StreamPtr absl_nonnull input_stream,
    const DataSlice& allow_nan, const DataSlice& ensure_ascii,
    const DataSlice& max_depth) {
  RETURN_IF_ERROR(ExpectPresentScalar("allow_nan", allow_nan, schema::kBool));
  RETURN_IF_ERROR(
      ExpectPresentScalar("ensure_ascii", ensure_ascii, schema::kBool));
  RETURN_IF_ERROR(ExpectInteger("max_depth", max_depth));
  ASSIGN_OR_RETURN(const int64_t max_depth_value,
                   ToArollaScalar<int64_t>(max_depth));
  return RunJsonStreamProcessor(
      std::move(executor), std::move(input_stream),
      internal::JsonSalvageStreamProcessor(internal::JsonSalvageOptions{
          .allow_nan = allow_nan.item().value<bool>(),
          .ensure_ascii = ensure_ascii.item().value<bool>(),
          .max_depth = max_depth_value,
      }));
}

absl::StatusOr<StreamPtr> JsonStreamPrettifyStream(
    ExecutorPtr absl_nonnull executor, StreamPtr absl_nonnull input_stream,
    const DataSlice& indent_string) {
  RETURN_IF_ERROR(
      ExpectPresentScalar("indent_string", indent_string, schema::kString));
  return RunJsonStreamProcessor(
      std::move(executor), std::move(input_stream),
      internal::JsonPrettifyStreamProcessor(internal::JsonPrettifyOptions{
          .indent_string =
              std::string(indent_string.item().value<arolla::Text>().view()),
      }));
}

absl::StatusOr<StreamPtr> JsonStreamHeadStream(ExecutorPtr
                                               absl_nonnull executor,
                                               StreamPtr
                                               absl_nonnull input_stream,
                                               const DataSlice& n) {
  RETURN_IF_ERROR(ExpectInteger("n", n));
  ASSIGN_OR_RETURN(const int64_t n_value, ToArollaScalar<int64_t>(n));
  return RunCompositeJsonStreamProcessor(
      std::move(executor), std::move(input_stream),
      internal::JsonCompactifyStreamProcessor(),
      internal::JsonHeadStreamProcessor(
          internal::JsonHeadOptions{.n = n_value}));
}

absl::StatusOr<StreamPtr> JsonStreamSelectNonemptyObjectsStream(
    ExecutorPtr absl_nonnull executor, StreamPtr absl_nonnull input_stream) {
  return RunCompositeJsonStreamProcessor(
      std::move(executor), std::move(input_stream),
      internal::JsonCompactifyStreamProcessor(),
      internal::JsonSelectNonemptyObjectsStreamProcessor());
}

absl::StatusOr<StreamPtr> JsonStreamSelectNonemptyArraysStream(
    ExecutorPtr absl_nonnull executor, StreamPtr absl_nonnull input_stream) {
  return RunCompositeJsonStreamProcessor(
      std::move(executor), std::move(input_stream),
      internal::JsonCompactifyStreamProcessor(),
      internal::JsonSelectNonemptyArraysStreamProcessor());
}

absl::StatusOr<StreamPtr> JsonStreamSelectNonnullStream(
    ExecutorPtr absl_nonnull executor, StreamPtr absl_nonnull input_stream) {
  return RunCompositeJsonStreamProcessor(
      std::move(executor), std::move(input_stream),
      internal::JsonCompactifyStreamProcessor(),
      internal::JsonSelectNonnullStreamProcessor());
}

absl::StatusOr<StreamPtr> JsonStreamGetObjectKeyValueStream(
    ExecutorPtr absl_nonnull executor, StreamPtr absl_nonnull input_stream,
    const DataSlice& key) {
  RETURN_IF_ERROR(ExpectPresentScalar("key", key, schema::kString));

  auto path_match_fn =
      [key_string = std::string(key.item().value<arolla::Text>().view())](
          absl::Span<const std::variant<int64_t, std::string>> path) -> bool {
    return path.size() == 1 &&
           std::holds_alternative<std::string>(path.back()) &&
           std::get<std::string>(path.back()) == key_string;
  };

  return RunCompositeJsonStreamProcessor(
      std::move(executor), std::move(input_stream),
      internal::JsonCompactifyStreamProcessor(),
      internal::JsonExtractValuesStreamProcessor(
          internal::JsonExtractValuesOptions{
              .path_match_fn = std::move(path_match_fn),
          }),
      internal::JsonGetArrayNthValueStreamProcessor(
          internal::JsonGetArrayNthValueOptions{.n = 0}));
}

absl::StatusOr<StreamPtr> JsonStreamGetObjectKeyValuesStream(
    ExecutorPtr absl_nonnull executor, StreamPtr absl_nonnull input_stream,
    const DataSlice& key) {
  RETURN_IF_ERROR(ExpectPresentScalar("key", key, schema::kString));

  auto path_match_fn =
      [key_string = std::string(key.item().value<arolla::Text>().view())](
          absl::Span<const std::variant<int64_t, std::string>> path) -> bool {
    return path.size() == 1 &&
           std::holds_alternative<std::string>(path.back()) &&
           std::get<std::string>(path.back()) == key_string;
  };

  return RunCompositeJsonStreamProcessor(
      std::move(executor), std::move(input_stream),
      internal::JsonCompactifyStreamProcessor(),
      internal::JsonExtractValuesStreamProcessor(
          internal::JsonExtractValuesOptions{
              .path_match_fn = std::move(path_match_fn),
          }));
}

absl::StatusOr<StreamPtr> JsonStreamImplodeArrayStream(
    ExecutorPtr absl_nonnull executor, StreamPtr absl_nonnull input_stream) {
  return RunCompositeJsonStreamProcessor(
      std::move(executor), std::move(input_stream),
      internal::JsonCompactifyStreamProcessor(),
      internal::JsonImplodeArrayStreamProcessor());
}

absl::StatusOr<StreamPtr> JsonStreamExplodeArrayStream(
    ExecutorPtr absl_nonnull executor, StreamPtr absl_nonnull input_stream) {
  return RunJsonStreamProcessor(std::move(executor), std::move(input_stream),
                                internal::JsonExplodeArrayStreamProcessor());
}

absl::StatusOr<StreamPtr> JsonStreamGetArrayNthValueStream(
    ExecutorPtr absl_nonnull executor, StreamPtr absl_nonnull input_stream,
    const DataSlice& n) {
  RETURN_IF_ERROR(ExpectInteger("n", n));
  ASSIGN_OR_RETURN(const int64_t n_value, ToArollaScalar<int64_t>(n));
  return RunJsonStreamProcessor(
      std::move(executor), std::move(input_stream),
      internal::JsonGetArrayNthValueStreamProcessor(
          internal::JsonGetArrayNthValueOptions{.n = n_value}));
}

absl::StatusOr<StreamPtr> JsonStreamUnquoteStream(ExecutorPtr
                                                  absl_nonnull executor,
                                                  StreamPtr
                                                  absl_nonnull input_stream) {
  return RunJsonStreamProcessor(std::move(executor), std::move(input_stream),
                                internal::JsonUnquoteStreamProcessor());
}

absl::StatusOr<StreamPtr> JsonStreamQuoteStream(ExecutorPtr
                                                absl_nonnull executor,
                                                StreamPtr
                                                absl_nonnull input_stream) {
  return RunJsonStreamProcessor(std::move(executor), std::move(input_stream),
                                internal::JsonQuoteStreamProcessor());
}

absl::StatusOr<StreamPtr> JsonStreamChunkValuesStream(
    ExecutorPtr absl_nonnull executor, StreamPtr absl_nonnull input_stream) {
  return RunJsonStreamProcessor(std::move(executor), std::move(input_stream),
                                internal::JsonChunkValuesStreamProcessor());
}

}  // namespace koladata::ops
