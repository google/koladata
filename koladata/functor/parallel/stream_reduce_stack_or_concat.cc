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
#include "koladata/functor/parallel/stream_reduce_stack_or_concat.h"

#include <array>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/basic_routine.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/operators/slices.h"

namespace koladata::functor::parallel {
namespace {

class StreamReduceStackOrConcatHooks final : public BasicRoutineHooks {
 public:
  enum class Mode { kStack, kConcat };

  StreamReduceStackOrConcatHooks(Mode mode, StreamWriterPtr absl_nonnull writer,
                                 int64_t ndim, int64_t rank,
                                 DataSlice initial_value,
                                 StreamPtr absl_nonnull input_stream)
      : writer_(std::move(writer)),
        input_stream_(std::move(input_stream)),
        rank_(rank),
        first_args_({
            DataSlice::CreatePrimitive(mode == Mode::kStack),
            DataSlice::CreatePrimitive(ndim),
            std::move(initial_value),
        }),
        args_({&first_args_[0], &first_args_[1], &first_args_[2]}) {
    DCHECK_EQ(writer_->value_qtype(), arolla::GetQType<DataSlice>());
    DCHECK_EQ(input_stream_->value_qtype(), arolla::GetQType<DataSlice>());
  }

  bool Interrupted() const final { return writer_->Orphaned(); }

  void OnCancel(absl::Status&& status) final {
    writer_->TryClose(std::move(status));
  }

  StreamReaderPtr absl_nullable Start() final {
    return Resume(input_stream_->MakeReader());
  }

  StreamReaderPtr absl_nullable Resume(  // clang-format hint
      StreamReaderPtr absl_nonnull reader) final {
    auto try_read_result = reader->TryRead();
    while (arolla::TypedRef* item = try_read_result.item()) {
      if (Interrupted()) {
        return nullptr;
      }
      const auto& value = item->UnsafeAs<DataSlice>();
      if (value.GetShape().rank() != rank_) {
        writer_->TryClose(absl::InvalidArgumentError(absl::StrFormat(
            "all input slices must have the same rank, got %d and %d", rank_,
            value.GetShape().rank())));
        return nullptr;
      }
      args_.push_back(&value);
      try_read_result = reader->TryRead();
    }
    if (absl::Status* status = try_read_result.close_status()) {
      if (!status->ok()) {
        writer_->TryClose(std::move(*status));
        return nullptr;
      }
      auto result = koladata::ops::ConcatOrStack(args_);
      if (!result.ok()) {
        writer_->TryClose(result.status());
      } else if (writer_->TryWrite(arolla::TypedRef::FromValue(*result))) {
        writer_->TryClose(absl::OkStatus());
      }
      return nullptr;
    }
    return reader;
  }

 private:
  const StreamWriterPtr absl_nonnull writer_;
  const StreamPtr absl_nonnull input_stream_;
  const int64_t rank_;
  std::array<DataSlice, 3> first_args_;
  std::vector<const DataSlice*> args_;
};

}  // namespace

absl::StatusOr<StreamPtr absl_nonnull> StreamReduceConcat(
    ExecutorPtr absl_nonnull executor, int64_t ndim, DataSlice initial_value,
    StreamPtr absl_nonnull input_stream) {
  if (ndim < 1) {
    return absl::InvalidArgumentError(
        absl::StrFormat("invalid ndim=%d for concat", ndim));
  }
  const int64_t rank = initial_value.GetShape().rank();
  if (ndim > rank) {
    return absl::InvalidArgumentError(
        absl::StrFormat("invalid ndim=%d for rank=%d concat", ndim, rank));
  }
  if (input_stream->value_qtype() != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("input stream has value_qtype=%s, expected DATA_SLICE",
                        input_stream->value_qtype()->name()));
  }
  auto [result, writer] = MakeStream(arolla::GetQType<DataSlice>());
  StartBasicRoutine(
      std::move(executor),
      std::make_unique<StreamReduceStackOrConcatHooks>(
          StreamReduceStackOrConcatHooks::Mode::kConcat, std::move(writer),
          ndim, rank, std::move(initial_value), std::move(input_stream)));
  return std::move(result);
}

absl::StatusOr<StreamPtr absl_nonnull> StreamReduceStack(
    ExecutorPtr absl_nonnull executor, int64_t ndim, DataSlice initial_value,
    StreamPtr absl_nonnull input_stream) {
  if (ndim < 0) {
    return absl::InvalidArgumentError(
        absl::StrFormat("invalid ndim=%d for stack", ndim));
  }
  const int64_t rank = initial_value.GetShape().rank();
  if (ndim > rank) {
    return absl::InvalidArgumentError(
        absl::StrFormat("invalid ndim=%d for rank=%d stack", ndim, rank));
  }
  if (input_stream->value_qtype() != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("input stream has value_qtype=%s, expected DATA_SLICE",
                        input_stream->value_qtype()->name()));
  }
  auto [result, writer] = MakeStream(arolla::GetQType<DataSlice>());
  StartBasicRoutine(
      std::move(executor),
      std::make_unique<StreamReduceStackOrConcatHooks>(
          StreamReduceStackOrConcatHooks::Mode::kStack, std::move(writer), ndim,
          rank, std::move(initial_value), std::move(input_stream)));
  return std::move(result);
}

}  // namespace koladata::functor::parallel
