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
#include "koladata/functor/parallel/future_operators.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/mutable_sequence.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/future.h"
#include "koladata/functor/parallel/future_qtype.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_composition.h"
#include "koladata/functor/parallel/stream_qtype.h"
#include "koladata/internal/op_utils/qexpr.h"
#include "koladata/iterables/iterable_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

class AsFutureOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.as_future",
        [input_slot = input_slots[0], output_slot](
            arolla::EvaluationContext* /*ctx*/,
            arolla::FramePtr frame) -> absl::Status {
          arolla::TypedValue input_value =
              arolla::TypedValue::FromSlot(input_slot, frame);
          auto [future, writer] = MakeFuture(input_slot.GetType());
          std::move(writer).SetValue(std::move(input_value));
          frame.Set(output_slot.UnsafeToSlot<FuturePtr>(), std::move(future));
          return absl::OkStatus();
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> AsFutureOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("requires exactly 1 argument");
  }
  if (!IsFutureQType(output_type) ||
      output_type->value_qtype() != input_types[0]) {
    return absl::InvalidArgumentError(
        "output qtype must be a future of the input type");
  }
  return std::make_shared<AsFutureOperator>(input_types, output_type);
}

namespace {

class GetFutureValueForTestingOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.get_future_value_for_testing",
        [input_slot = input_slots[0].UnsafeToSlot<FuturePtr>(), output_slot](
            arolla::EvaluationContext* /*ctx*/,
            arolla::FramePtr frame) -> absl::Status {
          const auto& future = frame.Get(input_slot);
          if (future == nullptr) {
            return absl::InvalidArgumentError("future is null");
          }
          ASSIGN_OR_RETURN(arolla::TypedValue value,
                           future->GetValueForTesting());
          return value.CopyToSlot(output_slot, frame);
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr>
GetFutureValueForTestingOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("requires exactly 1 argument");
  }
  if (input_types[0] != GetFutureQType(output_type)) {
    return absl::InvalidArgumentError(
        "argument must be a future of the output type");
  }
  return std::make_shared<GetFutureValueForTestingOperator>(input_types,
                                                            output_type);
}

namespace {

class UnwrapFutureToFutureOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.unwrap_future_to_future",
        [input_slot = input_slots[0].UnsafeToSlot<FuturePtr>(),
         output_slot = output_slot.UnsafeToSlot<FuturePtr>()](
            arolla::EvaluationContext* /*ctx*/,
            arolla::FramePtr frame) -> absl::Status {
          const auto& future = frame.Get(input_slot);
          if (future == nullptr) {
            return absl::InvalidArgumentError("future is null");
          }
          const auto& inner_future_qtype = future->value_qtype();
          if (!IsFutureQType(inner_future_qtype)) {
            return absl::InvalidArgumentError(
                "argument must be a future to a future");
          }
          auto [result_future, result_writer] =
              MakeFuture(inner_future_qtype->value_qtype());
          future->AddConsumer(
              [result_writer = std::move(result_writer)](
                  absl::StatusOr<arolla::TypedValue> value) mutable {
                if (!value.ok()) {
                  std::move(result_writer).SetValue(std::move(value));
                  return;
                }
                const auto& inner_future = value->UnsafeAs<FuturePtr>();
                if (inner_future == nullptr) {
                  std::move(result_writer)
                      .SetValue(
                          absl::InvalidArgumentError("inner future is null"));
                  return;
                }
                inner_future->AddConsumer(
                    [result_writer = std::move(result_writer)](
                        absl::StatusOr<arolla::TypedValue> value) mutable {
                      std::move(result_writer).SetValue(std::move(value));
                    });
              });
          frame.Set(output_slot, std::move(result_future));
          return absl::OkStatus();
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr>
UnwrapFutureToFutureOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("requires exactly 1 argument");
  }
  if (!IsFutureQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a future");
  }
  if (input_types[0] != GetFutureQType(output_type)) {
    return absl::InvalidArgumentError(
        "argument must be a future of the output type");
  }
  return std::make_shared<UnwrapFutureToFutureOperator>(input_types,
                                                        output_type);
}

namespace {

class UnwrapFutureToStreamOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.unwrap_future_to_stream",
        [input_slot = input_slots[0].UnsafeToSlot<FuturePtr>(),
         output_slot = output_slot.UnsafeToSlot<StreamPtr>()](
            arolla::EvaluationContext* /*ctx*/,
            arolla::FramePtr frame) -> absl::Status {
          const auto& future = frame.Get(input_slot);
          if (future == nullptr) {
            return absl::InvalidArgumentError("future is null");
          }
          const auto& inner_stream_qtype = future->value_qtype();
          if (!IsStreamQType(inner_stream_qtype)) {
            return absl::InvalidArgumentError(
                "argument must be a future to a stream");
          }
          auto [result_stream, result_writer] =
              MakeStream(inner_stream_qtype->value_qtype());
          future->AddConsumer(
              [result_writer = std::move(result_writer)](
                  absl::StatusOr<arolla::TypedValue> value) mutable {
                if (!value.ok()) {
                  std::move(*result_writer).Close(std::move(value).status());
                  return;
                }
                // Using chain to copy one stream to another is an overkill,
                // but works.
                StreamChain chain(std::move(result_writer));
                chain.Add(value->UnsafeAs<StreamPtr>());
              });
          frame.Set(output_slot, std::move(result_stream));
          return absl::OkStatus();
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr>
UnwrapFutureToStreamOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("requires exactly 1 argument");
  }
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  if (input_types[0] != GetFutureQType(output_type)) {
    return absl::InvalidArgumentError(
        "argument must be a future of the output type");
  }
  return std::make_shared<UnwrapFutureToStreamOperator>(input_types,
                                                        output_type);
}

namespace {

class StreamFromFutureOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.stream_from_future",
        [input_slot = input_slots[0].UnsafeToSlot<FuturePtr>(),
         output_slot = output_slot.UnsafeToSlot<StreamPtr>()](
            arolla::EvaluationContext* /*ctx*/,
            arolla::FramePtr frame) -> absl::Status {
          const auto& future = frame.Get(input_slot);
          if (future == nullptr) {
            return absl::InvalidArgumentError("future is null");
          }
          auto [result_stream, result_writer] =
              MakeStream(future->value_qtype());
          future->AddConsumer(
              [result_writer = std::move(result_writer)](
                  absl::StatusOr<arolla::TypedValue> value) mutable {
                if (!value.ok()) {
                  std::move(*result_writer).Close(std::move(value).status());
                  return;
                }
                result_writer->Write(value->AsRef());
                std::move(*result_writer).Close();
              });
          frame.Set(output_slot, std::move(result_stream));
          return absl::OkStatus();
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr>
StreamFromFutureOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("requires exactly 1 argument");
  }
  if (!IsFutureQType(input_types[0])) {
    return absl::InvalidArgumentError("input type must be a future");
  }
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  if (input_types[0]->value_qtype() != output_type->value_qtype()) {
    return absl::InvalidArgumentError("stream and future types must match");
  }
  return std::make_shared<StreamFromFutureOperator>(input_types, output_type);
}

namespace {

class FutureFromSingleValueStreamOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.future_from_single_value_stream",
        [input_slot = input_slots[0].UnsafeToSlot<StreamPtr>(),
         output_slot = output_slot.UnsafeToSlot<FuturePtr>()](
            arolla::EvaluationContext* /*ctx*/,
            arolla::FramePtr frame) -> absl::Status {
          const auto& stream = frame.Get(input_slot);
          if (stream == nullptr) {
            return absl::InvalidArgumentError("stream is null");
          }
          auto [result_future, result_writer] =
              MakeFuture(stream->value_qtype());
          Process(std::make_unique<State>(stream->MakeReader(), std::nullopt,
                                          std::move(result_writer)));
          frame.Set(output_slot, std::move(result_future));
          return absl::OkStatus();
        });
  }

 private:
  struct State {
    StreamReaderPtr reader;
    std::optional<arolla::TypedValue> value;
    FutureWriter writer;
  };

  static void Process(std::unique_ptr<State> state) {
    auto try_read_result = state->reader->TryRead();
    while (arolla::TypedRef* item = try_read_result.item()) {
      if (state->value.has_value()) {
        std::move(state->writer)
            .SetValue(
                absl::InvalidArgumentError("stream has more than one value, "
                                           "but we are trying to convert it "
                                           "to a future"));
        return;
      }
      state->value = arolla::TypedValue(*item);
      try_read_result = state->reader->TryRead();
    }
    if (absl::Status* status = try_read_result.close_status()) {
      if (!status->ok()) {
        std::move(state->writer).SetValue(std::move(*status));
        return;
      }
      if (!state->value.has_value()) {
        std::move(state->writer)
            .SetValue(
                absl::InvalidArgumentError("stream has no values, but we are "
                                           "trying to convert it to a future"));
        return;
      }
      std::move(state->writer).SetValue(*std::move(state->value));
      return;
    }
    state->reader->SubscribeOnce(
        [state = std::move(state)]() mutable { Process(std::move(state)); });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr>
FutureFromSingleValueStreamOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("requires exactly 1 argument");
  }
  if (!IsStreamQType(input_types[0])) {
    return absl::InvalidArgumentError("input type must be a stream");
  }
  if (!IsFutureQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a future");
  }
  if (input_types[0]->value_qtype() != output_type->value_qtype()) {
    return absl::InvalidArgumentError("stream and future types must match");
  }
  return std::make_shared<FutureFromSingleValueStreamOperator>(input_types,
                                                               output_type);
}

namespace {

class FutureIterableFromStreamOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.future_iterable_from_stream",
        [input_slot = input_slots[0].UnsafeToSlot<StreamPtr>(),
         output_slot = output_slot.UnsafeToSlot<FuturePtr>()](
            arolla::EvaluationContext* /*ctx*/,
            arolla::FramePtr frame) -> absl::Status {
          const auto& stream = frame.Get(input_slot);
          if (stream == nullptr) {
            return absl::InvalidArgumentError("stream is null");
          }
          auto [result_future, result_writer] =
              MakeFuture(iterables::GetIterableQType(stream->value_qtype()));
          Process(std::make_unique<State>(
              stream->MakeReader(), std::vector<arolla::TypedRef>(),
              std::move(result_writer), stream->value_qtype()));
          frame.Set(output_slot, std::move(result_future));
          return absl::OkStatus();
        });
  }

 private:
  struct State {
    StreamReaderPtr reader;
    std::vector<arolla::TypedRef> values;
    FutureWriter writer;
    arolla::QTypePtr value_qtype;

    auto OnError() {
      return [this](absl::Status&& status) {
        std::move(writer).SetValue(std::move(status));
      };
    }
  };

  static void Process(std::unique_ptr<State> state) {
    auto try_read_result = state->reader->TryRead();
    while (arolla::TypedRef* item = try_read_result.item()) {
      state->values.push_back(*item);
      try_read_result = state->reader->TryRead();
    }
    if (absl::Status* status = try_read_result.close_status()) {
      if (!status->ok()) {
        return state->OnError()(std::move(*status));
      }
      ASSIGN_OR_RETURN(auto sequence,
                       arolla::MutableSequence::Make(state->value_qtype,
                                                     state->values.size()),
                       state->OnError()(std::move(_)));
      for (int64_t i = 0; i < state->values.size(); ++i) {
        sequence.UnsafeSetRef(i, state->values[i]);
      }
      std::move(state->writer)
          .SetValue(arolla::TypedValue::FromValueWithQType(
              std::move(sequence).Finish(),
              iterables::GetIterableQType(state->value_qtype)));
      return;
    }
    state->reader->SubscribeOnce(
        [state = std::move(state)]() mutable { Process(std::move(state)); });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr>
FutureIterableFromStreamOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("requires exactly 1 argument");
  }
  if (!IsStreamQType(input_types[0])) {
    return absl::InvalidArgumentError("input type must be a stream");
  }
  if (!IsFutureQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a future");
  }
  if (!iterables::IsIterableQType(output_type->value_qtype())) {
    return absl::InvalidArgumentError(
        "output type must be a future to an iterable");
  }
  if (input_types[0]->value_qtype() !=
      output_type->value_qtype()->value_qtype()) {
    return absl::InvalidArgumentError("stream and iterable types must match");
  }
  return std::make_shared<FutureIterableFromStreamOperator>(input_types,
                                                            output_type);
}

}  // namespace koladata::functor::parallel
