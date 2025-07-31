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
#include "koladata/functor/parallel/stream_operators.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/sequence/sequence.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/permanent_event.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/call.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_call.h"
#include "koladata/functor/parallel/stream_composition.h"
#include "koladata/functor/parallel/stream_for.h"
#include "koladata/functor/parallel/stream_map.h"
#include "koladata/functor/parallel/stream_qtype.h"
#include "koladata/functor/parallel/stream_reduce.h"
#include "koladata/functor/parallel/stream_while.h"
#include "koladata/internal/op_utils/qexpr.h"
#include "koladata/iterables/iterable_qtype.h"
#include "koladata/operators/utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

class EmptyStreamLikeOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.empty_stream_like",
        [output_slot = output_slot.UnsafeToSlot<StreamPtr>(),
         value_qtype = output_slot.GetType()->value_qtype()](
            arolla::EvaluationContext* /*ctx*/, arolla::FramePtr frame) {
          auto [stream, writer] = MakeStream(value_qtype);
          std::move(*writer).Close();
          frame.Set(output_slot, std::move(stream));
        });
  }
};

}  // namespace

// empty_stream_with_value_qtype(
//     qtype[T], NON_DETERMINISTIC) -> STREAM[T]
absl::StatusOr<arolla::OperatorPtr>
EmptyStreamLikeOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("requires exactly 1 argument");
  }
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  if (input_types[0] != output_type) {
    return absl::InvalidArgumentError(
        "the first argument must have the same type as output");
  }
  return std::make_shared<EmptyStreamLikeOp>(input_types, output_type);
}

namespace {

class StreamChainFromStreamOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.stream_chain_from_stream",
        [input_slot = input_slots[0].UnsafeToSlot<StreamPtr>(),
         output_slot = output_slot.UnsafeToSlot<StreamPtr>(),
         value_qtype = output_slot.GetType()->value_qtype()](
            arolla::EvaluationContext* /*ctx*/, arolla::FramePtr frame) {
          auto [stream, writer] = MakeStream(value_qtype);
          frame.Set(output_slot, std::move(stream));
          Process(StreamChain(std::move(writer)),
                  frame.Get(input_slot)->MakeReader());
        });
  }

 private:
  static void Process(StreamChain chain_helper, StreamReaderPtr reader) {
    auto try_read_result = reader->TryRead();
    while (auto* item = try_read_result.item()) {
      chain_helper.Add(item->UnsafeAs<StreamPtr>());
      try_read_result = reader->TryRead();
    }
    if (auto* status = try_read_result.close_status()) {
      if (!status->ok()) {
        std::move(chain_helper).AddError(std::move(*status));
      }
      return;
    }
    reader->SubscribeOnce([chain_helper = std::move(chain_helper),
                           reader = std::move(reader)]() mutable {
      Process(std::move(chain_helper), std::move(reader));
    });
  }
};

}  // namespace

// stream_chain_from_stream(
//     STREAM[STREAM[T]], NON_DETERMINISTIC) -> STREAM[T]
absl::StatusOr<arolla::OperatorPtr>
StreamChainFromStreamOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 2) {
    return absl::InvalidArgumentError("requires exactly 2 arguments");
  }
  if (!IsStreamQType(input_types[0])) {
    return absl::InvalidArgumentError(
        "the first argument must be a stream of streams");
  }
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[1]));
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  if (input_types[0]->value_qtype() != output_type) {
    return absl::InvalidArgumentError(
        "the first argument's value type must match the output type");
  }
  return std::make_shared<StreamChainFromStreamOp>(input_types, output_type);
}

namespace {

class StreamChainOp : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    std::vector<arolla::TypedSlot::Slot<StreamPtr>> input_stream_slots;
    input_stream_slots.reserve(input_slots[0].SubSlotCount());
    for (int64_t i = 0; i < input_slots[0].SubSlotCount(); ++i) {
      input_stream_slots.push_back(
          input_slots[0].SubSlot(i).UnsafeToSlot<StreamPtr>());
    }
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.stream_chain",
        [input_stream_slots = std::move(input_stream_slots),
         output_slot = output_slot.UnsafeToSlot<StreamPtr>(),
         value_qtype = output_slot.GetType()->value_qtype()](
            arolla::EvaluationContext* /*ctx*/, arolla::FramePtr frame) {
          if (input_stream_slots.size() == 1) {
            // A performance optimization to avoid an extra stream copy.
            frame.Set(output_slot, frame.Get(input_stream_slots[0]));
            return;
          }
          auto [stream, writer] = MakeStream(value_qtype);
          frame.Set(output_slot, std::move(stream));
          StreamChain chain_helper(std::move(writer));
          for (const auto& input_stream_slot : input_stream_slots) {
            chain_helper.Add(frame.Get(input_stream_slot));
          }
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> StreamChainOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 3) {
    return absl::InvalidArgumentError("requires exactly 3 arguments");
  }
  if (!arolla::IsTupleQType(input_types[0])) {
    return absl::InvalidArgumentError("the first argument must be a tuple");
  }
  // The second argument is used for type inference and can be anything, so we
  // don't handle input_types[1] here.
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[2]));
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  for (const auto& type_field : input_types[0]->type_fields()) {
    if (type_field.GetType() != output_type) {
      return absl::InvalidArgumentError(
          "all tuple fields must have the same type as the output");
    }
  }
  return std::make_shared<StreamChainOp>(input_types, output_type);
}

namespace {

class StreamInterleaveFromStreamOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.stream_interleave_from_stream",
        [input_slot = input_slots[0].UnsafeToSlot<StreamPtr>(),
         output_slot = output_slot.UnsafeToSlot<StreamPtr>(),
         value_qtype = output_slot.GetType()->value_qtype()](
            arolla::EvaluationContext* /*ctx*/, arolla::FramePtr frame) {
          auto [stream, writer] = MakeStream(value_qtype);
          frame.Set(output_slot, std::move(stream));
          Process(StreamInterleave(std::move(writer)),
                  frame.Get(input_slot)->MakeReader());
        });
  }

 private:
  static void Process(StreamInterleave interleave_helper,
                      StreamReaderPtr reader) {
    auto try_read_result = reader->TryRead();
    while (auto* item = try_read_result.item()) {
      interleave_helper.Add(item->UnsafeAs<StreamPtr>());
      try_read_result = reader->TryRead();
    }
    if (auto* status = try_read_result.close_status()) {
      if (!status->ok()) {
        std::move(interleave_helper).AddError(*status);
      }
      return;
    }
    reader->SubscribeOnce([interleave_helper = std::move(interleave_helper),
                           reader = std::move(reader)]() mutable {
      Process(std::move(interleave_helper), std::move(reader));
    });
  }
};

}  // namespace

// stream_interleave_from_stream(
//     STREAM[STREAM[T]], NON_DETERMINISTIC) -> STREAM[T]
absl::StatusOr<arolla::OperatorPtr>
StreamInterleaveFromStreamOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 2) {
    return absl::InvalidArgumentError("requires exactly 2 arguments");
  }
  if (!IsStreamQType(input_types[0])) {
    return absl::InvalidArgumentError(
        "the first argument must be a stream of streams");
  }
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[1]));
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  if (input_types[0]->value_qtype() != output_type) {
    return absl::InvalidArgumentError(
        "the first argument's value type must match the output type");
  }
  return std::make_shared<StreamInterleaveFromStreamOp>(input_types,
                                                        output_type);
}

namespace {

class StreamInterleaveOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    std::vector<arolla::TypedSlot::Slot<StreamPtr>> input_stream_slots;
    input_stream_slots.reserve(input_slots[0].SubSlotCount());
    for (int64_t i = 0; i < input_slots[0].SubSlotCount(); ++i) {
      input_stream_slots.push_back(
          input_slots[0].SubSlot(i).UnsafeToSlot<StreamPtr>());
    }
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.stream_interleave",
        [input_stream_slots = std::move(input_stream_slots),
         output_slot = output_slot.UnsafeToSlot<StreamPtr>(),
         value_qtype = output_slot.GetType()->value_qtype()](
            arolla::EvaluationContext* /*ctx*/, arolla::FramePtr frame) {
          if (input_stream_slots.size() == 1) {
            // A performance optimization to avoid an extra stream copy.
            frame.Set(output_slot, frame.Get(input_stream_slots[0]));
            return;
          }
          auto [stream, writer] = MakeStream(value_qtype);
          frame.Set(output_slot, std::move(stream));
          StreamInterleave interleave_helper(std::move(writer));
          for (const auto& input_stream_slot : input_stream_slots) {
            interleave_helper.Add(frame.Get(input_stream_slot));
          }
        });
  }
};

}  // namespace

// stream_interleave(TUPLE[STREAM[T], ...], T, NON_DETERMINISTIC) -> STREAM[T]
absl::StatusOr<arolla::OperatorPtr>
StreamInterleaveOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 3) {
    return absl::InvalidArgumentError("requires exactly 3 arguments");
  }
  if (!arolla::IsTupleQType(input_types[0])) {
    return absl::InvalidArgumentError("the first argument must be a tuple");
  }
  // The second argument is used for type inference and can be anything, so we
  // don't handle input_types[1] here.
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[2]));
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  for (const auto& type_field : input_types[0]->type_fields()) {
    if (type_field.GetType() != output_type) {
      return absl::InvalidArgumentError(
          "all tuple fields must have the same type as the output");
    }
  }
  return std::make_shared<StreamInterleaveOp>(input_types, output_type);
}

namespace {

class StreamMakeOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.stream_make",
        [input_slot = input_slots[0],
         value_qtype = output_slot.GetType()->value_qtype(),
         output_slot = output_slot.UnsafeToSlot<StreamPtr>()](
            arolla::EvaluationContext* /*ctx*/, arolla::FramePtr frame) {
          const int64_t arg_count = input_slot.SubSlotCount();
          auto [stream, writer] = MakeStream(value_qtype, arg_count);
          frame.Set(output_slot, std::move(stream));
          for (int64_t i = 0; i < arg_count; ++i) {
            writer->Write(
                arolla::TypedRef::FromSlot(input_slot.SubSlot(i), frame));
          }
          std::move(*writer).Close();
        });
  }
};

}  // namespace

// stream_make(TUPLE[T, ...], T) -> STREAM[T]
absl::StatusOr<arolla::OperatorPtr> StreamMakeOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 2) {
    return absl::InvalidArgumentError("requires exactly 2 arguments");
  }
  if (!arolla::IsTupleQType(input_types[0])) {
    return absl::InvalidArgumentError("the first argument must be a tuple");
  }
  // The second argument is used for type inference and can be anything, so we
  // don't handle input_types[1] here.
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  for (const auto& field : input_types[0]->type_fields()) {
    if (field.GetType() != output_type->value_qtype()) {
      return absl::InvalidArgumentError(
          "all tuple fields must have the same type as the value type of the "
          "output");
    }
  }
  return std::make_shared<StreamMakeOp>(input_types, output_type);
}

namespace {

class StreamFromIterableOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.stream_from_iterable",
        [input_slot = input_slots[0].UnsafeToSlot<arolla::Sequence>(),
         output_slot = output_slot.UnsafeToSlot<StreamPtr>()](
            arolla::EvaluationContext* /*ctx*/, arolla::FramePtr frame) {
          const arolla::Sequence& sequence = frame.Get(input_slot);
          auto [stream, writer] =
              MakeStream(sequence.value_qtype(), sequence.size());
          frame.Set(output_slot, std::move(stream));
          for (int64_t i = 0; i < sequence.size(); ++i) {
            writer->Write(sequence.GetRef(i));
          }
          std::move(*writer).Close();
        });
  }
};

}  // namespace

// stream_from_iterable(ITERABLE[T]) -> STREAM[T]
absl::StatusOr<arolla::OperatorPtr>
StreamFromIterableOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("requires exactly 2 arguments");
  }
  if (!iterables::IsIterableQType(input_types[0])) {
    return absl::InvalidArgumentError("the first argument must be an iterable");
  }
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  if (output_type->value_qtype() != input_types[0]->value_qtype()) {
    return absl::InvalidArgumentError(
        "the value type of the output must be the same as the value type of "
        "the input");
  }
  return std::make_shared<StreamFromIterableOp>(input_types, output_type);
}

namespace {

auto MakeStreamMapFunctor(DataSlice functor_ds,
                          arolla::QTypePtr return_value_qtype) {
  return [functor_ds = std::move(functor_ds), return_value_qtype](
             arolla::TypedRef item) -> absl::StatusOr<arolla::TypedValue> {
    auto return_value = CallFunctorWithCompilationCache(functor_ds, {item}, {});
    if (return_value.ok() && return_value->GetType() != return_value_qtype)
        [[unlikely]] {
      return absl::InvalidArgumentError(absl::StrFormat(
          "The functor was called with `%s` as the return "
          "type, but the computation resulted in type `%s` "
          "instead. You can specify the expected output type "
          "via the `value_type_as=` parameter.",
          return_value_qtype->name(), return_value->GetType()->name()));
    }
    return return_value;
  };
}

class StreamMapOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.stream_map",
        [executor_slot = input_slots[0].UnsafeToSlot<ExecutorPtr>(),
         input_stream_slot = input_slots[1].UnsafeToSlot<StreamPtr>(),
         functor_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         return_value_qtype = output_slot.GetType()->value_qtype(),
         output_slot = output_slot.UnsafeToSlot<StreamPtr>()](
            arolla::EvaluationContext* /*ctx*/, arolla::FramePtr frame) {
          frame.Set(output_slot,
                    StreamMap(frame.Get(executor_slot),
                              frame.Get(input_stream_slot), return_value_qtype,
                              MakeStreamMapFunctor(frame.Get(functor_slot),
                                                   return_value_qtype)));
        });
  }
};

}  // namespace

// stream_map(
//     EXECUTOR, STREAM[S], DATA_SLICE, T, NON_DETERMINISTIC) -> STREAM[T]
absl::StatusOr<arolla::OperatorPtr> StreamMapOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 5) {
    return absl::InvalidArgumentError("requires exactly 5 arguments");
  }
  if (input_types[0] != arolla::GetQType<ExecutorPtr>()) {
    return absl::InvalidArgumentError("the first argument must be an executor");
  }
  if (!IsStreamQType(input_types[1])) {
    return absl::InvalidArgumentError("the second argument must be a stream");
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("the third argument must be a functor");
  }
  // The fourth argument is used for type inference and can be anything, so we
  // don't handle input_types[3] here.
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[4]));
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  return std::make_shared<StreamMapOp>(input_types, output_type);
}

namespace {

class StreamMapUnorderedOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.stream_map_unordered",
        [executor_slot = input_slots[0].UnsafeToSlot<ExecutorPtr>(),
         input_stream_slot = input_slots[1].UnsafeToSlot<StreamPtr>(),
         functor_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         return_value_qtype = output_slot.GetType()->value_qtype(),
         output_slot = output_slot.UnsafeToSlot<StreamPtr>()](
            arolla::EvaluationContext* /*ctx*/, arolla::FramePtr frame) {
          frame.Set(output_slot,
                    StreamMapUnordered(
                        frame.Get(executor_slot), frame.Get(input_stream_slot),
                        return_value_qtype,
                        MakeStreamMapFunctor(frame.Get(functor_slot),
                                             return_value_qtype)));
        });
  }
};

}  // namespace

// stream_map_unordered(
//     EXECUTOR, STREAM[S], DATA_SLICE, T, NON_DETERMINISTIC) -> STREAM[T]
absl::StatusOr<arolla::OperatorPtr>
StreamMapUnorderedOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 5) {
    return absl::InvalidArgumentError("requires exactly 5 arguments");
  }
  if (input_types[0] != arolla::GetQType<ExecutorPtr>()) {
    return absl::InvalidArgumentError("the first argument must be an executor");
  }
  if (!IsStreamQType(input_types[1])) {
    return absl::InvalidArgumentError("the second argument must be a stream");
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("the third argument must be a functor");
  }
  // The fourth argument is used for type inference and can be anything, so we
  // don't handle input_types[3] here.
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[4]));
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  return std::make_shared<StreamMapUnorderedOp>(input_types, output_type);
}

namespace {

class StreamReduceOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.stream_reduce",
        [executor_slot = input_slots[0].UnsafeToSlot<ExecutorPtr>(),
         functor_slot = input_slots[1].UnsafeToSlot<DataSlice>(),
         input_stream_slot = input_slots[2].UnsafeToSlot<StreamPtr>(),
         initial_value_slot = input_slots[3],
         output_slot = output_slot.UnsafeToSlot<StreamPtr>()](
            arolla::EvaluationContext* /*ctx*/, arolla::FramePtr frame) {
          auto functor =
              [functor_ds = frame.Get(functor_slot)](
                  arolla::TypedRef value,
                  arolla::TypedRef item) -> absl::StatusOr<arolla::TypedValue> {
            auto return_value =
                CallFunctorWithCompilationCache(functor_ds, {value, item}, {});
            if (return_value.ok() &&
                return_value->GetType() != value.GetType()) {
              return absl::InvalidArgumentError(absl::StrFormat(
                  "The functor was called with `%s` as the return "
                  "type, but the computation resulted in type `%s` "
                  "instead.",
                  value.GetType()->name(), return_value->GetType()->name()));
            }
            return return_value;
          };
          frame.Set(output_slot,
                    StreamReduce(
                        frame.Get(executor_slot),
                        arolla::TypedValue::FromSlot(initial_value_slot, frame),
                        frame.Get(input_stream_slot), std::move(functor)));
        });
  }
};

}  // namespace

// stream_reduce(
//     EXECUTOR, DATA_SLICE, STREAM[S], T, NON_DETERMINISTIC) -> STREAM[T]
absl::StatusOr<arolla::OperatorPtr> StreamReduceOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  if (input_types.size() != 5) {
    return absl::InvalidArgumentError("requires exactly 5 arguments");
  }
  if (input_types[0] != arolla::GetQType<ExecutorPtr>()) {
    return absl::InvalidArgumentError("the first argument must be an executor");
  }
  if (input_types[1] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("the second argument must be a functor");
  }
  if (!IsStreamQType(input_types[2])) {
    return absl::InvalidArgumentError("the third argument must be a stream");
  }
  if (input_types[3] != output_type->value_qtype()) {
    return absl::InvalidArgumentError(
        "the fourth argument must be the same as the value type of the output");
  }
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[4]));
  return std::make_shared<StreamReduceOp>(input_types, output_type);
}

namespace {

class StreamWhileReturnsOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.stream_while_returns",
        [executor_slot = input_slots[0].UnsafeToSlot<ExecutorPtr>(),
         condition_fn_slot = input_slots[1].UnsafeToSlot<DataSlice>(),
         body_fn_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         returns_slot = input_slots[3], state_slot = input_slots[4],
         output_slot = output_slot.UnsafeToSlot<StreamPtr>()](
            arolla::EvaluationContext* /*ctx*/,
            arolla::FramePtr frame) -> absl::Status {
          auto condition_fn = [condition_ds = frame.Get(condition_fn_slot)](
                                  absl::Span<const arolla::TypedRef> args,
                                  absl::Span<const std::string> kwnames)
              -> absl::StatusOr<arolla::TypedValue> {
            ASSIGN_OR_RETURN(
                auto result,
                CallFunctorWithCompilationCache(condition_ds, args, kwnames),
                _ << "error occurred while calling `condition_fn`");
            return result;
          };
          auto body_fn = [body_ds = frame.Get(body_fn_slot)](
                             absl::Span<const arolla::TypedRef> args,
                             absl::Span<const std::string> kwnames)
              -> absl::StatusOr<arolla::TypedValue> {
            ASSIGN_OR_RETURN(
                auto result,
                CallFunctorWithCompilationCache(body_ds, args, kwnames),
                _ << "error occurred while calling `body_fn`");
            return result;
          };
          ASSIGN_OR_RETURN(auto stream,
                           functor::parallel::StreamWhileReturns(
                               frame.Get(executor_slot),
                               std::move(condition_fn), std::move(body_fn),
                               arolla::TypedRef::FromSlot(returns_slot, frame),
                               arolla::TypedRef::FromSlot(state_slot, frame)));
          frame.Set(output_slot, std::move(stream));
          return absl::OkStatus();
        });
  }
};

}  // namespace

// _stream_while_returns(
//     executor: EXECUTOR,
//     condition_fn: DATA_SLICE,
//     body_fn: DATA_SLICE,
//     returns: T,
//     state: NAMEDTUPLE,
//     NON_DETERMINISTIC
// ) -> STREAM[T]
absl::StatusOr<arolla::OperatorPtr>
StreamWhileReturnsOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  if (input_types.size() != 6) {
    return absl::InvalidArgumentError("requires exactly 6 arguments");
  }
  if (input_types[0] != arolla::GetQType<ExecutorPtr>()) {
    return absl::InvalidArgumentError("1st argument must be an executor");
  }
  if (input_types[1] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("2nd argument must be a functor");
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("3rd argument must be a functor");
  }
  if (!arolla::IsNamedTupleQType(input_types[4])) {
    return absl::InvalidArgumentError("5th argument must be a namedtuple");
  }
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[5]));
  if (!IsStreamQType(output_type) ||
      output_type->value_qtype() != input_types[3]) {
    return absl::InvalidArgumentError(
        "output type must be a stream with the value type of the 4th argument");
  }
  return std::make_shared<StreamWhileReturnsOp>(input_types, output_type);
}

namespace {

class StreamWhileYieldsOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.stream_while_yields",
        [executor_slot = input_slots[0].UnsafeToSlot<ExecutorPtr>(),
         condition_fn_slot = input_slots[1].UnsafeToSlot<DataSlice>(),
         body_fn_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         yields_param_name_slot = input_slots[3].UnsafeToSlot<arolla::Text>(),
         yields_slot = input_slots[4], state_slot = input_slots[5],
         output_slot = output_slot.UnsafeToSlot<StreamPtr>()](
            arolla::EvaluationContext* /*ctx*/,
            arolla::FramePtr frame) -> absl::Status {
          auto condition_fn = [condition_ds = frame.Get(condition_fn_slot)](
                                  absl::Span<const arolla::TypedRef> args,
                                  absl::Span<const std::string> kwnames)
              -> absl::StatusOr<arolla::TypedValue> {
            ASSIGN_OR_RETURN(
                auto result,
                CallFunctorWithCompilationCache(condition_ds, args, kwnames),
                _ << "error occurred while calling `condition_fn`");
            return result;
          };
          auto body_fn = [body_ds = frame.Get(body_fn_slot)](
                             absl::Span<const arolla::TypedRef> args,
                             absl::Span<const std::string> kwnames)
              -> absl::StatusOr<arolla::TypedValue> {
            ASSIGN_OR_RETURN(
                auto result,
                CallFunctorWithCompilationCache(body_ds, args, kwnames),
                _ << "error occurred while calling `body_fn`");
            return result;
          };
          ASSIGN_OR_RETURN(
              auto stream,
              functor::parallel::StreamWhileYields(
                  frame.Get(executor_slot), std::move(condition_fn),
                  std::move(body_fn), frame.Get(yields_param_name_slot),
                  arolla::TypedRef::FromSlot(yields_slot, frame),
                  arolla::TypedRef::FromSlot(state_slot, frame)));
          frame.Set(output_slot, std::move(stream));
          return absl::OkStatus();
        });
  }
};

}  // namespace

// _stream_while_yields(
//     executor: EXECUTOR,
//     condition_fn: DATA_SLICE,
//     body_fn: DATA_SLICE,
//     yields_param_name: STRING,
//     yields: T,
//     state: NAMEDTUPLE,
//     NON_DETERMINISTIC
// ) -> STREAM[T]
absl::StatusOr<arolla::OperatorPtr>
StreamWhileYieldsOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  if (input_types.size() != 7) {
    return absl::InvalidArgumentError("requires exactly 7 arguments");
  }
  if (input_types[0] != arolla::GetQType<ExecutorPtr>()) {
    return absl::InvalidArgumentError("1st argument must be an executor");
  }
  if (input_types[1] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("2nd argument must be a functor");
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("3rd argument must be a functor");
  }
  if (input_types[3] != arolla::GetQType<arolla::Text>()) {
    return absl::InvalidArgumentError("4th argument must be a TEXT");
  }
  if (input_types[4] != output_type->value_qtype()) {
    return absl::InvalidArgumentError(
        "5th argument must be the same as the value type of the output");
  }
  if (!arolla::IsNamedTupleQType(input_types[5])) {
    return absl::InvalidArgumentError("6th argument must be a namedtuple");
  }
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[6]));
  return std::make_shared<StreamWhileYieldsOp>(input_types, output_type);
}

namespace {

class StreamForReturnsOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.stream_for_returns",
        [executor_slot = input_slots[0].UnsafeToSlot<ExecutorPtr>(),
         input_stream_slot = input_slots[1].UnsafeToSlot<StreamPtr>(),
         body_fn_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         finalize_fn_slot = input_slots[3], condition_fn_slot = input_slots[4],
         returns_slot = input_slots[5], state_slot = input_slots[6],
         output_slot = output_slot.UnsafeToSlot<StreamPtr>()](
            arolla::EvaluationContext* /*ctx*/,
            arolla::FramePtr frame) -> absl::Status {
          auto body_fn = [body_ds = frame.Get(body_fn_slot)](
                             absl::Span<const arolla::TypedRef> args,
                             absl::Span<const std::string> kwnames)
              -> absl::StatusOr<arolla::TypedValue> {
            ASSIGN_OR_RETURN(
                auto result,
                CallFunctorWithCompilationCache(body_ds, args, kwnames),
                _ << "error occurred while calling `body_fn`");
            return result;
          };
          StreamForFunctor finalize_functor;
          if (finalize_fn_slot.GetType() == arolla::GetQType<DataSlice>()) {
            finalize_functor =
                [finalize_ds =
                     frame.Get(finalize_fn_slot.UnsafeToSlot<DataSlice>())](
                    absl::Span<const arolla::TypedRef> args,
                    absl::Span<const std::string> kwnames)
                -> absl::StatusOr<arolla::TypedValue> {
              ASSIGN_OR_RETURN(
                  auto result,
                  CallFunctorWithCompilationCache(finalize_ds, args, kwnames),
                  _ << "error occurred while calling `finalize_fn`");
              return result;
            };
          }
          StreamForFunctor condition_functor;
          if (condition_fn_slot.GetType() == arolla::GetQType<DataSlice>()) {
            condition_functor =
                [condition_ds =
                     frame.Get(condition_fn_slot.UnsafeToSlot<DataSlice>())](
                    absl::Span<const arolla::TypedRef> args,
                    absl::Span<const std::string> kwnames)
                -> absl::StatusOr<arolla::TypedValue> {
              ASSIGN_OR_RETURN(
                  auto result,
                  CallFunctorWithCompilationCache(condition_ds, args, kwnames),
                  _ << "error occurred while calling `condition_fn`");
              return result;
            };
          }
          ASSIGN_OR_RETURN(
              auto stream,
              StreamForReturns(frame.Get(executor_slot),
                               frame.Get(input_stream_slot), std::move(body_fn),
                               std::move(finalize_functor),
                               std::move(condition_functor),
                               arolla::TypedRef::FromSlot(returns_slot, frame),
                               arolla::TypedRef::FromSlot(state_slot, frame)));
          frame.Set(output_slot, std::move(stream));
          return absl::OkStatus();
        });
  }
};

}  // namespace

// _stream_for_returns(
//     executor: EXECUTOR,
//     stream: STREAM[S],
//     body_fn: DATA_SLICE,
//     finalize_fn: DATA_SLICE | UNSPECIFIED,
//     condition_fn: DATA_SLICE | UNSPECIFIED,
//     initial_returns: T,
//     initial_state: NAMEDTUPLE,
//     NON_DETERMINISTIC
// ) -> STREAM[T]
absl::StatusOr<arolla::OperatorPtr>
StreamForReturnsOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  if (input_types.size() != 8) {
    return absl::InvalidArgumentError("requires exactly 8 arguments");
  }
  if (input_types[0] != arolla::GetQType<ExecutorPtr>()) {
    return absl::InvalidArgumentError("1st argument must be an executor");
  }
  if (!IsStreamQType(input_types[1])) {
    return absl::InvalidArgumentError("2nd argument must be a stream");
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("3rd argument must be a functor");
  }
  if (input_types[3] != arolla::GetQType<DataSlice>() &&
      input_types[3] != arolla::GetUnspecifiedQType()) {
    return absl::InvalidArgumentError(
        "4th argument must be a functor or unspecified");
  }
  if (input_types[4] != arolla::GetQType<DataSlice>() &&
      input_types[4] != arolla::GetUnspecifiedQType()) {
    return absl::InvalidArgumentError(
        "5th argument must be a functor or unspecified");
  }
  if (input_types[5] != output_type->value_qtype()) {
    return absl::InvalidArgumentError(
        "6th argument must be the same as the value type of the output");
  }
  if (!arolla::IsNamedTupleQType(input_types[6])) {
    return absl::InvalidArgumentError("7th argument must be a namedtuple");
  }
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[7]));
  return std::make_shared<StreamForReturnsOp>(input_types, output_type);
}

namespace {

class StreamForYieldsOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.stream_for_yields",
        [executor_slot = input_slots[0].UnsafeToSlot<ExecutorPtr>(),
         input_stream_slot = input_slots[1].UnsafeToSlot<StreamPtr>(),
         body_fn_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         finalize_fn_slot = input_slots[3], condition_fn_slot = input_slots[4],
         yields_param_name_slot = input_slots[5].UnsafeToSlot<arolla::Text>(),
         yields_slot = input_slots[6], state_slot = input_slots[7],
         output_slot = output_slot.UnsafeToSlot<StreamPtr>()](
            arolla::EvaluationContext* /*ctx*/,
            arolla::FramePtr frame) -> absl::Status {
          auto body_fn = [body_ds = frame.Get(body_fn_slot)](
                             absl::Span<const arolla::TypedRef> args,
                             absl::Span<const std::string> kwnames)
              -> absl::StatusOr<arolla::TypedValue> {
            ASSIGN_OR_RETURN(
                auto result,
                CallFunctorWithCompilationCache(body_ds, args, kwnames),
                _ << "error occurred while calling `body_fn`");
            return result;
          };
          StreamForFunctor finalize_functor;
          if (finalize_fn_slot.GetType() == arolla::GetQType<DataSlice>()) {
            finalize_functor =
                [finalize_ds =
                     frame.Get(finalize_fn_slot.UnsafeToSlot<DataSlice>())](
                    absl::Span<const arolla::TypedRef> args,
                    absl::Span<const std::string> kwnames)
                -> absl::StatusOr<arolla::TypedValue> {
              ASSIGN_OR_RETURN(
                  auto result,
                  CallFunctorWithCompilationCache(finalize_ds, args, kwnames),
                  _ << "error occurred while calling `finalize_fn`");
              return result;
            };
          }
          StreamForFunctor condition_functor;
          if (condition_fn_slot.GetType() == arolla::GetQType<DataSlice>()) {
            condition_functor =
                [condition_ds =
                     frame.Get(condition_fn_slot.UnsafeToSlot<DataSlice>())](
                    absl::Span<const arolla::TypedRef> args,
                    absl::Span<const std::string> kwnames)
                -> absl::StatusOr<arolla::TypedValue> {
              ASSIGN_OR_RETURN(
                  auto result,
                  CallFunctorWithCompilationCache(condition_ds, args, kwnames),
                  _ << "error occurred while calling `condition_fn`");
              return result;
            };
          }
          ASSIGN_OR_RETURN(
              auto stream,
              StreamForYields(frame.Get(executor_slot),
                              frame.Get(input_stream_slot), std::move(body_fn),
                              std::move(finalize_functor),
                              std::move(condition_functor),
                              frame.Get(yields_param_name_slot).view(),
                              arolla::TypedRef::FromSlot(yields_slot, frame),
                              arolla::TypedRef::FromSlot(state_slot, frame)));
          frame.Set(output_slot, std::move(stream));
          return absl::OkStatus();
        });
  }
};

}  // namespace

// _stream_for_yields(
//     executor: EXECUTOR,
//     stream: STREAM[S],
//     body_fn: DATA_SLICE,
//     finalize_fn: DATA_SLICE | UNSPECIFIED,
//     condition_fn: DATA_SLICE | UNSPECIFIED,
//     yields_param_name: STRING,
//     initial_yields: T,
//     initial_state: NAMEDTUPLE,
//     NON_DETERMINISTIC
// ) -> STREAM[T]
absl::StatusOr<arolla::OperatorPtr>
StreamForYieldsOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  if (input_types.size() != 9) {
    return absl::InvalidArgumentError("requires exactly 9 arguments");
  }
  if (input_types[0] != arolla::GetQType<ExecutorPtr>()) {
    return absl::InvalidArgumentError("1st argument must be an executor");
  }
  if (!IsStreamQType(input_types[1])) {
    return absl::InvalidArgumentError("2nd argument must be a stream");
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("3rd argument must be a functor");
  }
  if (input_types[3] != arolla::GetQType<DataSlice>() &&
      input_types[3] != arolla::GetUnspecifiedQType()) {
    return absl::InvalidArgumentError(
        "4th argument must be a functor or unspecified");
  }
  if (input_types[4] != arolla::GetQType<DataSlice>() &&
      input_types[4] != arolla::GetUnspecifiedQType()) {
    return absl::InvalidArgumentError(
        "5th argument must be a functor or unspecified");
  }
  if (input_types[5] != arolla::GetQType<arolla::Text>()) {
    return absl::InvalidArgumentError("6th argument must be a TEXT");
  }
  if (input_types[6] != output_type->value_qtype()) {
    return absl::InvalidArgumentError(
        "6th argument must be the same as the value type of the output");
  }
  if (!arolla::IsNamedTupleQType(input_types[7])) {
    return absl::InvalidArgumentError("7th argument must be a namedtuple");
  }
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[8]));
  return std::make_shared<StreamForYieldsOp>(input_types, output_type);
}

namespace {

class StreamCallOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.stream_call",
        [executor_slot = input_slots[0].UnsafeToSlot<ExecutorPtr>(),
         fn_slot = input_slots[1].UnsafeToSlot<DataSlice>(),
         args_slot = input_slots[2], kwargs_slot = input_slots[4],
         return_type = input_slots[3].GetType(),
         output_slot](arolla::EvaluationContext* /*ctx*/,
                      arolla::FramePtr frame) -> absl::Status {
          auto fn = [body_ds = frame.Get(fn_slot),
                     kwargs_qtype = kwargs_slot.GetType(),
                     output_type = output_slot.GetType(),
                     return_type](absl::Span<const arolla::TypedRef> args)
              -> absl::StatusOr<arolla::TypedValue> {
            ASSIGN_OR_RETURN(
                auto result,
                CallFunctorWithCompilationCache(
                    body_ds, args, arolla::GetFieldNames(kwargs_qtype)),
                _ << "error occurred while calling `fn`");
            if (result.GetType() != output_type &&
                result.GetType() != return_type) {
              return absl::InvalidArgumentError(absl::StrFormat(
                  "the functor was called with `%s` as the return type, but the"
                  " computation resulted in type `%s` instead; you can specify"
                  " the expected return type via the `return_type_as=`"
                  " parameter to the functor call",
                  return_type->name(), result.GetType()->name()));
            }
            return result;
          };
          const auto args = arolla::TypedRef::FromSlot(args_slot, frame);
          const auto kwargs = arolla::TypedRef::FromSlot(kwargs_slot, frame);
          std::vector<arolla::TypedRef> args_vector;
          args_vector.reserve(args.GetFieldCount() + kwargs.GetFieldCount());
          for (int64_t i = 0; i < args.GetFieldCount(); ++i) {
            args_vector.push_back(args.GetField(i));
          }
          for (int64_t i = 0; i < kwargs.GetFieldCount(); ++i) {
            args_vector.push_back(kwargs.GetField(i));
          }
          ASSIGN_OR_RETURN(
              auto stream,
              StreamCall(frame.Get(executor_slot), std::move(fn),
                         output_slot.GetType()->value_qtype(), args_vector));
          frame.Set(output_slot.UnsafeToSlot<StreamPtr>(), std::move(stream));
          return absl::OkStatus();
        });
  }
};

}  // namespace

// stream_call(
//     executor: EXECUTOR,
//     fn: DATA_SLICE,
//     args: TUPLE[...],
//     return_value_type: T,
//     kwargs: NAMEDTUPLE[...],
//     NON_DETERMINISTIC
// ) -> STREAM[T]
absl::StatusOr<arolla::OperatorPtr> StreamCallOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a stream");
  }
  if (input_types.size() != 6) {
    return absl::InvalidArgumentError("requires exactly 6 arguments");
  }
  if (input_types[0] != arolla::GetQType<ExecutorPtr>()) {
    return absl::InvalidArgumentError("1st argument must be an executor");
  }
  if (input_types[1] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("2nd argument must be a functor");
  }
  if (!arolla::IsTupleQType(input_types[2])) {
    return absl::InvalidArgumentError("3rd argument must be a tuple");
  }
  if (input_types[3] != output_type &&
      input_types[3] != output_type->value_qtype()) {
    return absl::InvalidArgumentError(
        "4th argument must be of the same type as the output type or the value "
        "type of the output");
  }
  if (!arolla::IsNamedTupleQType(input_types[4])) {
    return absl::InvalidArgumentError("5th argument must be a namedtuple");
  }
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[5]));
  return std::make_shared<StreamCallOp>(input_types, output_type);
}

namespace {

class UnsafeBlockingAwaitOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "koda_internal.parallel.unsafe_blocking_await",
        [input_stream_slot = input_slots[0].UnsafeToSlot<StreamPtr>(),
         output_slot](arolla::EvaluationContext* /*ctx*/,
                      arolla::FramePtr frame) -> absl::Status {
          const auto& stream = frame.Get(input_stream_slot);
          auto reader = stream->MakeReader();
          auto unsafe_blocking_read =
              [&reader]() -> StreamReader::TryReadResult {
            if (auto try_read_result = reader->TryRead();
                !try_read_result.empty()) {
              return try_read_result;
            }
            auto ready = arolla::PermanentEvent::Make();
            arolla::CancellationContext::Subscription subscription;
            if (auto cancellation_context =
                    arolla::CurrentCancellationContext();
                cancellation_context != nullptr) {
              subscription =
                  cancellation_context->Subscribe([ready] { ready->Notify(); });
            }
            reader->SubscribeOnce([ready] { ready->Notify(); });
            ready->Wait();
            return reader->TryRead();
          };
          {  // Expect an item.
            auto try_read_result = unsafe_blocking_read();
            if (auto* item = try_read_result.item()) {
              RETURN_IF_ERROR(item->CopyToSlot(output_slot, frame));  // OK
            } else if (auto* status = try_read_result.close_status()) {
              if (!status->ok()) {
                return std::move(*status);
              }
              return absl::InvalidArgumentError(
                  "expected a stream with a single item, got an empty stream");
            } else {
              return arolla::CheckCancellation();
            }
          }
          {  // Expect no more items.
            auto try_read_result = unsafe_blocking_read();
            if (try_read_result.item() != nullptr) {
              return absl::InvalidArgumentError(
                  "expected a stream with a single item, got a stream with "
                  "multiple items");
            } else if (auto* status = try_read_result.close_status()) {
              if (!status->ok()) {
                return std::move(*status);
              }
              return absl::OkStatus();
            } else {
              return arolla::CheckCancellation();
            }
          }
        });
  }
};

}  // namespace

// stream_unsafe_blocking_await(stream: STREAM) -> STREAM
absl::StatusOr<arolla::OperatorPtr>
UnsafeBlockingAwaitOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("requires exactly 1 argument");
  }
  if (!IsStreamQType(input_types[0])) {
    return absl::InvalidArgumentError("first argument must be a stream");
  }
  if (input_types[0]->value_qtype() != output_type) {
    return absl::InvalidArgumentError(
        "output type must be the same as the value type of the input stream");
  }
  return std::make_shared<UnsafeBlockingAwaitOp>(input_types, output_type);
}

}  // namespace koladata::functor::parallel
