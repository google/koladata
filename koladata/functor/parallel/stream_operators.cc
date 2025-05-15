// Copyright 2024 Google LLC
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
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/sequence.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/call.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_composition.h"
#include "koladata/functor/parallel/stream_map.h"
#include "koladata/functor/parallel/stream_qtype.h"
#include "koladata/iterables/iterable_qtype.h"
#include "koladata/operators/utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
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
    return arolla::MakeBoundOperator(
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
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<StreamChainOp>(input_types, output_type), input_types,
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
    return arolla::MakeBoundOperator(
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
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<StreamInterleaveOp>(input_types, output_type),
      input_types, output_type);
}

namespace {

class StreamMakeOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
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
    return arolla::MakeBoundOperator(
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
    return arolla::MakeBoundOperator(
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

}  // namespace koladata::functor::parallel
