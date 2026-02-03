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
#include "koladata/iterables/reduce_operators.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/sequence/sequence.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/op_utils/qexpr.h"
#include "koladata/iterables/iterable_qtype.h"
#include "koladata/operators/slices.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::iterables {
namespace {

class ReduceConcatOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "kd.iterables.reduce_concat",
        [seq_input_slot = input_slots[0].UnsafeToSlot<arolla::Sequence>(),
         initial_value_slot = input_slots[1].UnsafeToSlot<DataSlice>(),
         ndim_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          const auto& initial_value = frame.Get(initial_value_slot);
          const auto& ndim = frame.Get(ndim_slot);
          const arolla::Sequence& seq = frame.Get(seq_input_slot);
          DataSlice is_stack = DataSlice::CreatePrimitive(false);
          std::vector<const DataSlice*> args;
          args.reserve(3 + seq.size());
          args.push_back(&is_stack);
          args.push_back(&ndim);
          args.push_back(&initial_value);
          for (const DataSlice& slice : seq.UnsafeSpan<DataSlice>()) {
            args.push_back(&slice);
          }
          ASSIGN_OR_RETURN(auto result, ops::ConcatOrStack(args));
          frame.Set(output_slot, std::move(result));
          return absl::OkStatus();
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> ReduceConcatOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 3) {
    return absl::InvalidArgumentError("requires exactly 3 arguments");
  }
  if (!iterables::IsIterableQType(input_types[0])) {
    return absl::InvalidArgumentError(
        "requires first argument to be an iterable");
  }
  if (input_types[0]->value_qtype() != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be an iterable of DataSlices");
  }
  if (input_types[1] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires second argument to be DataSlice");
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires third argument to be DataSlice");
  }
  if (output_type != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("requires output type to be DataSlice");
  }

  return std::make_shared<ReduceConcatOperator>(input_types, output_type);
}

namespace {

class ReduceUpdatedBagOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "kd.iterables.reduce_updated_bag",
        [seq_input_slot = input_slots[0].UnsafeToSlot<arolla::Sequence>(),
         initial_value_slot = input_slots[1].UnsafeToSlot<DataBagPtr>(),
         output_slot = output_slot.UnsafeToSlot<DataBagPtr>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          const auto& initial_value = frame.Get(initial_value_slot);
          const arolla::Sequence& seq = frame.Get(seq_input_slot);
          std::vector<DataBagPtr> args;
          args.reserve(1 + seq.size());
          args.push_back(initial_value);
          for (const DataBagPtr& bag : seq.UnsafeSpan<DataBagPtr>()) {
            if (bag != nullptr) {
              args.push_back(bag->Freeze());
            }
          }
          std::reverse(args.begin(), args.end());
          frame.Set(output_slot, *DataBag::ImmutableEmptyWithFallbacks(args));
          return absl::OkStatus();
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr>
ReduceUpdatedBagOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 2) {
    return absl::InvalidArgumentError("requires exactly 2 arguments");
  }
  if (!iterables::IsIterableQType(input_types[0])) {
    return absl::InvalidArgumentError(
        "requires first argument to be an iterable");
  }
  if (input_types[0]->value_qtype() != arolla::GetQType<DataBagPtr>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be an iterable of DataBags");
  }
  if (input_types[1] != arolla::GetQType<DataBagPtr>()) {
    return absl::InvalidArgumentError("requires second argument to be DataBag");
  }
  if (output_type != arolla::GetQType<DataBagPtr>()) {
    return absl::InvalidArgumentError("requires output type to be DataBag");
  }

  return std::make_shared<ReduceUpdatedBagOperator>(input_types, output_type);
}

}  // namespace koladata::iterables
