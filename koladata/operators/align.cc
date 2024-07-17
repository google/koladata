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
#include "koladata/operators/align.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {

class AlignOperator : public arolla::QExprOperator {
 public:
  explicit AlignOperator(absl::Span<const arolla::QTypePtr> input_types)
      : arolla::QExprOperator(
            "kde.core.align",
            arolla::QExprOperatorSignature::Get(
                input_types, arolla::MakeTupleQType(input_types))) {}

 private:
  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final override {
    DCHECK_EQ(input_slots.size(), output_slot.SubSlotCount());
    std::vector<arolla::FrameLayout::Slot<DataSlice>> ds_input_slots;
    ds_input_slots.reserve(input_slots.size());
    for (const auto& input_slot : input_slots) {
      ds_input_slots.push_back(input_slot.UnsafeToSlot<DataSlice>());
    }
    return arolla::MakeBoundOperator(
        [ds_input_slots(std::move(ds_input_slots)), output_slot = output_slot](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          std::optional<DataSlice::JaggedShape> largest_shape;
          for (const auto& input_slot : ds_input_slots) {
            const DataSlice& input = frame.Get(input_slot);
            if (!largest_shape.has_value() ||
                input.GetShape().rank() > largest_shape->rank()) {
              largest_shape = input.GetShape();
            }
          }

          for (size_t i = 0; i < ds_input_slots.size(); ++i) {
            const auto& input_slot = ds_input_slots[i];
            const DataSlice& input = frame.Get(input_slot);
            ASSIGN_OR_RETURN(DataSlice output,
                             BroadcastToShape(input, largest_shape.value()),
                             ctx->set_status(std::move(_)));
            const auto& output_subslot =
                output_slot.SubSlot(i).UnsafeToSlot<DataSlice>();
            frame.Set(output_subslot, std::move(output));
          }
        });
  }
};

}  // namespace

// kde.core.align
absl::StatusOr<arolla::OperatorPtr> AlignOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  for (const auto& args_type : input_types) {
    if (args_type != arolla::GetQType<DataSlice>()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "arguments must be DataSlices, but got ", args_type->name()));
    }
  }

  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<AlignOperator>(input_types), input_types, output_type);
}

}  // namespace koladata::ops
