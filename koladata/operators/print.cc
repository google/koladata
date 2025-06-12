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
#include "koladata/operators/print.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/op_utils/print.h"
#include "koladata/internal/op_utils/qexpr.h"
#include "koladata/operators/utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {

class WithPrintOperator final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    std::vector<arolla::FrameLayout::Slot<DataSlice>> slots_to_print;
    auto sep_slot = input_slots[2].UnsafeToSlot<DataSlice>();
    auto end_slot = input_slots[3].UnsafeToSlot<DataSlice>();
    slots_to_print.reserve(input_slots[1].SubSlotCount() * 2 + 1);
    for (int64_t i = 0; i < input_slots[1].SubSlotCount(); ++i) {
      if (!slots_to_print.empty()) {
        slots_to_print.push_back(sep_slot);
      }
      slots_to_print.push_back(
          input_slots[1].SubSlot(i).UnsafeToSlot<DataSlice>());
    }
    slots_to_print.push_back(end_slot);
    return MakeBoundOperator(
        "kd.core.with_print",
        [input_slot = input_slots[0],
         slots_to_print = std::move(slots_to_print),
         output_slot = output_slot](arolla::EvaluationContext*,
                                    arolla::FramePtr frame) -> absl::Status {
          std::string message;
          for (const auto& slot : slots_to_print) {
            ASSIGN_OR_RETURN(auto str, DataSliceToStr(frame.Get(slot),
                                                      {.strip_quotes = true}));
            absl::StrAppend(&message, str);
          }
          ::koladata::internal::GetSharedPrinter().Print(message);
          input_slot.CopyTo(frame, output_slot, frame);
          return absl::OkStatus();
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> WithPrintOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 5) {
    return absl::InvalidArgumentError("requires 5 arguments");
  }
  if (!arolla::IsTupleQType(input_types[1])) {
    return absl::InvalidArgumentError(
        absl::StrCat("*args must be a tuple, got ", input_types[1]->name()));
  }
  for (size_t i = 0; i < input_types[1]->type_fields().size(); ++i) {
    if (input_types[1]->type_fields()[i].GetType() !=
        arolla::GetQType<DataSlice>()) {
      return absl::InvalidArgumentError(
          "requires `*args` arguments to be DataSlices");
    }
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("requires `sep` to be a DataSlice");
  }
  if (input_types[3] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("requires `end` to be a DataSlice");
  }
  RETURN_IF_ERROR(VerifyIsNonDeterministicToken(input_types[4]));
  return std::make_shared<WithPrintOperator>(input_types, output_type);
}

}  // namespace koladata::ops
