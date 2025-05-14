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
#include "koladata/functor/map_operator.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/map.h"
#include "koladata/operators/utils.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {
namespace {

class MapOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [fn_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         args_slot = input_slots[1],
         include_missing_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         kwargs_slot = input_slots[3],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          const auto& fn_data_slice = frame.Get(fn_slot);
          ASSIGN_OR_RETURN(bool include_missing,
                           ops::GetBoolArgument(frame.Get(include_missing_slot),
                                                "include_missing"),
                           ctx->set_status(std::move(_)));

          std::vector<DataSlice> args;
          args.reserve(args_slot.SubSlotCount() + kwargs_slot.SubSlotCount());
          for (int64_t i = 0; i < args_slot.SubSlotCount(); ++i) {
            args.push_back(
                frame.Get(args_slot.SubSlot(i).UnsafeToSlot<DataSlice>()));
          }

          auto kwargs_qtype = kwargs_slot.GetType();
          auto kwarg_names = arolla::GetFieldNames(kwargs_qtype);
          for (int64_t i = 0; i < kwargs_slot.SubSlotCount(); ++i) {
            args.push_back(
                frame.Get(kwargs_slot.SubSlot(i).UnsafeToSlot<DataSlice>()));
          }
          ASSIGN_OR_RETURN(
              auto result,
              functor::MapFunctorWithCompilationCache(
                  fn_data_slice, std::move(args), kwarg_names, include_missing),
              ctx->set_status(std::move(_)));
          frame.Set(output_slot, std::move(result));
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> MapOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 5) {
    return absl::InvalidArgumentError("requires exactly 5 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires functor argument to be DataSlice");
  }
  if (!arolla::IsTupleQType(input_types[1])) {
    return absl::InvalidArgumentError("requires args argument to be Tuple");
  }
  for (const auto& sub_field : input_types[1]->type_fields()) {
    if (sub_field.GetType() != arolla::GetQType<DataSlice>()) {
      return absl::InvalidArgumentError(
          "requires all values of the args argument to be DataSlices");
    }
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires include_missing argument to be DataSlice");
  }
  if (!arolla::IsNamedTupleQType(input_types[3])) {
    return absl::InvalidArgumentError(
        "requires kwargs argument to be NamedTuple");
  }
  for (const auto& sub_field : input_types[3]->type_fields()) {
    if (sub_field.GetType() != arolla::GetQType<DataSlice>()) {
      return absl::InvalidArgumentError(
          "requires all values of the kwargs argument to be DataSlices");
    }
  }
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[4]));
  if (output_type != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("requires output type to be DataSlice");
  }
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<MapOperator>(input_types, output_type), input_types,
      output_type);
}

}  // namespace koladata::functor
