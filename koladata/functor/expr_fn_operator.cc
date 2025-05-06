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
#include "koladata/functor/expr_fn_operator.h"

#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/auto_variables.h"
#include "koladata/functor/functor.h"
#include "koladata/internal/op_utils/qexpr.h"
#include "koladata/operators/utils.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

namespace {

absl::StatusOr<DataSlice> CreateExprFn(
    const DataSlice& returns, std::vector<absl::string_view> variable_names,
    std::vector<DataSlice> variable_values, const DataSlice& signature,
    const DataSlice& auto_variables) {
  ASSIGN_OR_RETURN(bool auto_variables_value,
                   ops::GetBoolArgument(auto_variables, "auto_variables"));
  ASSIGN_OR_RETURN(
      auto result,
      functor::CreateFunctor(returns, signature, std::move(variable_names),
                             std::move(variable_values)));
  if (auto_variables_value) {
    ASSIGN_OR_RETURN(result, functor::AutoVariables(result));
  }
  return result;
}

class ExprFnOperator : public arolla::QExprOperator {
 public:
  explicit ExprFnOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(input_types, arolla::GetQType<DataSlice>()) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "kd.functor.expr_fn",
        [returns_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         signature_slot = input_slots[1].UnsafeToSlot<DataSlice>(),
         auto_variables_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         vars_slot = input_slots[3],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          const auto& returns = frame.Get(returns_slot);
          const auto& signature = frame.Get(signature_slot);
          const auto& auto_variables = frame.Get(auto_variables_slot);
          std::vector<absl::string_view> var_names =
              ops::GetFieldNames(vars_slot);
          std::vector<DataSlice> var_values =
              ops::GetValueDataSlices(vars_slot, frame);
          ASSIGN_OR_RETURN(
              auto result,
              CreateExprFn(returns, std::move(var_names), std::move(var_values),
                           signature, auto_variables));
          frame.Set(output_slot, std::move(result));
          return absl::OkStatus();
        });
  }
};

}  // namespace

// kd.functor.expr_fn.
absl::StatusOr<arolla::OperatorPtr> ExprFnOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 5) {
    return absl::InvalidArgumentError("requires exactly 5 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be DataSlice");
  }
  if (input_types[1] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires second argument to be DataSlice");
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires third argument to be DataSlice");
  }
  RETURN_IF_ERROR(ops::VerifyNamedTuple(input_types[3]));
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[4]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<ExprFnOperator>(input_types), input_types, output_type);
}

}  // namespace koladata::functor
