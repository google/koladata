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
#include "koladata/functor/parallel/expr_operators.h"

#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/future_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {

AsyncEvalOp::AsyncEvalOp()
    : arolla::expr::ExprOperatorWithFixedSignature(
          "koda_internal.parallel.async_eval",
          arolla::expr::ExprOperatorSignature{
              {"executor"},
              {"op"},
              {.name = "inputs",
               .kind = arolla::expr::ExprOperatorSignature::Parameter::Kind::
                   kVariadicPositional}},
          "Evaluates the given operator with the given inputs "
          "asynchronously.\n"
          "\n"
          "This operator is intended for internal use only, since it works"
          " with an operator concept which is not exposed in the public API."
          " We can add a user-facing async_call later if needed.",
          arolla::FingerprintHasher(
              "::koladata::functor::parallel::AsyncEvalOp")
              .Finish()) {}

absl::StatusOr<arolla::expr::ExprAttributes> AsyncEvalOp::InferAttributes(
    absl::Span<const arolla::expr::ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  if (inputs[0].qtype() &&
      inputs[0].qtype() != arolla::GetQType<ExecutorPtr>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected executor, got %s", inputs[0].qtype()->name()));
  }
  if (inputs[1].qtype() &&
      inputs[1].qtype() != arolla::GetQType<arolla::expr::ExprOperatorPtr>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected inner operator, got %s", inputs[1].qtype()->name()));
  }
  if (inputs[1].qtype() && !inputs[1].qvalue()) {
    return absl::InvalidArgumentError("inner operator must be a literal");
  }
  if (!arolla::expr::HasAllAttrQTypes(inputs)) {
    return arolla::expr::ExprAttributes{};
  }
  const auto& op =
      inputs[1].qvalue()->UnsafeAs<arolla::expr::ExprOperatorPtr>();
  std::vector<arolla::expr::ExprAttributes> op_inputs;
  op_inputs.reserve(inputs.size() - 2);
  for (int i = 2; i < inputs.size(); ++i) {
    arolla::QTypePtr tpe = inputs[i].qtype();
    if (IsFutureQType(tpe)) {
      if (IsFutureQType(tpe->value_qtype())) {
        return absl::InvalidArgumentError(
            "futures to futures are not supported in async eval");
      }
      op_inputs.push_back(arolla::expr::ExprAttributes(tpe->value_qtype()));
    } else {
      op_inputs.push_back(inputs[i]);
    }
  }
  ASSIGN_OR_RETURN(auto attrs, op->InferAttributes(op_inputs));
  if (!attrs.qtype()) {
    return absl::InvalidArgumentError(
        "failed to infer attributes for the async operator");
  }
  return arolla::expr::ExprAttributes(GetFutureQType(attrs.qtype()));
}

}  // namespace koladata::functor::parallel
