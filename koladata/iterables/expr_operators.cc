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
#include "koladata/iterables/expr_operators.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/fingerprint.h"
#include "koladata/iterables/iterable_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::iterables {

GetIterableQTypeOp::GetIterableQTypeOp()
    : arolla::expr::ExprOperatorWithFixedSignature(
          "koda_internal.iterables.get_iterable_qtype",
          arolla::expr::ExprOperatorSignature{{"x"}},
          "Gets the iterable qtype for the given value qtype.",
          arolla::FingerprintHasher("::koladata::iterables::GetIterableQTypeOp")
              .Finish()) {}

absl::StatusOr<arolla::expr::ExprAttributes>
GetIterableQTypeOp::InferAttributes(
    absl::Span<const arolla::expr::ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  if (!inputs[0].qtype()) {
    return arolla::expr::ExprAttributes{};
  }
  if (inputs[0].qtype() != arolla::GetQTypeQType()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected x: QTYPE, got %s", inputs[0].qtype()->name()));
  }
  if (!inputs[0].qvalue()) {
    return arolla::expr::ExprAttributes(arolla::GetQTypeQType());
  }
  ASSIGN_OR_RETURN(arolla::QTypePtr x,
                   inputs[0].qvalue()->As<arolla::QTypePtr>());
  return arolla::expr::ExprAttributes(
      arolla::TypedRef::FromValue(GetIterableQType(x)));
}

}  // namespace koladata::iterables
