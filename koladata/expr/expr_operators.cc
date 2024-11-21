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
#include "koladata/expr/expr_operators.h"

#include <cstdint>
#include <memory>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"  // IWYU pragma: keep
#include "koladata/internal/ellipsis.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/string.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::expr {
namespace {

absl::Status ValidateTextLiteral(const arolla::expr::ExprAttributes& attr,
                                 absl::string_view param_name) {
  if (attr.qtype() != arolla::GetQType<arolla::Text>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected %s to be a %s, got %s", param_name,
                        arolla::GetQType<arolla::Text>()->name(),
                        attr.qtype() ? attr.qtype()->name() : "nullptr"));
  }
  if (!attr.qvalue()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected %s to be a literal", param_name));
  }
  return absl::OkStatus();
}

}  // namespace

InputOperator::InputOperator()
    : arolla::expr::ExprOperatorWithFixedSignature(
          "koda_internal.input",
          arolla::expr::ExprOperatorSignature{{"container_name"},
                                              {"input_key"}},
          "Koda input with DATA_SLICE qtype.\n"
          "\n"
          "Note that this operator cannot be evaluated.\n"
          "\n"
          "Args:\n"
          "  container_name: name of the container the input belongs to.\n"
          "  input_key: name of the input representing a DATA_SLICE value.",
          arolla::FingerprintHasher("::koladata::expr::InputOperator")
              .Finish()) {}

absl::StatusOr<arolla::expr::ExprAttributes> InputOperator::InferAttributes(
    absl::Span<const arolla::expr::ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  RETURN_IF_ERROR(ValidateTextLiteral(inputs[0], "container_name"));
  RETURN_IF_ERROR(ValidateTextLiteral(inputs[1], "input_key"));
  if (!arolla::IsIdentifier(
          inputs[0].qvalue().value().UnsafeAs<arolla::Text>().view())) {
    return absl::InvalidArgumentError(
        "expected container_name to be an identifier");
  }
  return arolla::expr::ExprAttributes{};
}

LiteralOperator::LiteralOperator(arolla::TypedValue value)
    : arolla::expr::ExprOperatorWithFixedSignature(
          "koda_internal.literal", arolla::expr::ExprOperatorSignature{},
          "Koda literal.",
          arolla::FingerprintHasher("::koladata::expr::LiteralOperator")
              .Combine(value.GetFingerprint())
              .Finish()),
      value_(std::move(value)) {}

absl::StatusOr<arolla::expr::ExprAttributes> LiteralOperator::InferAttributes(
    absl::Span<const arolla::expr::ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  return arolla::expr::ExprAttributes{value_};
}

absl::string_view LiteralOperator::py_qvalue_specialization_key() const {
  return "::koladata::expr::LiteralOperator";
}

const arolla::TypedValue& LiteralOperator::value() const { return value_; }

ToArollaInt64Operator::ToArollaInt64Operator()
    : arolla::expr::ExprOperatorWithFixedSignature(
          "koda_internal.to_arolla_int64",
          arolla::expr::ExprOperatorSignature({{"x"}},
                                              "koladata_default_boxing"),
          "Returns `x` converted into an arolla int64 value.\n"
          "\n"
          "Note that `x` must adhere to the following requirements:\n"
          "* `rank = 0`.\n"
          "* Have one of the following schemas: NONE, INT32, INT64, OBJECT, "
          "ANY.\n"
          "* Have a present value with type INT32 or INT64.\n"
          "\n"
          "In all other cases, an exception is raised.\n\n"
          "Args:\n"
          "  x: A DataItem to be converted into an arolla int64 value.",
          arolla::FingerprintHasher("::koladata::expr::ToArollaInt64Operator")
              .Finish()) {}

absl::StatusOr<arolla::expr::ExprAttributes>
ToArollaInt64Operator::InferAttributes(
    absl::Span<const arolla::expr::ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  if (!inputs[0].qtype()) {
    return arolla::expr::ExprAttributes{};  // Not ready yet.
  }
  if (inputs[0].qtype() != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected DATA_SLICE, got x: %s", inputs[0].qtype()->name()));
  }
  // Eval if possible.
  if (inputs[0].qvalue()) {
    ASSIGN_OR_RETURN(auto casted_value,
                     arolla::InvokeOperator("koda_internal.to_arolla_int64",
                                            {*inputs[0].qvalue()},
                                            arolla::GetQType<int64_t>()));
    return arolla::expr::ExprAttributes{std::move(casted_value)};
  }
  // Otherwise, return the output qtype.
  return arolla::expr::ExprAttributes{arolla::GetQType<int64_t>()};
}

// koladata_default_boxing

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kOperators},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(arolla::expr::RegisterOperator<InputOperator>(
                              "koda_internal.input")
                              .status());
          RETURN_IF_ERROR(
              RegisterOperator("koda_internal.ellipsis",
                               arolla::expr::MakeLambdaOperator(
                                   arolla::expr::ExprOperatorSignature{},
                                   arolla::expr::Literal(internal::Ellipsis{})))
                  .status());
          RETURN_IF_ERROR(
              arolla::expr::RegisterOperator(
                  "koda_internal.with_name",
                  std::make_shared<arolla::expr::NameAnnotation>(
                      /*aux_policy=*/"_koladata_annotation_with_name"))
                  .status());
          RETURN_IF_ERROR(arolla::expr::RegisterOperator(
                              "koda_internal.to_arolla_int64",
                              std::make_shared<ToArollaInt64Operator>())
                              .status());
          return absl::OkStatus();
        })

}  // namespace koladata::expr
