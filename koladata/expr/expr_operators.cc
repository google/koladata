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
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice_qtype.h"  // IWYU pragma: keep
#include "koladata/internal/non_deterministic_token.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
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

arolla::TypedValue FreezeDataBagOrDataSlice(arolla::TypedValue value) {
  if (value.GetType() == arolla::GetQType<DataBagPtr>()) {
    return arolla::TypedValue::FromValue(
        value.UnsafeAs<DataBagPtr>()->Freeze());
  } else if (value.GetType() == arolla::GetQType<DataSlice>()) {
    return arolla::TypedValue::FromValue(
        value.UnsafeAs<DataSlice>().FreezeBag());
  }
  return value;
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

arolla::expr::ExprNodePtr MakeLiteral(arolla::TypedValue value) {
  value = FreezeDataBagOrDataSlice(std::move(value));
  auto attr = arolla::expr::ExprAttributes(value);
  return arolla::expr::ExprNode::UnsafeMakeOperatorNode(
      expr::LiteralOperator::MakeLiteralOperator(std::move(value)), {},
      std::move(attr));
}

std::shared_ptr<LiteralOperator> LiteralOperator::MakeLiteralOperator(
    arolla::TypedValue value) {
  return std::make_shared<LiteralOperator>(
      FreezeDataBagOrDataSlice(std::move(value)), PrivateConstructorTag{});
}

LiteralOperator::LiteralOperator(arolla::TypedValue value,
                                 PrivateConstructorTag)
    : arolla::expr::ExprOperatorWithFixedSignature(
          absl::StrFormat("koda_internal.literal[%s]", value.Repr()),
          arolla::expr::ExprOperatorSignature{}, "Koda literal.",
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

absl::StatusOr<arolla::expr::ExprAttributes>
ToArollaValueOperator::InferAttributes(
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
    ASSIGN_OR_RETURN(
        auto casted_value,
        arolla::InvokeOperator(backend_operator_name_, {*inputs[0].qvalue()},
                               output_qtype_));
    return arolla::expr::ExprAttributes{std::move(casted_value)};
  }
  // Otherwise, return the output qtype.
  return arolla::expr::ExprAttributes{output_qtype_};
}

ToArollaInt64Operator::ToArollaInt64Operator()
    : ToArollaValueOperator(
          "koda_internal.to_arolla_int64",
          arolla::expr::ExprOperatorSignature({{"x"}},
                                              "koladata_default_boxing"),
          "Returns `x` converted into an arolla int64 value.\n"
          "\n"
          "Note that `x` must adhere to the following requirements:\n"
          "* `rank = 0`.\n"
          "* Have one of the following schemas: NONE, INT32, INT64, OBJECT.\n"
          "* Have a present value with type INT32 or INT64.\n"
          "\n"
          "In all other cases, an exception is raised.\n\n"
          "Args:\n"
          "  x: A DataItem to be converted into an arolla int64 value.",
          arolla::FingerprintHasher("::koladata::expr::ToArollaInt64Operator")
              .Finish(),
          "koda_internal.to_arolla_int64", arolla::GetQType<int64_t>()) {}

ToArollaTextOperator::ToArollaTextOperator()
    : ToArollaValueOperator(
          "koda_internal.to_arolla_text",
          arolla::expr::ExprOperatorSignature({{"x"}},
                                              "koladata_default_boxing"),
          "Returns `x` converted into an arolla text value.\n"
          "\n"
          "Note that `x` must adhere to the following requirements:\n"
          "* `rank = 0`.\n"
          "* Have one of the following schemas: NONE, STRING, OBJECT, ANY.\n"
          "* Have a present value with type TEXT.\n"
          "\n"
          "In all other cases, an exception is raised.\n\n"
          "Args:\n"
          "  x: A DataItem to be converted into an arolla text value.",
          arolla::FingerprintHasher("::koladata::expr::ToArollaTextOperator")
              .Finish(),
          "koda_internal.to_arolla_text", arolla::GetQType<arolla::Text>()) {}

NonDeterministicOperator::NonDeterministicOperator()
  : arolla::expr::ExprOperatorWithFixedSignature(
      "koda_internal.non_deterministic",
      arolla::expr::ExprOperatorSignature{{"arg"}, {"random"}},
      "Returns a non_deterministic value.",
      arolla::FingerprintHasher("::koladata::ops::NonDeterministicOp")
      .Finish()) {}

absl::StatusOr<arolla::expr::ExprAttributes>
NonDeterministicOperator::InferAttributes(
    absl::Span<const arolla::expr::ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  return arolla::expr::ExprAttributes(
      arolla::GetQType<internal::NonDeterministicToken>());
}

}  // namespace koladata::expr
