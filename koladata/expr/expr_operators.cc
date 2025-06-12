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
#include "koladata/expr/expr_operators.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/string.h"
#include "arolla/util/text.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"  // IWYU pragma: keep
#include "koladata/internal/non_deterministic_token.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::expr {
namespace {

static constexpr absl::string_view kInternalInput = "koda_internal.input";

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
          kInternalInput,
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
  auto op = expr::LiteralOperator::MakeLiteralOperator(std::move(value));
  // NOTE: We own the LiteralOperator::InferAttributes computation, and
  // therefore know that this cannot fail. It's therefore safe to dereference
  // the StatusOr without properly handling potential errors.
  auto output_attr = *op->InferAttributes({});
  return arolla::expr::ExprNode::UnsafeMakeOperatorNode(
      std::move(op), {}, std::move(output_attr));
}

std::shared_ptr<LiteralOperator> LiteralOperator::MakeLiteralOperator(
    arolla::TypedValue value) {
  return std::make_shared<LiteralOperator>(
      FreezeDataBagOrDataSlice(std::move(value)), PrivateConstructorTag{});
}

LiteralOperator::LiteralOperator(arolla::TypedValue value,
                                 PrivateConstructorTag)
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

bool IsInput(const arolla::expr::ExprNodePtr& node) {
  if (!node->is_op()) {
    return false;
  }
  return nullptr != arolla::fast_dynamic_downcast_final<const InputOperator*>(
                        arolla::expr::DecayRegisteredOperator(node->op())
                            .value_or(nullptr)
                            .get());
}

bool IsLiteral(const arolla::expr::ExprNodePtr& node) {
  if (node->is_op()) {
    return nullptr !=
           arolla::fast_dynamic_downcast_final<const LiteralOperator*>(
               arolla::expr::DecayRegisteredOperator(node->op())
                   .value_or(nullptr)
                   .get());
  }
  return node->is_literal();
}

absl::StatusOr<InputContainer> InputContainer::Create(
    absl::string_view cont_name) {
  arolla::expr::ExprOperatorPtr input_op =
      arolla::expr::ExprOperatorRegistry::GetInstance()->LookupOperatorOrNull(
          kInternalInput);
  if (input_op == nullptr) {
    return absl::InternalError(
        absl::StrCat("operator not found: ", kInternalInput));
  }

  return InputContainer(
      std::move(input_op),
      MakeLiteral(arolla::TypedValue::FromValue(arolla::Text(cont_name))));
}

absl::StatusOr<arolla::expr::ExprNodePtr> InputContainer::CreateInput(
    absl::string_view key) const {
  return arolla::expr::MakeOpNode(
      input_op_,
      {cont_name_,
       MakeLiteral(arolla::TypedValue::FromValue(arolla::Text(key)))});
}

absl::StatusOr<std::optional<std::string>> InputContainer::GetInputName(
    const arolla::expr::ExprNodePtr& node) const {
  if (!IsInput(node)) return std::nullopt;
  if (node->node_deps().size() != 2) {
    return absl::FailedPreconditionError("invalid koda_internal.input node");
  }
  if (node->node_deps()[0]->fingerprint() != cont_name_->fingerprint()) {
    return std::nullopt;
  }
  const std::optional<arolla::TypedValue>& val = node->node_deps()[1]->qvalue();
  if (!val.has_value()) {
    return absl::InvalidArgumentError("input name has no value");
  }
  ASSIGN_OR_RETURN(const arolla::Text& vname, val->As<arolla::Text>());
  return std::string(vname.view());
}

absl::StatusOr<std::vector<std::string>> InputContainer::ExtractInputNames(
    const arolla::expr::ExprNodePtr& node) const {
  std::vector<std::string> names;
  arolla::expr::PostOrder po(node);
  for (const auto& n : po.nodes()) {
    ASSIGN_OR_RETURN(auto name, GetInputName(n));
    if (name) {
      names.emplace_back(*std::move(name));
    }
  }
  return names;
}

}  // namespace koladata::expr
