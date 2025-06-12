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
#ifndef KOLADATA_EXPR_EXPR_OPERATORS_H_
#define KOLADATA_EXPR_EXPR_OPERATORS_H_

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"

namespace koladata::expr {

// Non-lowerable operator `koda_internal._input(container_name, input_key)`
// representing inputs (fixed output qtype) and belonging to the specified
// container.
class InputOperator final
    : public arolla::expr::BuiltinExprOperatorTag,
      public arolla::expr::ExprOperatorWithFixedSignature {
 public:
  InputOperator();

  absl::StatusOr<arolla::expr::ExprAttributes> InferAttributes(
      absl::Span<const arolla::expr::ExprAttributes> inputs) const final;
};

// Non-lowerable stateful operator `koda_internal.literal()` that wraps a
// TypedValue. This operator allows us to attach a view to non-DataSlice
// literals.
class LiteralOperator final
    : public arolla::expr::BuiltinExprOperatorTag,
      public arolla::expr::ExprOperatorWithFixedSignature {
  struct PrivateConstructorTag {};

 public:
  explicit LiteralOperator(arolla::TypedValue value, PrivateConstructorTag);

  static std::shared_ptr<LiteralOperator> MakeLiteralOperator(
      arolla::TypedValue value);

  absl::StatusOr<arolla::expr::ExprAttributes> InferAttributes(
      absl::Span<const arolla::expr::ExprAttributes> inputs) const final;

  absl::string_view py_qvalue_specialization_key() const final;

  const arolla::TypedValue& value() const;

 private:
  arolla::TypedValue value_;
};

// Base class for non-lowerable operators that converts a DataSlice to an Arolla
// type T. Supports evaluation at operator binding time if the provided input is
// a literal. Dispatches the actual conversion to a corresponding
// backend-operator.
class ToArollaValueOperator
    : public arolla::expr::BackendExprOperatorTag,
      public arolla::expr::ExprOperatorWithFixedSignature {
 public:
  ToArollaValueOperator(absl::string_view name,
                        arolla::expr::ExprOperatorSignature signature,
                        absl::string_view doc, arolla::Fingerprint fingerprint,
                        std::string backend_operator_name,
                        arolla::QTypePtr output_qtype)
      : ExprOperatorWithFixedSignature(name, std::move(signature), doc,
                                       fingerprint),
        backend_operator_name_(std::move(backend_operator_name)),
        output_qtype_(output_qtype) {}

  absl::StatusOr<arolla::expr::ExprAttributes> InferAttributes(
      absl::Span<const arolla::expr::ExprAttributes> inputs) const final;

 private:
  std::string backend_operator_name_;
  arolla::QTypePtr output_qtype_;
};

// Non-lowerable operator `koda_internal.to_arolla_int64(x)` that converts
// DataSlice to int64_t. Supports evaluation at operator binding time if the
// provided input is a literal.
class ToArollaInt64Operator final : public ToArollaValueOperator {
 public:
  ToArollaInt64Operator();
};

// Non-lowerable operator `koda_internal.to_arolla_text(x)` that converts
// DataSlice to Text. Supports evaluation at operator binding time if the
// provided input is a literal.
class ToArollaTextOperator final : public ToArollaValueOperator {
 public:
  ToArollaTextOperator();
};

// Return a literal Expr node.
arolla::expr::ExprNodePtr MakeLiteral(arolla::TypedValue value);

// Non-lowerable operator
// ```
// koda_internal.non_deterministic(
//     L._koladata_hidden_seed_leaf, <random int64>
// )
// ```
// that ensures that each Expr containing non-deterministic operators created
// in the same form has different Fingerprint and does not get literally folded
// during compilation.
class NonDeterministicOperator final
    : public arolla::expr::BackendExprOperatorTag,
      public arolla::expr::ExprOperatorWithFixedSignature {
 public:
  NonDeterministicOperator();

  absl::StatusOr<arolla::expr::ExprAttributes> InferAttributes(
      absl::Span<const arolla::expr::ExprAttributes> inputs) const final;
};

// Returns true if `node` is an operator node with InputOperator.
bool IsInput(const arolla::expr::ExprNodePtr& node);

// Returns true if `node` is either an arolla literal or an operator node with
// LiteralOperator.
bool IsLiteral(const arolla::expr::ExprNodePtr& node);

// Helper container to create Koda specific inputs
class InputContainer {
 public:
  static absl::StatusOr<InputContainer> Create(absl::string_view cont_name);

  // Creates koda_internal.input(cont_name, key)
  absl::StatusOr<arolla::expr::ExprNodePtr> CreateInput(
      absl::string_view key) const;

  // If the given node is an input from this container, returns its name,
  // otherwise returns nullopt. Returns an error when the node is an input
  // node but is malformed.
  absl::StatusOr<std::optional<std::string>> GetInputName(
      const arolla::expr::ExprNodePtr& node) const;

  // Traverses given node and finds all inputs from this container
  absl::StatusOr<std::vector<std::string>> ExtractInputNames(
      const arolla::expr::ExprNodePtr& node) const;

 private:
  InputContainer(arolla::expr::ExprOperatorPtr input_op,
                 arolla::expr::ExprNodePtr cont_name)
      : input_op_(std::move(input_op)), cont_name_(std::move(cont_name)) {};
  arolla::expr::ExprOperatorPtr input_op_;
  arolla::expr::ExprNodePtr cont_name_;
};

}  // namespace koladata::expr

#endif  // KOLADATA_EXPR_EXPR_OPERATORS_H_
