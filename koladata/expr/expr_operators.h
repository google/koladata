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
#ifndef KOLADATA_EXPR_EXPR_OPERATORS_H_
#define KOLADATA_EXPR_EXPR_OPERATORS_H_

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/typed_value.h"

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
 public:
  explicit LiteralOperator(arolla::TypedValue value);

  absl::StatusOr<arolla::expr::ExprAttributes> InferAttributes(
      absl::Span<const arolla::expr::ExprAttributes> inputs) const final;

  absl::string_view py_qvalue_specialization_key() const final;

  const arolla::TypedValue& value() const;

 private:
  arolla::TypedValue value_;
};

}  // namespace koladata::expr

#endif  // KOLADATA_EXPR_EXPR_OPERATORS_H_
