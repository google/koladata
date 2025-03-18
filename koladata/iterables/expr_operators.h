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
#ifndef KOLADATA_ITERABLES_EXPR_OPERATORS_H_
#define KOLADATA_ITERABLES_EXPR_OPERATORS_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"

namespace koladata::iterables {

// koda_internal.iterables.get_iterable_qtype
class GetIterableQTypeOp final
    : public arolla::expr::BackendExprOperatorTag,
      public arolla::expr::ExprOperatorWithFixedSignature {
 public:
  GetIterableQTypeOp();

  absl::StatusOr<arolla::expr::ExprAttributes> InferAttributes(
      absl::Span<const arolla::expr::ExprAttributes> inputs) const final;
};

}  // namespace koladata::iterables

#endif  // KOLADATA_ITERABLES_EXPR_OPERATORS_H_
