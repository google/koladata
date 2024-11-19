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
#ifndef KOLADATA_EXPR_EXPR_EVAL_H_
#define KOLADATA_EXPR_EXPR_EVAL_H_

#include <string>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"

namespace koladata::expr {

// Name of the leaf used to pass a hidden seed value to the expression.
// This leaf is not visible in the expression and is not passed to "eval".
constexpr absl::string_view kHiddenSeedLeafKey = "_koladata_hidden_seed_leaf";

// Evaluates the given expression with the provided inputs and variables.
// The expression must refer to inputs/variables via InputOperator, not via
// leaves and placeholders.
// Expr transformation and compilation is cached.
absl::StatusOr<arolla::TypedValue> EvalExprWithCompilationCache(
    const arolla::expr::ExprNodePtr& expr,
    absl::Span<const std::pair<std::string, arolla::TypedRef>> inputs,
    absl::Span<const std::pair<std::string, arolla::TypedRef>> variables);

// Retrieves the list of variables used in the given expression.
// This reuses the same cache as EvalExprWithCompilationCache, so it is cheap
// to call this method before/after evaluating the expression.
absl::StatusOr<std::vector<std::string>> GetExprVariables(
    const arolla::expr::ExprNodePtr& expr);

// Retrieves the list of inputs used in the given expression.
// This reuses the same cache as EvalExprWithCompilationCache, so it is cheap
// to call this method before/after evaluating the expression.
absl::StatusOr<std::vector<std::string>> GetExprInputs(
    const arolla::expr::ExprNodePtr& expr);

// Clears the expr transformation and compilation caches.
void ClearCompilationCache();

}  // namespace koladata::expr

#endif  // KOLADATA_EXPR_EXPR_EVAL_H_
