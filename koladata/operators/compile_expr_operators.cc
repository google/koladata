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
// Arolla compiler extensions for Koda expr operators.

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "koladata/expr/expr_operators.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/extensions.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::expr {
namespace {

// Node transformation extension that returns an understandable error if a kola
// input is encountered. Without it, the error message is more cryptic and looks
// unintentional.
absl::StatusOr<arolla::expr::ExprNodePtr> TransformInputOperatorNode(
    const arolla::expr::DynamicEvaluationEngineOptions& options,
    arolla::expr::ExprNodePtr node) {
  ASSIGN_OR_RETURN(auto decayed_op,
                   arolla::expr::DecayRegisteredOperator(node->op()));
  if (arolla::fast_dynamic_downcast_final<const InputOperator*>(
          decayed_op.get()) != nullptr) {
    return absl::FailedPreconditionError(
        absl::StrFormat("%s cannot be evaluated - please provide data "
                        "to `I` inputs and substitute all `V` variables",
                        arolla::expr::ToDebugString(node)));
  } else {
    return node;
  }
}

// Node transformation that replaces `kd.literal` with `arolla.literal`.
absl::StatusOr<arolla::expr::ExprNodePtr> TransformLiteralOperatorNode(
    const arolla::expr::DynamicEvaluationEngineOptions& options,
    arolla::expr::ExprNodePtr node) {
  ASSIGN_OR_RETURN(auto decayed_op,
                   arolla::expr::DecayRegisteredOperator(node->op()));
  if (const auto* op =
          arolla::fast_dynamic_downcast_final<const LiteralOperator*>(
              decayed_op.get())) {
    return arolla::expr::Literal(op->value());
  } else {
    return node;
  }
}

AROLLA_INITIALIZER(
        .reverse_deps = {"@phony/operators:qexpr"}, .init_fn = [] {
          arolla::expr::eval_internal::CompilerExtensionRegistry::GetInstance()
              .RegisterNodeTransformationFn(TransformInputOperatorNode);
          arolla::expr::eval_internal::CompilerExtensionRegistry::GetInstance()
              .RegisterNodeTransformationFn(TransformLiteralOperatorNode);
        })

}  // namespace
}  // namespace koladata::expr
