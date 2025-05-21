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
#include "koladata/functor/parallel/transform.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/quote.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/visitors/substitution.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_eval.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/expr/non_determinism.h"
#include "koladata/functor/auto_variables.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/parallel/execution_context.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/non_deterministic_token.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

// Adds variables to simplify further transformation of the functor.
// The following subexpression become their own variables:
// - all inputs
// - all leaves (the only expected leaf is the non-deterministic leaf)
// - all operations that have a custom replacement in the execution context.
// - the inputs to such operations, except for the literal-only ones.
absl::StatusOr<DataSlice> AddVariables(
    DataSlice functor, const expr::InputContainer& variable_container,
    absl::AnyInvocable<absl::StatusOr<const ExecutionContext::Replacement*>(
        const arolla::expr::ExprNodePtr&) const>
        find_replacement_fn) {
  ASSIGN_OR_RETURN(DataSlice signature, functor.GetAttr(kSignatureAttrName));
  ASSIGN_OR_RETURN(auto attr_names, functor.GetAttrNames());

  absl::flat_hash_set<arolla::Fingerprint> extra_nodes_to_extract;
  auto find_matches_and_inputs =
      [&extra_nodes_to_extract, &find_replacement_fn, &variable_container](
          arolla::expr::ExprNodePtr node,
          absl::Span<const std::monostate* const> visits)
      -> absl::StatusOr<std::monostate> {
    if (expr::IsInput(node) || node->is_leaf()) {
      // Check that it's not already a variable, as it makes no sense to
      // create a new variable pointing to another variable.
      ASSIGN_OR_RETURN(auto var_name, variable_container.GetInputName(node));
      if (!var_name) {
        // This simplifies processing below, as we can assume that all
        // non-simple exprs only have variables and not inputs or leaves.
        extra_nodes_to_extract.insert(node->fingerprint());
      }
      return std::monostate();
    }
    ASSIGN_OR_RETURN(const auto* replacement, find_replacement_fn(node));
    if (replacement) {
      extra_nodes_to_extract.insert(node->fingerprint());
      // We could build a set for faster lookups, but currently the size
      // is always small.
      const auto& literal_indices =
          replacement->argument_transformation.keep_literal_argument_indices();
      // Also extract children, to guarantee that the parallel version of
      // the operator receives futures and not eagerly evaluated values.
      for (int64_t child_i = 0; child_i < node->node_deps().size(); ++child_i) {
        const auto& child = node->node_deps()[child_i];
        if (absl::c_contains(literal_indices, child_i) &&
            expr::IsLiteral(child)) {
          continue;
        }
        ASSIGN_OR_RETURN(auto child_var_name,
                         variable_container.GetInputName(child));
        if (!child_var_name) {
          extra_nodes_to_extract.insert(child->fingerprint());
        }
      }
    }
    return std::monostate();
  };

  for (const auto& attr_name : attr_names) {
    if (attr_name == kSignatureAttrName) {
      continue;
    }
    ASSIGN_OR_RETURN(DataSlice var, functor.GetAttr(attr_name));
    DCHECK(var.is_item());
    if (var.is_item() && var.item().holds_value<arolla::expr::ExprQuote>()) {
      ASSIGN_OR_RETURN(auto var_expr,
                       var.item().value<arolla::expr::ExprQuote>().expr());
      ASSIGN_OR_RETURN(
          std::monostate unused_visit_result,
          arolla::expr::PostOrderTraverse(var_expr, find_matches_and_inputs));
      (void)unused_visit_result;  // We don't care about the result.
    }
  }
  return AutoVariables(functor, std::move(extra_nodes_to_extract));
}

// Applies the given replacement to the given expression. The top-level node
// of the expression must be the source operator of the replacement.
absl::StatusOr<arolla::expr::ExprNodePtr> ApplyReplacement(
    arolla::expr::ExprNodePtr var_expr, const ExecutionContextPtr& context,
    const ExecutionContext::Replacement& replacement) {
  std::vector<arolla::expr::ExprNodePtr> new_deps;
  new_deps.reserve(
      var_expr->node_deps().size() +
      std::max<size_t>(1, replacement.argument_transformation.arguments_size())
      // We subtract 1 since ORIGINAL_ARGUMENTS will be replaced with the
      // original arguments.
      - 1);
  const auto& transformation = replacement.argument_transformation;
  for (const auto& arg : transformation.arguments()) {
    switch (arg) {
      case ExecutionConfig::ArgumentTransformation::ORIGINAL_ARGUMENTS: {
        new_deps.insert(new_deps.end(), var_expr->node_deps().begin(),
                        var_expr->node_deps().end());
        break;
      }
      case ExecutionConfig::ArgumentTransformation::EXECUTION_CONTEXT: {
        new_deps.push_back(
            arolla::expr::Literal(arolla::TypedValue::FromValue(context)));
        break;
      }
      case ExecutionConfig::ArgumentTransformation::EXECUTOR: {
        new_deps.push_back(arolla::expr::Literal(
            arolla::TypedValue::FromValue(context->executor())));
        break;
      }
      case ExecutionConfig::ArgumentTransformation::NON_DETERMINISTIC_TOKEN: {
        ASSIGN_OR_RETURN(new_deps.emplace_back(),
                         expr::GenNonDeterministicToken());
        break;
      }
      default: {
        return absl::InternalError(
            absl::StrCat("unknown replacement arg: ", arg));
      }
    }
  }
  return arolla::expr::MakeOpNode(replacement.op, std::move(new_deps));
}

// Creates a lambda operator that evaluates the given expression asynchronously.
// It expects the variables as inputs in the same order as in variable_names.
absl::StatusOr<arolla::expr::ExprOperatorPtr> CreateAsyncLambdaOp(
    arolla::expr::ExprNodePtr expr,
    absl::Span<const std::string> variable_names,
    const expr::InputContainer& variable_container) {
  auto lambda_signature =
      arolla::expr::ExprOperatorSignature::MakeArgsN(variable_names.size());
  absl::flat_hash_map<arolla::Fingerprint, arolla::expr::ExprNodePtr> subs;
  subs.reserve(lambda_signature.parameters.size());
  for (int64_t variable_index = 0; variable_index < variable_names.size();
       ++variable_index) {
    arolla::expr::ExprNodePtr param_expr = arolla::expr::Placeholder(
        lambda_signature.parameters[variable_index].name);
    ASSIGN_OR_RETURN(
        arolla::expr::ExprNodePtr variable_expr,
        variable_container.CreateInput(variable_names[variable_index]));
    subs.emplace(variable_expr->fingerprint(), std::move(param_expr));
  }
  ASSIGN_OR_RETURN(auto lambda_expr, arolla::expr::SubstituteByFingerprint(
                                         std::move(expr), subs));
  return arolla::expr::LambdaOperator::Make(lambda_signature,
                                            std::move(lambda_expr));
}

// Creates the async eval node for the default parallel execution.
absl::StatusOr<arolla::expr::ExprNodePtr> CreateAsyncEval(
    arolla::expr::ExprOperatorPtr lambda_op,
    absl::Span<const std::string> variable_names,
    const expr::InputContainer& variable_container,
    const ExecutorPtr& executor) {
  std::vector<arolla::expr::ExprNodePtr> op_inputs;
  op_inputs.reserve(variable_names.size() + 2);
  auto executor_literal =
      arolla::expr::Literal(arolla::TypedValue::FromValue(executor));
  op_inputs.push_back(executor_literal);
  op_inputs.push_back(arolla::expr::Literal(
      arolla::TypedValue::FromValue(std::move(lambda_op))));
  for (const auto& name : variable_names) {
    ASSIGN_OR_RETURN(arolla::expr::ExprNodePtr variable_expr,
                     variable_container.CreateInput(name));
    ASSIGN_OR_RETURN(
        op_inputs.emplace_back(),
        arolla::expr::CallOp("koda_internal.parallel.future_from_parallel",
                             {executor_literal, std::move(variable_expr)}, {}));
  }
  ASSIGN_OR_RETURN(
      arolla::expr::ExprNodePtr async_expr,
      arolla::expr::BindOp("koda_internal.parallel.async_eval", op_inputs, {}));
  return arolla::expr::CallOp(
      "koda_internal.parallel.parallel_from_future",
      {std::move(async_expr), expr::GenNonDeterministicToken()}, {});
}

// This is a default replacement that is used when there is no custom
// replacement. It uses future_from_parallel+async_eval+parallel_from_future
// to transform the expression into a parallel version.
absl::StatusOr<arolla::expr::ExprNodePtr> ApplyDefaultReplacement(
    arolla::expr::ExprNodePtr var_expr,
    const expr::InputContainer& variable_container,
    const ExecutorPtr& executor) {
  if (expr::IsInput(var_expr) || var_expr->is_leaf()) {
    // Inputs are already valid parallel types, and leaves are only the
    // non-deterministic leaf which is also a valid parallel type.
    return std::move(var_expr);
  }
  if (expr::IsLiteral(var_expr)) {
    // This is more lightweight than async_eval, even though that would also
    // work.
    return arolla::expr::BindOp("koda_internal.parallel.as_parallel",
                                {std::move(var_expr)}, {});
  }
  // Because of the AddVariables logic, the expression here can only have
  // variables, but not inputs or leaves.
  ASSIGN_OR_RETURN(auto variable_names, expr::GetExprVariables(var_expr));
  ASSIGN_OR_RETURN(auto input_names, expr::GetExprInputs(var_expr));
  DCHECK(input_names.empty());
  DCHECK(arolla::expr::GetLeafKeys(var_expr).empty());
  ASSIGN_OR_RETURN(auto lambda_op,
                   CreateAsyncLambdaOp(std::move(var_expr), variable_names,
                                       variable_container));
  return CreateAsyncEval(std::move(lambda_op), variable_names,
                         variable_container, executor);
}
}  // namespace

// The transformation is done in 2 steps:
// 1. We add variables to the functor in such a way that all further decisions
//    can be made based on the top-level operator of each variable expression
//    only, without further Expr traversals.
// 2. For each variable, we either apply the default replacement (async_eval)
//    or use the custom replacement provided by the execution context.
//
// The resulting functor expects proper parallel types for its inputs, and
// guarantees that each variable is also of a proper parallel type.
absl::StatusOr<DataSlice> TransformToParallel(
    const ExecutionContextPtr& context, DataSlice functor,
    const internal::NonDeterministicToken& non_deterministic_token) {
  ASSIGN_OR_RETURN(bool is_functor, IsFunctor(functor));
  if (!is_functor) {
    return absl::InvalidArgumentError("functor must be a functor");
  }
  ASSIGN_OR_RETURN(expr::InputContainer variable_container,
                   expr::InputContainer::Create("V"));

  const auto& replacements = context->operator_replacements();
  auto find_replacement = [&replacements](const arolla::expr::ExprNodePtr& node)
      -> absl::StatusOr<const ExecutionContext::Replacement*> {
    ASSIGN_OR_RETURN(auto node_op,
                     arolla::expr::DecayRegisteredOperator(node->op()));
    if (node_op == nullptr) {
      return nullptr;
    }
    auto it = replacements.find(node_op->fingerprint());
    if (it == replacements.end()) {
      return nullptr;
    }
    return &it->second;
  };

  ASSIGN_OR_RETURN(functor, AddVariables(std::move(functor), variable_container,
                                         find_replacement));
  ASSIGN_OR_RETURN(DataSlice signature, functor.GetAttr(kSignatureAttrName));
  ASSIGN_OR_RETURN(auto attr_names, functor.GetAttrNames());
  if (attr_names.size() < 2) {
    return absl::InternalError(
        "functor must have at least 2 attributes, returns and __signature__");
  }

  std::vector<absl::string_view> var_names;
  std::vector<DataSlice> var_values;
  std::optional<DataSlice> returns;
  var_names.reserve(attr_names.size() - 2);
  var_values.reserve(attr_names.size() - 2);
  for (const auto& attr_name : attr_names) {
    if (attr_name == kSignatureAttrName) {
      continue;
    }
    ASSIGN_OR_RETURN(DataSlice var, functor.GetAttr(attr_name));
    DCHECK(var.is_item());
    arolla::expr::ExprNodePtr var_expr;
    if (var.is_item() && var.item().holds_value<arolla::expr::ExprQuote>()) {
      ASSIGN_OR_RETURN(var_expr,
                       var.item().value<arolla::expr::ExprQuote>().expr());
    } else {
      var_expr = arolla::expr::Literal(std::move(var));
    }
    ASSIGN_OR_RETURN(const auto* replacement, find_replacement(var_expr));
    if (replacement) {
      ASSIGN_OR_RETURN(var_expr, ApplyReplacement(std::move(var_expr), context,
                                                  *replacement));
    } else {
      ASSIGN_OR_RETURN(var_expr, ApplyDefaultReplacement(std::move(var_expr),
                                                         variable_container,
                                                         context->executor()));
    }
    var = DataSlice::CreateFromScalar(
        arolla::expr::ExprQuote(std::move(var_expr)));
    if (attr_name == kReturnsAttrName) {
      returns = std::move(var);
    } else {
      var_names.push_back(attr_name);
      var_values.push_back(std::move(var));
    }
  }
  if (!returns) {
    return absl::InternalError("functor must have 'returns' attribute");
  }
  return CreateFunctor(*returns, signature, std::move(var_names),
                       std::move(var_values));
}

}  // namespace koladata::functor::parallel
