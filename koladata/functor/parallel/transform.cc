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
#include "absl/base/no_destructor.h"
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
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/quote.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/visitors/substitution.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_eval.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/expr/non_determinism.h"
#include "koladata/functor/auto_variables.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/parallel/execution_config.pb.h"
#include "koladata/functor/parallel/execution_context.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/signature_utils.h"
#include "koladata/functor_storage.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/object_factories.h"
#include "koladata/signature.h"
#include "koladata/signature_storage.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

// Adds names to the literal arguments of the replacement ops that should not
// be kept as literals. Together with AddVariables, this ensures that we extract
// those arguments into new variables, while not touching other copies of the
// same literal in the expression.
// We could use any other "identity" operator instead of annotation.name, but
// annotation.name has the benefit of being removed by AutoVariables,
// therefore the resulting functor is simpler to debug.
absl::StatusOr<DataSlice> AddNamesToLiteralArgumentsOfReplacementOps(
    DataSlice functor,
    absl::AnyInvocable<absl::StatusOr<const ExecutionContext::Replacement*>(
        const arolla::expr::ExprNodePtr&) const>
        find_replacement_fn) {
  ASSIGN_OR_RETURN(DataSlice signature, functor.GetAttr(kSignatureAttrName));
  ASSIGN_OR_RETURN(auto attr_names, functor.GetAttrNames());

  auto transform = [&find_replacement_fn](arolla::expr::ExprNodePtr node)
      -> absl::StatusOr<arolla::expr::ExprNodePtr> {
    ASSIGN_OR_RETURN(const auto* replacement, find_replacement_fn(node));
    if (!replacement) {
      return std::move(node);
    }
    std::vector<arolla::expr::ExprNodePtr> new_deps;
    bool updated = false;
    // We could build a set, or require it to be sorted so that we can
    // iterate over the inverse, but currently the size is always small so
    // it does not matter.
    const auto& literal_indices =
        replacement->argument_transformation.keep_literal_argument_indices();
    for (int64_t child_i = 0; child_i < node->node_deps().size(); ++child_i) {
      const auto& child = node->node_deps()[child_i];
      if (!expr::IsLiteral(child)) {
        continue;
      }
      if (absl::c_contains(literal_indices, child_i)) {
        continue;
      }
      if (!updated) {
        updated = true;
        // We copy only if we need to update, to avoid useless work.
        new_deps = node->node_deps();
      }
      ASSIGN_OR_RETURN(
          new_deps[child_i],
          arolla::expr::CallOp("annotation.name",
                               {child, arolla::expr::Literal(arolla::Text(
                                           "_parallel_literal"))}));
    }
    if (!updated) {
      return std::move(node);
    }
    return arolla::expr::MakeOpNode(node->op(), std::move(new_deps));
  };

  std::vector<absl::string_view> var_names;
  std::vector<DataSlice> var_values;
  std::optional<DataSlice> returns;
  int64_t expected_num_vars = attr_names.size();
  expected_num_vars = std::max<int64_t>(0, expected_num_vars - 2);
  var_names.reserve(expected_num_vars);
  var_values.reserve(expected_num_vars);
  for (const auto& attr_name : attr_names) {
    if (attr_name == kSignatureAttrName) {
      continue;
    }
    ASSIGN_OR_RETURN(DataSlice var, functor.GetAttr(attr_name));
    DCHECK(var.is_item());
    if (var.is_item() && var.item().holds_value<arolla::expr::ExprQuote>()) {
      ASSIGN_OR_RETURN(auto var_expr,
                       var.item().value<arolla::expr::ExprQuote>().expr());
      ASSIGN_OR_RETURN(auto new_expr,
                       arolla::expr::Transform(var_expr, transform));
      var = DataSlice::CreateFromScalar(
          arolla::expr::ExprQuote(std::move(new_expr)));
    }
    if (attr_name == kReturnsAttrName) {
      returns = std::move(var);
    } else {
      var_names.emplace_back(attr_name);
      var_values.emplace_back(std::move(var));
    }
  }
  if (!returns.has_value()) {
    return absl::InternalError("no 'returns' after transformation");
  }
  return CreateFunctor(returns.value(), signature, std::move(var_names),
                       std::move(var_values));
}

// Adds variables to simplify further transformation of the functor.
// The following subexpression become their own variables:
// - all inputs
// - all leaves (the only expected leaf is the non-deterministic leaf)
// - all operations that have a custom replacement in the execution context.
// - the inputs to such operations, except literals.
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
      for (const auto& child : node->node_deps()) {
        if (expr::IsLiteral(child)) {
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

absl::StatusOr<DataSlice> MakeParallelDefaultValueMarker() {
  ASSIGN_OR_RETURN(auto present,
                   DataSlice::Create(internal::DataItem(arolla::kPresent),
                                     internal::DataItem(schema::kMask)));
  // We have an attribute to make this slightly nicer when printed, it is not
  // used for any other purpose.
  ASSIGN_OR_RETURN(auto res,
                   CreateUu(DataBag::Empty(), "__parameter_default_value__",
                            {"parallel_default_value"}, {std::move(present)}));
  return res.FreezeBag();
}

const DataSlice& ParallelDefaultValueMarker() {
  // No errors are possible here, so we ignore status.
  static absl::NoDestructor<DataSlice> val{*MakeParallelDefaultValueMarker()};
  return *val;
}

// Returns a signature where parameters with a default value receive a
// new special default value ParallelDefaultValueMarker(), and a hash map
// from parameter name to the actual default value.
// This is needed because the default values of a functor parameter can only be
// DataItems, and here we need to replace them with a future to a DataItem.
absl::StatusOr<
    std::pair<DataSlice, absl::flat_hash_map<std::string, DataSlice>>>
ExtractDefaultsFromSignature(const DataSlice& signature) {
  ASSIGN_OR_RETURN(auto cpp_signature, KodaSignatureToCppSignature(signature));
  std::vector<Signature::Parameter> parameters = cpp_signature.parameters();
  absl::flat_hash_map<std::string, DataSlice> defaults;
  for (auto& param : parameters) {
    if (param.default_value) {
      defaults[param.name] = *param.default_value;
      param.default_value = ParallelDefaultValueMarker();
    }
  }
  ASSIGN_OR_RETURN(auto new_cpp_signature, Signature::Create(parameters));
  ASSIGN_OR_RETURN(auto new_signature,
                   CppSignatureToKodaSignature(new_cpp_signature));
  return std::make_pair(std::move(new_signature), std::move(defaults));
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
      {std::move(executor_literal), std::move(async_expr),
       expr::GenNonDeterministicToken()},
      {});
}

// Applies the parallel replacement to the given literal.
absl::StatusOr<arolla::expr::ExprNodePtr> ApplyLiteralReplacement(
    arolla::expr::ExprNodePtr var_expr) {
  // This is more lightweight than async_eval, even though that would also
  // work.
  return arolla::expr::BindOp("koda_internal.parallel.as_parallel",
                              {std::move(var_expr)}, {});
}

// Applies the parallel replacement to the given input or leaf.
absl::StatusOr<arolla::expr::ExprNodePtr> ApplyInputReplacement(
    arolla::expr::ExprNodePtr var_expr,
    const expr::InputContainer& variable_container,
    const expr::InputContainer& input_container,
    const absl::flat_hash_map<std::string, DataSlice>& defaults) {
  if (var_expr->is_leaf()) {
    if (var_expr->leaf_key() != expr::kNonDeterministicTokenLeafKey) {
      return absl::InvalidArgumentError(
          absl::StrCat("unexpected leaf node: ", var_expr->leaf_key()));
    }
    return std::move(var_expr);
  }
  ASSIGN_OR_RETURN(auto var_name, variable_container.GetInputName(var_expr));
  if (var_name) {
    // Variables are already in parallel form.
    return std::move(var_expr);
  }
  ASSIGN_OR_RETURN(auto input_name, input_container.GetInputName(var_expr));
  if (!input_name) {
    return absl::InvalidArgumentError(absl::StrCat(
        "found an input node that is neither an input nor a variable: ",
        arolla::expr::ToDebugString(var_expr)));
  }
  auto it = defaults.find(*input_name);
  if (it == defaults.end()) {
    // Inputs that are not the default value are already in parallel form.
    return std::move(var_expr);
  }
  return arolla::expr::CallOp(
      "koda_internal.parallel._get_value_or_parallel_default",
      {std::move(var_expr),
       // We pass the default value marker here even though it's a global
       // constant to avoid having to expose it to Python in some other way.
       arolla::expr::Literal(ParallelDefaultValueMarker()),
       arolla::expr::Literal(it->second)});
}

// This is a default replacement that is used when there is no custom
// replacement. It uses future_from_parallel+async_eval+parallel_from_future
// to transform the expression into a parallel version.
absl::StatusOr<arolla::expr::ExprNodePtr> ApplyDefaultReplacement(
    arolla::expr::ExprNodePtr var_expr,
    const expr::InputContainer& variable_container,
    const ExecutorPtr& executor) {
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

absl::StatusOr<bool> IsAnnotationChainOnAVariable(
    const arolla::expr::ExprNodePtr& node,
    const expr::InputContainer& variable_container) {
  ASSIGN_OR_RETURN(auto inner_node,
                   arolla::expr::StripTopmostAnnotations(node));
  ASSIGN_OR_RETURN(auto var_name, variable_container.GetInputName(inner_node));
  return var_name.has_value();
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
  ASSIGN_OR_RETURN(expr::InputContainer input_container,
                   expr::InputContainer::Create("I"));

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

  ASSIGN_OR_RETURN(functor, AddNamesToLiteralArgumentsOfReplacementOps(
                                std::move(functor), find_replacement));
  ASSIGN_OR_RETURN(functor, AddVariables(std::move(functor), variable_container,
                                         find_replacement));
  ASSIGN_OR_RETURN(DataSlice orig_signature,
                   functor.GetAttr(kSignatureAttrName));
  ASSIGN_OR_RETURN((auto [signature, defaults]),
                   ExtractDefaultsFromSignature(orig_signature));
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
    } else if (expr::IsInput(var_expr) || var_expr->is_leaf()) {
      ASSIGN_OR_RETURN(var_expr, ApplyInputReplacement(
                                     std::move(var_expr), variable_container,
                                     input_container, defaults));
    } else if (expr::IsLiteral(var_expr)) {
      ASSIGN_OR_RETURN(var_expr, ApplyLiteralReplacement(std::move(var_expr)));
    } else {
      ASSIGN_OR_RETURN(
          bool is_annotation_chain_on_a_variable,
          IsAnnotationChainOnAVariable(var_expr, variable_container));
      if (is_annotation_chain_on_a_variable) {
        // Do nothing, keep var_expr as is. The default replacement might fail
        // if the variable being annotated is a stream. This relies on the
        // following assumptions:
        // - The first argument of each annotation operator is always returned
        //   as-is, therefore we can do it for streams or other parallel types
        //   without issue.
        // - Other arguments of annotation operators can be kept as-is without
        //   parallel transformation (but note that we only guarantee that they
        //   will be kept as is for those that are scalar literals; for more
        //   complex arguments they might be transformed into the parallel form
        //   if they also appear elsewhere in the expression).
      } else {
        ASSIGN_OR_RETURN(var_expr, ApplyDefaultReplacement(
                                       std::move(var_expr), variable_container,
                                       context->executor()));
      }
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
