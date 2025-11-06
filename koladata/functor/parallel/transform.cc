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
#include "absl/base/nullability.h"
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
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_eval.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/expr/non_determinism.h"
#include "koladata/functor/auto_variables.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/parallel/transform_config.pb.h"
#include "koladata/functor/parallel/transform_config.h"
#include "koladata/functor/signature_utils.h"
#include "koladata/functor_storage.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/object_factories.h"
#include "koladata/operators/slices.h"
#include "koladata/signature.h"
#include "koladata/signature_storage.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

inline constexpr absl::string_view kExecutorParamName = "_executor";

// Adds names to the literal arguments of the replacement ops that should not
// be kept as literals. Together with AddVariables, this ensures that we extract
// those arguments into new variables, while not touching other copies of the
// same literal in the expression.
// We could use any other "identity" operator instead of annotation.name, but
// annotation.name has the benefit of being removed by AutoVariables,
// therefore the resulting functor is simpler to debug.
absl::StatusOr<DataSlice> AddNamesToLiteralArgumentsOfReplacementOps(
    DataSlice functor,
    absl::AnyInvocable<
        absl::StatusOr<const ParallelTransformConfig::Replacement*>(
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
      var = DataSlice::CreatePrimitive(
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
  return CreateFunctor(*returns, signature, std::move(var_names),
                       std::move(var_values));
}

// Adds variables to simplify further transformation of the functor.
// The following subexpression become their own variables:
// - all inputs
// - all leaves (the only expected leaf is the non-deterministic leaf)
// - all operations that have a custom replacement in the config.
// - the inputs to such operations, except literals.
absl::StatusOr<DataSlice> AddVariables(
    DataSlice functor, const expr::InputContainer& variable_container,
    absl::AnyInvocable<
        absl::StatusOr<const ParallelTransformConfig::Replacement*>(
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

// This class is responsible for transformations of
// sub-functors. It makes sure we allocate new unique variable names and
// avoid transforming the same sub-functor twice.
// To use it, call Transform() multiple times, and then use Finish() once to get
// the new variables created by the manager.
class InnerTransformManager {
 public:
  struct InnerTransformResult {
    std::string var_name;
    bool is_future;
  };

  InnerTransformManager(const ParallelTransformConfigPtr absl_nonnull& config,
                        const DataSlice& functor,
                        const DataSlice::AttrNamesSet& attr_names,
                        const expr::InputContainer& variable_container,
                        const arolla::expr::ExprNodePtr& executor_input)
      : config_(config),
        functor_(functor),
        variable_container_(variable_container),
        used_var_names_(attr_names),
        executor_input_(executor_input) {}

  absl::StatusOr<InnerTransformResult> Transform(absl::string_view var_name) {
    // This caching only solves the case where the same functor is called
    // multiple times in the same parent functor. However, if it is called, say,
    // from a parent functor and from a child functor, it will be transformed
    // twice. We can potentially improve this later by making this cache more
    // global.
    auto it = results_.find(var_name);
    if (it != results_.end()) {
      return it->second;
    }
    InnerTransformResult result{
        .var_name = absl::StrCat("_transformed_", var_name),
        .is_future = false};
    int64_t next_suffix = 0;
    while (used_var_names_.contains(result.var_name)) {
      result.var_name =
          absl::StrCat("_transformed_", var_name, "_", next_suffix++);
    }
    used_var_names_.insert(result.var_name);
    ASSIGN_OR_RETURN(DataSlice var, functor_.GetAttr(var_name));
    if (!var.item().holds_value<arolla::expr::ExprQuote>()) {
      ASSIGN_OR_RETURN(
          auto transformed_var,
          TransformEager(arolla::TypedValue::FromValue(std::move(var))));
      ASSIGN_OR_RETURN(auto transformed_var_slice,
                       std::move(transformed_var).As<DataSlice>());
      new_vars_.emplace_back(result.var_name, std::move(transformed_var_slice));
      results_.emplace(var_name, result);
      return result;
    }
    ASSIGN_OR_RETURN(auto var_expr,
                     var.item().value<arolla::expr::ExprQuote>().expr());
    if (expr::IsLiteral(var_expr)) {
      if (!var_expr->qvalue()) {
        return absl::InternalError("Literal does not have a value");
      }
      ASSIGN_OR_RETURN(auto transformed_var,
                       TransformEager(*var_expr->qvalue()));
      auto new_var_expr = arolla::expr::Literal(std::move(transformed_var));
      auto new_var = DataSlice::CreatePrimitive(
          arolla::expr::ExprQuote{std::move(new_var_expr)});
      new_vars_.emplace_back(result.var_name, std::move(new_var));
      results_.emplace(var_name, result);
      return result;
    }
    if (!config_->allow_runtime_transforms()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "The parallel transformation requires that all sub-functors being"
          " called are specified as literals, instead of being computed"
          " dynamically, so that we can transform them recursively. In case you"
          " are calling a sub-functor that is computed dynamically, but do not"
          " need to recursively transform it (evaluating this sub-functor"
          " single-threaded is fine), you can use"
          " kd.functor.call_fn_normally_when_parallel and its variants to call"
          " the sub-functor. In case you do need to evaluate a dynamically"
          " computed sub-functor in a parallel fashion, you can pass"
          " allow_runtime_transforms=True to kd.parallel.transform, but this"
          " will be slower and should not be used in production."
          "\nThe offending sub-functor ",
          var_name, ": ", arolla::Repr(var),
          " . The whole functor: ", arolla::Repr(functor_)));
    }
    result.is_future = true;
    ASSIGN_OR_RETURN(auto input_expr,
                     variable_container_.CreateInput(var_name));
    ASSIGN_OR_RETURN(auto transformed_var,
                     TransformLazy(std::move(input_expr)));
    new_vars_.emplace_back(result.var_name,
                           DataSlice::CreatePrimitive(arolla::expr::ExprQuote{
                               std::move(transformed_var)}));
    results_.emplace(var_name, result);
    return result;
  }

  std::vector<std::pair<std::string, DataSlice>> Finish() && {
    return std::move(new_vars_);
  }

 private:
  absl::StatusOr<arolla::TypedValue> TransformEager(
      arolla::TypedValue sub_functor) {
    // Note that currently, we never reach this code path when sub_functor
    // is a slice of functors, since we can never have a slice of functors as
    // a variable in a functor (we need to have a list as a variable +
    // a kd.explode() expr). So "Many" is a bit misleading here, but I prefer
    // to keep one function for both eager and lazy transformation.
    return TransformManyToParallel(config_, std::move(sub_functor));
  }

  absl::StatusOr<arolla::expr::ExprNodePtr> TransformLazy(
      arolla::expr::ExprNodePtr sub_functor) {
    return arolla::expr::CallOp(
        "koda_internal.parallel._async_transform_many",
        {executor_input_, arolla::expr::Literal(config_),
         std::move(sub_functor)});
  }

  const ParallelTransformConfigPtr absl_nonnull& config_;
  const DataSlice& functor_;
  const expr::InputContainer& variable_container_;
  DataSlice::AttrNamesSet used_var_names_;
  const arolla::expr::ExprNodePtr& executor_input_;
  absl::flat_hash_map<std::string, InnerTransformResult> results_;
  std::vector<std::pair<std::string, DataSlice>> new_vars_;
};

// Creates a lambda operator that evaluates the given replacement op.
// The operator expects first all arguments from `functor_future_indices`,
// and then all other arguments wrapped in a tuple.
absl::StatusOr<arolla::expr::ExprOperatorPtr> CreateAsyncReplacementLambdaOp(
    const arolla::expr::ExprOperatorPtr& replacement_op, int64_t total_deps,
    const std::vector<int64_t>& functor_future_indices) {
  auto lambda_signature = arolla::expr::ExprOperatorSignature::MakeArgsN(
      1 + functor_future_indices.size());
  arolla::expr::ExprNodePtr tuple_param =
      arolla::expr::Placeholder(lambda_signature.parameters.back().name);

  std::vector<absl::StatusOr<arolla::expr::ExprNodePtr>> inner_deps;
  inner_deps.reserve(total_deps);
  DCHECK(absl::c_is_sorted(functor_future_indices));
  DCHECK(functor_future_indices.empty() ||
         functor_future_indices.back() < total_deps);
  int64_t next_functor = 0;
  int64_t tuple_index = 0;
  for (int64_t i = 0; i < total_deps; ++i) {
    bool is_functor = (next_functor < functor_future_indices.size() &&
                       functor_future_indices[next_functor] == i);
    if (is_functor) {
      inner_deps.push_back(arolla::expr::Placeholder(
          lambda_signature.parameters[next_functor++].name));
    } else {
      inner_deps.push_back(arolla::expr::CallOp(
          "core.get_nth", {tuple_param, arolla::expr::Literal(tuple_index++)}));
    }
  }
  ASSIGN_OR_RETURN(arolla::expr::ExprNodePtr lambda_expr,
                   arolla::expr::CallOp(replacement_op, std::move(inner_deps)));
  return arolla::expr::LambdaOperator::Make(lambda_signature,
                                            std::move(lambda_expr));
}

// Creates an expression that applies the given replacement op asynchronously
// after waiting for all transformed functors to be ready.
absl::StatusOr<arolla::expr::ExprNodePtr> CreateAsyncReplacement(
    const arolla::expr::ExprOperatorPtr& replacement_op,
    std::vector<arolla::expr::ExprNodePtr> new_deps,
    std::vector<int64_t> functor_future_indices,
    const expr::InputContainer& variable_container,
    const arolla::expr::ExprNodePtr& executor_input) {
  // We put all arguments except the functor futures into a tuple, so that
  // we do not wait for them even if they are futures themselves.
  std::vector<arolla::expr::ExprNodePtr> tuple_elements;
  tuple_elements.reserve(new_deps.size() - functor_future_indices.size());
  std::vector<arolla::expr::ExprNodePtr> outer_deps;
  outer_deps.reserve(3 + functor_future_indices.size());
  outer_deps.push_back(executor_input);
  ASSIGN_OR_RETURN(auto lambda_op, CreateAsyncReplacementLambdaOp(
                                       replacement_op, new_deps.size(),
                                       functor_future_indices));
  outer_deps.push_back(arolla::expr::Literal(std::move(lambda_op)));
  DCHECK(absl::c_is_sorted(functor_future_indices));
  DCHECK(functor_future_indices.empty() ||
         functor_future_indices.back() < new_deps.size());
  int64_t next_functor = 0;
  for (int64_t i = 0; i < new_deps.size(); ++i) {
    bool is_functor = (next_functor < functor_future_indices.size() &&
                       functor_future_indices[next_functor] == i);
    if (is_functor) {
      ++next_functor;
      outer_deps.push_back(std::move(new_deps[i]));
    } else {
      tuple_elements.push_back(std::move(new_deps[i]));
    }
  }
  if (next_functor != functor_future_indices.size()) {
    return absl::InternalError(
        "Not all functor future indices were used in the replacement");
  }
  ASSIGN_OR_RETURN(arolla::expr::ExprNodePtr tuple_expr,
                   arolla::expr::BindOp("core.make_tuple", tuple_elements, {}));
  outer_deps.push_back(std::move(tuple_expr));
  ASSIGN_OR_RETURN(arolla::expr::ExprNodePtr async_expr,
                   arolla::expr::BindOp("koda_internal.parallel.async_eval",
                                        outer_deps, {}));
  return arolla::expr::CallOp(
      "koda_internal.parallel.unwrap_future_to_parallel",
      {std::move(async_expr), expr::GenNonDeterministicToken()});
}

// Applies the given replacement to the given expression. The top-level node
// of the expression must be the source operator of the replacement.
absl::StatusOr<arolla::expr::ExprNodePtr> ApplyReplacement(
    arolla::expr::ExprNodePtr var_expr,
    const ParallelTransformConfigPtr absl_nonnull& config,
    const ParallelTransformConfig::Replacement& replacement,
    const arolla::expr::ExprNodePtr& executor_input,
    const expr::InputContainer& variable_container,
    InnerTransformManager& inner_transform_manager) {
  std::vector<arolla::expr::ExprNodePtr> new_deps;
  new_deps.reserve(
      var_expr->node_deps().size() +
      std::max<size_t>(1, replacement.argument_transformation.arguments_size())
      // We subtract 1 since ORIGINAL_ARGUMENTS will be replaced with the
      // original arguments.
      - 1);
  const auto& transformation = replacement.argument_transformation;
  std::vector<int64_t> functor_future_indices;
  for (const auto& arg : transformation.arguments()) {
    switch (arg) {
      case ParallelTransformConfigProto::ArgumentTransformation::
          ORIGINAL_ARGUMENTS: {
        const auto& functor_indices = transformation.functor_argument_indices();
        for (int64_t child_i = 0; child_i < var_expr->node_deps().size();
             ++child_i) {
          arolla::expr::ExprNodePtr child = var_expr->node_deps()[child_i];
          if (absl::c_contains(functor_indices, child_i)) {
            ASSIGN_OR_RETURN(auto child_var_name,
                             variable_container.GetInputName(child));
            if (!child_var_name) {
              return absl::InternalError(absl::StrCat(
                  "expected a variable for the functor argument ", child_i,
                  " but got: ", arolla::expr::ToDebugString(child)));
            }
            ASSIGN_OR_RETURN(
                auto transform_result,
                inner_transform_manager.Transform(*child_var_name));
            ASSIGN_OR_RETURN(child, variable_container.CreateInput(
                                        transform_result.var_name));
            if (transform_result.is_future) {
              functor_future_indices.push_back(new_deps.size());
            }
          }
          new_deps.push_back(std::move(child));
        }
        break;
      }
      case ParallelTransformConfigProto::ArgumentTransformation::
          EXECUTION_CONTEXT: {
        new_deps.push_back(arolla::expr::Literal(config));
        break;
      }
      case ParallelTransformConfigProto::ArgumentTransformation::EXECUTOR: {
        new_deps.push_back(executor_input);
        break;
      }
      case ParallelTransformConfigProto::ArgumentTransformation::
          NON_DETERMINISTIC_TOKEN: {
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
  if (functor_future_indices.empty()) {
    return arolla::expr::MakeOpNode(replacement.op, std::move(new_deps));
  } else {
    return CreateAsyncReplacement(replacement.op, std::move(new_deps),
                                  std::move(functor_future_indices),
                                  variable_container, executor_input);
  }
}

absl::StatusOr<DataSlice> MakeParallelDefaultValueMarker() {
  ASSIGN_OR_RETURN(auto present,
                   DataSlice::Create(internal::DataItem(arolla::kPresent),
                                     internal::DataItem(schema::kMask)));
  // We have an attribute to make this slightly nicer when printed, it is not
  // used for any other purpose.
  ASSIGN_OR_RETURN(
      auto res, CreateUu(DataBag::EmptyMutable(), "__parameter_default_value__",
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
// We also prepend an additional positional-only parameter for the executor.
absl::StatusOr<
    std::pair<DataSlice, absl::flat_hash_map<std::string, DataSlice>>>
ComputeUpdatedSignature(const DataSlice& signature) {
  ASSIGN_OR_RETURN(auto cpp_signature, KodaSignatureToCppSignature(signature));
  std::vector<Signature::Parameter> parameters = cpp_signature.parameters();
  absl::flat_hash_map<std::string, DataSlice> defaults;
  for (auto& param : parameters) {
    if (param.default_value) {
      defaults[param.name] = *param.default_value;
      param.default_value = ParallelDefaultValueMarker();
    }
  }
  parameters.insert(parameters.begin(),
                    {.name = std::string(kExecutorParamName),
                     .kind = Signature::Parameter::Kind::kPositionalOnly});
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
    const arolla::expr::ExprNodePtr& executor_input) {
  std::vector<arolla::expr::ExprNodePtr> op_inputs;
  op_inputs.reserve(variable_names.size() + 2);
  op_inputs.push_back(executor_input);
  op_inputs.push_back(arolla::expr::Literal(
      arolla::TypedValue::FromValue(std::move(lambda_op))));
  for (const auto& name : variable_names) {
    ASSIGN_OR_RETURN(arolla::expr::ExprNodePtr variable_expr,
                     variable_container.CreateInput(name));
    ASSIGN_OR_RETURN(
        op_inputs.emplace_back(),
        arolla::expr::CallOp("koda_internal.parallel.future_from_parallel",
                             {executor_input, std::move(variable_expr)}, {}));
  }
  ASSIGN_OR_RETURN(
      arolla::expr::ExprNodePtr async_expr,
      arolla::expr::BindOp("koda_internal.parallel.async_eval", op_inputs, {}));
  return arolla::expr::CallOp(
      "koda_internal.parallel.parallel_from_future",
      {executor_input, std::move(async_expr), expr::GenNonDeterministicToken()},
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
    const arolla::expr::ExprNodePtr& executor_input) {
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
                         variable_container, executor_input);
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
//    or use the custom replacement provided by the config.
//
// The resulting functor expects proper parallel types for its inputs, and
// guarantees that each variable is also of a proper parallel type.
absl::StatusOr<DataSlice> TransformToParallel(  // clang-format hint
    const ParallelTransformConfigPtr absl_nonnull& config, DataSlice functor) {
  ASSIGN_OR_RETURN(bool is_functor, IsFunctor(functor));
  if (!is_functor) {
    return absl::InvalidArgumentError("functor must be a functor");
  }
  ASSIGN_OR_RETURN(expr::InputContainer variable_container,
                   expr::InputContainer::Create("V"));
  ASSIGN_OR_RETURN(expr::InputContainer input_container,
                   expr::InputContainer::Create("I"));
  ASSIGN_OR_RETURN(auto executor_input,
                   input_container.CreateInput(kExecutorParamName));

  const auto& replacements = config->operator_replacements();
  auto find_replacement = [&replacements](const arolla::expr::ExprNodePtr& node)
      -> absl::StatusOr<const ParallelTransformConfig::Replacement*> {
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
  // At this point, we have processed the keep_literal_argument_indices
  // argument of the replacement configuration, and have the following property
  // instead: for each operator that has a replacement, all its arguments that
  // are not literals must be replaced with a variable, and all its literal
  // arguments must be kept as is.
  ASSIGN_OR_RETURN(functor, AddVariables(std::move(functor), variable_container,
                                         find_replacement));
  // At this point, we have the following properties:
  // - for each operator that has a custom replacement, all its arguments are
  //   either a variable (V.smth), or a literal in case the corresponding
  //   argument must be kept as a literal.
  // - each operator that has a custom replacement will only appear as the
  //   top-level operator of a variable expression, and not deeper inside it.
  //   Together with the previous property, it means that operators with a
  //   custom replacement will only appear in expressions that look like
  //   V.smth = kd.my_op(V.arg1, V.arg2, ...)
  // - inputs and leaves (non-deterministic leaf) will only appear as a whole
  //   variable expression, but not inside a more complex expression. So we can
  //   have V.smth = I.smth, but not V.smth = I.smth + 1.
  ASSIGN_OR_RETURN(DataSlice orig_signature,
                   functor.GetAttr(kSignatureAttrName));
  ASSIGN_OR_RETURN((auto [signature, defaults]),
                   ComputeUpdatedSignature(orig_signature));
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
  InnerTransformManager inner_transform_manager(
      config, functor, attr_names, variable_container, executor_input);
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
      ASSIGN_OR_RETURN(
          var_expr, ApplyReplacement(std::move(var_expr), config, *replacement,
                                     executor_input, variable_container,
                                     inner_transform_manager));
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
        ASSIGN_OR_RETURN(var_expr, ApplyDefaultReplacement(std::move(var_expr),
                                                           variable_container,
                                                           executor_input));
      }
    }
    var = DataSlice::CreatePrimitive(
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
  auto inner_vars = std::move(inner_transform_manager).Finish();
  var_names.reserve(var_names.size() + inner_vars.size());
  var_values.reserve(var_values.size() + inner_vars.size());
  for (const auto& [name, value] : inner_vars) {
    var_names.push_back(name);
    var_values.push_back(value);
  }
  return CreateFunctor(*returns, signature, std::move(var_names),
                       std::move(var_values));
}

absl::StatusOr<arolla::TypedValue> TransformManyToParallel(
    const ParallelTransformConfigPtr absl_nonnull& config,
    arolla::TypedValue functors) {
  if (functors.GetType() == arolla::GetUnspecifiedQType()) {
    return functors;
  }
  if (functors.GetType() != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        absl::StrCat("expected a DataSlice or unspecified, got ",
                     functors.GetType()->name()));
  }
  ASSIGN_OR_RETURN(DataSlice fns, std::move(functors).As<DataSlice>());
  ASSIGN_OR_RETURN(DataSlice fns_flat, fns.Flatten(0, std::nullopt));
  ASSIGN_OR_RETURN(DataSlice unique_fns,
                   ops::Unique(std::move(fns_flat),
                               /*sort=*/DataSlice::CreatePrimitive(false)));
  std::vector<DataSlice> unique_transformed_fns_vec;
  unique_transformed_fns_vec.reserve(unique_fns.size());
  for (int64_t i = 0; i < unique_fns.size(); ++i) {
    ASSIGN_OR_RETURN(DataSlice fn,
                     ops::Take(unique_fns, DataSlice::CreatePrimitive(i)));
    ASSIGN_OR_RETURN(DataSlice transformed_fn,
                     TransformToParallel(config, std::move(fn)));
    unique_transformed_fns_vec.emplace_back(std::move(transformed_fn));
  }
  DataSlice is_stack = DataSlice::CreatePrimitive(true);
  DataSlice ndim = DataSlice::CreatePrimitive(0);
  std::vector<const DataSlice*> stack_args;
  stack_args.reserve(2 + unique_transformed_fns_vec.size());
  stack_args.push_back(&is_stack);
  stack_args.push_back(&ndim);
  for (int64_t i = 0; i < unique_transformed_fns_vec.size(); ++i) {
    stack_args.push_back(&unique_transformed_fns_vec[i]);
  }
  ASSIGN_OR_RETURN(DataSlice unique_transformed_fns,
                   ops::ConcatOrStack(stack_args));
  ASSIGN_OR_RETURN(DataSlice transformed_fns,
                   ops::Translate(std::move(fns), std::move(unique_fns),
                                  std::move(unique_transformed_fns)));
  return arolla::TypedValue::FromValue(std::move(transformed_fns));
}

}  // namespace koladata::functor::parallel
