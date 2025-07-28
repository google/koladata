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
#include "koladata/functor/call.h"

#include <cstdint>
#include <stack>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/expr/expr_eval.h"
#include "koladata/functor/signature_utils.h"
#include "koladata/functor_storage.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/object_id.h"
#include "koladata/signature.h"
#include "koladata/signature_storage.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

namespace {

struct VariableProcessingFrame {
  std::string variable_name;
  std::vector<std::string> dependencies;
  int64_t next_dependency_index = 0;
};

enum class VariableState {
  kInStack,
  kVisited,
};

// Returns the order in which the variables should be
// evaluated through topological sorting.
// We can add caching for this later if needed.
absl::StatusOr<std::vector<std::string>> GetVariableEvaluationOrder(
    const DataSlice& functor) {
  // We implement depth-first search using our own stack to avoid recursion.
  std::stack<VariableProcessingFrame> stack;
  absl::flat_hash_map<std::string, VariableState> variable_state;

  auto reach_variable = [&stack, &functor, &variable_state](
                            absl::string_view variable_name) -> absl::Status {
    auto [it, was_inserted] =
        variable_state.emplace(variable_name, VariableState::kInStack);
    if (!was_inserted) {
      if (it->second == VariableState::kInStack) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "variable [%s] has a dependency cycle", variable_name));
      }
      return absl::OkStatus();
    }
    ASSIGN_OR_RETURN(auto variable, functor.GetAttr(variable_name));
    if (variable.item().holds_value<arolla::expr::ExprQuote>()) {
      ASSIGN_OR_RETURN(auto expr,
                       variable.item().value<arolla::expr::ExprQuote>().expr());
      ASSIGN_OR_RETURN(auto dependencies, expr::GetExprVariables(expr));
      stack.push({.variable_name = std::string(variable_name),
                  .dependencies = std::move(dependencies)});
    } else {
      stack.push(
          {.variable_name = std::string(variable_name), .dependencies = {}});
    }
    return absl::OkStatus();
  };

  RETURN_IF_ERROR(reach_variable(kReturnsAttrName));
  std::vector<std::string> res;
  while (!stack.empty()) {
    auto& state = stack.top();
    if (state.next_dependency_index >= state.dependencies.size()) {
      variable_state[state.variable_name] = VariableState::kVisited;
      res.push_back(state.variable_name);
      stack.pop();
      continue;
    }
    RETURN_IF_ERROR(
        reach_variable(state.dependencies[state.next_dependency_index++]));
  }
  return res;
}

struct FunctorPreprocessingCache {
  Signature signature;
  std::vector<std::string> variable_evaluation_order;
};

absl::StatusOr<FunctorPreprocessingCache> ProcessFunctorMetadata(
    const DataSlice& functor) {
  ASSIGN_OR_RETURN(auto signature_item, functor.GetAttr(kSignatureAttrName));
  // We use detach_default_values_db=true to prevent ownership loop when storing
  // the preprocessed signature in the data bag cache.
  ASSIGN_OR_RETURN(auto signature,
                   KodaSignatureToCppSignature(
                       signature_item, /*detach_default_values_db=*/true));
  ASSIGN_OR_RETURN(auto eval_order, GetVariableEvaluationOrder(functor));
  if (eval_order.empty() || eval_order.back() != kReturnsAttrName) {
    return absl::InternalError(
        "variable evaluation order does not end with returns");
  }
  return FunctorPreprocessingCache{std::move(signature), std::move(eval_order)};
}

}  // namespace

absl::StatusOr<arolla::TypedValue> CallFunctorWithCompilationCache(
    const DataSlice& functor, absl::Span<const arolla::TypedRef> args,
    absl::Span<const std::string> kwnames) {
  ASSIGN_OR_RETURN(bool is_functor, IsFunctor(functor));
  if (!is_functor) {
    return absl::InvalidArgumentError(
        absl::StrCat("the first argument of kd.call must be a functor, got ",
                     DataSliceRepr(functor)));
  }

  internal::ObjectId functor_id = functor.item().value<internal::ObjectId>();
  auto bag = functor.GetBag();
  DCHECK(bag != nullptr);  // validated in IsFunctor

  auto cached_data =
      bag->GetCachedMetadataOrNull<FunctorPreprocessingCache>(functor_id);
  if (cached_data == nullptr) {
    ASSIGN_OR_RETURN(auto data, ProcessFunctorMetadata(functor));
    cached_data = bag->SetCachedMetadata(functor_id, std::move(data));
  }
  DCHECK(cached_data != nullptr);

  ASSIGN_OR_RETURN(
      auto bound_arguments,
      BindArguments(cached_data->signature, args, kwnames, std::move(bag)));
  std::vector<arolla::TypedValue> computed_variable_holder;
  std::vector<std::pair<std::string, arolla::TypedRef>> inputs;
  std::vector<std::pair<std::string, arolla::TypedRef>> variables;
  const auto& parameters = cached_data->signature.parameters();
  inputs.reserve(parameters.size());
  for (int64_t i = 0; i < parameters.size(); ++i) {
    inputs.emplace_back(parameters[i].name, bound_arguments[i].AsRef());
  }
  computed_variable_holder.reserve(
      cached_data->variable_evaluation_order.size());
  variables.reserve(cached_data->variable_evaluation_order.size());

  for (const auto& variable_name : cached_data->variable_evaluation_order) {
    ASSIGN_OR_RETURN(auto variable, functor.GetAttr(variable_name));
    if (variable.item().holds_value<arolla::expr::ExprQuote>()) {
      ASSIGN_OR_RETURN(auto expr,
                       variable.item().value<arolla::expr::ExprQuote>().expr());
      // This passes all variables computed so far, even those not used, and
      // EvalExprWithCompilationCache will traverse all provided variables,
      // so this is O(num_variables**2). We can optimize this later if needed.
      auto variable_value =
          expr::EvalExprWithCompilationCache(expr, inputs, variables);
      if (!variable_value.ok()) {
        // Not using ASSIGN_OR_RETURN to avoid an extra source location.
        return variable_value.status();
      }
      computed_variable_holder.push_back(*std::move(variable_value));
    } else {
      computed_variable_holder.push_back(
          arolla::TypedValue::FromValue(std::move(variable)));
    }
    variables.emplace_back(variable_name,
                           computed_variable_holder.back().AsRef());
  }
  return computed_variable_holder.back();
}

}  // namespace koladata::functor
