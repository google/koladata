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
#include "koladata/functor/functor.h"

#include <algorithm>
#include <cstddef>
#include <stack>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/function_ref.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/quote.h"
#include "arolla/util/repr.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/expr/expr_eval.h"
#include "koladata/functor/default_signature.h"
#include "koladata/functor_storage.h"
#include "koladata/internal/data_item.h"
#include "koladata/object_factories.h"
#include "koladata/signature_storage.h"

namespace koladata::functor {

namespace {

absl::Status ValidateReturn(const DataSlice& returns) {
  if (!returns.is_item()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("returns must be a data item, but has shape: %s",
                        arolla::Repr(returns.GetShape())));
  }
  if (!returns.item().has_value()) {
    return absl::InvalidArgumentError("returns must be present");
  }
  return absl::OkStatus();
}

absl::Status ValidateArg(const DataSlice& returns, absl::string_view arg_name) {
  if (!returns.is_item()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("variable [%s] must be a data item, but has shape: %s",
                        arg_name, arolla::Repr(returns.GetShape())));
  }
  return absl::OkStatus();
}

absl::Status ProcessSignature(const DataSlice& signature,
                              std::vector<absl::string_view>& variable_names,
                              std::vector<DataSlice>& variable_values) {
  if (!signature.item().has_value()) {
    std::vector<std::string> inputs;
    for (const auto& value : variable_values) {
      if (value.item().holds_value<arolla::expr::ExprQuote>()) {
        ASSIGN_OR_RETURN(auto var_expr,
                         value.item().value<arolla::expr::ExprQuote>().expr());
        ASSIGN_OR_RETURN(auto var_inputs, expr::GetExprInputs(var_expr));
        inputs.insert(inputs.end(), var_inputs.begin(), var_inputs.end());
      }
    }
    std::sort(inputs.begin(), inputs.end());
    inputs.erase(std::unique(inputs.begin(), inputs.end()), inputs.end());
    ASSIGN_OR_RETURN(auto default_signature,
                     DefaultKodaSignatureFromInputs(inputs));
    variable_names.push_back(kSignatureAttrName);
    variable_values.push_back(std::move(default_signature));
  } else {
    // Verify that the signature is valid.
    RETURN_IF_ERROR(KodaSignatureToCppSignature(signature).status());
    variable_names.push_back(kSignatureAttrName);
    variable_values.push_back(signature);
  }
  return absl::OkStatus();
}

absl::StatusOr<DataSlice> CreateFunctorImpl(
    const DataSlice& signature, std::vector<absl::string_view> variable_names,
    std::vector<DataSlice> variable_values) {
  RETURN_IF_ERROR(ProcessSignature(signature, variable_names, variable_values));
  DataBagPtr result_db = DataBag::EmptyMutable();
  ASSIGN_OR_RETURN(auto result, ObjectCreator::FromAttrs(
                                    result_db, std::move(variable_names),
                                    std::move(variable_values)));
  result_db->UnsafeMakeImmutable();
  return result;
}

enum class VariableState {
  kInStack,
  kVisited,
};

struct VariableProcessingFrame {
  std::string variable_name;
  DataSlice variable_value;
  std::vector<std::string> dependencies;
  size_t next_dependency_index = 0;
};

}  // namespace

absl::Status ForEachReachableVariable(
    const DataSlice& functor,
    absl::FunctionRef<absl::Status(absl::string_view, const DataSlice&)>
        visitor) {
  ASSIGN_OR_RETURN(bool is_functor, IsFunctor(functor));
  if (!is_functor) return absl::InvalidArgumentError("functor expected");

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
    ASSIGN_OR_RETURN(auto variable, functor.GetAttrOrMissing(variable_name));
    DCHECK(variable.is_item());
    std::vector<std::string> dependencies;
    if (variable.item().holds_value<arolla::expr::ExprQuote>()) {
      ASSIGN_OR_RETURN(
          auto expr, variable.item().value<arolla::expr::ExprQuote>().expr());
      ASSIGN_OR_RETURN(dependencies, expr::GetExprVariables(expr));
    }
    stack.push({.variable_name = std::string(variable_name),
                .variable_value = std::move(variable),
                .dependencies = std::move(dependencies)});
    return absl::OkStatus();
  };

  RETURN_IF_ERROR(reach_variable(kReturnsAttrName));
  while (!stack.empty()) {
    auto& state = stack.top();
    if (state.next_dependency_index >= state.dependencies.size()) {
      variable_state[state.variable_name] = VariableState::kVisited;
      RETURN_IF_ERROR(
          visitor(state.variable_name, state.variable_value));
      stack.pop();
      continue;
    }
    RETURN_IF_ERROR(
        reach_variable(state.dependencies[state.next_dependency_index++]));
  }
  return absl::OkStatus();
}

namespace {

// Validates that the functor has no undefined variable references or cycles.
// `variable_names` must include all defined variable names (including
// 'returns' and '__signature__').
absl::Status ValidateAllVariablesDefined(
    const DataSlice& functor,
    absl::Span<const absl::string_view> variable_names) {
  absl::flat_hash_set<absl::string_view> defined(variable_names.begin(),
                                                 variable_names.end());
  std::vector<std::string> undefined;
  RETURN_IF_ERROR(ForEachReachableVariable(
      functor,
      [&undefined, &defined](absl::string_view name, const DataSlice&) {
        if (!defined.contains(name)) {
          undefined.push_back(std::string(name));
        }
        return absl::OkStatus();
      }));
  if (!undefined.empty()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "undefined variables: ", absl::StrJoin(undefined, ", ")));
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<DataSlice> CreateFunctor(
    const DataSlice& returns, const DataSlice& signature,
    std::vector<absl::string_view> variable_names,
    std::vector<DataSlice> variable_values) {
  RETURN_IF_ERROR(ValidateReturn(returns));
  RETURN_IF_ERROR(ValidateArg(signature, "signature"));
  for (size_t i = 0; i < variable_names.size(); ++i) {
    RETURN_IF_ERROR(ValidateArg(variable_values[i], variable_names[i]));
  }
  variable_names.insert(variable_names.begin(), kReturnsAttrName);
  variable_values.insert(variable_values.begin(), returns);
  ASSIGN_OR_RETURN(auto result,
                   CreateFunctorImpl(signature, variable_names,
                                     std::move(variable_values)));
  RETURN_IF_ERROR(ValidateAllVariablesDefined(result, variable_names));
  return result;
}

}  // namespace koladata::functor
