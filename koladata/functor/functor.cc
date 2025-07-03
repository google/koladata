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
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
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
#include "arolla/util/status_macros_backport.h"

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
  DataBagPtr result_db = DataBag::Empty();
  ASSIGN_OR_RETURN(auto result, ObjectCreator::FromAttrs(
                                    result_db, std::move(variable_names),
                                    std::move(variable_values)));
  result_db->UnsafeMakeImmutable();
  return result;
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
  return CreateFunctorImpl(signature, std::move(variable_names),
                           std::move(variable_values));
}

}  // namespace koladata::functor
