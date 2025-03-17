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
#include "koladata/functor/functor.h"

#include <algorithm>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/expr/expr_eval.h"
#include "koladata/functor/default_signature.h"
#include "koladata/functor/signature_storage.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/object_id.h"
#include "koladata/object_factories.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

absl::StatusOr<DataSlice> CreateFunctor(
    const DataSlice& returns, const std::optional<DataSlice>& signature,
    absl::Span<const std::pair<std::string, DataSlice>> variables) {
  if (!returns.is_item()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("returns must be a data item, but has shape: %s",
                        arolla::Repr(returns.GetShape())));
  }

  std::vector<absl::string_view> variable_names;
  std::vector<DataSlice> variable_values;
  variable_names.reserve(variables.size() + 2);
  variable_values.reserve(variables.size() + 2);
  variable_names.push_back(kReturnsAttrName);
  variable_values.push_back(returns);
  for (const auto& [name, value] : variables) {
    if (!value.is_item()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "variable [%s] must be a data item, but has shape: %s", name,
          arolla::Repr(value.GetShape())));
    }
    variable_names.push_back(name);
    variable_values.push_back(value);
  }

  if (!signature.has_value()) {
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
    RETURN_IF_ERROR(KodaSignatureToCppSignature(signature.value()).status());
    variable_names.push_back(kSignatureAttrName);
    variable_values.push_back(signature.value());
  }
  DataBagPtr result_db = DataBag::Empty();
  ASSIGN_OR_RETURN(auto result, ObjectCreator::FromAttrs(
                                    result_db, variable_names,
                                    variable_values));
  result_db->UnsafeMakeImmutable();
  return result;
}

absl::StatusOr<bool> IsFunctor(const DataSlice& slice) {
  if (!slice.is_item()) {
    return false;
  }
  if (slice.GetBag() == nullptr) {
    return false;
  }
  if (slice.item().dtype() != arolla::GetQType<internal::ObjectId>()) {
    return false;
  }
  ASSIGN_OR_RETURN(auto returns, slice.HasAttr(kReturnsAttrName));
  if (returns.IsEmpty()) {
    return false;
  }
  ASSIGN_OR_RETURN(auto signature, slice.HasAttr(kSignatureAttrName));
  if (signature.IsEmpty()) {
    return false;
  }
  return true;
}

}  // namespace koladata::functor
