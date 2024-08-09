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

#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/functor/signature_storage.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/object_factories.h"
#include "arolla/expr/quote.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

absl::StatusOr<DataSlice> CreateFunctor(
    const DataSlice& returns, const DataSlice& signature,
    absl::Span<const std::pair<std::string, DataSlice>> variables) {
  if (returns.GetShape().rank() != 0 ||
      !returns.item().holds_value<arolla::expr::ExprQuote>()) {
    return absl::InvalidArgumentError("returns must hold a quoted Expr");
  }
  // Verify that the signature is valid.
  RETURN_IF_ERROR(KodaSignatureToCppSignature(signature).status());
  std::vector<absl::string_view> variable_names;
  std::vector<DataSlice> variable_values;
  variable_names.reserve(variables.size() + 2);
  variable_values.reserve(variables.size() + 2);
  variable_names.push_back(kReturnsAttrName);
  variable_values.push_back(returns);
  variable_names.push_back(kSignatureAttrName);
  variable_values.push_back(signature);
  for (const auto& [name, value] : variables) {
    if (value.GetShape().rank() != 0) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "variable [%s] must be a data item, but has shape: %s", name,
          arolla::Repr(value.GetShape())));
    }
    variable_names.push_back(name);
    variable_values.push_back(value);
  }
  AdoptionQueue adoption_queue;
  for (const auto& value : variable_values) {
    adoption_queue.Add(value);
  }
  auto db = DataBag::Empty();
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db));
  return ObjectCreator::FromAttrs(db, variable_names, variable_values);
}

absl::StatusOr<bool> IsFunctor(const DataSlice& slice) {
  if (slice.GetShape().rank() != 0) {
    return false;
  }
  static absl::NoDestructor<DataSlice> missing{*DataSlice::Create(
      internal::DataItem(arolla::kMissing), internal::DataItem(schema::kMask))};
  // TODO: use HasAttr when it is implemented.
  ASSIGN_OR_RETURN(auto returns,
                   slice.GetAttrWithDefault(kReturnsAttrName, *missing));
  if (!returns.present_count()) {
    return false;
  }
  ASSIGN_OR_RETURN(auto signature,
                   slice.GetAttrWithDefault(kSignatureAttrName, *missing));
  if (!signature.present_count()) {
    return false;
  }
  return true;
}

}  // namespace koladata::functor
