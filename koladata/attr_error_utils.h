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
#ifndef KOLADATA_ATTR_ERROR_UTILS_H_
#define KOLADATA_ATTR_ERROR_UTILS_H_

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {

// NOTE: See how this can be used for List / Dict access as well.
//
// Assembles an Error for a Set / Get attribute on primitives. The Error message
// produced depends on whether the schema is primitive or not as well as on if
// it is a DataSlice / DataItem and if it is a schema.
absl::Status AttrOnPrimitiveError(const DataSlice& slice);

absl::Status AttrOnPrimitiveError(const internal::DataItem& item,
                                  const internal::DataItem& schema);
absl::Status AttrOnPrimitiveError(const internal::DataSliceImpl& slice,
                                  const internal::DataItem& schema);

// Checks that SetAttr can be legally called on this DataSlice.
absl::Status CheckEligibleForSetAttr(const DataSlice& slice);

// Validates that attr lookup is possible on the values of `impl`. If OK(),
// `impl` is guaranteed to contain only Items and `db != nullptr`.
absl::Status ValidateAttrLookupAllowed(const DataSlice& slice);

template <typename ImplT>
absl::Status ValidateAttrLookupAllowed(const DataBagPtr& db, const ImplT& impl,
                                       const internal::DataItem& schema) {
  if (schema.is_primitive_schema() || impl.ContainsAnyPrimitives()) {
    RETURN_IF_ERROR(AttrOnPrimitiveError(impl, schema));
  }
  if (schema.is_itemid_schema()) {
    return absl::InvalidArgumentError("ITEMIDs do not allow attribute access");
  }
  if (db == nullptr) {
    return absl::InvalidArgumentError(
        "the DataSlice is a reference without a bag");
  }
  return absl::OkStatus();
}


}  // namespace koladata

#endif  // KOLADATA_ATTR_ERROR_UTILS_H_
