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
#include "koladata/attr_error_utils.h"

#include <cstdint>
#include <optional>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/view_types.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {

absl::Status CheckEligibleForSetAttr(const DataSlice& slice) {
  if (slice.GetSchemaImpl().is_primitive_schema() ||
      slice.ContainsAnyPrimitives()) {
    RETURN_IF_ERROR(AttrOnPrimitiveError(slice));
  }
  if (slice.GetSchemaImpl().is_itemid_schema()) {
    return absl::InvalidArgumentError("ITEMIDs do not allow attribute access");
  }
  if (slice.GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "the DataSlice is a reference without a bag");
  }
  return absl::OkStatus();
}

namespace {
std::optional<std::pair<int64_t, internal::DataItem>> GetFirstIndexAndPrimitive(
    const internal::DataSliceImpl& slice) {
  std::optional<std::pair<int64_t, internal::DataItem>> result;
  slice.VisitValues([&]<class T>(const arolla::DenseArray<T>& array) {
    if (result.has_value()) {
      return;
    }
    if constexpr (!std::is_same_v<T, internal::ObjectId>) {
      if (!array.empty()) {
        array.ForEachPresent([&](int64_t id, arolla::view_type_t<T> val_view) {
          if (result.has_value()) {
            return;
          }
          result = {id, internal::DataItem(T(std::move(val_view)))};
        });
      }
    }
  });
  return result;
}

}  // namespace

absl::Status AttrOnPrimitiveError(const DataSlice& slice) {
  const internal::DataItem& schema = slice.GetSchemaImpl();
  if (schema.is_primitive_schema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("primitives do not have attributes, got %v", schema));
  }
  if (slice.is_item()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "primitives do not have attributes, got %v DataItem with primitive "
        "%v",
        schema, slice.item()));
  }
  auto id_and_val = GetFirstIndexAndPrimitive(slice.slice());
  if (id_and_val.has_value()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "primitives do not have attributes, got %v DataSlice with at least "
        "one primitive %v at ds.flatten().S[%d]",
        schema, id_and_val->second, id_and_val->first));
  }
  // NOTE: This is a fallback that is future proof, but cannot happen in
  // practice.
  return absl::InvalidArgumentError(absl::StrFormat(
      "primitives do not have attributes, got %v DataSlice with primitive "
      "values",
      schema));
}

absl::Status AttrOnPrimitiveError(const internal::DataItem& item,
                                  const internal::DataItem& schema) {
  RETURN_IF_ERROR(AttrOnPrimitiveError(*DataSlice::Create(item, schema)));
  return absl::OkStatus();
}

absl::Status AttrOnPrimitiveError(const internal::DataSliceImpl& slice,
                                  const internal::DataItem& schema) {
  RETURN_IF_ERROR(
      AttrOnPrimitiveError(*DataSlice::CreateWithFlatShape(slice, schema)));
  return absl::OkStatus();
}

absl::Status ValidateAttrLookupAllowed(const DataSlice& slice) {
  return slice.VisitImpl([&](const auto& impl) -> absl::Status {
    return ValidateAttrLookupAllowed(slice.GetBag(), impl,
                                     slice.GetSchemaImpl());
  });
}

}  // namespace koladata
