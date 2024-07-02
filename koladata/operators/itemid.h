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
#ifndef KOLADATA_OPERATORS_ITEMID_H_
#define KOLADATA_OPERATORS_ITEMID_H_

#include <cstdint>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/itemid.h"
#include "koladata/internal/schema_utils.h"
#include "arolla/qtype/qtype_traits.h"

namespace koladata::ops {

// kde.core.itemid_bits
inline absl::StatusOr<DataSlice> ItemIdBits(const DataSlice& ds,
                                            const DataSlice& last) {
  if (last.GetShape().rank() != 0) {
    return absl::InvalidArgumentError("last must be an item");
  }
  if (last.dtype() != arolla::GetQType<int32_t>() &&
      last.dtype() != arolla::GetQType<int64_t>()) {
    return absl::InvalidArgumentError("last must be an integer");
  }

  const internal::DataItem& item = last.item();
  if (!item.has_value()) {
    return absl::InvalidArgumentError("last cannot be missing");
  }
  if (!schema::VerifySchemaForItemIds(ds.GetSchemaImpl())) {
    return absl::InvalidArgumentError(
        "the schema of the ds must be itemid, any, or object");
  }
  int64_t val =
      item.holds_value<int>() ? item.value<int>() : item.value<int64_t>();
  return ds.VisitImpl([&](const auto& impl) {
    return DataSlice::Create(internal::ItemIdBits()(impl, val), ds.GetShape(),
                             internal::DataItem(schema::kInt64), ds.GetDb());
  });
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_ITEMID_H_
