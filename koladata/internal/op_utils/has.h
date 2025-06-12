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
#ifndef KOLADATA_INTERNAL_OP_UTILS_HAS_H_
#define KOLADATA_INTERNAL_OP_UTILS_HAS_H_

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/unit.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"

namespace koladata::internal {

// Returns the presence data as DenseArray.
arolla::DenseArray<arolla::Unit> PresenceDenseArray(const DataSliceImpl& ds);

// Returns the presence data. If all missing returns an empty DataSlice.
struct HasOp {
  absl::StatusOr<DataSliceImpl> operator()(
      const DataSliceImpl& ds) const {
    return DataSliceImpl::Create(PresenceDenseArray(ds));
  }

  absl::StatusOr<DataItem> operator()(const DataItem& item) const {
    if (item.has_value()) {
      return DataItem(arolla::Unit());
    }
    return DataItem();
  }
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_HAS_H_
