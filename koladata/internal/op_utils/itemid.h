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
#ifndef KOLADATA_INTERNAL_OP_UTILS_ITEMID_H_
#define KOLADATA_INTERNAL_OP_UTILS_ITEMID_H_

#include <utility>

#include "absl/numeric/int128.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/base62.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/text.h"

namespace koladata::internal {

// Returns a base62 string representation of the ItemId.
struct EncodeItemId {
  absl::StatusOr<DataItem> operator()(const DataItem& item) const {
    if (!item.has_value()) {
      return DataItem();
    }
    if (item.dtype() != arolla::GetQType<internal::ObjectId>()) {
      return absl::InvalidArgumentError(
          "cannot use encode_itemid on primitives");
    }
    internal::ObjectId id = item.value<internal::ObjectId>();
    absl::uint128 val = id.ToRawInt128();
    return internal::DataItem(EncodeBase62(val));
  }

  absl::StatusOr<DataSliceImpl> operator()(const DataSliceImpl& slice) const {
    if (slice.is_empty_and_unknown()) {
      return DataSliceImpl::CreateEmptyAndUnknownType(slice.size());
    }
    if (slice.dtype() != arolla::GetQType<internal::ObjectId>()) {
      return absl::InvalidArgumentError(
          "cannot use encode_itemid on primitives");
    }
    auto op = arolla::CreateDenseOp(
        [&](ObjectId id) { return EncodeBase62(id.ToRawInt128()); });
    arolla::DenseArray<arolla::Text> res =
        op(slice.values<internal::ObjectId>());
    return DataSliceImpl::Create(std::move(res));
  }
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_ITEMID_H_
