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
#ifndef KOLADATA_OPERATORS_LOGICAL_H_
#define KOLADATA_OPERATORS_LOGICAL_H_

#include <utility>

#include "absl/status/statusor.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_op.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/has.h"
#include "koladata/internal/op_utils/presence_and.h"
#include "koladata/internal/op_utils/presence_or.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

// kde.core.apply_mask.
inline absl::StatusOr<DataSlice> ApplyMask(const DataSlice& obj,
                                           const DataSlice& mask) {
  return DataSliceOp<internal::PresenceAndOp>()(obj, mask, obj.GetSchemaImpl(),
                                                obj.GetDb());
}

// kde.logical.coalesce.
inline absl::StatusOr<DataSlice> Coalesce(const DataSlice& x,
                                          const DataSlice& y) {
  auto res_db = DataBag::CommonDataBag({x.GetDb(), y.GetDb()});
  ASSIGN_OR_RETURN(auto aligned_slices, AlignSchemas({x, y}));
  return DataSliceOp<internal::PresenceOrOp>()(
      aligned_slices.slices[0], aligned_slices.slices[1],
      aligned_slices.common_schema, std::move(res_db));
}

// kde.logical.has.
inline absl::StatusOr<DataSlice> Has(const DataSlice& obj) {
  return DataSliceOp<internal::HasOp>()(
      obj, obj.GetShapePtr(), internal::DataItem(schema::kMask), obj.GetDb());
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_LOGICAL_H_
