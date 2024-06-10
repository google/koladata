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
#ifndef KOLADATA_OPERATORS_SELECT_H_
#define KOLADATA_OPERATORS_SELECT_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/reverse_select.h"
#include "koladata/internal/op_utils/select.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

// kde.core.select.
inline absl::StatusOr<DataSlice> Select(const DataSlice& ds,
                                        const DataSlice& filter,
                                        const bool expand_filter) {
  const internal::DataItem& schema = filter.GetSchemaImpl();

  if (schema != schema::kAny && schema != schema::kObject &&
      schema != schema::kMask) {
    return absl::InvalidArgumentError(
        "the schema of the filter DataSlice should only be Any, Object or "
        "Mask");
  }
  const DataSlice::JaggedShapePtr& fltr_shape =
      expand_filter ? ds.GetShapePtr() : filter.GetShapePtr();
  BroadcastHelper fltr(filter, fltr_shape);
  RETURN_IF_ERROR(fltr.status());
  return ds.VisitImpl([&](const auto& ds_impl) {
    return fltr->VisitImpl([&](const auto& filter_impl)
                               -> absl::StatusOr<DataSlice> {
      ASSIGN_OR_RETURN((auto [result_ds, result_shape]),
                       internal::SelectOp()(ds_impl, ds.GetShapePtr(),
                                            filter_impl, fltr->GetShapePtr()));
      return DataSlice::Create(std::move(result_ds), std::move(result_shape),
                               ds.GetSchemaImpl(), ds.GetDb());
    });
  });
}

// kde.core.reverse_select.
inline absl::StatusOr<DataSlice> ReverseSelect(const DataSlice& ds,
                                               const DataSlice& filter) {
  const internal::DataItem& schema = filter.GetSchemaImpl();

  if (schema != schema::kAny && schema != schema::kObject &&
      schema != schema::kMask) {
    return absl::InvalidArgumentError(
        "the schema of the filter DataSlice should only be Any, Object or "
        "Mask");
  }
  auto ds_shape = ds.GetShapePtr();
  auto filter_shape = filter.GetShapePtr();
  if (ds_shape->rank() != filter_shape->rank()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "the rank of the ds and filter DataSlice must be the same. Got "
        "rank(ds): ",
        ds_shape->rank(), ", rank(filter): ", filter_shape->rank()));
  }
  return ds.VisitImpl([&](const auto& ds_impl) {
    return filter.VisitImpl(
        [&](const auto& filter_impl) -> absl::StatusOr<DataSlice> {
          ASSIGN_OR_RETURN(
              auto res, internal::ReverseSelectOp()(ds_impl, ds_shape,
                                                    filter_impl, filter_shape));
          return DataSlice::Create(std::move(res), filter_shape,
                                   ds.GetSchemaImpl(), ds.GetDb());
        });
  });
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_SELECT_H_
