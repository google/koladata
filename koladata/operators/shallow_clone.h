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
#ifndef KOLADATA_OPERATORS_SHALLOW_CLONE_H_
#define KOLADATA_OPERATORS_SHALLOW_CLONE_H_

#include <utility>
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/op_utils/extract.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

// kde.core._shallow_clone_with_schema
inline absl::StatusOr<DataSlice> ShallowCloneWithSchema(
    const DataSlice& ds, const DataSlice& schema) {
  const auto& db = ds.GetDb();
  if (db == nullptr) {
    return absl::InvalidArgumentError("cannot clone without a DataBag");
  }
  const auto& schema_db = schema.GetDb();
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  const auto& schema_impl = schema.impl<internal::DataItem>();
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks_span = fb_finder.GetFlattenFallbacks();
  return ds.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    internal::DataBagImplPtr result_db_impl;
    auto result_slice_impl = impl;
    internal::DataItem result_schema_impl;
    if (schema_db == nullptr || schema_db == db) {
      ASSIGN_OR_RETURN(
          std::tie(result_db_impl, result_slice_impl, result_schema_impl),
          internal::ShallowCloneOp()(impl, schema_impl, db->GetImpl(),
                                     fallbacks_span));
    } else {
      FlattenFallbackFinder schema_fb_finder(*schema_db);
      auto schema_fallbacks_span = schema_fb_finder.GetFlattenFallbacks();
      ASSIGN_OR_RETURN(
          std::tie(result_db_impl, result_slice_impl, result_schema_impl),
          internal::ShallowCloneOp()(impl, schema_impl, db->GetImpl(),
                                     fallbacks_span, schema_db->GetImpl(),
                                     schema_fallbacks_span));
    }
    const auto result_db = DataBag::FromImpl(std::move(result_db_impl));
    return DataSlice::Create(result_slice_impl, ds.GetShape(),
                             result_schema_impl, result_db);
  });
}

// kde.core._shallow_clone
inline absl::StatusOr<DataSlice> ShallowClone(const DataSlice& ds) {
  return ShallowCloneWithSchema(ds, ds.GetSchema());
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_SHALLOW_CLONE_H_
