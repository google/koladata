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
#ifndef KOLADATA_OPERATORS_EXTRACT_H_
#define KOLADATA_OPERATORS_EXTRACT_H_

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

// kde.core._extract_with_schema
inline absl::StatusOr<DataSlice> ExtractWithSchema(const DataSlice& ds,
                                                   const DataSlice& schema) {
  const auto& db = ds.GetDb();
  if (db == nullptr) {
    return absl::InvalidArgumentError("cannot extract without a DataBag");
  }
  const auto& schema_db = schema.GetDb();
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  const auto& schema_impl = schema.impl<internal::DataItem>();
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks_span = fb_finder.GetFlattenFallbacks();
  return ds.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    internal::DataBagImplPtr result_db_impl;
    if (schema_db == nullptr || schema_db == db) {
      ASSIGN_OR_RETURN(result_db_impl,
                       internal::ExtractOp()(impl, schema_impl, db->GetImpl(),
                                             fallbacks_span));
    } else {
      FlattenFallbackFinder schema_fb_finder(*schema_db);
      auto schema_fallbacks_span = schema_fb_finder.GetFlattenFallbacks();
      ASSIGN_OR_RETURN(result_db_impl,
                       internal::ExtractOp()(
                           impl, schema_impl, db->GetImpl(), fallbacks_span,
                           schema_db->GetImpl(), schema_fallbacks_span));
    }
    const auto result_db = DataBag::FromImpl(std::move(result_db_impl));
    return DataSlice::Create(impl, ds.GetShape(), schema_impl, result_db);
  });
}

// kde.core._extract
inline absl::StatusOr<DataSlice> Extract(const DataSlice& ds) {
  return ExtractWithSchema(ds, ds.GetSchema());
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_EXTRACT_H_
