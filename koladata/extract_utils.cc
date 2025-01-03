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
#include "koladata/extract_utils.h"

#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/op_utils/extract.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace extract_utils_internal {

absl::StatusOr<DataSlice> ExtractWithSchema(const DataSlice& ds,
                                            const DataSlice& schema) {
  const auto& db = ds.GetBag();
  if (db == nullptr) {
    return absl::InvalidArgumentError("cannot extract without a DataBag");
  }
  const auto& schema_db = schema.GetBag();
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  const auto& schema_impl = schema.impl<internal::DataItem>();
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks_span = fb_finder.GetFlattenFallbacks();
  return ds.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    auto result_db = DataBag::Empty();
    ASSIGN_OR_RETURN(auto result_db_impl, result_db->GetMutableImpl());
    internal::ExtractOp extract_op(&result_db_impl.get());
    if (schema_db != nullptr && schema_db != db) {
      FlattenFallbackFinder schema_fb_finder(*schema_db);
      auto schema_fallbacks = schema_fb_finder.GetFlattenFallbacks();
      RETURN_IF_ERROR(extract_op(
          impl, schema_impl, db->GetImpl(), std::move(fallbacks_span),
          &(schema_db->GetImpl()), std::move(schema_fallbacks)));
    } else {
      RETURN_IF_ERROR(extract_op(
          impl, schema_impl, db->GetImpl(), std::move(fallbacks_span),
          /*schema_databag=*/nullptr,
          /*schema_fallbacks=*/internal::DataBagImpl::FallbackSpan()));
    }
    result_db->UnsafeMakeImmutable();
    return DataSlice::Create(impl, ds.GetShape(), schema_impl, result_db);
  });
}

absl::StatusOr<DataSlice> Extract(const DataSlice& ds) {
  return ExtractWithSchema(ds, ds.GetSchema());
}

}  // namespace extract_utils_internal
}  // namespace koladata
