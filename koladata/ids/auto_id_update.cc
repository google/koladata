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
#include "koladata/ids/auto_id_update.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/util/status_macros_backport.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/op_utils/auto_values_update.h"

namespace koladata::ids {

absl::StatusOr<DataBagPtr> AutoIdUpdate(const DataSlice& ds) {
  const auto schema = ds.GetSchema();
  const auto& schema_impl = schema.impl<internal::DataItem>();
  const auto& db = ds.GetBag();
  if (db == nullptr) {
    return absl::InvalidArgumentError(
        "cannot auto_id_update without a DataBag");
  }
  const auto& db_impl = db->GetImpl();
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks_span = fb_finder.GetFlattenFallbacks();
  return ds.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataBagPtr> {
    auto auto_values_db = DataBag::EmptyMutable();
    ASSIGN_OR_RETURN(auto& auto_values_db_impl,
                     auto_values_db->GetMutableImpl());
    internal::AutoIdsUpdateOp auto_ids_op(&auto_values_db_impl);
    RETURN_IF_ERROR(
        auto_ids_op(impl, schema_impl, db_impl, fallbacks_span));
    auto_values_db->UnsafeMakeImmutable();
    return auto_values_db;
  });
}



}  // namespace koladata::ids
