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
#include "koladata/ids/auto_id_cleanup_update.h"


#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/op_utils/auto_values_update.h"

namespace koladata::ids {

absl::StatusOr<DataBagPtr> AutoIdCleanupUpdate(const DataSlice& ds) {
  auto db = ds.GetBag();
  if (db == nullptr) {
    return absl::InvalidArgumentError(
        "cannot auto_id_cleanup_update without a DataBag");
  }
  const auto& db_impl = db->GetImpl();
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks_span = fb_finder.GetFlattenFallbacks();
  return ds.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataBagPtr> {
    auto update_db = DataBag::EmptyMutable();
    ASSIGN_OR_RETURN(auto& update_db_impl, update_db->GetMutableImpl());
    internal::AutoIdCleanupUpdateOp auto_id_cleanup_op(&update_db_impl);
    RETURN_IF_ERROR(auto_id_cleanup_op(impl, ds.GetSchemaImpl(), db_impl,
                                       fallbacks_span));
    update_db->UnsafeMakeImmutable();
    return update_db;
  });
}

}  // namespace koladata::ids
