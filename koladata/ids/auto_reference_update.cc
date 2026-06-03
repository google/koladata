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
#include "koladata/ids/auto_reference_update.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/util/status_macros_backport.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/op_utils/auto_values_update.h"

namespace koladata::ids {

absl::StatusOr<DataBagPtr> AutoReferenceUpdate(const DataSlice& ds,
                                               const DataSlice& input_ds) {
  const auto& db = ds.GetBag();
  const auto schema = ds.GetSchema();
  const auto& schema_impl = schema.impl<internal::DataItem>();
  const auto input_schema = input_ds.GetSchema();
  const auto& input_schema_impl = input_schema.impl<internal::DataItem>();
  if (db == nullptr) {
    return absl::InvalidArgumentError(
        "cannot auto_reference_update without a DataBag");
  }
  const auto& input_db = input_ds.GetBag();
  if (input_db == nullptr) {
    return absl::InvalidArgumentError(
        "cannot auto_reference_update without a DataBag for input");
  }
  const auto& db_impl = db->GetImpl();
  const auto& input_db_impl = input_db->GetImpl();
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks_span = fb_finder.GetFlattenFallbacks();
  FlattenFallbackFinder input_fb_finder(*input_db);
  auto input_fallbacks_span = input_fb_finder.GetFlattenFallbacks();
  return ds.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataBagPtr> {
    return input_ds.VisitImpl(
        [&](const auto& input_impl) -> absl::StatusOr<DataBagPtr> {
          auto auto_values_db = DataBag::EmptyMutable();
          ASSIGN_OR_RETURN(auto& auto_values_db_impl,
                           auto_values_db->GetMutableImpl());
          internal::AutoReferenceUpdateOp assign_op(&auto_values_db_impl);
          RETURN_IF_ERROR(assign_op(impl, schema_impl, db_impl, fallbacks_span,
                                    input_impl, input_schema_impl,
                                    input_db_impl, input_fallbacks_span));
          auto_values_db->UnsafeMakeImmutable();
          return auto_values_db;
        });
  });
}

}  // namespace koladata::ids
