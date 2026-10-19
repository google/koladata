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
#include "koladata/contrib/get_attr_or_item.h"

#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/operators/utils.h"
#include "koladata/schema_utils.h"

namespace koladata::contrib {
namespace {

// Looks up a scalar `key` in a DataSlice `ds` with an entity, dict, or list
// schema.
absl::StatusOr<DataSlice> LookupInStructSlice(const DataSlice& ds,
                                              const DataSlice& key) {
  if (ds.GetBag() == nullptr) {
    return absl::InvalidArgumentError("cannot lookup without a DataBag");
  }
  const DataSlice schema = ds.GetSchema();
  if (schema.IsDictSchema()) {
    return ds.GetFromDict(key);
  }
  if (schema.IsListSchema()) {
    return ds.GetFromList(key);
  }
  ASSIGN_OR_RETURN(absl::string_view attr, ops::GetStringArgument(key, "key"));
  return ds.GetAttr(attr);
}

}  // namespace

absl::StatusOr<DataSlice> GetAttrOrItem(const DataSlice& ds,
                                        const DataSlice& key) {
  if (!key.is_item()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("argument `key` must be an item, got a slice of "
                        "rank %d > 0",
                        key.GetShape().rank()));
  }
  const internal::DataItem& ds_schema = ds.GetSchemaImpl();
  if (ds_schema == schema::kNone) {
    return ds;
  }
  if (ds_schema == schema::kObject) {
    return absl::UnimplementedError(
        "lookup in DataSlices with OBJECT schema is not supported yet");
  }
  if (ds_schema.is_struct_schema()) {
    return LookupInStructSlice(ds, key);
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "get_attr_or_item requires DataSlice to have entity, dict, list, or "
      "OBJECT schema, got %s",
      DescribeSliceSchema(ds)));
}

}  // namespace koladata::contrib
