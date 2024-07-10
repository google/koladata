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
#ifndef KOLADATA_OPERATORS_NOFOLLOW_H_
#define KOLADATA_OPERATORS_NOFOLLOW_H_

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

// kde.core.get_nofollowed_schema.
inline absl::StatusOr<DataSlice> GetNoFollowedSchema(
    const DataSlice& schema_ds) {
  return schema_ds.GetNoFollowedSchema();
}

// kde.core.follow.
inline absl::StatusOr<DataSlice> Follow(const DataSlice& ds) {
  ASSIGN_OR_RETURN(auto nofollowed_schema_item,
                   schema::GetNoFollowedSchemaItem(ds.GetSchemaImpl()));
  return ds.WithSchema(nofollowed_schema_item);
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_NOFOLLOW_H_
