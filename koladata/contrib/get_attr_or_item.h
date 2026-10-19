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
#ifndef KOLADATA_CONTRIB_GET_ATTR_OR_ITEM_H_
#define KOLADATA_CONTRIB_GET_ATTR_OR_ITEM_H_

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"

namespace koladata::contrib {

// Looks up a scalar key in a DataSlice of entities, dicts, lists, or OBJECTs.
// See the docstring of kd_ext.contrib.get_attr_or_item for details.
//
// kd_ext.contrib.get_attr_or_item
absl::StatusOr<DataSlice> GetAttrOrItem(const DataSlice& ds,
                                        const DataSlice& key);

}  // namespace koladata::contrib

#endif  // KOLADATA_CONTRIB_GET_ATTR_OR_ITEM_H_
