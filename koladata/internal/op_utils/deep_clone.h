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
#ifndef KOLADATA_INTERNAL_OP_UTILS_DEEP_CLONE_H_
#define KOLADATA_INTERNAL_OP_UTILS_DEEP_CLONE_H_

#include <utility>
#include "absl/status/statusor.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"

namespace koladata::internal {

// Creates a slice with a deep copy of the given slice and nothing else. All the
// reached objects get new ItemIds and their attributes are copied with updated
// references to the new ItemIds.
//
// If there are multiple references to one object in the given slice, or
// multiple ways to reach one object through the attributes, there will be
// exactly one clone made per input object.
//
// Returns a new DataSlice.
class DeepCloneOp {
 public:
  explicit DeepCloneOp(DataBagImpl* new_databag) : new_databag_(new_databag) {}

  absl::StatusOr<DataSliceImpl> operator()(
      const DataSliceImpl& ds, const DataItem& schema,
      const DataBagImpl& databag,
      DataBagImpl::FallbackSpan fallbacks = {}) const;

  absl::StatusOr<DataItem> operator()(
      const DataItem& item, const DataItem& schema, const DataBagImpl& databag,
      DataBagImpl::FallbackSpan fallbacks = {}) const;

 private:
  DataBagImpl* new_databag_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_DEEP_CLONE_H_
