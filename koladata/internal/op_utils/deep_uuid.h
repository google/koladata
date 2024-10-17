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
#ifndef KOLADATA_INTERNAL_OP_UTILS_DEEP_UUID_H_
#define KOLADATA_INTERNAL_OP_UTILS_DEEP_UUID_H_

#include <utility>
#include "absl/status/statusor.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"

namespace koladata::internal {

// Recursively computes uuid for the given slice.
class DeepUuidOp {
 public:
  explicit DeepUuidOp() = default;

  absl::StatusOr<DataSliceImpl> operator()(
      const DataItem& seed, const DataSliceImpl& ds, const DataItem& schema,
      const DataBagImpl& databag,
      DataBagImpl::FallbackSpan fallbacks = {}) const;

  absl::StatusOr<DataItem> operator()(
      const DataItem& seed, const DataItem& item, const DataItem& schema,
      const DataBagImpl& databag,
      DataBagImpl::FallbackSpan fallbacks = {}) const;
};

}  // namespace koladata::internal


#endif  // KOLADATA_INTERNAL_OP_UTILS_DEEP_UUID_H_
