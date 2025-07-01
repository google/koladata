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
#ifndef KOLADATA_INTERNAL_OP_UTILS_DEEP_EQUIVALENT_H_
#define KOLADATA_INTERNAL_OP_UTILS_DEEP_EQUIVALENT_H_

#include <cstddef>
#include <string>
#include <vector>

#include "absl/log/check.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"

namespace koladata::internal {

class DeepEquivalentOp {
 public:
  explicit DeepEquivalentOp(DataBagImpl* new_databag)
      : new_databag_(new_databag) {}

  absl::StatusOr<DataSliceImpl> operator()(
      const DataSliceImpl& lhs_ds, const DataItem& lhs_schema,
      const DataBagImpl& lhs_databag, DataBagImpl::FallbackSpan lhs_fallbacks,
      const DataSliceImpl& rhs_ds, const DataItem& rhs_schema,
      const DataBagImpl& rhs_databag,
      DataBagImpl::FallbackSpan rhs_fallbacks) const;

  absl::StatusOr<std::vector<std::string>> GetDiffPaths(
      const DataSliceImpl& ds, const DataItem& schema,
      const DataBagImpl& databag, DataBagImpl::FallbackSpan fallbacks,
      size_t max_count = 5) const;

 private:
  DataBagImpl* new_databag_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_DEEP_EQUIVALENT_H_
