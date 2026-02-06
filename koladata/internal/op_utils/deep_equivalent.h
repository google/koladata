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
#include <vector>

#include "absl/log/check.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/op_utils/deep_diff.h"

namespace koladata::internal {

// Compares two slices and fills the databag with the result of the comparison.
//
// It builds (with the new ObjectIds) a common structure for the lhs and rhs
// slices. For each difference found, it creates a special object with the diff
// details.
//
// The result is stored in the new_databag_. And additional method is provided
// to flatten the result to a list of paths.
class DeepEquivalentOp {
 public:
  struct DeepEquivalentParams {
    // If true, only the attributes present in the expected_value are compared.
    bool partial = false;
    // If true, the schema ObjectIds are compared.
    bool schemas_equality = false;
    // If true, the ObjectIds are compared.
    bool ids_equality = false;
  };

  explicit DeepEquivalentOp(DataBagImpl* new_databag,
                            DeepEquivalentParams params)
      : new_databag_(new_databag), params_(params) {}

  // With provided lhs and rhs items, fills the new_databag_ with the result of
  // the comparison.
  // Returns a DataSliceImpl, that can be traversed with new_databag_.
  absl::StatusOr<DataSliceImpl> operator()(
      const DataSliceImpl& lhs_ds, const DataItem& lhs_schema,
      const DataBagImpl& lhs_databag, DataBagImpl::FallbackSpan lhs_fallbacks,
      const DataSliceImpl& rhs_ds, const DataItem& rhs_schema,
      const DataBagImpl& rhs_databag,
      DataBagImpl::FallbackSpan rhs_fallbacks) const;

  // With provided lhs and rhs items, fills the new_databag_ with the result of
  // the comparison.
  // Returns a DataItem, that can be traversed with new_databag_.
  absl::StatusOr<DataItem> operator()(
      const DataItem& lhs_item, const DataItem& lhs_schema,
      const DataBagImpl& lhs_databag, DataBagImpl::FallbackSpan lhs_fallbacks,
      const DataItem& rhs_item, const DataItem& rhs_schema,
      const DataBagImpl& rhs_databag,
      DataBagImpl::FallbackSpan rhs_fallbacks) const;

  // For the provided slice, returns the paths in the new_databag_ that leads to
  // the diff items.
  absl::StatusOr<std::vector<DeepDiff::DiffItem>> GetDiffPaths(
      const DataSliceImpl& ds, const DataItem& schema,
      size_t max_count = 5) const;

  // For the provided item, returns the paths in the new_databag_ that leads to
  // the diff items.
  absl::StatusOr<std::vector<DeepDiff::DiffItem>> GetDiffPaths(
      const DataItem& item, const DataItem& schema, size_t max_count = 5) const;

 private:
  DataBagImpl* new_databag_;
  DeepEquivalentParams params_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_DEEP_EQUIVALENT_H_
