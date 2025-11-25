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
#ifndef KOLADATA_INTERNAL_OP_UTILS_DEEP_SCHEMA_COMPATIBLE_H_
#define KOLADATA_INTERNAL_OP_UTILS_DEEP_SCHEMA_COMPATIBLE_H_

#include <cstddef>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/functional/function_ref.h"
#include "absl/log/check.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/op_utils/traverse_helper.h"

namespace koladata::internal {

// Checks compatibility of two schemas.
//
// Also fills the databag with the result of the schemas comparison. This
// databag can be used to get the details of the differences between the
// schemas.
//
// The result is stored in the new_databag_. An additional method is provided
// to flatten the result to a list of paths.
class DeepSchemaCompatibleOp {
 public:
  struct DiffItem {
    std::vector<TraverseHelper::TransitionKey> path;
    DataItem item;
    DataItem schema;
  };

  struct SchemaCompatibleParams {
    // If true, check only attributes present in to_schema.
    bool partial = true;
  };

  // A callback function that can be used to check if two DataItems are
  // compatible.
  using CompatibleCallback =
      absl::FunctionRef<bool(const DataItem&, const DataItem&)>;

  explicit DeepSchemaCompatibleOp(
      DataBagImpl* new_databag, SchemaCompatibleParams params,
      CompatibleCallback callback ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : new_databag_(new_databag), params_(params), callback_(callback) {}

  // Returns a pair of a boolean value indicating if the schemas are compatible
  // and a DataItem, that can be traversed with new_databag_.
  absl::StatusOr<std::pair<bool, DataItem>> operator()(
      const DataItem& from_schema, const DataBagImpl& from_databag,
      DataBagImpl::FallbackSpan from_fallbacks, const DataItem& to_schema,
      const DataBagImpl& to_databag,
      DataBagImpl::FallbackSpan to_fallbacks) const;

  // For the provided item, returns the paths in the new_databag_ that leads to
  // the diff items.
  absl::StatusOr<std::vector<DiffItem>> GetDiffPaths(
      const DataItem& item, const DataItem& schema, size_t max_count = 5) const;

 private:
  DataBagImpl* new_databag_;
  SchemaCompatibleParams params_;
  CompatibleCallback callback_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_DEEP_SCHEMA_COMPATIBLE_H_
