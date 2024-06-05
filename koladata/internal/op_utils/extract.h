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
#ifndef KOLADATA_INTERNAL_OP_UTILS_EXTRACT_H_
#define KOLADATA_INTERNAL_OP_UTILS_EXTRACT_H_

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"

namespace koladata::internal {

// Extracts DataSliceImpl / DataItem.
struct ExtractOp {
 public:
  absl::StatusOr<DataBagImplPtr> operator()(
      const DataSliceImpl& ds, const DataItem& schema,
      const DataBagImpl& databag,
      DataBagImpl::FallbackSpan fallbacks = {}) const;

  absl::StatusOr<DataBagImplPtr> operator()(
      const DataItem& item, const DataItem& schema, const DataBagImpl& databag,
      DataBagImpl::FallbackSpan fallbacks = {}) const;

  absl::StatusOr<DataBagImplPtr> operator()(
      const DataSliceImpl& ds, const DataItem& schema,
      const DataBagImpl& databag, DataBagImpl::FallbackSpan fallbacks,
      const DataBagImpl& schema_databag,
      DataBagImpl::FallbackSpan schema_fallbacks) const;

  absl::StatusOr<DataBagImplPtr> operator()(
      const DataItem& item, const DataItem& schema, const DataBagImpl& databag,
      DataBagImpl::FallbackSpan fallbacks, const DataBagImpl& schema_databag,
      DataBagImpl::FallbackSpan schema_fallbacks) const;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_EXTRACT_H_
