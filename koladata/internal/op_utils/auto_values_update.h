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
#ifndef KOLADATA_INTERNAL_OP_UTILS_AUTO_VALUES_UPDATE_H_
#define KOLADATA_INTERNAL_OP_UTILS_AUTO_VALUES_UPDATE_H_

#include <string_view>

#include "absl/status/status.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"

namespace koladata::internal {

// kd.core.auto_id_update operator.
class AutoIdsUpdateOp {
 public:
  static constexpr std::string_view kAutoIdPrefix = "__AUTO_ID__";

  explicit AutoIdsUpdateOp(DataBagImpl* new_databag)
      : new_databag_(new_databag) {}

  absl::Status operator()(const DataSliceImpl& ds, const DataItem& schema,
                          const DataBagImpl& databag,
                          DataBagImpl::FallbackSpan fallbacks = {}) const;

  absl::Status operator()(const DataItem& item, const DataItem& schema,
                          const DataBagImpl& databag,
                          DataBagImpl::FallbackSpan fallbacks = {}) const;

 private:
  DataBagImpl* new_databag_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_AUTO_VALUES_UPDATE_H_
