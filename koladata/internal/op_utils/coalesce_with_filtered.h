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
#ifndef KOLADATA_INTERNAL_OP_UTILS_COALESCE_WITH_FILTERED_H_
#define KOLADATA_INTERNAL_OP_UTILS_COALESCE_WITH_FILTERED_H_

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "koladata/internal/op_utils/has.h"
#include "koladata/internal/op_utils/presence_and.h"
#include "koladata/internal/op_utils/presence_or.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

// Function for `left | (right & has(filter))`.
template <typename ImplT>
absl::StatusOr<ImplT> CoalesceWithFiltered(const ImplT& filter,
                                           const ImplT& left,
                                           const ImplT& right) {
  ASSIGN_OR_RETURN(auto filter_mask, HasOp()(filter));
  ASSIGN_OR_RETURN(auto right_filtered, PresenceAndOp()(right, filter_mask));
  return PresenceOrOp</*disjoint=*/false>()(left, right_filtered);
}

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_COALESCE_WITH_FILTERED_H_
