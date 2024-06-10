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
#ifndef KOLADATA_OPERATORS_COLLAPSE_H_
#define KOLADATA_OPERATORS_COLLAPSE_H_

#include <cstddef>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/op_utils/collapse.h"

namespace koladata::ops {

// kde.logical.collapse.
inline absl::StatusOr<DataSlice> Collapse(const DataSlice& ds) {
  const auto& shape = ds.GetShape();
  size_t rank = shape.rank();
  if (rank == 0) {
    return absl::InvalidArgumentError(
        "kd.collapse is not supported for DataItem.");
  }
  return DataSlice::Create(
      internal::CollapseOp()(ds.slice(), shape.edges().back()),
      shape.RemoveDims(rank - 1), ds.GetSchemaImpl(), ds.GetDb());
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_COLLAPSE_H_
