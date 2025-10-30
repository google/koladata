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
#ifndef KOLADATA_INTERNAL_OP_UTILS_SELECT_H_
#define KOLADATA_INTERNAL_OP_UTILS_SELECT_H_

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "koladata/internal/data_slice.h"

namespace koladata::internal {

// Selects elements in the first argument if the filter mask is present and
// filters out missing items.
struct SelectOp {
  struct Result {
    DataSliceImpl data_slice_impl;
    arolla::JaggedDenseArrayShape shape;
  };

  absl::StatusOr<Result> operator()(
      const DataSliceImpl& ds_impl,
      const arolla::JaggedDenseArrayShape& ds_shape,
      const DataSliceImpl& filter,
      const arolla::JaggedDenseArrayShape& filter_shape);
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_SELECT_H_
