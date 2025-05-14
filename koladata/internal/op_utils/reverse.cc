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
#include "koladata/internal/op_utils/reverse.h"

#include <cstddef>
#include <cstdint>
#include <utility>

#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/util/view_types.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::internal {

DataSliceImpl ReverseOp::operator()(
    const DataSliceImpl& ds_impl,
    const arolla::JaggedDenseArrayShape& ds_shape) {
  if (ds_shape.rank() == 0 || ds_impl.is_empty_and_unknown()) {
    return ds_impl;
  }
  absl::Span<const int64_t> split_points =
      ds_shape.edges().back().edge_values().values.span();
  SliceBuilder builder(ds_impl.size());
  ds_impl.VisitValues([&]<class T>(const arolla::DenseArray<T>& values) {
    arolla::DenseArrayBuilder<T> values_builder(values.size());
    size_t split_point_id = 0;
    values.ForEachPresent([&](int64_t offset, arolla::view_type_t<T> value) {
      while (split_points[split_point_id + 1] <= offset) {
        ++split_point_id;
      }
      int64_t new_offset = split_points[split_point_id + 1] - offset - 1;
      values_builder.Set(split_points[split_point_id] + new_offset, value);
    });
    auto new_values = std::move(values_builder).Build();
    builder.InsertIfNotSet<T>(new_values.bitmap, {}, new_values.values);
  });
  return std::move(builder).Build();
}

}  // namespace koladata::internal
