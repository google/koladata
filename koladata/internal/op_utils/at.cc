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
#include "koladata/internal/op_utils/at.h"

#include <strings.h>

#include <cstdint>
#include <optional>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/slice_builder.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

absl::StatusOr<DataSliceImpl> AtOp(const DataSliceImpl& ds,
                                   const arolla::DenseArray<int64_t>& indices) {
  if (ds.is_empty_and_unknown()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(indices.size());
  }

  auto process_dense_array = [&]<class T>(const arolla::DenseArray<T>& array)
      -> absl::StatusOr<arolla::DenseArray<T>> {
    // We cannot call DenseArrayAtOp since it does not support negative indices.
    int64_t size = array.size();
    auto at_fn =
        [&array,
         &size](int64_t id) -> arolla::OptionalValue<arolla::view_type_t<T>> {
      if (id < 0) {
        id += size;
      }
      if (id < 0 || id >= size || !array.present(id)) {
        return std::nullopt;
      }
      return array.values[id];
    };
    // We need to explicitly specify the template argument for
    // Text/string_view to work properly.
    return arolla::CreateDenseOp<decltype(at_fn), T>(at_fn)(indices);
  };

  // TODO: keep only necessary allocation ids.
  if (ABSL_PREDICT_TRUE(ds.is_single_dtype())) {
    DataSliceImpl res_impl;
    RETURN_IF_ERROR(ds.VisitValues(
        [&]<class T>(const arolla::DenseArray<T>& array) -> absl::Status {
          ASSIGN_OR_RETURN(auto res, process_dense_array(array));
          res_impl = DataSliceImpl::CreateWithAllocIds(ds.allocation_ids(),
                                                       std::move(res));
          return absl::OkStatus();
        }));
    return std::move(res_impl);
  } else {
    SliceBuilder builder(indices.size(), ds.allocation_ids());
    RETURN_IF_ERROR(ds.VisitValues(
        [&]<class T>(const arolla::DenseArray<T>& array) -> absl::Status {
          ASSIGN_OR_RETURN(auto res, process_dense_array(array));
          builder.InsertIfNotSet<T>(res.bitmap, {}, res.values);
          return absl::OkStatus();
        }));

    return std::move(builder).Build();
  }
}
}  // namespace koladata::internal
