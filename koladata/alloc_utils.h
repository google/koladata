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
#ifndef KOLADATA_ALLOC_UTILS_H_
#define KOLADATA_ALLOC_UTILS_H_

#include <cstdint>
#include <utility>

#include "absl/functional/overload.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"

namespace koladata {

// Allocates new ObjectId(s) (with type determined with alloc_single_fn and
// alloc_many_fn) with the same shape and sparsity as `shape_and_mask_from`.
// NOTE: The capacity of allocations is equal to the number of present items in
// `shape_and_mask_from`.
template <typename AllocateSingleFn, typename AllocateManyFn>
absl::StatusOr<DataSlice> AllocateLike(const DataSlice& shape_and_mask_from,
                                       AllocateSingleFn alloc_single_fn,
                                       AllocateManyFn alloc_many_fn,
                                       const internal::DataItem& schema,
                                       const DataBagPtr& db = nullptr) {
  return shape_and_mask_from.VisitImpl(absl::Overload(
      [&](const internal::DataItem& item) {
        return DataSlice::Create(item.has_value()
                                     ? internal::DataItem(alloc_single_fn())
                                     : internal::DataItem(),
                                     schema, db);
      },
      [&](const internal::DataSliceImpl& slice) {
        auto alloc_id = alloc_many_fn(slice.present_count());
        arolla::DenseArrayBuilder<internal::ObjectId> result_impl_builder(
            slice.size());
        int64_t i = 0;
        slice.VisitValues([&](const auto& array) {
          array.ForEachPresent([&](int64_t id, const auto& _) {
            result_impl_builder.Set(id, alloc_id.ObjectByOffset(i++));
          });
        });
        return DataSlice::Create(
            internal::DataSliceImpl::CreateObjectsDataSlice(
                std::move(result_impl_builder).Build(),
                internal::AllocationIdSet(alloc_id)),
            shape_and_mask_from.GetShape(),
            schema, db);
      }));
}

}  // namespace koladata

#endif  // KOLADATA_ALLOC_UTILS_H_
