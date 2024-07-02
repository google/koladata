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
#ifndef KOLADATA_DATA_SLICE_OP_H_
#define KOLADATA_DATA_SLICE_OP_H_

#include <memory>
#include <type_traits>
#include <utility>

#include "absl/status/statusor.h"
#include "koladata/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/shape_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {

// `OpImpl` must be defined for both orders (DataSliceImpl, DataItem) and
// (DataItem, DataSliceImpl) in order to not broadcast scalars to highest rank.
template <class OpImpl, class = void, class = void>
struct IsDefinedOnMixedImpl : std::false_type {};

template <class OpImpl>
struct IsDefinedOnMixedImpl<
    OpImpl,
    std::void_t<decltype(OpImpl()(std::declval<internal::DataSliceImpl>(),
                                  std::declval<internal::DataItem>()))>,
    std::void_t<decltype(OpImpl()(std::declval<internal::DataItem>(),
                                  std::declval<internal::DataSliceImpl>()))>>
    : std::true_type {};

// Utility to invoke operator functors on DataSlice args. It dispatches the
// functor to a correct implementation, depending on what implementations
// DataSlices hold.
template <class OpImpl>
struct DataSliceOp {
  // Use for operators accepting a single DataSlice as argument.
  // ArgType&& args... are passed unmodified to the underlying OpImpl.
  template <class DataSlice, class... ArgType>
  absl::StatusOr<DataSlice> operator()(const DataSlice& ds,
                                       DataSlice::JaggedShape shape,
                                       internal::DataItem schema,
                                       std::shared_ptr<DataBag> db,
                                       ArgType&&... args) {
    return ds.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
      return DataSlice::Create(OpImpl()(impl, std::forward<ArgType>(args)...),
                               std::move(shape), std::move(schema),
                               std::move(db));
    });
  }

  // Use for operators accepting 2 DataSlices.
  // ArgType&& args... are passed unmodified to the underlying OpImpl.
  template <class DataSlice, class... ArgType>
  absl::StatusOr<DataSlice> operator()(const DataSlice& ds_1,
                                       const DataSlice& ds_2,
                                       internal::DataItem schema,
                                       std::shared_ptr<DataBag> db,
                                       ArgType&&... args) {
    if constexpr (IsDefinedOnMixedImpl<OpImpl>::value) {
      ASSIGN_OR_RETURN(auto aligned_inputs,
                       shape::AlignNonScalars({ds_1, ds_2}));
      const auto& aligned_ds_1 = aligned_inputs.first[0];
      const auto& aligned_ds_2 = aligned_inputs.first[1];
      return aligned_ds_1.VisitImpl([&](const auto& impl_1) {
        return aligned_ds_2.VisitImpl([&](const auto& impl_2) {
          return DataSlice::Create(
              OpImpl()(impl_1, impl_2, std::forward<ArgType>(args)...),
              std::move(aligned_inputs.second), std::move(schema),
              std::move(db));
        });
      });
    } else {
      ASSIGN_OR_RETURN(auto aligned_inputs, shape::Align({ds_1, ds_2}));
      const auto& aligned_ds_1 = aligned_inputs[0];
      return aligned_ds_1.VisitImpl([&](const auto& impl_1) {
        using ImplT = typename std::decay_t<decltype(impl_1)>;
        const auto& impl_2 = aligned_inputs[1].template impl<ImplT>();
        return DataSlice::Create(
            OpImpl()(impl_1, impl_2, std::forward<ArgType>(args)...),
            aligned_ds_1.GetShape(), std::move(schema), std::move(db));
      });
    }
  }
};

}  // namespace koladata

#endif  // KOLADATA_DATA_SLICE_OP_H_
