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
#ifndef KOLADATA_SHAPE_SHAPE_UTILS_H_
#define KOLADATA_SHAPE_SHAPE_UTILS_H_

#include <functional>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::shape {

// Returns compile-time true value in case T is a std::reference_wrapper of a
// certain type
template <class T>
struct is_reference_wrapper : std::false_type {};

template <class T>
struct is_reference_wrapper<std::reference_wrapper<T>> : std::true_type {};

template <class T>
constexpr bool is_reference_wrapper_v = is_reference_wrapper<T>::value;

// Returns a const reference (const T&) to a value T from both
// std::reference_wrapper<T> and from T.
template <class T>
const auto& get_referred_value(const T& value) {
  if constexpr (is_reference_wrapper_v<T>) {
    return value.get();
  } else {
    return value;
  }
}

// Returns a shape with the highest rank. All shapes must be broadcastable to
// this resulting shape. In case they are not, the appropriate Status error is
// returned.
template <class T, class RefTOrT = T>
absl::StatusOr<typename T::JaggedShapePtr> GetCommonShape(
    const std::vector<RefTOrT>& slices) {
  if (slices.empty()) {
    return absl::InvalidArgumentError(
        "computing a common shape requires at least 1 input");
  }
  typename T::JaggedShapePtr shape = nullptr;
  for (const auto& slice : slices) {
    if (shape == nullptr ||
        shape->rank() < get_referred_value(slice).GetShape().rank()) {
      shape = get_referred_value(slice).GetShapePtr();
    }
  }
  DCHECK_NE(shape, nullptr);
  for (const auto& slice : slices) {
    if (!get_referred_value(slice).GetShape().IsBroadcastableTo(*shape)) {
      return absl::InvalidArgumentError("shapes are not compatible");
    }
  }
  return shape;
}

// Returns the collection of broadcasted DataSlices to a common shape, i.e. a
// shape with a highest rank (among the slices) that all slices are
// broadcastable to.
template <class T, class RefTOrT = T>
absl::StatusOr<std::vector<T>> Align(const std::vector<RefTOrT>& slices) {
  ASSIGN_OR_RETURN(auto shape, GetCommonShape<T>(slices));
  std::vector<T> aligned_slices;
  aligned_slices.reserve(slices.size());

  for (const auto& slice : slices) {
    ASSIGN_OR_RETURN(auto expanded_slice,
                     get_referred_value(slice).BroadcastToShape(shape));
    aligned_slices.push_back(std::move(expanded_slice));
  }
  return aligned_slices;
}

// Returns the collection of broadcasted DataSlices to a common shape, i.e. a
// shape with a highest rank (among the slices) that all slices are
// broadcastable to. Unlike `Align`, scalars (rank-0) are left as-is and are not
// broadcasted.
template <class T, class RefTOrT = T>
absl::StatusOr<std::pair<std::vector<T>, typename T::JaggedShapePtr>>
AlignNonScalars(const std::vector<RefTOrT>& slices) {
  ASSIGN_OR_RETURN(auto shape, GetCommonShape<T>(slices));
  std::vector<T> aligned_slices;
  aligned_slices.reserve(slices.size());
  for (const auto& slice : slices) {
    const auto& slice_v = get_referred_value(slice);
    if (slice_v.GetShape().rank() == 0) {
      aligned_slices.push_back(slice_v);
    } else {
      ASSIGN_OR_RETURN(auto expanded_slice, slice_v.BroadcastToShape(shape));
      aligned_slices.push_back(std::move(expanded_slice));
    }
  }
  return std::make_pair(std::move(aligned_slices), std::move(shape));
}

}  // namespace koladata::shape

#endif  // KOLADATA_SHAPE_SHAPE_UTILS_H_
