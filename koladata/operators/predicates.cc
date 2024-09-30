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
#include "koladata/operators/predicates.h"

#include <type_traits>

#include "absl/functional/overload.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"

namespace koladata::ops {

namespace {

template <typename T>
struct IsPrimitiveT : std::true_type {};

template <>
struct IsPrimitiveT<internal::ObjectId> : std::false_type {};

}  // namespace

absl::StatusOr<DataSlice> IsPrimitive(const DataSlice& x) {
  if (x.GetSchema().IsPrimitiveSchema()) {
    return DataSlice::Create(
        internal::DataItem(arolla::Unit()),
                           internal::DataItem(schema::kMask), nullptr);
  }
  bool contains_only_primitives = x.VisitImpl(absl::Overload(
      [](const internal::DataItem& item) {
        return item.VisitValue([]<class T>(const T& value) {
          return IsPrimitiveT<T>::value;
        });
      },
      [](const internal::DataSliceImpl& slice) {
        bool res = true;
        slice.VisitValues([&]<class T>(const arolla::DenseArray<T>& values) {
          res &= IsPrimitiveT<T>::value;
        });
        return res;
      }));
  return DataSlice::Create(
      internal::DataItem(arolla::OptionalUnit(contains_only_primitives)),
      internal::DataItem(schema::kMask), nullptr);
}

}  // namespace koladata::ops
