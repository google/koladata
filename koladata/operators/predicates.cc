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

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/missing_value.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace {

template<typename T>
bool IsPrimitiveValue() {
  if constexpr(std::is_same_v<T, internal::MissingValue>) {
    return true;
  } else {
    return arolla::meta::contains_v<schema::supported_primitive_dtypes,
                                    T>;
  }
}

}  // namespace

absl::StatusOr<DataSlice> IsPrimitive(const DataSlice& x) {
  if (x.GetSchema().IsPrimitiveSchema()) {
    return DataSlice::Create(
        internal::DataItem(arolla::Unit()),
                           internal::DataItem(schema::kMask), nullptr);
  }
  return x.VisitImpl(
      []<class Impl>(const Impl& impl) -> absl::StatusOr<DataSlice> {
        if constexpr (std::is_same_v<Impl, internal::DataItem>) {
          return impl.VisitValue([]<class T>(const T& value) {
            return DataSlice::Create(
                internal::DataItem(
                    arolla::OptionalUnit(IsPrimitiveValue<T>())),
                internal::DataItem(schema::kMask), nullptr);
          });
        } else {
          bool contains_only_primitives = true;
          RETURN_IF_ERROR(
              impl.VisitValues([&contains_only_primitives]<class T>(
                                   const arolla::DenseArray<T>& values) {
                if (!IsPrimitiveValue<T>()) {
                  contains_only_primitives = false;
                }
                return absl::OkStatus();
              }));
          return DataSlice::Create(internal::DataItem(arolla::OptionalUnit(
                                       contains_only_primitives)),
                                   internal::DataItem(schema::kMask), nullptr);
        }
      });
}

}  // namespace koladata::ops
