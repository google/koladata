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
#ifndef KOLADATA_POINTWISE_UTILS_H
#define KOLADATA_POINTWISE_UTILS_H

#include <cstdint>
#include <utility>

#include "absl/functional/overload.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/meta.h"
#include "arolla/util/view_types.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/slice_builder.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {

// Applies a function `f` pointwise to all present items in `x`, returning a
// DataSlice with the same shape and bag, and with the specified schema. `f`
// may remove present items by returning missing.
//
// `f` must accept two arguments: the first of type `arolla::meta::type<T>`,
// and the second of type `arolla::view_type_t<T>`. This must accept all `T` in
// `ScalarVariant` other than `MissingValue`. The first argument is used to
// disambiguate `T`, because `view_type_t<T>` is not unique for all `T`.
//
// `f` must return either `absl::StatusOr<U>` for some `U` in `ScalarVariant`,
// `absl::StatusOr<OptionalValue<U>>` for some `U` in `ScalarVariant`, or
// `absl::StatusOr<internal::DataItem>`.
//
// Example:
//
//   ApplyUnaryPointWiseFn(
//     x,
//     [&]<typename T>(arolla::meta::type<T>, auto value) {
//       if (std::is_same_t<T, int32_t>) {
//         return value + 1;
//       }
//       return absl::InvalidArgumentError("expected int32 dtype");
//     },
//     internal::DataItem(schema::kInt32));
//
template <typename Fn>
absl::StatusOr<DataSlice> ApplyUnaryPointwiseFn(const DataSlice& x, const Fn& f,
                                                internal::DataItem schema) {
  return x.VisitImpl(absl::Overload(
      [&](const internal::DataItem& impl) -> absl::StatusOr<DataSlice> {
        if (!impl.has_value()) {
          return DataSlice::Create(internal::DataItem(), schema, x.GetBag());
        }
        ASSIGN_OR_RETURN(
            auto result_value,
            impl.VisitValue([&]<typename U>(const U& value)
                                -> absl::StatusOr<internal::DataItem> {
              if constexpr (std::is_same_v<U, internal::MissingValue>) {
                return internal::DataItem();
              } else {
                ASSIGN_OR_RETURN(
                    auto result_value,
                    f(arolla::meta::type<U>{}, arolla::view_type_t<U>(value)));
                return internal::DataItem(std::move(result_value));
              }
            }));
        return DataSlice::Create(std::move(result_value), schema, x.GetBag());
      },
      [&](const internal::DataSliceImpl& impl) -> absl::StatusOr<DataSlice> {
        internal::SliceBuilder result_builder(x.size());
        RETURN_IF_ERROR(impl.VisitValues(
            [&]<typename U>(
                const arolla::DenseArray<U>& values) -> absl::Status {
              absl::Status status = absl::OkStatus();
              values.ForEachPresent([&](int64_t i, auto value) {
                if (!status.ok()) {
                  return;
                }
                auto result_or = f(arolla::meta::type<U>{}, value);
                if (!result_or.ok()) {
                  status = std::move(result_or).status();
                  return;
                }
                result_builder.InsertIfNotSet(i, *std::move(result_or));
              });
              return status;
            }));
        return DataSlice::Create(std::move(result_builder).Build(),
                                 x.GetShape(), schema, x.GetBag());
      }));
}

}  // namespace koladata

#endif  // KOLADATA_POINTWISE_UTILS_H
