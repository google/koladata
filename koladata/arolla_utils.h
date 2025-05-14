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
#ifndef KOLADATA_AROLLA_UTILS_H_
#define KOLADATA_AROLLA_UTILS_H_

#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/casting.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {

// Returns an Arolla value from DataSlice. In case of DataItem, it returns a
// scalar value, while in case of DataSlice it returns a DenseArray. If this is
// not possible, it raises an appropriate error. This method is supported only
// if all primitives in DataSlice have the same dtype.
//
// If `fallback_schema` is provided as a primitive schema, it is used to create
// an arolla value in case `ds` is empty-and-unknown.
absl::StatusOr<arolla::TypedValue> DataSliceToArollaValue(
    const DataSlice& ds,
    const internal::DataItem& fallback_schema = internal::DataItem());

// The same as above, but returns a TypedRef. This means it doesn't work for
// empty slices.
absl::StatusOr<arolla::TypedRef> DataSliceToArollaRef(const DataSlice& ds);

// Returns a TypedRef from a DataSlice. If the DataSlice owns its value, the
// returned TypedRef refers to the owned value. Otherwise, it refers to a
// TypedValue that is appended to `typed_value_holder`.
//
// If `fallback_schema` is provided as a primitive schema, it is used to create
// an arolla value in case `ds` is empty-and-unknown.
absl::StatusOr<arolla::TypedRef> DataSliceToOwnedArollaRef(
    const DataSlice& slice, std::vector<arolla::TypedValue>& typed_value_holder,
    const internal::DataItem& fallback_schema = internal::DataItem());

// Returns a new DataSlice created from DenseArray of primitive types. Supported
// only for array items of the same primitive value type.
absl::StatusOr<DataSlice> DataSliceFromPrimitivesDenseArray(
    arolla::TypedRef values);

// Returns a new DataSlice created from Arolla Array of primitive types.
// Supported only for array items of the same primitive value type.
absl::StatusOr<DataSlice> DataSliceFromPrimitivesArray(arolla::TypedRef values);

// Returns a new DataSlice created from an Arolla value. `schema` can optionally
// be provided to specify the schema of the result. Otherwise, it will be
// inferred from the type of `arolla_value`. Note that the provided `schema`
// _must_ be compatible with the provided `arolla_value`.
absl::StatusOr<DataSlice> DataSliceFromArollaValue(
    arolla::TypedRef arolla_value, DataSlice::JaggedShape shape,
    const internal::DataItem& schema = internal::DataItem());

// Returns a DenseArray of primitives from DataSlice, if possible. Otherwise,
// raises appropriate error. This method is supported only if all primitives in
// DataSlice have the same dtype or DataSlice has primitive schema and all-empty
// items.
absl::StatusOr<arolla::TypedValue> DataSliceToDenseArray(const DataSlice& ds);

// Casts `x` to type `T` using narrow casting (allowing OBJECT -> T casts).
template <typename T>
absl::StatusOr<T> ToArollaScalar(const DataSlice& x) {
  if (x.GetShape().rank() != 0) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected rank 0, but got rank=%d", x.GetShape().rank()));
  }
  if (!x.item().has_value()) {
    return absl::InvalidArgumentError("expected a present value");
  }
  ASSIGN_OR_RETURN(auto x_casted,
                   CastToNarrow(x, internal::DataItem(schema::GetDType<T>())));
  return x_casted.item().value<T>();
}

// Casts `x` to type `OptionalValue<T>` using narrow casting (allowing OBJECT ->
// T casts).
template <typename T>
absl::StatusOr<arolla::OptionalValue<T>> ToArollaOptionalScalar(
    const DataSlice& x) {
  if (x.GetShape().rank() != 0) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected rank 0, but got rank=%d", x.GetShape().rank()));
  }
  ASSIGN_OR_RETURN(auto x_casted,
                   CastToNarrow(x, internal::DataItem(schema::GetDType<T>())));
  if (x_casted.item().has_value()) {
    return x_casted.item().value<T>();
  } else {
    return arolla::OptionalValue<T>();
  }
}

// Casts `x` to `DenseArray<T>` using narrow casting (allowing OBJECT -> T
// casts).
template <typename T>
absl::StatusOr<arolla::DenseArray<T>> ToArollaDenseArray(const DataSlice& x) {
  ASSIGN_OR_RETURN(auto x_casted,
                   CastToNarrow(x, internal::DataItem(schema::GetDType<T>())));
  if (x_casted.IsEmpty()) {
    return arolla::CreateEmptyDenseArray<T>(x.GetShape().size());
  }
  if (x_casted.is_item()) {
    return internal::DataSliceImpl::Create(1, x_casted.item()).values<T>();
  } else {
    return x_casted.slice().values<T>();
  }
}

}  // namespace koladata

#endif  // KOLADATA_AROLLA_UTILS_H_
