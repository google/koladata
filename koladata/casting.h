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
#ifndef KOLADATA_CASTING_H_
#define KOLADATA_CASTING_H_

#include <vector>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"

namespace koladata {

// Casts the given slice to INT32.
//
// The following schemas are supported: {NONE, BOOL, INT32, INT64, FLOAT32,
// FLOAT64, STRING, BYTES, OBJECT, ANY}. Slices with non-primitive schemas are
// required to contain only supported primitive values.
absl::StatusOr<DataSlice> ToInt32(const DataSlice& slice);

// Casts the given slice to INT64.
//
// The following schemas are supported: {NONE, BOOL, INT32, INT64, FLOAT32,
// FLOAT64, STRING, BYTES, OBJECT, ANY}. Slices with non-primitive schemas are
// required to contain only supported primitive values.
absl::StatusOr<DataSlice> ToInt64(const DataSlice& slice);

// Casts the given slice to FLOAT32.
//
// The following schemas are supported: {NONE, BOOL, INT32, INT64, FLOAT32,
// FLOAT64, STRING, BYTES, OBJECT, ANY}. Slices with non-primitive schemas are
// required to contain only supported primitive values.
absl::StatusOr<DataSlice> ToFloat32(const DataSlice& slice);

// Casts the given slice to FLOAT64.
//
// The following schemas are supported: {NONE, BOOL, INT32, INT64, FLOAT32,
// FLOAT64, STRING, BYTES, OBJECT, ANY}. Slices with non-primitive schemas are
// required to contain only supported primitive values.
absl::StatusOr<DataSlice> ToFloat64(const DataSlice& slice);

// Casts the given slice to NONE.
//
// All schemas are supported, as long as the slice is empty.
absl::StatusOr<DataSlice> ToNone(const DataSlice& slice);

// Casts the given slice to EXPR.
//
// The following schemas are supported: {NONE, EXPR, OBJECT, ANY}. Slices with
// non-primitive schemas are required to only contain ExprQuote values.
absl::StatusOr<DataSlice> ToExpr(const DataSlice& slice);

// Casts the given slice to STRING.
//
// The following schemas are supported: {NONE, STRING, BYTES, MASK, BOOL, INT32,
// INT64, FLOAT32, FLOAT64, OBJECT, ANY}. Slices with non-primitive schemas are
// required to only contain the previously listed values. Note that Bytes values
// are converted through b'foo' -> "b'foo'". Use `Decode` to decode BYTES to
// STRING using the UTF-8 encoding.
absl::StatusOr<DataSlice> ToStr(const DataSlice& slice);

// Casts the given slice to BYTES.
//
// The following schemas are supported: {NONE, BYTES, OBJECT, ANY}.
// Slices with non-primitive schemas are required to only contain Bytes values.
// Use `Encode` to encode STRING to BYTES using the UTF-8 encoding.
absl::StatusOr<DataSlice> ToBytes(const DataSlice& slice);

// Converts the given slice to STRING using UTF-8 decoding.
//
// The following schemas are supported: {NONE, STRING, BYTES, OBJECT, ANY}.
// Slices with non-primitive schemas are required to only contain the previously
// listed values.
absl::StatusOr<DataSlice> Decode(const DataSlice& slice);

// Converts the given slice to BYTES using UTF-8 encoding.
//
// The following schemas are supported: {NONE, STRING, BYTES, OBJECT, ANY}.
// Slices with non-primitive schemas are required to only contain the previously
// listed values.
absl::StatusOr<DataSlice> Encode(const DataSlice& slice);

// Casts the given slice to MASK.
//
// The following schemas are supported: {NONE, MASK, OBJECT, ANY}. Slices with
// non-primitive schemas are required to only contain Unit values.
absl::StatusOr<DataSlice> ToMask(const DataSlice& slice);

// Casts the given slice to BOOL.
//
// The following schemas are supported: {NONE, BOOL, INT32, INT64, FLOAT32,
// FLOAT64, OBJECT, ANY}. Slices with non-primitive schemas are required to only
// contain previously listed primitive values.
absl::StatusOr<DataSlice> ToBool(const DataSlice& slice);

// Casts the given slice to ANY.
//
// All input schemas are supported.
absl::StatusOr<DataSlice> ToAny(const DataSlice& slice);

// Casts the given slice to ITEMID.
//
// The following schemas are supported: {NONE, ITEMID, OBJECT, ANY} as well as
// entity schemas. Present values are required to be ObjectIds.
absl::StatusOr<DataSlice> ToItemId(const DataSlice& slice);

// Casts the given slice to SCHEMA.
//
// The following schemas are supported: {NONE, SCHEMA, OBJECT, ANY}. Slices with
// non-primitive schemas are required to only contain Schema values.
absl::StatusOr<DataSlice> ToSchema(const DataSlice& slice);

// Casts the given slice to the provided entity schema.
//
// The following schemas are supported: {NONE, ITEMID, OBJECT, ANY} as well as
// entity schemas. Note that the provided entity schema is not validated to
// match any existing schema attributes in the slice's DataBag.
absl::StatusOr<DataSlice> ToEntity(const DataSlice& slice,
                                   const internal::DataItem& entity_schema);

// Casts the given slice to OBJECT.
//
// All schemas are supported. Slices with non-primitive values are required to
// have an attached DataBag. Entity slices have their schema embedded into the
// attached DataBag.
//
// `validate_schema` indicates whether the schema is validated to match existing
// schema attributes in case of an Entity slice.
absl::StatusOr<DataSlice> ToObject(const DataSlice& slice,
                                   bool validate_schema = true);

// Casts the given slice to the given schema using implicit casting rules.
//
// Requires that `CommonSchema(schema, slice.GetSchema()) == schema`. Note that
// the casting may _not_ fail, assuming this condition is satisfied.
absl::StatusOr<DataSlice> CastToImplicit(const DataSlice& slice,
                                         const internal::DataItem& schema);

// Casts the given slice to the given schema using explicit casting rules.
//
// `CastToExplicit(slice, schema)` dispatches to the appropriate To<Schema>
// function and allows for more relaxed casting rules. Note that the casting may
// fail.
//
// If `validate_schema` is true, the schema is validated to match existing
// schema attributes if the slice is casted to OBJECT. If false, the schema
// attributes are set to the new schema without any validation.
absl::StatusOr<DataSlice> CastToExplicit(const DataSlice& slice,
                                         const internal::DataItem& schema,
                                         bool validate_schema = true);

// Casts the given slice to the given schema.
//
// Requires that `CommonSchema(schema, GetNarrowedSchema(slice.GetSchema())) ==
// schema`. Note that the casting may _not_ fail, assuming this condition is
// satisfied.
//
// This is a more relaxed version of `CastToImplicit` that allows for schema
// narrowing (e.g. casting an OBJECT slice to INT64 as long as the data is
// implicitly castable to INT64).
absl::StatusOr<DataSlice> CastToNarrow(const DataSlice& slice,
                                       const internal::DataItem& schema);

struct SchemaAlignedSlices {
  std::vector<DataSlice> slices;
  internal::DataItem common_schema;
};

// Aligns the given slices to a common schema.
//
// If a common schema cannot be computed, an error is returned.
absl::StatusOr<SchemaAlignedSlices> AlignSchemas(std::vector<DataSlice> slices);

}  // namespace koladata

#endif  // KOLADATA_CASTING_H_
