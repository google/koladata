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
#ifndef KOLADATA_OPERATORS_PREDICATES_H_
#define KOLADATA_OPERATORS_PREDICATES_H_

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"

namespace koladata::ops {

// Returns true if the DataSlice has a primitive schema or only contains
// primitives if the schema is OBJECT or SCHEMA.
absl::StatusOr<DataSlice> IsPrimitive(const DataSlice& x);

// Returns a MASK DataSlice with present for each item in `x` that is primitive.
absl::StatusOr<DataSlice> HasPrimitive(const DataSlice& x);

// Returns true if the DataSlice has an Entity schema or only contains entities
// if the schema is OBJECT.
absl::StatusOr<DataSlice> IsEntity(const DataSlice& x);

// Returns a MASK DataSlice with present for each item in `x` that is an Entity.
absl::StatusOr<DataSlice> HasEntity(const DataSlice& x);

// Returns true if the DataSlice has a List schema or only contains lists if the
// schema is OBJECT.
absl::StatusOr<DataSlice> IsList(const DataSlice& x);

// Returns a MASK DataSlice with present for each item in `x` that is a List.
absl::StatusOr<DataSlice> HasList(const DataSlice& x);

// Returns true if the DataSlice has a Dict schema or only contains dicts if the
// schema is OBJECT.
absl::StatusOr<DataSlice> IsDict(const DataSlice& x);

// Returns a MASK DataSlice with present for each item in `x` that is a Dict.
absl::StatusOr<DataSlice> HasDict(const DataSlice& x);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_PREDICATES_H_
