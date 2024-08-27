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
#ifndef KOLADATA_SCHEMA_UTILS_H_
#define KOLADATA_SCHEMA_UTILS_H_

#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"

namespace koladata {

// Returns the common schema of the underlying data. If the schema is ambiguous
// (e.g. the slice holds ObjectIds, or the data is mixed but there is no common
// type), the schema of the original slice is returned.
//
// Example:
//  * GetNarrowedSchema(kd.slice([1])) -> INT32.
//  * GetNarrowedSchema(kd.slice([1, 2.0], OBJECT)) -> FLOAT32.
//  * GetNarrowedSchema(kd.slice([None, None], OBJECT)) -> NONE.
internal::DataItem GetNarrowedSchema(const DataSlice& slice);

}  // namespace koladata

#endif  // KOLADATA_SCHEMA_UTILS_H_
