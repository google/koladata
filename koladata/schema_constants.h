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
#ifndef KOLADATA_OPERATORS_SCHEMA_CONSTANTS_H_
#define KOLADATA_OPERATORS_SCHEMA_CONSTANTS_H_

#include "absl/types/span.h"
#include "koladata/data_slice.h"

namespace koladata {

// Returns all defined Schema constants as Schema slices.
absl::Span<const DataSlice> SupportedSchemas();

// Returns the ANY_PRIMITIVE filter schema constant as a DataSlice.
// This is a named schema used in schema filters to match any primitive type.
const DataSlice& AnyPrimitiveFilter();

// Returns the ANY_SCHEMA filter schema constant as a DataSlice.
// This is a named schema used in schema filters to match any schema type.
const DataSlice& AnySchemaFilter();

}  // namespace koladata

#endif  // KOLADATA_OPERATORS_SCHEMA_CONSTANTS_H_
