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
#ifndef KOLADATA_METADATA_UTILS_H_
#define KOLADATA_METADATA_UTILS_H_

#include <string>
#include <vector>
#include "absl/status/statusor.h"
#include "koladata/data_slice.h"

namespace koladata {

// Returns metadata for a given slice of schemas.
absl::StatusOr<DataSlice> GetMetadataForSchemaSlice(const DataSlice& schema_ds);

// Returns a slice of attribute names, ordered according to the schema metadata.
// * For slice with Struct schema, the order is defined in metadata of the
//   schema.
// * For slice with SCHEMA schema, the order is defined in metadata of the
// stored schema.
// * For slice with OBJECT schema, the order is defined in metadata of the
// embedded schema.
absl::StatusOr<DataSlice> GetOrderedAttrNames(const DataSlice& ds);

// Return a vector of attribute names, ordered according to the schema metadata.
// If `assert_order_specified` is false and the schema metadata is not
// available, the attribute names are returned in the lexicographic order.
absl::StatusOr<std::vector<std::string>> GetOrderedOrLexicographicAttrNames(
    const DataSlice& ds, bool assert_order_specified = true);

}  // namespace koladata


#endif  // KOLADATA_METADATA_UTILS_H_
