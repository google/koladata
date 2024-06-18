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
#ifndef KOLADATA_INTERNAL_UUID_OBJECT_H_
#define KOLADATA_INTERNAL_UUID_OBJECT_H_

#include <vector>
#include <cstdint>
#include <functional>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {

// Creates Uuid object based on fingerprint of names and values of the kwargs.
DataItem CreateUuidFromFields(
    absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> values);

// Creates Uuid object based on fingerprint of names and values of the kwargs.
// All DataSliceImpl's in kwargs must have the same size.
absl::StatusOr<DataSliceImpl> CreateUuidFromFields(
    absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataSliceImpl>> values);

// Creates a Uuid object for a schema, based on names and values of the schemas.
// Does not check that the DataItems are schemas.
DataItem CreateSchemaUuidFromFields(
    absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> items);

// Creates Uuid object based on the provided salt and the main object.
// Resulted object will have the same offset and allocation capacity
// as the main object.
// Returns missing value if main_object is missing.
// IsUuid() will be true.
//
// If a specific UUID flag is passed for `uuid_flag` other properties will also
// be true. E.g. for uuid_flag == kUuidImplicitSchemaFlag, IsSchema() and
// IsImplicitSchema() will also return true.
template <int64_t uuid_flag = ObjectId::kUuidFlag>
absl::StatusOr<DataItem> CreateUuidWithMainObject(const DataItem& main_object,
                                                  absl::string_view salt);

// Creates Uuid object based on the provided salt and the main objects.
// Resulted object will have the same offset and allocation capacity
// as the main objects.
// Returns missing value for missing main objects.
// IsUuid() will be true.
//
// If a specific UUID flag is passed for `uuid_flag` other properties will also
// be true. E.g. for uuid_flag == kUuidImplicitSchemaFlag, IsSchema() and
// IsImplicitSchema() will also return true.
template <int64_t uuid_flag = ObjectId::kUuidFlag>
absl::StatusOr<DataSliceImpl> CreateUuidWithMainObject(
    const DataSliceImpl& main_object, absl::string_view salt);

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_UUID_OBJECT_H_
