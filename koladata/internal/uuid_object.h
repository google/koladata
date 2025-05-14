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

#include <cstdint>
#include <functional>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/util/fingerprint.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {

// Used to dispatch which Uuids to create(referring to data included to be
// included in the ObjectId, not schemas).
enum class UuidType {
  kDefault = 0,
  kList,
  kDict,
};

ObjectId CreateUuidObject(arolla::Fingerprint fingerprint,
                          UuidType uuid_type = UuidType::kDefault);

// Creates Uuid object based on fingerprint of names and values of the kwargs.
DataItem CreateUuidFromFields(
    absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> values,
    UuidType uuid_type = UuidType::kDefault);

// Creates Uuid object based on fingerprint of names and values of the kwargs.
// All DataSliceImpl's in kwargs must have the same size.
absl::StatusOr<DataSliceImpl> CreateUuidFromFields(
    absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataSliceImpl>> values,
    UuidType uuid_type = UuidType::kDefault);

// Creates a slice of Uuid objects that share the same allocation.
// The size of the allocation is `size`. Each item in the result is distinct.
// Conceptually the result is a slice containing
// [fingerprint(seed, size, i) for i in range(size)]
absl::StatusOr<DataSliceImpl> CreateUuidsWithAllocationSize(
    absl::string_view seed, int64_t size);

// Creates Uuid object for a list, based on values of items.
DataItem CreateListUuidFromItemsAndFields(
    absl::string_view seed, const DataSliceImpl& list_items,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> values);

// Creates Uuid object for a dict, based on values of keys and values.
DataItem CreateDictUuidFromKeysValuesAndFields(
    absl::string_view seed, const DataSliceImpl& dict_keys,
    const DataSliceImpl& dict_values,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> values);

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
