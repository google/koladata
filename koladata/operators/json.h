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
#ifndef KOLADATA_OPERATORS_JSON_H_
#define KOLADATA_OPERATORS_JSON_H_

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/non_deterministic_token.h"

namespace koladata::ops {

// kde.json.from_json
absl::StatusOr<DataSlice> FromJson(DataSlice x, DataSlice schema,
                                   DataSlice default_number_schema,
                                   DataSlice on_invalid, DataSlice keys_attr,
                                   DataSlice values_attr,
                                   internal::NonDeterministicToken);

// kde.json.to_json
absl::StatusOr<DataSlice> ToJson(DataSlice x, DataSlice indent,
                                 DataSlice ensure_ascii, DataSlice keys_attr,
                                 DataSlice values_attr);

namespace json_internal {

// The following functions convert C++ representations of JSON primitive values
// into DataItems, given a requested result schema (which may be OBJECT), or
// return an error if the schema and value are incompatible. Functions that
// have a `default_number_schema_impl` argument will default to using that
// schema for numeric types if `schema_impl` is OBJECT.

absl::StatusOr<internal::DataItem> JsonBoolToDataItem(
    bool value, const internal::DataItem& schema_impl);

absl::StatusOr<internal::DataItem> JsonNumberToDataItem(
    int64_t value, const internal::DataItem& schema_impl,
    const internal::DataItem& default_number_schema_impl);

absl::StatusOr<internal::DataItem> JsonNumberToDataItem(
    uint64_t value, const internal::DataItem& schema_impl,
    const internal::DataItem& default_number_schema_impl);

absl::StatusOr<internal::DataItem> JsonNumberToDataItem(
    double value, absl::string_view value_str,
    const internal::DataItem& schema_impl,
    const internal::DataItem& default_number_schema_impl);

absl::StatusOr<internal::DataItem> JsonStringToDataItem(
    std::string value, const internal::DataItem& schema_impl);

// Creates and returns a Koda list DataItem from a vector of values, which come
// from the items of a converted JSON array.
absl::StatusOr<internal::DataItem> JsonArrayToList(
    std::vector<internal::DataItem> json_array_values,
    const internal::DataItem& schema_impl,
    const internal::DataItem& value_schema_impl, bool embed_schema,
    const DataBagPtr& bag);

// Creates and returns a Koda entity DataItem from a pair of parallel vectors
// of keys and values, which come from the keys and values of a converted JSON
// object.
absl::StatusOr<internal::DataItem> JsonObjectToEntity(
    std::vector<std::string> json_object_keys,
    std::vector<internal::DataItem> json_object_values,
    const internal::DataItem& schema_impl,
    const std::optional<absl::string_view>& keys_attr,
    const std::optional<absl::string_view>& values_attr, bool embed_schema,
    const DataBagPtr& bag);

// Creates and returns a Koda dict DataItem from a pair of parallel vectors
// of keys and values, which come from the keys and values of a converted JSON
// object.
absl::StatusOr<internal::DataItem> JsonObjectToDict(
    std::vector<std::string> json_object_keys,
    std::vector<internal::DataItem> json_object_values,
    const internal::DataItem& schema_impl, bool embed_schema,
    const DataBagPtr& bag);

}  // namespace json_internal
}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_JSON_H_
