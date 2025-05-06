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
#ifndef KOLADATA_INTERNAL_SCHEMA_ATTRS_H_
#define KOLADATA_INTERNAL_SCHEMA_ATTRS_H_

#include "absl/strings/string_view.h"

namespace koladata::schema {

// "Normal" attributes related to schemas.
//
// Stores the schema of an ObjectId with OBJECT schema.
constexpr absl::string_view kSchemaAttr = "__schema__";

// Schema attributes.
//
// Schema of the values of a List.
constexpr absl::string_view kListItemsSchemaAttr = "__items__";
// Schema of the keys of a Dict.
constexpr absl::string_view kDictKeysSchemaAttr = "__keys__";
// Schema of the values of a Dict.
constexpr absl::string_view kDictValuesSchemaAttr = "__values__";
// Stores the name of a named schema.
constexpr absl::string_view kSchemaNameAttr = "__schema_name__";
// Stores schema metadata.
constexpr absl::string_view kSchemaMetadataAttr = "__schema_metadata__";

// Seeds for UUID generation.
//
// Seed for implicit schemas.
constexpr absl::string_view kImplicitSchemaSeed = "__implicit_schema__";
// Seed for no-follow schemas.
constexpr absl::string_view kNoFollowSchemaSeed = "__nofollow_schema__";
// Seed for metadata.
constexpr absl::string_view kMetadataSeed = "__metadata__";

}  // namespace koladata::schema

#endif  // KOLADATA_INTERNAL_SCHEMA_ATTRS_H_
