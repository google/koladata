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
#ifndef KOLADATA_PROTO_PROTO_SCHEMA_UTILS_H_
#define KOLADATA_PROTO_PROTO_SCHEMA_UTILS_H_

#include "absl/strings/string_view.h"

namespace koladata {
namespace schema {

// Stores a STRING containing the full name of the proto message.
constexpr absl::string_view kProtoSchemaMetadataFullNameAttr =
    "__proto_schema_metadata_full_name__";

// For all primitive scalar fields with custom default values in a proto
// message, there will be a metadata attr `proto_default_value_<field_name>`
// containing that default value.
constexpr absl::string_view kProtoSchemaMetadataDefaultValueAttrPrefix =
    "proto_default_value_";

}  // namespace schema

// TODO: Define operators to access default values.

}  // namespace koladata

#endif  // KOLADATA_PROTO_PROTO_SCHEMA_UTILS_H_
