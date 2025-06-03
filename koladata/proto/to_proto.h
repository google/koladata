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
#ifndef KOLADATA_PROTO_TO_PROTO_H_
#define KOLADATA_PROTO_TO_PROTO_H_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/message.h"

namespace koladata {

// Converts a rank-1 DataSlice to a list of proto messages of a single type.
//
// Koda data structures are converted to equivalent proto messages, primitive
// fields, repeated fields, maps, and enums, based on the proto schema. Koda
// entity attributes are converted to message fields with the same name, if
// those fields exist, otherwise they are ignored.
//
// Koda slices with mixed underlying dtypes are tolerated wherever the proto
// conversion is defined for all dtypes, regardless of schema.
//
// Koda entity attributes that are parenthesized fully-qualified extension
// paths (e.g. "(package_name.some_extension)") are converted to extensions,
// if those extensions exist in the descriptor pool of the messages' common
// descriptor, otherwise they are ignored.
//
// If this method returns a non-OK status, the contents of the messages pointed
// to by `messages` are unspecified but valid.
absl::Status ToProto(
    const DataSlice& slice,
    absl::Span<::google::protobuf::Message* /*absl_nonnull*/ const> messages);

// Converts a schema to a proto descriptor.
//
// The schema must be an entity schema. Its attributes can be primitive schemas,
// lists, dicts, and entity schemas. The following schemas are ignored when
// encountered during the traversal of the schema:
// * NONE
// * ITEMID
// * OBJECT
// * LIST schemas whose items are not primitive or entity schemas.
// * DICT schemas whose keys are not integral primitives or STRING.
// * DICT schemas whose values are not primitive or entity schemas.
// If an attribute name of an entity schema is not a valid proto field name,
// then the attribute is ignored.
//
// Recursive schemas are supported.
//
// If `warnings` is passed in non-null, it will be populated with warnings
// about parts of the schema that were ignored. If it is empty after the
// function returns, then no part of the schema was ignored.
//
// This function returns a non-OK status for serious issues, e.g. if the given
// schema is not an entity schema.
//
// The return type is a FileDescriptorProto, which is a proto representation of
// a proto file. The remaining parameters are used to populate it:
// * `file_name` is the name of the proto file. There is no materialized file
//   because the FileDescriptorProto is created in memory, so you can leave it
//   empty or set it to a dummy value.
// * `descriptor_package_name` is the proto package name. If it is not provided,
//   the package name will be set to
//   "koladata.ephemeral.schema_${schema_fingerprint_string}". This makes it
//   possible to put descriptors for various schemas in the same descriptor
//   pool.
// * `root_message_name` is the name of the root message in the returned "proto
//   file", which contains a single top-level message for the given schema;
//   sub-schemas are nested within it.
absl::StatusOr<google::protobuf::FileDescriptorProto> ProtoDescriptorFromSchema(
    const DataSlice& schema, std::vector<std::string>* warnings = nullptr,
    absl::string_view file_name = "",
    std::optional<absl::string_view> descriptor_package_name = std::nullopt,
    absl::string_view root_message_name = "RootSchema");

// Converts a DataSlice to a list of proto messages of a single type.
//
// The DataSlice is flattened as a first step, and the conversion to proto is
// then handled by `ToProto`.
absl::StatusOr<std::vector<std::unique_ptr<google::protobuf::Message>>> ToProtoMessages(
    const DataSlice& x, const google::protobuf::Message* /*absl_nonnull*/ message_prototype);

}  // namespace koladata

#endif  // KOLADATA_PROTO_TO_PROTO_H_
