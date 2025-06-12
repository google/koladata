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
#ifndef KOLADATA_PROTO_FROM_PROTO_H_
#define KOLADATA_PROTO_FROM_PROTO_H_

#include <optional>

#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"

namespace koladata {

// Converts a list of proto messages of the same type to a rank-1 DataSlice.
//
// Messages, primitive fields, repeated fields, and maps are converted to
// equivalent Koda structures: objects/entities, primitives, lists, and dicts,
// respectively. Enums are converted to INT32. The attribute names on the Koda
// objects match the field names in the proto definition. See below for methods
// to convert proto extensions to attributes alongside regular fields.
//
// The DataBag `db` must be mutable, and the converted proto data is added to
// it. The result DataSlice will use `db` as its DataBag. If this method
// returns a non-OK status, the contents of `db` are unspecified.
//
// If `itemids` is nullopt, all item ids will be allocated dynamically.
// Otherwise, `itemids` must be a rank-1 ITEMID DataSlice with the same size as
// `messages`. These item ids will be used as the ids of the root objects, and
// child item ids for any nested objects/lists/etc. will be deterministically
// generated based on them, so that the item ids in the result are fully
// deterministic. Use this feature with care to avoid unexpected collisions
// when passing the same `itemids` to multiple invocations of `FromProto`.
//
// If `schema` is nullopt, the result schema will be determined from the
// message structure, and the result schema's itemid will be computed
// deterministically from the proto message descriptor's fully-qualified name.
// If `schema` is an OBJECT schema, then the result will be the same as if
// `schema` were nullopt, but converted to OBJECT recursively.
//
// If `schema` is an entity schema, the set of attr names on that schema is the
// set of fields and extensions that are converted, overriding the default
// behavior and ignoring `extensions`. Attr names that start with '(' and end
// with ')' are interpreted as fully-qualified extension names and cause the
// corresponding extension to be converted if present on the message. If a
// sub-schema of this schema is OBJECT, the corresponding sub-messages are
// converted using the OBJECT rules above.
//
// Proto extensions are ignored by default unless `extensions` is specified (or
// if an explicit entity schema with parenthesized attrs is used; see above).
// The format of each extension specified in `extensions` is a dot-separated
// sequence of field names and/or extension names, where extension names are
// fully-qualified extension paths surrounded by parentheses. This sequence of
// fields and extensions is traversed during conversion, in addition to the
// default behavior of traversing all fields. For example:
//
//   "path.to.field.(package_name.some_extension)"
//   "path.to.repeated_field.(package_name.some_extension)"
//   "path.to.map_field.values.(package_name.some_extension)"
//   "path.(package_name.some_extension).(package_name2.nested_extension)"
//
// Extensions are looked up using the descriptor pool from `messages`, using
// `DescriptorPool::FindExtensionByName`. The Koda attribute names for the
// extension fields are parenthesized fully-qualified extension paths (e.g.
// "(package_name.some_extension)" or
// "(package_name.SomeMessage.some_extension)".)
absl::StatusOr<DataSlice> FromProto(
    const absl_nonnull DataBagPtr& db,
    absl::Span<const ::google::protobuf::Message* absl_nonnull const> messages,
    absl::Span<const std::string_view> extensions = {},
    const std::optional<DataSlice>& itemids = std::nullopt,
    const std::optional<DataSlice>& schema = std::nullopt);

// Same as above, but the result uses a new immutable DataBag, and if possible,
// that DataBag is forked from schema->GetBag() to avoid a schema extraction.
absl::StatusOr<DataSlice> FromProto(
    absl::Span<const ::google::protobuf::Message* absl_nonnull const> messages,
    absl::Span<const std::string_view> extensions = {},
    const std::optional<DataSlice>& itemids = std::nullopt,
    const std::optional<DataSlice>& schema = std::nullopt);

// Same as above, but takes a single proto and returns a DataItem.
absl::StatusOr<DataSlice> FromProto(
    const google::protobuf::Message& message,
    absl::Span<const std::string_view> extensions = {},
    const std::optional<DataSlice>& itemids = std::nullopt,
    const std::optional<DataSlice>& schema = std::nullopt);

// Converts a proto descriptor to an entity schema.
//
// This is similar to the schema of the result of FromProto when FromProto is
// called with a nullopt schema, but includes schemas for all message fields
// unconditionally instead of only fields with populated data.
//
// The DataBag `db` must be mutable, and the converted schema is added to it.
// The result DataSlice will use `db` as its DataBag. If this method returns a
// non-OK status, the contents of `db` are unspecified.
//
// TODO: Add a way to filter fields by name.
absl::StatusOr<DataSlice> SchemaFromProto(
    const absl_nonnull DataBagPtr& db,
    const ::google::protobuf::Descriptor* absl_nonnull descriptor,
    absl::Span<const std::string_view> extensions = {});

// Same as above, but the result uses a new immutable DataBag.
absl::StatusOr<DataSlice> SchemaFromProto(
    const ::google::protobuf::Descriptor* absl_nonnull descriptor,
    absl::Span<const std::string_view> extensions = {});

}  // namespace koladata

#endif  // KOLADATA_PROTO_FROM_PROTO_H_
