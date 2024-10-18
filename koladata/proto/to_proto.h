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


#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "google/protobuf/descriptor.h"
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
    absl::Span<const absl::Nonnull<::google::protobuf::Message*>> messages);

}  // namespace koladata

#endif  // KOLADATA_PROTO_TO_PROTO_H_
