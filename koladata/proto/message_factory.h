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
#ifndef KOLADATA_PROTO_MESSAGE_FACTORY_H_
#define KOLADATA_PROTO_MESSAGE_FACTORY_H_

#include <memory>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/message.h"

namespace koladata {

// Helper function to create a prototype message for a given message_name.
// Returns nullptr if the message_name is not found in the generated descriptor
// pool.
// The proto must be linked in the binary, otherwise returns nullptr.
std::unique_ptr<::google::protobuf::Message> /*absl_nullable*/ CreateProtoMessagePrototype(
    absl::string_view message_name);

// Finds a proto message with the given name and deserializes it from the
// given serialized_proto.
//
// Returns an error if the message_name is not found in the generated descriptor
// pool, or if the serialized_proto is not a valid proto message.
absl::StatusOr<std::unique_ptr<::google::protobuf::Message>> DeserializeProtoByName(
    absl::string_view message_name, absl::string_view serialized_proto);

}  // namespace koladata

#endif  // KOLADATA_PROTO_MESSAGE_FACTORY_H_
