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
#include "koladata/proto/message_factory.h"

#include <memory>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"

namespace koladata {

using ::google::protobuf::Descriptor;
using ::google::protobuf::Message;

std::unique_ptr<::google::protobuf::Message> absl_nullable CreateProtoMessagePrototype(
    absl::string_view message_name) {
  const Descriptor* descriptor =
      google::protobuf::DescriptorPool::generated_pool()->FindMessageTypeByName(
          message_name);
  if (descriptor == nullptr) {
    return nullptr;
  }
  return std::unique_ptr<Message>(google::protobuf::MessageFactory::generated_factory()
                                      ->GetPrototype(descriptor)
                                      ->New());
}

absl::StatusOr<std::unique_ptr<::google::protobuf::Message>> DeserializeProtoByName(
    absl::string_view message_name, absl::string_view serialized_proto) {
  auto message = CreateProtoMessagePrototype(message_name);
  if (message == nullptr) {
    return absl::NotFoundError(
        absl::StrFormat("failed to create proto message %s", message_name));
  }
  if (!message->ParseFromString(serialized_proto)) {
    return absl::InvalidArgumentError(
        absl::StrFormat("failed to parse proto message %s", message_name));
  }
  return message;
}

}  // namespace koladata
