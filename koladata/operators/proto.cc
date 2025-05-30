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
#include "koladata/operators/proto.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/operators/masking.h"
#include "koladata/operators/shapes.h"
#include "koladata/proto/from_proto.h"
#include "koladata/proto/to_proto.h"
#include "koladata/schema_utils.h"
#include "koladata/uuid_utils.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "google/protobuf/util/json_util.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace {

absl::StatusOr<const google::protobuf::Message* /*absl_nonnull*/> GetMessagePrototype(
    const DataSlice& proto_path) {
  RETURN_IF_ERROR(
      ExpectPresentScalar("proto_path", proto_path, schema::kString));
  auto message_descriptor_name_value =
      proto_path.item().value<arolla::Text>().view();
  const auto* message_descriptor =
      google::protobuf::DescriptorPool::generated_pool()->FindMessageTypeByName(
          message_descriptor_name_value);
  if (message_descriptor == nullptr) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "proto message `%s` not found in C++ generated descriptor pool",
        message_descriptor_name_value));
  }
  const auto* message_prototype =
      google::protobuf::MessageFactory::generated_factory()->GetPrototype(
          message_descriptor);
  if (message_prototype == nullptr) {
    // Should be unreachable in practice?
    return absl::InvalidArgumentError(
        absl::StrFormat("prototype for message descriptor `%s` not found in "
                        "C++ generated message factory",
                        message_descriptor_name_value));
  }
  return message_prototype;
}

absl::StatusOr<std::vector<absl::string_view>> GetExtensions(
    const DataSlice& extensions) {
  std::vector<absl::string_view> extensions_value;
  if (!IsUnspecifiedDataSlice(extensions)) {
    if (extensions.GetShape().rank() != 1 ||
        extensions.present_count() != extensions.size() ||
        extensions.GetSchemaImpl() != schema::kString) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected extensions to be a 1D all-present slice of STRING, got %v",
          DataSliceRepr(extensions)));
    }
    extensions_value.reserve(extensions.size());
    extensions.slice().values<arolla::Text>().ForEachPresent(
        [&](int64_t id, arolla::view_type_t<arolla::Text> value) {
          extensions_value.push_back(value);
        });
  }
  return extensions_value;
}

absl::StatusOr<DataSlice> FromProtoMessages(
    const std::vector<std::unique_ptr<google::protobuf::Message>>& messages,
    const DataSlice::JaggedShape& input_shape,
    const DataSlice& input_mask,
    const DataSlice& parse_error_mask,
    const DataSlice& extensions,
    const DataSlice& itemids,
    const DataSlice& schema,
    const DataSlice& on_invalid) {
  std::vector<google::protobuf::Message*> message_ptrs;
  message_ptrs.reserve(messages.size());
  for (const auto& message : messages) {
    message_ptrs.push_back(message.get());
  }

  ASSIGN_OR_RETURN((std::vector<absl::string_view> extensions_value),
                   GetExtensions(extensions));
  std::optional<DataSlice> itemids_value;
  if (!IsUnspecifiedDataSlice(itemids)) {
    ASSIGN_OR_RETURN(itemids_value,
                     Reshape(itemids, itemids.GetShape().FlattenDims(
                                          0, itemids.GetShape().rank())));
  }
  std::optional<DataSlice> schema_value;
  if (!IsUnspecifiedDataSlice(schema)) {
    schema_value = schema;
  }

  ASSIGN_OR_RETURN(auto result, FromProto(message_ptrs, extensions_value,
                                          itemids_value, schema_value));

  // (result & input_mask & ~parse_error_mask) | (on_invalid & parse_error_mask)
  ASSIGN_OR_RETURN(auto not_parse_error_mask, HasNot(parse_error_mask));
  ASSIGN_OR_RETURN(auto result_mask,
                   ApplyMask(input_mask, std::move(not_parse_error_mask)));
  ASSIGN_OR_RETURN(result, ApplyMask(result, std::move(result_mask)));
  if (!IsUnspecifiedDataSlice(on_invalid)) {
    ASSIGN_OR_RETURN(auto error_flat, ApplyMask(on_invalid, parse_error_mask));
    ASSIGN_OR_RETURN(result, Coalesce(result, std::move(error_flat)));
  }
  return result.Reshape(input_shape);
}

}  // namespace

absl::StatusOr<DataSlice> FromProtoBytes(
    const DataSlice& x,
    const DataSlice& proto_path,
    const DataSlice& extensions,
    const DataSlice& itemids,
    const DataSlice& schema,
    const DataSlice& on_invalid,
    const internal::NonDeterministicToken&) {
  RETURN_IF_ERROR(ExpectBytes("x", x));
  ASSIGN_OR_RETURN(const auto* message_prototype,
                   GetMessagePrototype(proto_path));
  ASSIGN_OR_RETURN(auto x_flat, Reshape(x, x.GetShape().FlattenDims(
                                               0, x.GetShape().rank())));
  bool raise_on_invalid = IsUnspecifiedDataSlice(on_invalid);

  std::vector<std::unique_ptr<google::protobuf::Message>> messages;
  messages.reserve(x_flat.size());
  arolla::DenseArrayBuilder<arolla::Unit> parse_error_mask_builder(
      x_flat.size());
  for (int64_t i = 0; i < x_flat.size(); ++i) {
    auto message = std::unique_ptr<google::protobuf::Message>(message_prototype->New());
    if (x_flat.slice().present(i)) {
      if (!message->ParsePartialFromString(
          x_flat.slice().values<arolla::Bytes>().values[i])) {
        if (raise_on_invalid) {
          return absl::InvalidArgumentError(absl::StrFormat(
              "failed to parse input as a binary proto of type `%v`",
              message_prototype->GetDescriptor()->full_name()));
        } else {
          parse_error_mask_builder.Add(i, arolla::kUnit);
        }
      }
    }
    messages.push_back(std::move(message));
  }
  ASSIGN_OR_RETURN(
      auto parse_error_mask_flat,
      DataSlice::Create(internal::DataSliceImpl::Create(
                            std::move(parse_error_mask_builder).Build()),
                        x_flat.GetShape(), internal::DataItem(schema::kMask)));
  ASSIGN_OR_RETURN(auto input_mask_flat, ops::Has(std::move(x_flat)));
  return FromProtoMessages(messages, x.GetShape(), std::move(input_mask_flat),
                           std::move(parse_error_mask_flat), extensions,
                           itemids, schema, on_invalid);
}

absl::StatusOr<DataSlice> FromProtoJson(
    const DataSlice& x,
    const DataSlice& proto_path,
    const DataSlice& extensions,
    const DataSlice& itemids,
    const DataSlice& schema,
    const DataSlice& on_invalid,
    const internal::NonDeterministicToken&) {
  RETURN_IF_ERROR(ExpectString("x", x));
  ASSIGN_OR_RETURN(const auto* message_prototype,
                   GetMessagePrototype(proto_path));
  ASSIGN_OR_RETURN(auto x_flat, Reshape(x, x.GetShape().FlattenDims(
                                               0, x.GetShape().rank())));
  bool raise_on_invalid = IsUnspecifiedDataSlice(on_invalid);

  std::vector<std::unique_ptr<google::protobuf::Message>> messages;
  messages.reserve(x_flat.size());
  arolla::DenseArrayBuilder<arolla::Unit> parse_error_mask_builder(
      x_flat.size());
  for (int64_t i = 0; i < x_flat.size(); ++i) {
    auto message = std::unique_ptr<google::protobuf::Message>(message_prototype->New());
    if (x_flat.slice().present(i)) {
      auto status = google::protobuf::util::JsonStringToMessage(
          x_flat.slice().values<arolla::Text>().values[i],
          message.get());
      if (!status.ok()) {
        if (raise_on_invalid) {
          return absl::InvalidArgumentError(absl::StrFormat(
              "failed to parse input as a JSON-format proto of type `%v`",
              message_prototype->GetDescriptor()->full_name()));
        } else {
          parse_error_mask_builder.Add(i, arolla::kUnit);
        }
      }
    }
    messages.push_back(std::move(message));
  }
  ASSIGN_OR_RETURN(
      auto parse_error_mask_flat,
      DataSlice::Create(internal::DataSliceImpl::Create(
                            std::move(parse_error_mask_builder).Build()),
                        x_flat.GetShape(), internal::DataItem(schema::kMask)));
  ASSIGN_OR_RETURN(auto input_mask_flat, ops::Has(std::move(x_flat)));
  return FromProtoMessages(messages, x.GetShape(), std::move(input_mask_flat),
                           std::move(parse_error_mask_flat), extensions,
                           itemids, schema, on_invalid);
}

namespace {

absl::StatusOr<std::vector<std::unique_ptr<google::protobuf::Message>>> ToProtoMessages(
    const DataSlice& x, const DataSlice& proto_path) {
  ASSIGN_OR_RETURN(const auto* message_prototype,
                   GetMessagePrototype(proto_path));
  ASSIGN_OR_RETURN(auto x_flat, Reshape(x, x.GetShape().FlattenDims(
                                               0, x.GetShape().rank())));
  std::vector<google::protobuf::Message*> messages;
  messages.reserve(x_flat.size());
  std::vector<std::unique_ptr<google::protobuf::Message>> owned_messages;
  owned_messages.reserve(x_flat.size());
  for (int64_t i = 0; i < x_flat.size(); ++i) {
    auto message = std::unique_ptr<google::protobuf::Message>(message_prototype->New());
    messages.push_back(message.get());
    owned_messages.push_back(std::move(message));
  }
  RETURN_IF_ERROR(ToProto(std::move(x_flat), std::move(messages)));
  return std::move(owned_messages);
}

}  // namespace

absl::StatusOr<DataSlice> ToProtoBytes(const DataSlice& x,
                                       const DataSlice& proto_path) {
  ASSIGN_OR_RETURN(auto messages, ToProtoMessages(x, proto_path));
  arolla::DenseArrayBuilder<arolla::Bytes> result_builder(x.size());
  for (int64_t i = 0; i < x.size(); ++i) {
    result_builder.Add(i, messages[i]->SerializePartialAsString());
  }
  ASSIGN_OR_RETURN(
      auto result,
      DataSlice::Create(
          internal::DataSliceImpl::Create(std::move(result_builder).Build()),
          x.GetShape(), internal::DataItem(schema::kBytes)));
  ASSIGN_OR_RETURN(auto mask, Has(x));
  return ApplyMask(std::move(result), std::move(mask));
}

absl::StatusOr<DataSlice> ToProtoJson(const DataSlice& x,
                                      const DataSlice& proto_path) {
  ASSIGN_OR_RETURN(auto messages, ToProtoMessages(x, proto_path));
  arolla::DenseArrayBuilder<arolla::Text> result_builder(x.size());
  for (int64_t i = 0; i < x.size(); ++i) {
    std::string json;
    RETURN_IF_ERROR(google::protobuf::util::MessageToJsonString(*messages[i], &json));
    result_builder.Add(i, arolla::Text(std::move(json)));
  }
  ASSIGN_OR_RETURN(
      auto result,
      DataSlice::Create(
          internal::DataSliceImpl::Create(std::move(result_builder).Build()),
          x.GetShape(), internal::DataItem(schema::kString)));
  ASSIGN_OR_RETURN(auto mask, Has(x));
  return ApplyMask(std::move(result), std::move(mask));
}

absl::StatusOr<DataSlice> SchemaFromProtoPath(const DataSlice& proto_path,
                                              const DataSlice& extensions) {
  ASSIGN_OR_RETURN(const auto* message_prototype,
                   GetMessagePrototype(proto_path));
  ASSIGN_OR_RETURN((std::vector<absl::string_view> extensions_value),
                   GetExtensions(extensions));
  auto db = DataBag::Empty();
  ASSIGN_OR_RETURN(auto result,
                   SchemaFromProto(db, message_prototype->GetDescriptor(),
                                   std::move(extensions_value)));
  db->UnsafeMakeImmutable();
  return std::move(result);
}

}  // namespace koladata::ops
