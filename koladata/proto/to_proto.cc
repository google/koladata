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
#include "koladata/proto/to_proto.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/trampoline_executor.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/operators/lists.h"
#include "koladata/operators/masking.h"
#include "koladata/operators/shapes.h"
#include "koladata/operators/slices.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/message.h"
#include "arolla/util/status_macros_backport.h"

using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Message;
using ::google::protobuf::Reflection;

namespace koladata {
namespace {

// Note: no explicit error handling of type errors, field must always have the
// matching type.
template <typename T>
void SetField(T value, const FieldDescriptor& field, Message& message,
              const Reflection& refl) {
  if constexpr (std::is_same_v<T, int32_t>) {
    if (field.enum_type() != nullptr) {
      refl.SetEnumValue(&message, &field, value);
    } else {
      refl.SetInt32(&message, &field, value);
    }
  } else if constexpr (std::is_same_v<T, uint32_t>) {
    refl.SetUInt32(&message, &field, value);
  } else if constexpr (std::is_same_v<T, int64_t>) {
    refl.SetInt64(&message, &field, value);
  } else if constexpr (std::is_same_v<T, uint64_t>) {
    refl.SetUInt64(&message, &field, value);
  } else if constexpr (std::is_same_v<T, double>) {
    refl.SetDouble(&message, &field, value);
  } else if constexpr (std::is_same_v<T, float>) {
    refl.SetFloat(&message, &field, value);
  } else if constexpr (std::is_same_v<T, bool>) {
    refl.SetBool(&message, &field, value);
  } else if constexpr (std::is_same_v<T, std::string>) {
    refl.SetString(&message, &field, std::move(value));
  }
}

// Calls `func` with a single null pointer with type `T*`, where `T` is the C++
// type that corresponds to the proto `cpp_type` enum, as a hacky way to pass
// it `T` statically. `func` must return an `absl::Status`. Enums are treated
// as int32_t, which matches the proto reflection APIs.
template <typename F>
absl::Status CallWithPrimitiveFieldCppType(FieldDescriptor::CppType cpp_type,
                                           F func) {
  switch (cpp_type) {
    case FieldDescriptor::CPPTYPE_INT32:
      return func(static_cast<int32_t*>(nullptr));
    case FieldDescriptor::CPPTYPE_INT64:
      return func(static_cast<int64_t*>(nullptr));
    case FieldDescriptor::CPPTYPE_UINT32:
      return func(static_cast<uint32_t*>(nullptr));
    case FieldDescriptor::CPPTYPE_UINT64:
      return func(static_cast<uint64_t*>(nullptr));
    case FieldDescriptor::CPPTYPE_DOUBLE:
      return func(static_cast<double*>(nullptr));
    case FieldDescriptor::CPPTYPE_FLOAT:
      return func(static_cast<float*>(nullptr));
    case FieldDescriptor::CPPTYPE_BOOL:
      return func(static_cast<bool*>(nullptr));
    case FieldDescriptor::CPPTYPE_ENUM:
      return func(static_cast<int32_t*>(nullptr));
    case FieldDescriptor::CPPTYPE_STRING:
      return func(static_cast<std::string*>(nullptr));
    default:
      // Should be unreachable.
      return absl::InvalidArgumentError(absl::StrFormat(
          "invalid primitive cpp_type enum value %d", cpp_type));
  }
}

// Converts a Koda primitive value of type SrcT to a proto primitive value of
// type DstT. Because of the interactions between the two type systems, this
// behavior needs to be defined specially.
//
// Note: field is expected to have a primitive cpp_type() matching DstT.
template <typename DstT, typename SrcT>
absl::StatusOr<DstT> Convert(const FieldDescriptor& field,
                             arolla::QTypePtr dtype,
                             arolla::view_type_t<SrcT> value) {
  if constexpr (std::is_same_v<SrcT, int32_t> && std::is_integral_v<DstT>) {
    // Check range for int32 -> uint32 and int32 -> uint64.
    if (std::is_unsigned_v<DstT> && value < 0) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "value %d out of range for proto field %s with value type %s", value,
          field.name(), field.cpp_type_name()));
    }
    return value;
  }
  if constexpr (std::is_same_v<SrcT, int64_t> && std::is_integral_v<DstT>) {
    // Check range for int64 -> int32 and int64 -> uint32.
    if ((std::is_same_v<DstT, int32_t> || std::is_same_v<DstT, uint32_t>) &&
        (value < std::numeric_limits<DstT>::min() ||
         value > std::numeric_limits<DstT>::max())) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "value %d out of range for proto field %s with value type %s", value,
          field.name(), field.cpp_type_name()));
    }
    // Note: Special case for int64 -> uint64: Because Koda has no unsigned
    // integer types, we convert uint64 to int64 (with 2's complement wrapping)
    // in FromProto, so we are more tolerant here to ensure that all uint64
    // values can be round-tripped.
    return value;
  }
  if constexpr (std::is_integral_v<SrcT> && !std::is_same_v<SrcT, bool> &&
                std::is_floating_point_v<DstT>) {
    // Allow storing integers in floating-point fields if they are in the range
    // of integers that the floating-point field can store exactly.
    //
    // Note that we could reasonably try to store integers that are outside of
    // this range but still are representable exactly (e.g. large powers of 2).
    // This is difficult to get right: the naive approach of round-tripping and
    // comparing causes UB if we round to outside of the integer's representable
    // range. It is also hard to explain to users.
    if constexpr (std::is_same_v<DstT, float>) {
      if (value < -(1 << 24) || value > (1 << 24)) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "value %d is not in the range of integers that can be exactly "
            "represented by proto field %s with value type %s",
            value, field.name(), field.cpp_type_name()));
      }
      return value;
    }
    if constexpr (std::is_same_v<DstT, double>) {
      if (value < -(1L << 53) || value > (1L << 53)) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "value %d is not in the range of integers that can be exactly "
            "represented by proto field %s with value type %s",
            value, field.name(), field.cpp_type_name()));
      }
      return value;
    }
  }
  if constexpr (std::is_same_v<SrcT, arolla::Unit> &&
                std::is_same_v<DstT, bool>) {
    // Store MASK present as true. MASK missing is stored as unset implicitly.
    return true;
  }
  if constexpr (std::is_same_v<SrcT, DstT> && (std::is_floating_point_v<DstT> ||
                                               std::is_same_v<DstT, bool>)) {
    return value;
  }
  if constexpr (std::is_same_v<SrcT, arolla::Bytes> &&
                std::is_same_v<DstT, std::string>) {
    if (field.type() == FieldDescriptor::TYPE_BYTES) {
      return std::string(value);
    }
  }
  if constexpr (std::is_same_v<SrcT, arolla::Text> &&
                std::is_same_v<DstT, std::string>) {
    if (field.type() == FieldDescriptor::TYPE_STRING) {
      return std::string(value);
    }
  }

  std::string value_repr;
  if constexpr (!std::is_same_v<SrcT, void>) {
    value_repr = arolla::Repr(SrcT(std::move(value)));
  }

  return absl::InvalidArgumentError(absl::StrFormat(
      "invalid proto field %s with value type %s for Koda value %s with dtype "
      "%s",
      field.name(), field.type_name(), value_repr, dtype->name()));
}

// Returns an error if the oneof containing `field` (if it is in a oneof) is
// already set on `message`.
absl::Status EnsureOneofUnset(const FieldDescriptor& field, Message& message,
                              const Reflection& refl) {
  const auto* oneof = field.containing_oneof();
  if (oneof != nullptr && refl.HasOneof(message, oneof)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "multiple fields set in proto oneof %s, already had %s but attempted "
        "to set %s",
        oneof->name(), refl.GetOneofFieldDescriptor(message, oneof)->name(),
        field.name()));
  }
  return absl::OkStatus();
}

// Forward declarations for recursion.
absl::Status FillProtoMessageBreakRecursion(
    DataSlice slice, const Descriptor& message_descriptor,
    std::vector<Message* absl_nonnull> messages,
    internal::TrampolineExecutor& executor);

absl::Status FillProtoRepeatedMessageField(
    const DataSlice& attr_slice, const FieldDescriptor& field_descriptor,
    absl::Span<Message* absl_nonnull const> parent_messages,
    internal::TrampolineExecutor& executor) {
  if (!attr_slice.IsList()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("proto repeated message field %s expected Koda "
                        "DataSlice to contain only Lists but got %s",
                        field_descriptor.name(), DataSliceRepr(attr_slice)));
  }

  ASSIGN_OR_RETURN(DataSlice items, ops::Explode(attr_slice, 1));
  std::vector<Message*> child_messages;
  child_messages.reserve(items.GetShape().size());
  const auto& splits = items.GetShape().edges().back().edge_values().values;
  DCHECK_EQ(splits.size(), parent_messages.size() + 1);
  for (int64_t i = 0; i < parent_messages.size(); ++i) {
    auto& message = *parent_messages[i];
    const auto& refl = *message.GetReflection();
    for (int64_t j = splits[i]; j < splits[i + 1]; ++j) {
      auto* child_message = refl.AddMessage(&message, &field_descriptor);
      child_messages.push_back(child_message);
    }
  }
  return FillProtoMessageBreakRecursion(std::move(items),
                                        *field_descriptor.message_type(),
                                        std::move(child_messages), executor);
}

absl::Status FillProtoRepeatedPrimitiveField(
    const DataSlice& attr_slice, const FieldDescriptor& field_descriptor,
    absl::Span<Message* absl_nonnull const> parent_messages) {
  if (!attr_slice.IsList()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("proto repeated primitive field %s expected Koda "
                        "DataSlice to contain only Lists but got %s",
                        field_descriptor.name(), DataSliceRepr(attr_slice)));
  }

  ASSIGN_OR_RETURN(DataSlice items, ops::Explode(attr_slice, 1));
  if (items.present_count() != items.size()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "proto repeated field %s cannot represent missing values, but got %s",
        field_descriptor.name(), DataSliceRepr(items)));
  }
  const auto& splits = items.GetShape().edges().back().edge_values().values;
  return CallWithPrimitiveFieldCppType(
      field_descriptor.cpp_type(),
      [&]<typename DstT>(ABSL_ATTRIBUTE_UNUSED DstT*) -> absl::Status {
        for (int64_t i = 0; i < parent_messages.size(); ++i) {
          auto& message = *parent_messages[i];
          const auto& refl = *message.GetReflection();
          auto repeated_field_ref = refl.GetMutableRepeatedFieldRef<DstT>(
              &message, &field_descriptor);
          const int64_t split_begin = splits[i];
          const int64_t split_end = splits[i + 1];
          repeated_field_ref.Clear();

          // If there are multiple dtypes, this will perform multiple passes
          // over the repeated field. On the first pass, we use Add to populate
          // the repeated field, including dummy entries for missing values, and
          // on any further passes, we use Set and skip missing values.
          bool is_first_dtype = true;
          RETURN_IF_ERROR(items.slice().VisitValues(
              [&]<typename SrcT>(
                  const arolla::DenseArray<SrcT>& values) -> absl::Status {
                DCHECK(is_first_dtype ||
                       repeated_field_ref.size() == values.size());
                for (int64_t j = split_begin; j < split_end; ++j) {
                  // Note: because `MutableRepeatedFieldRef<std::string>` only
                  // has an `Add(const std::string&)` method, this makes an
                  // extra copy of any string/bytes values. This could be
                  // improved if we could get a `const std::string&` from
                  // `arolla::Text`.
                  const auto& value = values[j];
                  DstT field_value{};
                  if (value.present) {
                    ASSIGN_OR_RETURN(
                        field_value,
                        (Convert<DstT, SrcT>(field_descriptor,
                                             attr_slice.dtype(), value.value)));
                  }
                  if (is_first_dtype) {
                    repeated_field_ref.Add(field_value);
                  } else if (value.present) {
                    repeated_field_ref.Set(j - split_begin, field_value);
                  }
                }
                is_first_dtype = false;
                return absl::OkStatus();
              }));
        }
        return absl::OkStatus();
      });
}

absl::Status FillProtoMessageField(
    const DataSlice& attr_slice, const FieldDescriptor& field_descriptor,
    absl::Span<Message* absl_nonnull const> parent_messages,
    internal::TrampolineExecutor& executor) {
  ASSIGN_OR_RETURN(DataSlice mask, ops::Has(attr_slice));
  ASSIGN_OR_RETURN(
      DataSlice dense_attr_slice,
      ops::Select(attr_slice, mask, DataSlice::CreatePrimitive(false)));

  std::vector<Message* absl_nonnull> dense_child_messages;
  RETURN_IF_ERROR(
      mask.slice().VisitValues([&](const auto& values) -> absl::Status {
        absl::Status status = absl::OkStatus();
        values.ForEachPresent([&](int64_t id, auto value) {
          if (!status.ok()) {
            return;
          }

          auto& parent_message = *parent_messages[id];
          const auto& refl = *parent_message.GetReflection();
          auto oneof_status =
              EnsureOneofUnset(field_descriptor, parent_message, refl);
          if (!oneof_status.ok()) {
            status = std::move(oneof_status);
            return;
          }

          dense_child_messages.push_back(
              refl.MutableMessage(&parent_message, &field_descriptor));
        });
        return status;
      }));

  return FillProtoMessageBreakRecursion(
      std::move(dense_attr_slice), *field_descriptor.message_type(),
      std::move(dense_child_messages), executor);
}

absl::Status FillProtoPrimitiveField(
    const DataSlice& attr_slice, const FieldDescriptor& field_descriptor,
    absl::Span<Message* absl_nonnull const> parent_messages) {
  DCHECK_EQ(attr_slice.size(), parent_messages.size());
  return attr_slice.slice().VisitValues(
      [&]<typename SrcT>(const arolla::DenseArray<SrcT>& values) {
        return CallWithPrimitiveFieldCppType(
            field_descriptor.cpp_type(),
            [&]<typename DstT>(ABSL_ATTRIBUTE_UNUSED DstT*) -> absl::Status {
              absl::Status status = absl::OkStatus();
              values.ForEachPresent(
                  [&](int64_t id, arolla::view_type_t<SrcT> value) {
                    if (!status.ok()) {
                      return;
                    }

                    auto field_value_or = Convert<DstT, SrcT>(
                        field_descriptor, attr_slice.dtype(), std::move(value));
                    if (!field_value_or.ok()) {
                      status = std::move(field_value_or).status();
                      return;
                    }

                    auto& message = *parent_messages[id];
                    auto& refl = *message.GetReflection();
                    auto oneof_status =
                        EnsureOneofUnset(field_descriptor, message, refl);
                    if (!oneof_status.ok()) {
                      status = std::move(oneof_status);
                      return;
                    }

                    SetField(*std::move(field_value_or), field_descriptor,
                             message, refl);
                  });
              return status;
            });
      });
}

absl::Status FillProtoMapField(
    const DataSlice& attr_slice, const FieldDescriptor& field_descriptor,
    absl::Span<Message* absl_nonnull const> parent_messages,
    internal::TrampolineExecutor& executor) {
  if (!attr_slice.IsDict()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "proto map field %s expected Koda DataSlice to contain only Dicts but "
        "got %s",
        field_descriptor.name(), DataSliceRepr(attr_slice)));
  }

  ASSIGN_OR_RETURN(DataSlice keys, attr_slice.GetDictKeys());
  ASSIGN_OR_RETURN(DataSlice values, attr_slice.GetDictValues());
  const auto& items_shape = keys.GetShape();

  std::vector<Message* absl_nonnull> child_messages;
  child_messages.reserve(items_shape.size());
  const auto& splits = items_shape.edges().back().edge_values().values;
  for (int64_t i = 0; i < parent_messages.size(); ++i) {
    auto& message = *parent_messages[i];
    const auto& refl = *message.GetReflection();
    for (int64_t j = splits[i]; j < splits[i + 1]; ++j) {
      child_messages.push_back(refl.AddMessage(&message, &field_descriptor));
    }
  }
  DCHECK_EQ(child_messages.size(), items_shape.size());

  RETURN_IF_ERROR(FillProtoPrimitiveField(
      keys, *field_descriptor.message_type()->map_key(), child_messages));
  if (field_descriptor.message_type()->map_value()->message_type() == nullptr) {
    RETURN_IF_ERROR(FillProtoPrimitiveField(
        values, *field_descriptor.message_type()->map_value(), child_messages));
  } else {
    RETURN_IF_ERROR(FillProtoMessageField(
        values, *field_descriptor.message_type()->map_value(), child_messages,
        executor));
  }
  return absl::OkStatus();
}

absl::Status FillProtoField(
    const DataSlice& attr_slice, const FieldDescriptor& field_descriptor,
    absl::Span<Message* absl_nonnull const> parent_messages,
    internal::TrampolineExecutor& executor) {
  if (field_descriptor.is_map()) {
    return FillProtoMapField(attr_slice, field_descriptor, parent_messages,
                             executor);
  } else if (field_descriptor.is_repeated()) {
    if (field_descriptor.message_type() != nullptr) {
      return FillProtoRepeatedMessageField(attr_slice, field_descriptor,
                                           parent_messages, executor);
    } else {
      return FillProtoRepeatedPrimitiveField(attr_slice, field_descriptor,
                                             parent_messages);
    }
  } else {
    if (field_descriptor.message_type() != nullptr) {
      return FillProtoMessageField(attr_slice, field_descriptor,
                                   parent_messages, executor);
    } else {
      return FillProtoPrimitiveField(attr_slice, field_descriptor,
                                     parent_messages);
    }
  }
}

absl::Status FillProtoMessage(const DataSlice& slice,
                              const Descriptor& message_descriptor,
                              absl::Span<Message* absl_nonnull const> messages,
                              internal::TrampolineExecutor& executor) {
  if (slice.GetSchema().IsPrimitiveSchema()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "proto message should have only entities/objects, found %s",
        slice.dtype()->name()));
  }

  ASSIGN_OR_RETURN(const auto& attr_names,
                   slice.GetAttrNames(/*union_object_attrs=*/true));
  for (const auto& attr_name : attr_names) {
    const FieldDescriptor* field = nullptr;
    if (attr_name.starts_with('(') && attr_name.ends_with(')')) {
      // Interpret attrs with parentheses as fully-qualified extension paths.
      const auto ext_full_path =
          absl::string_view(attr_name).substr(1, attr_name.size() - 2);
      field =
          message_descriptor.file()->pool()->FindExtensionByName(ext_full_path);
    } else {
      field = message_descriptor.FindFieldByName(attr_name);
    }
    if (field != nullptr) {
      ASSIGN_OR_RETURN(const auto& attr_slice,
                       slice.GetAttrOrMissing(attr_name));
      if (!attr_slice.IsEmpty()) {
        RETURN_IF_ERROR(FillProtoField(attr_slice, *field, messages, executor));
      }
    }
  }
  return absl::OkStatus();
}

absl::Status FillProtoMessageBreakRecursion(
    DataSlice slice, const Descriptor& message_descriptor,
    std::vector<Message* absl_nonnull> messages,
    internal::TrampolineExecutor& executor) {
  executor.Enqueue([slice = std::move(slice), &message_descriptor,
                    messages = std::move(messages),
                    &executor]() -> absl::Status {
    return FillProtoMessage(slice, message_descriptor, messages, executor);
  });
  return absl::OkStatus();
}

bool SetFieldTypeFromDType(const schema::DType& dtype,
                           google::protobuf::FieldDescriptorProto* field,
                           std::vector<std::string>* warnings,
                           absl::string_view message_full_name,
                           absl::string_view attr_name) {
  const schema::DTypeId type_id = dtype.type_id();
  switch (type_id) {
    case schema::kInt32.type_id():
      field->set_type(google::protobuf::FieldDescriptorProto::TYPE_INT32);
      return true;
    case schema::kInt64.type_id():
      field->set_type(google::protobuf::FieldDescriptorProto::TYPE_INT64);
      return true;
    case schema::kFloat32.type_id():
      field->set_type(google::protobuf::FieldDescriptorProto::TYPE_FLOAT);
      return true;
    case schema::kFloat64.type_id():
      field->set_type(google::protobuf::FieldDescriptorProto::TYPE_DOUBLE);
      return true;
    case schema::kBool.type_id():
    case schema::kMask.type_id():
      field->set_type(google::protobuf::FieldDescriptorProto::TYPE_BOOL);
      return true;
    case schema::kString.type_id():
      field->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
      return true;
    case schema::kBytes.type_id():
      field->set_type(google::protobuf::FieldDescriptorProto::TYPE_BYTES);
      return true;
    default:
      if (warnings != nullptr) {
        warnings->push_back(absl::StrCat("ignored type: ", dtype.name(),
                                         " and hence not adding proto field ",
                                         message_full_name, ".", attr_name));
      }
      return false;
  }
}

inline bool IsUnderscore(char c) { return c == '_'; }

bool IsValidProtoFieldName(absl::string_view attr_name) {
  // According to
  // https://protobuf.dev/reference/protobuf/proto3-spec/#identifiers, valid
  // field names use the following grammar:
  //
  // letter = "A" ... "Z" | "a" ... "z"
  //
  // decimalDigit = "0" ... "9"
  //
  // ident = letter { letter | decimalDigit | "_" }
  //
  // fieldName = ident
  if (attr_name.empty()) {
    return false;
  }
  if (!absl::ascii_isalpha(attr_name[0])) {
    return false;
  }
  for (char c : attr_name) {
    if (!absl::ascii_isalpha(c) && !absl::ascii_isdigit(c) &&
        !IsUnderscore(c)) {
      return false;
    }
  }
  return true;
}

std::string NestedMessageNameForAttribute(absl::string_view attr_name,
                                          const DataSlice& attr_schema) {
  DCHECK(IsValidProtoFieldName(attr_name));

  // The message name of proto3 map entries must be the CamelCase of the map
  // field name plus "Entry". That is a hard requirement of valid proto3 maps.
  constexpr absl::string_view schema_suffix = "Schema";
  constexpr absl::string_view dict_suffix = "Entry";
  const absl::string_view& suffix =
      attr_schema.IsDictSchema() ? dict_suffix : schema_suffix;

  bool capitalize_next = true;
  std::string result;
  result.reserve(attr_name.size() + suffix.size());

  for (char c : attr_name) {
    if (IsUnderscore(c)) {
      capitalize_next = true;
    } else if (capitalize_next) {
      result.push_back(absl::ascii_toupper(c));
      capitalize_next = false;
    } else {
      result.push_back(c);
    }
  }
  absl::StrAppend(&result, suffix);

  return result;
}

struct DescriptorInfo {
  google::protobuf::DescriptorProto& descriptor_proto;
  std::string message_full_name;
};

std::optional<DescriptorInfo>
SetFieldTypeAndPossiblyCreateUnpopulatedMessageDescriptor(
    google::protobuf::DescriptorProto& descriptor_proto,
    absl::string_view message_full_name, google::protobuf::FieldDescriptorProto& field,
    absl::string_view attr_name, const DataSlice& attr_schema,
    absl::flat_hash_map<internal::DataItem, std::string>& descriptor_map) {
  field.set_type(google::protobuf::FieldDescriptorProto::TYPE_MESSAGE);
  if (!attr_schema.IsDictSchema()) {
    // Why the condition of not being a dict schema?
    // Dict schemas are handled separately because the proto descriptor must
    // contain one map entry message per map field. That is a hard requirement
    // of proto3 maps.
    // So we need to create a nested message descriptor for each map field, and
    // we cannot reuse it for other map fields.
    std::string& nested_message_full_name = descriptor_map[attr_schema.item()];
    if (!nested_message_full_name.empty()) {
      field.set_type_name(nested_message_full_name);
      return std::nullopt;
    }
  }
  std::string nested_message_name =
      NestedMessageNameForAttribute(attr_name, attr_schema);
  std::string nested_message_full_name =
      absl::StrCat(message_full_name, ".", nested_message_name);
  field.set_type_name(nested_message_full_name);
  google::protobuf::DescriptorProto& nested_descriptor_proto =
      *descriptor_proto.add_nested_type();
  nested_descriptor_proto.set_name(std::move(nested_message_name));
  if (!attr_schema.IsDictSchema()) {
    descriptor_map[attr_schema.item()] = nested_message_full_name;
  }
  return DescriptorInfo{
      .descriptor_proto = nested_descriptor_proto,
      .message_full_name = std::move(nested_message_full_name),
  };
}

// In Proto3, the key_type can be any integral or string type (so, any scalar
// type except for floating point types and bytes).
bool IsAcceptableMapKeySchema(const DataSlice& schema) {
  if (!schema.IsPrimitiveSchema()) {
    return false;
  }
  const auto& dtype = schema.item().value<schema::DType>();
  switch (dtype.type_id()) {
    case schema::kInt32.type_id():
    case schema::kInt64.type_id():
    case schema::kBool.type_id():
    case schema::kMask.type_id():
    case schema::kString.type_id():
      return true;
    default:
      return false;
  }
}

absl::Status PopulateDescriptor(
    const DataSlice& schema, google::protobuf::DescriptorProto& descriptor_proto,
    absl::string_view message_full_name,
    absl::flat_hash_map<internal::DataItem, std::string>& descriptor_map,
    std::vector<std::string>* warnings) {
  RETURN_IF_ERROR(schema.VerifyIsEntitySchema());
  ASSIGN_OR_RETURN(
      const auto attr_names, schema.GetAttrNames(),
      _ << "failed to get attribute names for: " << DataSliceRepr(schema));
  for (const std::string& attr_name : attr_names) {
    if (!IsValidProtoFieldName(attr_name)) {
      if (warnings != nullptr) {
        warnings->push_back(
            absl::StrCat("ignored attribute name ", attr_name,
                         " encountered in schema ", DataSliceRepr(schema),
                         " because it is not a valid proto field name"));
      }
      continue;
    }
    google::protobuf::FieldDescriptorProto field;
    ASSIGN_OR_RETURN(DataSlice attr, schema.GetAttr(attr_name),
                     _ << "failed to get attribute: " << attr_name
                       << " for: " << DataSliceRepr(schema));
    RETURN_IF_ERROR(attr.VerifyIsSchema());
    if (attr.IsPrimitiveSchema()) {
      field.set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
      if (!SetFieldTypeFromDType(attr.item().value<schema::DType>(), &field,
                                 warnings, message_full_name, attr_name)) {
        continue;
      }
    } else if (attr.IsListSchema()) {
      field.set_label(google::protobuf::FieldDescriptorProto::LABEL_REPEATED);
      ASSIGN_OR_RETURN(auto items_schema,
                       attr.GetAttr(schema::kListItemsSchemaAttr));
      RETURN_IF_ERROR(items_schema.VerifyIsSchema());
      if (items_schema.IsPrimitiveSchema()) {
        if (!SetFieldTypeFromDType(items_schema.item().value<schema::DType>(),
                                   &field, warnings, message_full_name,
                                   attr_name)) {
          continue;
        }
      } else if (items_schema.IsEntitySchema()) {
        auto info = SetFieldTypeAndPossiblyCreateUnpopulatedMessageDescriptor(
            descriptor_proto, message_full_name, field, attr_name, items_schema,
            descriptor_map);
        if (info.has_value()) {
          RETURN_IF_ERROR(PopulateDescriptor(
              items_schema, info->descriptor_proto, info->message_full_name,
              descriptor_map, warnings));
        }
      } else {
        if (warnings != nullptr) {
          warnings->push_back(absl::StrCat(
              "unsupported LIST schema type: ", DataSliceRepr(attr),
              ". Supported LIST schemas must have items with either a "
              "primitive schema or an entity schema"));
        }
        continue;
      }
    } else if (attr.IsDictSchema()) {
      field.set_label(google::protobuf::FieldDescriptorProto::LABEL_REPEATED);
      auto dict_info =
          SetFieldTypeAndPossiblyCreateUnpopulatedMessageDescriptor(
              descriptor_proto, message_full_name, field, attr_name, attr,
              descriptor_map);
      if (dict_info.has_value()) {
        ASSIGN_OR_RETURN(auto key_schema,
                         attr.GetAttr(schema::kDictKeysSchemaAttr));
        RETURN_IF_ERROR(key_schema.VerifyIsSchema());
        if (!IsAcceptableMapKeySchema(key_schema)) {
          if (warnings != nullptr) {
            warnings->push_back(absl::StrCat(
                "unsupported DICT schema type: ", DataSliceRepr(attr),
                ". Supported DICT schemas must have keys that are integral "
                "types or strings (floats, bytes and non-primitive keys are "
                "not supported)"));
          }
          continue;
        }
        ASSIGN_OR_RETURN(auto value_schema,
                         attr.GetAttr(schema::kDictValuesSchemaAttr));
        RETURN_IF_ERROR(value_schema.VerifyIsSchema());
        google::protobuf::FieldDescriptorProto key_field;
        key_field.set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
        key_field.set_name("key");
        key_field.set_number(1);
        if (!SetFieldTypeFromDType(key_schema.item().value<schema::DType>(),
                                   &key_field, warnings,
                                   dict_info->message_full_name, "key")) {
          continue;
        }
        google::protobuf::FieldDescriptorProto value_field;
        value_field.set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
        value_field.set_name("value");
        value_field.set_number(2);
        if (value_schema.IsPrimitiveSchema()) {
          if (!SetFieldTypeFromDType(value_schema.item().value<schema::DType>(),
                                     &value_field, warnings,
                                     dict_info->message_full_name, "value")) {
            continue;
          }
        } else if (value_schema.IsEntitySchema()) {
          // Map values that are complex entities need messages.
          // These messages must exist on the same or higher level as the map
          // field to be valid proto3.
          auto value_info =
              SetFieldTypeAndPossiblyCreateUnpopulatedMessageDescriptor(
                  descriptor_proto, message_full_name, value_field,
                  absl::StrCat(attr_name, "_entry_value"), value_schema,
                  descriptor_map);
          if (value_info.has_value()) {
            RETURN_IF_ERROR(PopulateDescriptor(
                value_schema, value_info->descriptor_proto,
                value_info->message_full_name, descriptor_map, warnings));
          }
        } else {
          if (warnings != nullptr) {
            warnings->push_back(absl::StrCat(
                "unsupported DICT schema type: ", DataSliceRepr(attr),
                ". Supported DICT schemas must have values with either a "
                "primitive schema or an entity schema"));
          }
          continue;
        }
        dict_info->descriptor_proto.mutable_options()->set_map_entry(true);
        dict_info->descriptor_proto.mutable_field()->Add(std::move(key_field));
        dict_info->descriptor_proto.mutable_field()->Add(
            std::move(value_field));
      }
    } else if (attr.IsEntitySchema()) {
      field.set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
      auto info = SetFieldTypeAndPossiblyCreateUnpopulatedMessageDescriptor(
          descriptor_proto, message_full_name, field, attr_name, attr,
          descriptor_map);
      if (info.has_value()) {
        RETURN_IF_ERROR(PopulateDescriptor(attr, info->descriptor_proto,
                                           info->message_full_name,
                                           descriptor_map, warnings));
      }
    } else {
      if (warnings != nullptr) {
        warnings->push_back(
            absl::StrCat("unsupported schema type: ", DataSliceRepr(attr)));
      }
      continue;
    }
    field.set_name(attr_name);
    field.set_number(descriptor_proto.field().size() + 1);
    *descriptor_proto.add_field() = std::move(field);
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status ToProto(
    const DataSlice& slice,
    absl::Span<::google::protobuf::Message* absl_nonnull const> messages) {
  if (slice.GetShape().rank() != 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected 1-D DataSlice, got ndim=%d", slice.GetShape().rank()));
  }

  if (slice.size() != messages.size()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected slice and messages to have the same size, got %d and %d",
        slice.size(), messages.size()));
  }

  if (messages.empty()) {
    return absl::OkStatus();
  }

  const Descriptor* message_descriptor = messages[0]->GetDescriptor();
  for (const Message* message : messages) {
    if (message->GetDescriptor() != message_descriptor) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected all messages to have the same type, got %s and %s",
          message_descriptor->full_name(),
          message->GetDescriptor()->full_name()));
    }
  }

  if (slice.IsEmpty()) {
    return absl::OkStatus();
  }

  return internal::TrampolineExecutor::Run([&](auto& executor) -> absl::Status {
    return FillProtoMessage(slice, *message_descriptor, messages, executor);
  });
}

absl::StatusOr<google::protobuf::FileDescriptorProto> ProtoDescriptorFromSchema(
    const DataSlice& schema, std::vector<std::string>* warnings,
    absl::string_view file_name,
    std::optional<absl::string_view> descriptor_package_name,
    absl::string_view root_message_name) {
  RETURN_IF_ERROR(schema.VerifyIsEntitySchema());
  absl::flat_hash_map<internal::DataItem, std::string> descriptor_map;
  google::protobuf::FileDescriptorProto descriptor;
  descriptor.set_name(file_name);
  if (descriptor_package_name.has_value()) {
    descriptor.set_package(*descriptor_package_name);
  } else {
    descriptor.set_package(
        absl::StrCat("koladata.ephemeral.schema_",
                     schema.item().StableFingerprint().AsString()));
  }
  google::protobuf::DescriptorProto* descriptor_proto = descriptor.add_message_type();
  descriptor_proto->set_name(root_message_name);
  std::string message_full_name =
      absl::StrCat(descriptor.package(), ".", root_message_name);
  descriptor_map.insert({schema.item(), message_full_name});
  RETURN_IF_ERROR(PopulateDescriptor(schema, *descriptor_proto,
                                     std::move(message_full_name),
                                     descriptor_map, warnings));
  return descriptor;
}

absl::StatusOr<std::vector<std::unique_ptr<google::protobuf::Message>>> ToProtoMessages(
    const DataSlice& x, const google::protobuf::Message* absl_nonnull message_prototype) {
  std::vector<google::protobuf::Message*> messages;
  messages.reserve(x.size());
  std::vector<std::unique_ptr<google::protobuf::Message>> owned_messages;
  owned_messages.reserve(x.size());
  for (int64_t i = 0; i < x.size(); ++i) {
    auto message = std::unique_ptr<google::protobuf::Message>(message_prototype->New());
    messages.push_back(message.get());
    owned_messages.push_back(std::move(message));
  }
  RETURN_IF_ERROR(ToProto(std::move(x).Flatten(), std::move(messages)));
  return std::move(owned_messages);
}

}  // namespace koladata
