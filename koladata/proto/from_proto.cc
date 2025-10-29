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
#include "koladata/proto/from_proto.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/adoption_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/trampoline_executor.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/object_factories.h"
#include "koladata/operators/slices.h"
#include "koladata/proto/proto_schema_utils.h"
#include "koladata/shape_utils.h"
#include "koladata/uuid_utils.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "arolla/util/status_macros_backport.h"

using ::google::protobuf::Descriptor;
using ::google::protobuf::DescriptorPool;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Message;
using ::google::protobuf::Reflection;

namespace koladata {
namespace {

// Extension specifier parsing.

struct ExtensionMap {
  // Extension fields that should be converted in this message.
  //
  // Key: "(" + field->full_name() + ")"
  absl::flat_hash_map<std::string, const FieldDescriptor* absl_nonnull>
      extension_fields;

  // Extension maps for sub-messages of this message.
  //
  // Key: field name of submessage field for normal fields, or
  // "(" + field->full_name() + ")" for extension fields.
  absl::flat_hash_map<std::string, std::unique_ptr<ExtensionMap>>
      submessage_extension_maps;
};

absl::Status ParseExtensionInto(const google::protobuf::DescriptorPool& pool,
                                absl::string_view extension_specifier,
                                ExtensionMap& root_extension_map) {
  ExtensionMap* absl_nonnull extension_map = &root_extension_map;

  auto get_or_create_sub_map = [](ExtensionMap& map,
                                  absl::string_view ext_name) {
    std::unique_ptr<ExtensionMap>& sub_map =
        map.submessage_extension_maps[ext_name];
    if (sub_map == nullptr) {
      sub_map = std::make_unique<ExtensionMap>();
    }
    return sub_map.get();
  };

  // When we encounter a '(', set `in_ext_path`, and accumulate path pieces
  // into `ext_path_pieces` until we encounter a ')', then clear `in_ext_path`.
  bool in_ext_path = false;
  std::vector<absl::string_view> ext_path_pieces;

  const std::vector<absl::string_view> pieces =
      absl::StrSplit(extension_specifier, absl::ByChar('.'));
  for (size_t i_piece = 0; i_piece < pieces.size(); ++i_piece) {
    const auto& piece = pieces[i_piece];
    if (piece.starts_with('(')) {
      if (!ext_path_pieces.empty()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "invalid extension path (unexpected opening parenthesis): \"%s\"",
            extension_specifier));
      }
      in_ext_path = true;
    }
    if (piece.ends_with(')')) {
      if (!in_ext_path) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "invalid extension path (unexpected closing parenthesis): \"%s\"",
            extension_specifier));
      }
      ext_path_pieces.push_back(piece);
      in_ext_path = false;

      // Note: `ext_name` includes starting and ending parens.
      auto ext_name = absl::StrJoin(ext_path_pieces, ".");
      DCHECK_GE(ext_name.size(), 2);
      auto ext_full_path =
          absl::string_view(&ext_name.data()[1], ext_name.size() - 2);
      ext_path_pieces.clear();

      const auto* ext_field_descriptor =
          pool.FindExtensionByName(ext_full_path);
      if (ext_field_descriptor == nullptr) {
        return absl::InvalidArgumentError(
            absl::StrFormat("extension not found: \"%s\"", ext_full_path));
      }

      if (i_piece == pieces.size() - 1) {
        extension_map->extension_fields[ext_name] = ext_field_descriptor;
      } else {
        extension_map = get_or_create_sub_map(*extension_map, ext_name);
      }
      continue;
    }

    if (in_ext_path) {
      ext_path_pieces.push_back(piece);
    } else {
      if (i_piece == pieces.size() - 1) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "invalid extension path (trailing non-extension field): \"%s\"",
            extension_specifier));
      }
      extension_map = get_or_create_sub_map(*extension_map, piece);
    }
  }

  if (in_ext_path) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "invalid extension path (missing closing parenthesis): \"%s\"",
        extension_specifier));
  }

  return absl::OkStatus();
}

absl::StatusOr<ExtensionMap> ParseExtensions(
    absl::Span<const absl::string_view> extensions,
    const DescriptorPool& pool) {
  ExtensionMap result;
  for (const auto& extension_path : extensions) {
    RETURN_IF_ERROR(ParseExtensionInto(pool, extension_path, result));
  }
  return result;
}

const ExtensionMap* GetChildExtensionMap(const ExtensionMap* extension_map,
                                         absl::string_view field_name) {
  if (extension_map != nullptr) {
    const auto& sub_maps = extension_map->submessage_extension_maps;
    if (auto lookup = sub_maps.find(field_name); lookup != sub_maps.end()) {
      return lookup->second.get();
    }
  }
  return nullptr;
}

// Shape / Schema / ItemId Helpers.

absl::StatusOr<std::optional<DataSlice>> GetChildAttrSchema(
    const std::optional<DataSlice>& schema, absl::string_view attr_name) {
  if (!schema.has_value()) {
    return std::nullopt;
  }
  if (schema->item() == schema::kObject) {
    return schema;  // return OBJECT;
  }
  ASSIGN_OR_RETURN(auto child_schema, schema->GetAttr(attr_name));
  return child_schema;
}

absl::StatusOr<std::optional<DataSlice>> GetMessageListItemsSchema(
    const std::optional<DataSlice>& schema) {
  return GetChildAttrSchema(schema, schema::kListItemsSchemaAttr);
}

absl::StatusOr<std::optional<DataSlice>> GetPrimitiveListItemsSchema(
    const std::optional<DataSlice>& schema) {
  if (!schema.has_value() || schema->item() == schema::kObject) {
    return std::nullopt;
  }
  ASSIGN_OR_RETURN(auto child_schema,
                   schema->GetAttr(schema::kListItemsSchemaAttr));
  return child_schema;
}

absl::StatusOr<DataSlice> CreateBareProtoUuSchema(
    const DataBagPtr& db, const Descriptor& message_descriptor) {
  return CreateUuSchema(
      db, absl::StrFormat("__from_proto_%s__", message_descriptor.full_name()),
      {}, {});
}

// Returns a metadata OBJECT for the given proto schema.
absl::StatusOr<DataSlice> CreateMessageSchemaMetadata(
    const DataBagPtr& db, const DataSlice& schema,
    const Descriptor& message_descriptor) {
  ASSIGN_OR_RETURN(auto metadata, CreateMetadata(db, schema, {}, {}));
  RETURN_IF_ERROR(metadata.SetAttr(schema::kProtoSchemaMetadataFullNameAttr,
                                   DataSlice::CreatePrimitive(arolla::Text(
                                       message_descriptor.full_name()))));
  return metadata;
}

constexpr static absl::string_view kChildItemIdSeed = "__from_proto_child__";

// Returns a rank-1 DataSlice of ITEMID containing unique uuids for each index
// in the 2D shape `items_shape` (or nullopt if `parent_itemid` is nullopt).
// The result is flattened, but has the same total size as `items_shape`.
//
// If not nullopt, `parent_itemid` must be a rank-1 DataSlice of ITEMID with the
// same size as the first dimension of `items_shape`. Each child item id is a
// deterministic function of its parent item id (determined by the first dim)
// and its index into the second dim.
absl::StatusOr<std::optional<DataSlice>> MakeFlatChildIndexItemUuids(
    const std::optional<DataSlice>& parent_itemid,
    const DataSlice::JaggedShape& items_shape) {
  DCHECK_EQ(items_shape.rank(), 2);

  if (!parent_itemid.has_value()) {
    return std::nullopt;
  }

  // Ideally we'd call something like `M.array.agg_index` to make
  // `list_index`. This is tricky to do from C++, and the equivalent
  // code is only a few lines anyway.
  arolla::DenseArrayBuilder<int64_t> flat_index_builder(items_shape.size());
  const auto& splits = items_shape.edges()[1].edge_values().values;
  for (int64_t i_split = 0; i_split < splits.size() - 1; ++i_split) {
    for (int64_t i = splits[i_split]; i < splits[i_split + 1]; ++i) {
      flat_index_builder.Add(i, i - splits[i_split]);
    }
  }
  ASSIGN_OR_RETURN(
      auto index,
      DataSlice::Create(internal::DataSliceImpl::Create(
                            std::move(flat_index_builder).Build()),
                        items_shape, internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(auto child_itemids,
                   CreateUuidFromFields(kChildItemIdSeed, {"parent", "index"},
                                        {*parent_itemid, std::move(index)}));
  ASSIGN_OR_RETURN(
      auto flat_child_itemids,
      std::move(child_itemids)
          .Reshape(items_shape.FlattenDims(0, items_shape.rank())));
  return std::move(flat_child_itemids);
}

absl::StatusOr<DataSlice> DefaultValueFromProtoPrimitiveField(
    const FieldDescriptor& field_descriptor) {
  DCHECK(field_descriptor.message_type() == nullptr);
  internal::DataItem default_item;
  switch (field_descriptor.cpp_type()) {
    case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
      return DataSlice::CreatePrimitive(field_descriptor.default_value_int32());
    case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
      return DataSlice::CreatePrimitive(static_cast<int32_t>(
          field_descriptor.default_value_enum()->number()));
    case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
      return DataSlice::CreatePrimitive(field_descriptor.default_value_int64());
    case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
      return DataSlice::CreatePrimitive(
          static_cast<int64_t>(field_descriptor.default_value_uint32()));
    case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
      return DataSlice::CreatePrimitive(
          static_cast<int64_t>(field_descriptor.default_value_uint64()));
    case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
      return DataSlice::CreatePrimitive(
          field_descriptor.default_value_double());
    case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
      return DataSlice::CreatePrimitive(field_descriptor.default_value_float());
    case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
      return DataSlice::CreatePrimitive(field_descriptor.default_value_bool());
    case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
      if (field_descriptor.type() == google::protobuf::FieldDescriptor::TYPE_BYTES) {
        return DataSlice::CreatePrimitive(
            arolla::Bytes(field_descriptor.default_value_string()));
      } else {
        return DataSlice::CreatePrimitive(
            arolla::Text(field_descriptor.default_value_string()));
      }
      break;
    default:
      DCHECK(false);
      return absl::InvalidArgumentError(
          absl::StrFormat("expected primitive proto field, got %v",
                          field_descriptor.DebugString()));
  }
}

// Forward declarations for "recursion" via an enqueued callback.
absl::Status FromProtoMessageBreakRecursion(
    const absl_nonnull DataBagPtr& db, const Descriptor& message_descriptor,
    std::vector<const Message* absl_nonnull> messages,
    std::optional<DataSlice> itemid, std::optional<DataSlice> schema,
    const ExtensionMap* absl_nullable extension_map,
    internal::TrampolineExecutor& executor, std::optional<DataSlice>& result);

// Returns a rank-1 DataSlice of Lists converted from a repeated message field
// on a vector of messages.
absl::Status ListFromProtoRepeatedMessageField(
    const absl_nonnull DataBagPtr& db, absl::string_view attr_name,
    absl::string_view field_name, const FieldDescriptor& field_descriptor,
    absl::Span<const Message* absl_nonnull const> parent_messages,
    const std::optional<DataSlice>& parent_itemid,
    const std::optional<DataSlice>& parent_schema,
    const ExtensionMap* absl_nullable parent_extension_map,
    internal::TrampolineExecutor& executor, std::optional<DataSlice>& result) {
  bool is_empty = true;
  arolla::DenseArrayBuilder<arolla::Unit> lists_mask_builder(
      parent_messages.size());
  DataSlice::JaggedShape parent_shape =
      DataSlice::JaggedShape::FlatFromSize(parent_messages.size());
  shape::ShapeBuilder shape_builder(parent_shape);
  std::vector<const Message* absl_nonnull> flat_child_messages;
  for (int64_t i = 0; i < parent_messages.size(); ++i) {
    const auto& parent_message = *parent_messages[i];
    const auto& refl = *parent_message.GetReflection();
    const auto& field_ref =
        refl.GetRepeatedFieldRef<Message>(parent_message, &field_descriptor);
    shape_builder.Add(field_ref.size());
    for (const auto& child_message : field_ref) {
      flat_child_messages.push_back(&child_message);
    }
    if (!field_ref.empty()) {
      lists_mask_builder.Add(i, arolla::kUnit);
      is_empty = false;
    }
  }
  if (is_empty) {
    // result is already std::nullopt
    return absl::OkStatus();
  }

  struct CallbackVars {
    std::optional<DataSlice> schema;
    std::optional<DataSlice> itemid;
    DataSlice::JaggedShape items_shape;
    std::optional<DataSlice> lists_mask;
    std::optional<DataSlice> flat_items;
  };
  auto vars = std::make_unique<CallbackVars>();

  ASSIGN_OR_RETURN(vars->schema, GetChildAttrSchema(parent_schema, attr_name));
  ASSIGN_OR_RETURN(
      vars->itemid,
      MakeChildListAttrItemIds(parent_itemid, kChildItemIdSeed, attr_name));
  const auto* extension_map =
      GetChildExtensionMap(parent_extension_map, field_name);

  ASSIGN_OR_RETURN(vars->items_shape, std::move(shape_builder).Build());
  ASSIGN_OR_RETURN(auto items_schema, GetMessageListItemsSchema(vars->schema));
  ASSIGN_OR_RETURN(
      auto flat_items_itemid,
      MakeFlatChildIndexItemUuids(vars->itemid, vars->items_shape));
  ASSIGN_OR_RETURN(vars->lists_mask,
                   DataSlice::Create(internal::DataSliceImpl::Create(
                                         std::move(lists_mask_builder).Build()),
                                     DataSlice::JaggedShape::FlatFromSize(
                                         parent_messages.size()),
                                     internal::DataItem(schema::kMask)));
  RETURN_IF_ERROR(FromProtoMessageBreakRecursion(
      db, *field_descriptor.message_type(), std::move(flat_child_messages),
      std::move(flat_items_itemid), std::move(items_schema), extension_map,
      executor, vars->flat_items));

  executor.Enqueue([db = db, vars = std::move(vars),
                    &result]() -> absl::Status {
    ASSIGN_OR_RETURN(
        auto items,
        std::move(*vars->flat_items).Reshape(std::move(vars->items_shape)));
    ASSIGN_OR_RETURN(
        auto lists,
        CreateListLike(db, std::move(*vars->lists_mask), std::move(items),
                       std::nullopt, std::nullopt, vars->itemid));
    if (vars->schema.has_value() && vars->schema->item() == schema::kObject) {
      ASSIGN_OR_RETURN(lists, ToObject(std::move(lists)));
    }
    result = std::move(lists);
    return absl::OkStatus();
  });
  return absl::OkStatus();
}

// Returns a rank-1 DataSlice of Lists of primitives converted from a repeated
// primitive field on a vector of messages.
absl::StatusOr<std::optional<DataSlice>> ListFromProtoRepeatedPrimitiveField(
    const absl_nonnull DataBagPtr& db, absl::string_view attr_name,
    absl::string_view field_name, const FieldDescriptor& field_descriptor,
    absl::Span<const Message* absl_nonnull const> parent_messages,
    const std::optional<DataSlice>& parent_itemid,
    const std::optional<DataSlice>& parent_schema) {
  auto to_slice = [&]<typename T, typename U>()
      -> absl::StatusOr<std::optional<DataSlice>> {
    int64_t num_items = 0;
    for (int64_t i = 0; i < parent_messages.size(); ++i) {
      const auto& parent_message = *parent_messages[i];
      const auto* refl = parent_message.GetReflection();
      const auto& field_ref =
          refl->GetRepeatedFieldRef<U>(parent_message, &field_descriptor);
      num_items += field_ref.size();
    }
    if (num_items == 0) {
      return std::nullopt;
    }

    arolla::DenseArrayBuilder<T> flat_items_builder(num_items);
    arolla::DenseArrayBuilder<arolla::Unit> lists_mask_builder(
        parent_messages.size());
    DataSlice::JaggedShape parent_shape =
        DataSlice::JaggedShape::FlatFromSize(parent_messages.size());
    shape::ShapeBuilder shape_builder(parent_shape);
    int64_t i_next_flat_item = 0;
    for (int64_t i = 0; i < parent_messages.size(); ++i) {
      const auto& parent_message = *parent_messages[i];
      const auto& refl = *parent_message.GetReflection();
      const auto& field_ref =
          refl.GetRepeatedFieldRef<U>(parent_message, &field_descriptor);
      shape_builder.Add(field_ref.size());
      for (const U& item_value : field_ref) {
        flat_items_builder.Add(i_next_flat_item, item_value);
        ++i_next_flat_item;
      }
      if (!field_ref.empty()) {
        lists_mask_builder.Add(i, arolla::kUnit);
      }
    }

    ASSIGN_OR_RETURN(auto schema, GetChildAttrSchema(parent_schema, attr_name));
    ASSIGN_OR_RETURN(
        auto itemid,
        MakeChildListAttrItemIds(parent_itemid, kChildItemIdSeed, attr_name));

    ASSIGN_OR_RETURN(auto items_shape, std::move(shape_builder).Build());
    ASSIGN_OR_RETURN(
        auto items,
        DataSlice::Create(internal::DataSliceImpl::Create(
                              std::move(flat_items_builder).Build()),
                          std::move(items_shape),
                          internal::DataItem(schema::GetDType<T>())));
    ASSIGN_OR_RETURN(auto items_schema, GetPrimitiveListItemsSchema(schema));
    if (items_schema.has_value()) {
      // We could probably improve performance by using the correct backing
      // DenseArray type based on `schema` instead of casting afterward, but
      // that has a lot more cases to handle, and only has an effect if the
      // user provides an explicit schema that disagrees with the proto field
      // schemas, which should be rare.
      //
      // `validate_schema` is a no-op for primitives, so we disable it.
      ASSIGN_OR_RETURN(items, CastToExplicit(items, items_schema->item(),
                                             /*validate_schema=*/false));
    }

    ASSIGN_OR_RETURN(
        auto lists_mask,
        DataSlice::Create(
            internal::DataSliceImpl::Create(
                std::move(lists_mask_builder).Build()),
            DataSlice::JaggedShape::FlatFromSize(parent_messages.size()),
            internal::DataItem(schema::kMask)));
    ASSIGN_OR_RETURN(auto lists,
                     CreateListLike(db, std::move(lists_mask), std::move(items),
                                    std::nullopt, std::nullopt, itemid));
    if (schema.has_value() && schema->item() == schema::kObject) {
      ASSIGN_OR_RETURN(lists, ToObject(std::move(lists)));
    }
    return lists;
  };

  switch (field_descriptor.cpp_type()) {
    case FieldDescriptor::CPPTYPE_INT32:
      return to_slice.operator()<int32_t, int32_t>();
    case FieldDescriptor::CPPTYPE_INT64:
      return to_slice.operator()<int64_t, int64_t>();
    case FieldDescriptor::CPPTYPE_UINT32:
      return to_slice.operator()<int64_t, uint32_t>();
    case FieldDescriptor::CPPTYPE_UINT64:
      return to_slice.operator()<int64_t, uint64_t>();
    case FieldDescriptor::CPPTYPE_DOUBLE:
      return to_slice.operator()<double, double>();
    case FieldDescriptor::CPPTYPE_FLOAT:
      return to_slice.operator()<float, float>();
    case FieldDescriptor::CPPTYPE_BOOL:
      return to_slice.operator()<bool, bool>();
    case FieldDescriptor::CPPTYPE_ENUM:
      return to_slice.operator()<int32_t, int32_t>();
    case FieldDescriptor::CPPTYPE_STRING:
      if (field_descriptor.type() == FieldDescriptor::TYPE_STRING) {
        return to_slice.operator()<arolla::Text, std::string>();
      } else {  // TYPE_BYTES
        return to_slice.operator()<arolla::Bytes, std::string>();
      }
    default:
      return absl::InvalidArgumentError(absl::StrFormat(
          "unexpected proto field C++ type %d", field_descriptor.cpp_type()));
  }
}

// Returns a rank-1 DataSlice of objects or entities converted from a proto
// non-repeated message field on a vector of messages.
absl::Status FromProtoMessageField(
    const absl_nonnull DataBagPtr& db, absl::string_view attr_name,
    absl::string_view field_name, const FieldDescriptor& field_descriptor,
    absl::Span<const Message* absl_nonnull const> parent_messages,
    const std::optional<DataSlice>& parent_itemid,
    const std::optional<DataSlice>& parent_schema,
    const ExtensionMap* absl_nullable parent_extension_map,
    bool ignore_field_presence, internal::TrampolineExecutor& executor,
    std::optional<DataSlice>& result) {
  bool is_empty = true;
  arolla::DenseArrayBuilder<arolla::Unit> mask_builder(parent_messages.size());
  std::vector<const Message* absl_nonnull> packed_child_messages;
  packed_child_messages.reserve(parent_messages.size());
  for (int64_t i = 0; i < parent_messages.size(); ++i) {
    const auto* parent_message = parent_messages[i];
    const auto* refl = parent_message->GetReflection();
    if (ignore_field_presence ||
        refl->HasField(*parent_message, &field_descriptor)) {
      packed_child_messages.push_back(
          &refl->GetMessage(*parent_message, &field_descriptor));
      mask_builder.Add(i, arolla::kUnit);
      is_empty = false;
    }
  }
  if (is_empty) {
    // result is already std::nullopt
    return absl::OkStatus();
  }

  ASSIGN_OR_RETURN(auto schema, GetChildAttrSchema(parent_schema, attr_name));
  ASSIGN_OR_RETURN(
      auto itemid,
      MakeChildObjectAttrItemIds(parent_itemid, kChildItemIdSeed, attr_name));
  const auto* extension_map =
      GetChildExtensionMap(parent_extension_map, field_name);

  struct CallbackVars {
    std::optional<DataSlice> mask;
    std::optional<DataSlice> packed_values;
  };
  auto vars = std::make_unique<CallbackVars>();

  ASSIGN_OR_RETURN(
      vars->mask,
      DataSlice::Create(
          internal::DataSliceImpl::Create(std::move(mask_builder).Build()),
          DataSlice::JaggedShape::FlatFromSize(parent_messages.size()),
          internal::DataItem(schema::kMask)));
  ASSIGN_OR_RETURN(auto packed_itemid,
                   [&]() -> absl::StatusOr<std::optional<DataSlice>> {
                     if (!itemid.has_value()) {
                       return std::nullopt;
                     }
                     ASSIGN_OR_RETURN(
                         auto packed_itemid,
                         ops::Select(*itemid, *vars->mask,
                                     DataSlice::CreatePrimitive(false)));
                     return packed_itemid;
                   }());

  RETURN_IF_ERROR(FromProtoMessageBreakRecursion(
      db, *field_descriptor.message_type(), std::move(packed_child_messages),
      std::move(packed_itemid), std::move(schema), extension_map, executor,
      vars->packed_values));

  executor.Enqueue([vars = std::move(vars), &result]() -> absl::Status {
    ASSIGN_OR_RETURN(result, ops::InverseSelect(std::move(*vars->packed_values),
                                                std::move(*vars->mask)));
    return absl::OkStatus();
  });

  return absl::OkStatus();
}

// Returns a rank-1 DataSlice of primitives converted from a proto non-repeated
// primitive field on a vector of messages.
absl::StatusOr<std::optional<DataSlice>> FromProtoPrimitiveField(
    absl::string_view attr_name, const FieldDescriptor& field_descriptor,
    absl::Span<const Message* absl_nonnull const> parent_messages,
    const std::optional<DataSlice>& parent_schema,
    const std::optional<DataSlice>& parent_schema_metadata,
    bool ignore_field_presence = false) {
  // Populate the field default value if requested.
  if (field_descriptor.has_default_value() && !parent_schema.has_value()) {
    DCHECK(parent_schema_metadata.has_value());
    if (parent_schema_metadata.has_value()) {
      ASSIGN_OR_RETURN(auto default_value,
                       DefaultValueFromProtoPrimitiveField(field_descriptor));
      RETURN_IF_ERROR(parent_schema_metadata->SetAttr(
          absl::StrCat(schema::kProtoSchemaMetadataDefaultValueAttrPrefix,
                       attr_name),
          default_value));
    }
  }

  auto to_slice = [&]<typename T, typename F>(
                      F get) -> absl::StatusOr<std::optional<DataSlice>> {
    bool is_empty = true;
    arolla::DenseArrayBuilder<T> builder(parent_messages.size());
    for (int64_t i = 0; i < parent_messages.size(); ++i) {
      const auto& parent_message = *parent_messages[i];
      const auto* refl = parent_message.GetReflection();
      if (ignore_field_presence || !field_descriptor.has_presence() ||
          refl->HasField(parent_message, &field_descriptor)) {
        builder.Add(i, get(*refl, *parent_messages[i]));
        is_empty = false;
      }
    }
    if (is_empty) {
      return std::nullopt;
    }

    ASSIGN_OR_RETURN(
        DataSlice result,
        DataSlice::Create(
            internal::DataSliceImpl::Create(std::move(builder).Build()),
            DataSlice::JaggedShape::FlatFromSize(parent_messages.size()),
            internal::DataItem(schema::GetDType<T>())));
    ASSIGN_OR_RETURN(auto schema, GetChildAttrSchema(parent_schema, attr_name));
    if (schema.has_value() && schema->item() != schema::kObject) {
      // We could probably improve performance by using the correct backing
      // DenseArray type based on `schema` instead of casting afterward, but
      // that has a lot more cases to handle, and only has an effect if the
      // user provides an explicit schema that disagrees with the proto field
      // schemas, which should be rare.
      //
      // `validate_schema` is a no-op for primitives, so we disable it.
      return CastToExplicit(std::move(result), schema->item(),
                            /*validate_schema=*/false);
    }
    return std::move(result);
  };

  switch (field_descriptor.cpp_type()) {
    case FieldDescriptor::CPPTYPE_INT32:
      return to_slice.operator()<int32_t>(
          [&field_descriptor](const Reflection& refl, const Message& message) {
            return refl.GetInt32(message, &field_descriptor);
          });
    case FieldDescriptor::CPPTYPE_INT64:
      return to_slice.operator()<int64_t>(
          [&field_descriptor](const Reflection& refl, const Message& message) {
            return refl.GetInt64(message, &field_descriptor);
          });
    case FieldDescriptor::CPPTYPE_UINT32:
      return to_slice.operator()<int64_t>(
          [&field_descriptor](const Reflection& refl, const Message& message) {
            return refl.GetUInt32(message, &field_descriptor);
          });
    case FieldDescriptor::CPPTYPE_UINT64:
      return to_slice.operator()<int64_t>(
          [&field_descriptor](const Reflection& refl, const Message& message) {
            return refl.GetUInt64(message, &field_descriptor);
          });
    case FieldDescriptor::CPPTYPE_DOUBLE:
      return to_slice.operator()<double>(
          [&field_descriptor](const Reflection& refl, const Message& message) {
            return refl.GetDouble(message, &field_descriptor);
          });
    case FieldDescriptor::CPPTYPE_FLOAT:
      return to_slice.operator()<float>(
          [&field_descriptor](const Reflection& refl, const Message& message) {
            return refl.GetFloat(message, &field_descriptor);
          });
    case FieldDescriptor::CPPTYPE_BOOL:
      return to_slice.operator()<bool>(
          [&field_descriptor](const Reflection& refl, const Message& message) {
            return refl.GetBool(message, &field_descriptor);
          });
    case FieldDescriptor::CPPTYPE_ENUM:
      return to_slice.operator()<int32_t>(
          [&field_descriptor](const Reflection& refl, const Message& message) {
            return refl.GetEnumValue(message, &field_descriptor);
          });
    case FieldDescriptor::CPPTYPE_STRING:
      if (field_descriptor.type() == FieldDescriptor::TYPE_STRING) {
        return to_slice.operator()<arolla::Text>(
            [&field_descriptor](const Reflection& refl,
                                const Message& message) {
              return refl.GetString(message, &field_descriptor);
            });
      } else {  // TYPE_BYTES
        return to_slice.operator()<arolla::Bytes>(
            [&field_descriptor](const Reflection& refl,
                                const Message& message) {
              return refl.GetString(message, &field_descriptor);
            });
      }
    default:
      return absl::InvalidArgumentError(absl::StrFormat(
          "unexpected proto field C++ type %d", field_descriptor.cpp_type()));
  }
}

// Returns a rank-1 DataSlice of Dicts converted from a proto map field on a
// vector of messages.
absl::Status DictFromProtoMapField(
    const absl_nonnull DataBagPtr& db, absl::string_view attr_name,
    absl::string_view field_name, const FieldDescriptor& field_descriptor,
    absl::Span<const Message* absl_nonnull const> parent_messages,
    const std::optional<DataSlice>& parent_itemid,
    const std::optional<DataSlice>& parent_schema,
    const ExtensionMap* absl_nullable parent_extension_map,
    internal::TrampolineExecutor& executor, std::optional<DataSlice>& result) {
  bool is_empty = true;
  arolla::DenseArrayBuilder<arolla::Unit> dicts_mask_builder(
      parent_messages.size());
  DataSlice::JaggedShape parent_shape =
      DataSlice::JaggedShape::FlatFromSize(parent_messages.size());
  shape::ShapeBuilder shape_builder(parent_shape);
  std::vector<const Message* absl_nonnull> flat_item_messages;
  for (int64_t i = 0; i < parent_messages.size(); ++i) {
    const auto& parent_message = *parent_messages[i];
    const auto* refl = parent_message.GetReflection();
    const auto& field_ref =
        refl->GetRepeatedFieldRef<Message>(parent_message, &field_descriptor);
    shape_builder.Add(field_ref.size());
    for (const auto& item_message : field_ref) {
      flat_item_messages.push_back(&item_message);
    }
    if (!field_ref.empty()) {
      dicts_mask_builder.Add(i, arolla::kUnit);
      is_empty = false;
    }
  }
  if (is_empty) {
    // result is already std::nullopt
    return absl::OkStatus();
  }

  struct CallbackVars {
    std::optional<DataSlice> schema;
    std::optional<DataSlice> itemid;
    DataSlice::JaggedShape items_shape;
    DataSlice dicts_mask;
    std::optional<DataSlice> flat_keys;
    std::optional<DataSlice> flat_values;
  };
  auto vars = std::make_unique<CallbackVars>();

  const auto* extension_map =
      GetChildExtensionMap(parent_extension_map, field_name);
  ASSIGN_OR_RETURN(vars->schema, GetChildAttrSchema(parent_schema, attr_name));
  ASSIGN_OR_RETURN(
      vars->itemid,
      MakeChildDictAttrItemIds(parent_itemid, kChildItemIdSeed, attr_name));
  ASSIGN_OR_RETURN(vars->items_shape, std::move(shape_builder).Build());
  ASSIGN_OR_RETURN(
      auto flat_items_itemid,
      MakeFlatChildIndexItemUuids(vars->itemid, vars->items_shape));
  const Descriptor& map_item_descriptor = *field_descriptor.message_type();
  ASSIGN_OR_RETURN(vars->dicts_mask,
                   DataSlice::Create(internal::DataSliceImpl::Create(
                                         std::move(dicts_mask_builder).Build()),
                                     DataSlice::JaggedShape::FlatFromSize(
                                         parent_messages.size()),
                                     internal::DataItem(schema::kMask)));

  // We set `ignore_field_presence` here because even though the `key` and
  // `value` fields of the map item message are marked as `optional` (report
  // having field presence via their field descriptors), the proto `Map` API
  // treats them as default-valued if they are unset, so we want them to be
  // converted to their default values in these DataSlices instead of being
  // missing.
  ASSIGN_OR_RETURN(
      vars->flat_keys,
      FromProtoPrimitiveField(schema::kDictKeysSchemaAttr,
                              *map_item_descriptor.map_key(),
                              flat_item_messages, vars->schema, std::nullopt,
                              /*ignore_field_presence=*/true));
  if (map_item_descriptor.map_value()->message_type()) {
    RETURN_IF_ERROR(FromProtoMessageField(
        db, schema::kDictValuesSchemaAttr, "values",
        *map_item_descriptor.map_value(), flat_item_messages, flat_items_itemid,
        vars->schema, extension_map,
        /*ignore_field_presence=*/true, executor, vars->flat_values));
  } else {
    ASSIGN_OR_RETURN(
        vars->flat_values,
        FromProtoPrimitiveField(schema::kDictValuesSchemaAttr,
                                *map_item_descriptor.map_value(),
                                flat_item_messages, vars->schema, std::nullopt,
                                /*ignore_field_presence=*/true));
  }

  executor.Enqueue([db = db, vars = std::move(vars),
                    &result]() -> absl::Status {
    ASSIGN_OR_RETURN(auto keys,
                     std::move(vars->flat_keys)->Reshape(vars->items_shape));
    ASSIGN_OR_RETURN(
        auto values,
        std::move(vars->flat_values)->Reshape(std::move(vars->items_shape)));

    ASSIGN_OR_RETURN(
        auto dicts,
        CreateDictLike(db,
                       /*shape_and_mask_from=*/std::move(vars->dicts_mask),
                       /*keys=*/std::move(keys),
                       /*values=*/std::move(values),
                       /*schema=*/std::nullopt,
                       /*key_schema=*/std::nullopt,
                       /*value_schema=*/std::nullopt,
                       /*itemid=*/vars->itemid));
    if (vars->schema.has_value() && vars->schema->item() == schema::kObject) {
      ASSIGN_OR_RETURN(dicts, ToObject(std::move(dicts)));
    }
    result = std::move(dicts);
    return absl::OkStatus();
  });
  return absl::OkStatus();
}

// Returns a rank-1 DataSlice converted from a proto field (of any kind) on a
// vector of messages.
absl::Status FromProtoField(
    const absl_nonnull DataBagPtr& db, absl::string_view attr_name,
    absl::string_view field_name, const FieldDescriptor& field_descriptor,
    absl::Span<const Message* absl_nonnull const> parent_messages,
    const std::optional<DataSlice>& parent_itemid,
    const std::optional<DataSlice>& parent_schema,
    const std::optional<DataSlice>& parent_schema_metadata,
    const ExtensionMap* absl_nullable parent_extension_map,
    bool ignore_field_presence, internal::TrampolineExecutor& executor,
    std::optional<DataSlice>& result) {
  if (field_descriptor.is_map()) {
    return DictFromProtoMapField(db, attr_name, field_name, field_descriptor,
                                 parent_messages, parent_itemid, parent_schema,
                                 parent_extension_map, executor, result);
  } else if (field_descriptor.is_repeated()) {
    if (field_descriptor.message_type() != nullptr) {
      return ListFromProtoRepeatedMessageField(
          db, attr_name, field_name, field_descriptor, parent_messages,
          parent_itemid, parent_schema, parent_extension_map, executor, result);
    } else {
      ASSIGN_OR_RETURN(result,
                       ListFromProtoRepeatedPrimitiveField(
                           db, attr_name, field_name, field_descriptor,
                           parent_messages, parent_itemid, parent_schema));
      return absl::OkStatus();
    }
  } else {
    if (field_descriptor.message_type() != nullptr) {
      return FromProtoMessageField(db, attr_name, field_name, field_descriptor,
                                   parent_messages, parent_itemid,
                                   parent_schema, parent_extension_map,
                                   ignore_field_presence, executor, result);
    } else {
      ASSIGN_OR_RETURN(result,
                       FromProtoPrimitiveField(
                           attr_name, field_descriptor, parent_messages,
                           parent_schema, parent_schema_metadata,
                           /*ignore_field_presence=*/ignore_field_presence));
      return absl::OkStatus();
    }
  }
}

// Returns a size-0 rank-1 DataSlice "converted" from a vector of 0 proto
// messages.
absl::StatusOr<DataSlice> FromZeroProtoMessages(
    const absl_nonnull DataBagPtr& db, const std::optional<DataSlice>& schema) {
  if (schema.has_value()) {
    return DataSlice::Create(
        internal::DataSliceImpl::CreateEmptyAndUnknownType(0),
        DataSlice::JaggedShape::FlatFromSize(0), schema->item(), db);
  }
  return DataSlice::Create(
      internal::DataSliceImpl::CreateEmptyAndUnknownType(0),
      DataSlice::JaggedShape::FlatFromSize(0),
      internal::DataItem(schema::kObject), db);
}

// Returns a rank-1 DataSlice of objects or entities converted from a vector of
// uniform-type proto messages.
absl::Status FromProtoMessage(
    const absl_nonnull DataBagPtr& db, const Descriptor& message_descriptor,
    absl::Span<const Message* absl_nonnull const> messages,
    const std::optional<DataSlice>& itemid,
    const std::optional<DataSlice>& schema,
    const ExtensionMap* absl_nullable extension_map,
    internal::TrampolineExecutor& executor, std::optional<DataSlice>& result) {
  DCHECK(!messages.empty());

  struct FieldVars {
    const FieldDescriptor* absl_nonnull field_descriptor;
    absl::string_view attr_name;
    std::optional<DataSlice> value;
  };

  struct CallbackVars {
    size_t num_messages;
    const Descriptor* message_descriptor;
    std::optional<DataSlice> itemid;
    std::optional<DataSlice> requested_schema;
    std::optional<DataSlice> allocated_schema;
    DataSlice::AttrNamesSet schema_attr_names;
    std::vector<FieldVars> fields;
  };
  auto vars = std::make_unique<CallbackVars>();
  vars->num_messages = messages.size();
  vars->message_descriptor = &message_descriptor;
  vars->itemid = itemid;
  vars->requested_schema = schema;
  if (!schema.has_value()) {
    ASSIGN_OR_RETURN(vars->allocated_schema,
                     CreateBareProtoUuSchema(db, message_descriptor));
  }

  if (schema.has_value() && schema->IsStructSchema()) {
    // For explicit entity schemas, use the schema attr names as the list of
    // fields and extensions to convert.
    ASSIGN_OR_RETURN(vars->schema_attr_names, schema->GetAttrNames());
    vars->fields.reserve(vars->schema_attr_names.size());
    for (const auto& attr_name : vars->schema_attr_names) {
      if (attr_name.starts_with('(') && attr_name.ends_with(')')) {
        // Interpret attrs with parentheses as fully-qualified extension paths.
        const auto ext_full_path =
            absl::string_view(attr_name).substr(1, attr_name.size() - 2);
        const auto* field =
            message_descriptor.file()->pool()->FindExtensionByName(
                ext_full_path);
        if (field == nullptr) {
          return absl::InvalidArgumentError(
              absl::StrFormat("extension not found: \"%s\"", ext_full_path));
        }
        if (field->containing_type() != &message_descriptor) {
          return absl::InvalidArgumentError(absl::StrFormat(
              "extension \"%s\" exists, but isn't an extension "
              "on target message type \"%s\", expected \"%s\"",
              field->full_name(), message_descriptor.full_name(),
              field->containing_type()->full_name()));
        }
        vars->fields.emplace_back(field, attr_name);
      } else {
        const auto* field = message_descriptor.FindFieldByName(attr_name);
        if (field != nullptr) {
          vars->fields.emplace_back(field, attr_name);
        }
      }
    }
  } else {
    // For unset and OBJECT schemas, convert all fields + requested extensions.
    const int64_t num_fields =
        message_descriptor.field_count() +
        ((extension_map != nullptr) ? extension_map->extension_fields.size()
                                    : 0);
    vars->fields.reserve(num_fields);
    for (int i_field = 0; i_field < message_descriptor.field_count();
         ++i_field) {
      const auto* field = message_descriptor.field(i_field);
      vars->fields.emplace_back(field, field->name());
    }
    if (extension_map != nullptr) {
      for (const auto& [attr_name, field] : extension_map->extension_fields) {
        if (field->containing_type() != &message_descriptor) {
          return absl::InvalidArgumentError(absl::StrFormat(
              "extension \"%s\" exists, but isn't an extension "
              "on target message type \"%s\", expected \"%s\"",
              field->full_name(), message_descriptor.full_name(),
              field->containing_type()->full_name()));
        }
        vars->fields.emplace_back(field, attr_name);
      }
    }
  }

  std::optional<DataSlice> allocated_schema_metadata;
  if (vars->allocated_schema.has_value()) {
    ASSIGN_OR_RETURN(allocated_schema_metadata,
                     CreateMessageSchemaMetadata(db, *vars->allocated_schema,
                                                 message_descriptor));
  }

  // NOTE: `vars->fields[i].value` must remain pointer-stable after this point.
  for (auto& field_vars : vars->fields) {
    RETURN_IF_ERROR(FromProtoField(
        db, field_vars.attr_name, field_vars.attr_name,
        *field_vars.field_descriptor, messages, itemid, schema,
        allocated_schema_metadata, extension_map,
        /*ignore_field_presence=*/false, executor, field_vars.value));
  }

  executor.Enqueue([db = db, vars = std::move(vars),
                    &result]() -> absl::Status {
    std::vector<absl::string_view> value_attr_names;
    std::vector<DataSlice> values;
    for (auto& field_vars : vars->fields) {
      if (field_vars.value.has_value()) {
        DCHECK(!field_vars.value->IsEmpty());
        values.push_back(*std::move(field_vars.value));
        value_attr_names.push_back(field_vars.attr_name);
      }
    }

    auto result_shape =
        DataSlice::JaggedShape::FlatFromSize(vars->num_messages);
    if (vars->requested_schema.has_value()) {
      RETURN_IF_ERROR(vars->requested_schema->VerifyIsSchema());
      if (vars->requested_schema->item() == schema::kObject) {
        ASSIGN_OR_RETURN(result, ObjectCreator::Shaped(
                                     db, std::move(result_shape),
                                     /*attr_names=*/std::move(value_attr_names),
                                     /*values=*/std::move(values),
                                     /*itemid=*/vars->itemid));
      } else {  // schema != OBJECT
        ASSIGN_OR_RETURN(
            result,
            EntityCreator::Shaped(db, std::move(result_shape),
                                  /*attr_names=*/std::move(value_attr_names),
                                  /*values=*/std::move(values),
                                  /*schema=*/std::move(vars->requested_schema),
                                  /*overwrite_schema=*/false,
                                  /*itemid=*/vars->itemid));
      }
    } else {  // schema == nullopt
      ASSIGN_OR_RETURN(result, EntityCreator::Shaped(
                                   db, std::move(result_shape),
                                   /*attr_names=*/std::move(value_attr_names),
                                   /*values=*/std::move(values),
                                   /*schema=*/std::move(vars->allocated_schema),
                                   /*overwrite_schema=*/true,
                                   /*itemid=*/vars->itemid));
    }
    return absl::OkStatus();
  });
  return absl::OkStatus();
}

absl::Status FromProtoMessageBreakRecursion(
    const absl_nonnull DataBagPtr& db, const Descriptor& message_descriptor,
    std::vector<const Message* absl_nonnull> messages,
    std::optional<DataSlice> itemid, std::optional<DataSlice> schema,
    const ExtensionMap* absl_nullable extension_map,
    internal::TrampolineExecutor& executor, std::optional<DataSlice>& result) {
  executor.Enqueue([db = db, message_descriptor = &message_descriptor,
                    messages = std::move(messages), itemid = std::move(itemid),
                    schema = std::move(schema), extension_map,
                    &executor, &result]() -> absl::Status {
    return FromProtoMessage(db, *message_descriptor, messages, itemid, schema,
                            extension_map, executor, result);
  });
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<DataSlice> FromProto(
    const absl_nonnull DataBagPtr& db,
    absl::Span<const Message* absl_nonnull const> messages,
    absl::Span<const absl::string_view> extensions,
    const std::optional<DataSlice>& itemid,
    const std::optional<DataSlice>& schema) {
  if (schema.has_value()) {
    RETURN_IF_ERROR(schema->VerifyIsSchema());
    if (schema->GetBag() != db) {
      AdoptionQueue adoption_queue;
      adoption_queue.Add(*schema);
      RETURN_IF_ERROR(adoption_queue.AdoptInto(*db));
    }
  }
  if (messages.empty()) {
    return FromZeroProtoMessages(db, schema);
  }

  for (const Message* message : messages) {
    if (message == nullptr) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected all messages be non-null"));
    }
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

  ASSIGN_OR_RETURN(
      const ExtensionMap extension_map,
      ParseExtensions(extensions, *message_descriptor->file()->pool()));

  std::optional<DataSlice> result;
  RETURN_IF_ERROR(internal::TrampolineExecutor::Run([&](auto& executor) {
    return FromProtoMessage(db, *message_descriptor, messages, itemid, schema,
                            &extension_map, executor, result);
  }));
  return std::move(*result);
}

// Same as above, but the result uses a new immutable DataBag, and if possible,
// that DataBag is forked from schema->GetBag() to avoid a schema extraction.
absl::StatusOr<DataSlice> FromProto(
    absl::Span<const ::google::protobuf::Message* absl_nonnull const> messages,
    absl::Span<const std::string_view> extensions,
    const std::optional<DataSlice>& itemids,
    const std::optional<DataSlice>& schema) {
  DataBagPtr bag;
  if (schema.has_value() && schema->GetBag() != nullptr &&
      schema->GetBag()->GetFallbacks().empty()) {
    ASSIGN_OR_RETURN(bag, schema->GetBag()->Fork());
  } else {
    bag = DataBag::EmptyMutable();
  }
  ASSIGN_OR_RETURN(auto result,
                   FromProto(bag, messages, extensions, itemids, schema));
  bag->UnsafeMakeImmutable();
  return result;
}

// Same as above, but takes a single proto and returns a DataItem.
absl::StatusOr<DataSlice> FromProto(
    const google::protobuf::Message& message,
    absl::Span<const std::string_view> extensions,
    const std::optional<DataSlice>& itemids,
    const std::optional<DataSlice>& schema) {
  ASSIGN_OR_RETURN(auto result_slice,
                   FromProto({&message}, extensions, itemids, schema));
  return result_slice.Reshape(DataSlice::JaggedShape::Empty());
}

namespace {

using DescriptorWithExtensionMap =
    std::tuple<const Descriptor*, const ExtensionMap*>;

absl::StatusOr<DataSlice> SchemaFromProtoPrimitiveField(
    const FieldDescriptor& field_descriptor) {
  DCHECK(field_descriptor.message_type() == nullptr);
  internal::DataItem schema_item;
  switch (field_descriptor.cpp_type()) {
    case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
    case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
      schema_item = internal::DataItem(schema::kInt32);
      break;
    case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
    case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
    case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
      schema_item = internal::DataItem(schema::kInt64);
      break;
    case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
      schema_item = internal::DataItem(schema::kFloat64);
      break;
    case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
      schema_item = internal::DataItem(schema::kFloat32);
      break;
    case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
      schema_item = internal::DataItem(schema::kBool);
      break;
    case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
      if (field_descriptor.type() == google::protobuf::FieldDescriptor::TYPE_BYTES) {
        schema_item = internal::DataItem(schema::kBytes);
      } else {
        schema_item = internal::DataItem(schema::kString);
      }
      break;
    default:
      DCHECK(false);
      return absl::InvalidArgumentError(
          absl::StrFormat("expected primitive proto field, got %v",
                          field_descriptor.DebugString()));
  }
  return DataSlice::Create(schema_item, internal::DataItem(schema::kSchema));
}

// Forward declaration for recursion.
absl::StatusOr<DataSlice> SchemaFromProtoMessageDescriptorBreakRecursion(
    const absl_nonnull DataBagPtr& bag, const Descriptor& message_descriptor,
    const ExtensionMap* absl_nullable extension_map,
    absl::flat_hash_set<DescriptorWithExtensionMap>&
        converted_message_descriptors,
    internal::TrampolineExecutor& executor);

// Sets parent_message_schema.<attr_name> to a dict schema for the given proto
// map field. Child fields are populated asynchronously using `executor`.
absl::Status FillDictSchemaFromProtoMapFieldDescriptor(
    const DataSlice& parent_message_schema, absl::string_view attr_name,
    const FieldDescriptor& field_descriptor,
    const ExtensionMap* absl_nullable parent_extension_map,
    absl::flat_hash_set<DescriptorWithExtensionMap>&
        converted_message_descriptors,
    internal::TrampolineExecutor& executor) {
  const DataBagPtr& bag = parent_message_schema.GetBag();
  DCHECK(bag != nullptr);
  ASSIGN_OR_RETURN(auto key_schema,
                   SchemaFromProtoPrimitiveField(
                       *field_descriptor.message_type()->map_key()));
  ASSIGN_OR_RETURN(auto value_schema, [&]() -> absl::StatusOr<DataSlice> {
    const auto& map_value = *field_descriptor.message_type()->map_value();
    if (map_value.message_type() == nullptr) {
      // Optimization: handle scalar primitive here instead of using the full
      // SchemaFromProtoMessageDescriptorBreakRecursion.
      return SchemaFromProtoPrimitiveField(map_value);
    } else {
      const auto* extension_map =
          GetChildExtensionMap(parent_extension_map, attr_name);
      return SchemaFromProtoMessageDescriptorBreakRecursion(
          bag, *map_value.message_type(), extension_map,
          converted_message_descriptors, executor);
    }
  }());
  ASSIGN_OR_RETURN(auto dict_schema,
                   CreateDictSchema(bag, key_schema, value_schema));
  RETURN_IF_ERROR(parent_message_schema.SetAttr(attr_name, dict_schema));
  return absl::OkStatus();
}

// Sets parent_message_schema.<attr_name> to a list schema for the given proto
// repeated message field. Child fields are populated asynchronously using
// `executor`.
absl::Status FillListSchemaFromProtoRepeatedMessageFieldDescriptor(
    const DataSlice& parent_message_schema, absl::string_view attr_name,
    const FieldDescriptor& field_descriptor,
    const ExtensionMap* absl_nullable parent_extension_map,
    absl::flat_hash_set<DescriptorWithExtensionMap>&
        converted_message_descriptors,
    internal::TrampolineExecutor& executor) {
  DCHECK(field_descriptor.message_type() != nullptr);
  const DataBagPtr& bag = parent_message_schema.GetBag();
  DCHECK(bag != nullptr);
  const auto* extension_map =
      GetChildExtensionMap(parent_extension_map, attr_name);
  ASSIGN_OR_RETURN(auto item_schema,
                   SchemaFromProtoMessageDescriptorBreakRecursion(
                       bag, *field_descriptor.message_type(), extension_map,
                       converted_message_descriptors, executor));
  ASSIGN_OR_RETURN(auto list_schema, CreateListSchema(bag, item_schema));
  RETURN_IF_ERROR(parent_message_schema.SetAttr(attr_name, list_schema));
  return absl::OkStatus();
}

// Sets parent_message_schema.<attr_name> to a schema for the given proto
// message field. Child fields are populated asynchronously using `executor`.
absl::Status FillSchemaFromProtoMessageFieldDescriptor(
    const DataSlice& parent_message_schema, absl::string_view attr_name,
    const FieldDescriptor& field_descriptor,
    const ExtensionMap* absl_nullable parent_extension_map,
    absl::flat_hash_set<DescriptorWithExtensionMap>&
        converted_message_descriptors,
    internal::TrampolineExecutor& executor) {
  DCHECK(field_descriptor.message_type() != nullptr);
  const DataBagPtr& bag = parent_message_schema.GetBag();
  DCHECK(bag != nullptr);
  const auto* extension_map =
      GetChildExtensionMap(parent_extension_map, attr_name);
  ASSIGN_OR_RETURN(auto schema,
                   SchemaFromProtoMessageDescriptorBreakRecursion(
                       bag, *field_descriptor.message_type(), extension_map,
                       converted_message_descriptors, executor));
  RETURN_IF_ERROR(parent_message_schema.SetAttr(attr_name, schema));
  return absl::OkStatus();
}

// Sets parent_message_schema.<attr_name> to a schema for the given proto field.
// Child fields are populated asynchronously using `executor`.
absl::Status FillSchemaFromProtoFieldDescriptor(
    const DataSlice& parent_message_schema,
    const DataSlice& parent_schema_metadata,
    absl::string_view attr_name, const FieldDescriptor& field_descriptor,
    const ExtensionMap* absl_nullable parent_extension_map,
    absl::flat_hash_set<DescriptorWithExtensionMap>&
        converted_message_descriptors,
    internal::TrampolineExecutor& executor) {
  if (field_descriptor.message_type() == nullptr) {
    // Primitive field (scalar or repeated).
    const DataBagPtr& bag = parent_message_schema.GetBag();
    DCHECK(bag != nullptr);
    ASSIGN_OR_RETURN(auto schema,
                     SchemaFromProtoPrimitiveField(field_descriptor));
    if (field_descriptor.is_repeated()) {
      ASSIGN_OR_RETURN(schema, CreateListSchema(bag, std::move(schema)));
    }
    RETURN_IF_ERROR(parent_message_schema.SetAttr(attr_name, schema));
    if (field_descriptor.has_default_value()) {
      ASSIGN_OR_RETURN(auto default_value,
                       DefaultValueFromProtoPrimitiveField(field_descriptor));
      RETURN_IF_ERROR(parent_schema_metadata.SetAttr(
          absl::StrCat(schema::kProtoSchemaMetadataDefaultValueAttrPrefix,
                       attr_name),
          std::move(default_value)));
    }
    return absl::OkStatus();
  }

  if (field_descriptor.is_map()) {
    return FillDictSchemaFromProtoMapFieldDescriptor(
        parent_message_schema, attr_name, field_descriptor,
        parent_extension_map, converted_message_descriptors, executor);
  } else if (field_descriptor.is_repeated()) {
    return FillListSchemaFromProtoRepeatedMessageFieldDescriptor(
        parent_message_schema, attr_name, field_descriptor,
        parent_extension_map, converted_message_descriptors, executor);
  } else {
    return FillSchemaFromProtoMessageFieldDescriptor(
        parent_message_schema, attr_name, field_descriptor,
        parent_extension_map, converted_message_descriptors, executor);
  }
}

// Populates fields on `schema` for the given proto message.
absl::StatusOr<DataSlice> SchemaFromProtoMessageDescriptor(
    const absl_nonnull DataBagPtr& bag, const Descriptor& message_descriptor,
    const ExtensionMap* absl_nullable extension_map,
    absl::flat_hash_set<DescriptorWithExtensionMap>&
        converted_message_descriptors,
    internal::TrampolineExecutor& executor) {
  ASSIGN_OR_RETURN(auto schema,
                   CreateBareProtoUuSchema(bag, message_descriptor));
  if (converted_message_descriptors.contains(
          std::make_tuple(&message_descriptor, extension_map))) {
    return schema;  // Prevent infinite recursion.
  }

  ASSIGN_OR_RETURN(auto metadata, CreateMessageSchemaMetadata(
                                      bag, schema, message_descriptor));
  for (int i_field = 0; i_field < message_descriptor.field_count(); ++i_field) {
    const auto* field = message_descriptor.field(i_field);
    RETURN_IF_ERROR(FillSchemaFromProtoFieldDescriptor(
        schema, metadata, field->name(), *field, extension_map,
        converted_message_descriptors, executor));
  }
  if (extension_map != nullptr) {
    for (const auto& [attr_name, field] : extension_map->extension_fields) {
      if (field->containing_type() != &message_descriptor) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "extension \"%s\" exists, but isn't an extension "
            "on target message type \"%s\", expected \"%s\"",
            field->full_name(), message_descriptor.full_name(),
            field->containing_type()->full_name()));
      }
      RETURN_IF_ERROR(FillSchemaFromProtoFieldDescriptor(
          schema, metadata, attr_name, *field, extension_map,
          converted_message_descriptors, executor));
    }
  }
  converted_message_descriptors.emplace(&message_descriptor, extension_map);
  return schema;
}

absl::StatusOr<DataSlice> SchemaFromProtoMessageDescriptorBreakRecursion(
    const absl_nonnull DataBagPtr& bag, const Descriptor& message_descriptor,
    const ExtensionMap* absl_nullable extension_map,
    absl::flat_hash_set<DescriptorWithExtensionMap>&
        converted_message_descriptors,
    internal::TrampolineExecutor& executor) {
  // This schema is just a function of `bag` and `message_descriptor`, so we
  // can compute and return it synchronously before the queued implementation
  // runs.
  ASSIGN_OR_RETURN(auto schema,
                   CreateBareProtoUuSchema(bag, message_descriptor));
  executor.Enqueue([bag = bag, message_descriptor = &message_descriptor,
                    extension_map, &converted_message_descriptors,
                    &executor]() -> absl::Status {
    ASSIGN_OR_RETURN(auto unused_schema,
                     SchemaFromProtoMessageDescriptor(
                         bag, *message_descriptor, extension_map,
                         converted_message_descriptors, executor));
    return absl::OkStatus();
  });
  return schema;
}

}  // namespace

absl::StatusOr<DataSlice> SchemaFromProto(
    const absl_nonnull DataBagPtr& db,
    const ::google::protobuf::Descriptor* absl_nonnull descriptor,
    absl::Span<const std::string_view> extensions) {
  ASSIGN_OR_RETURN(const ExtensionMap extension_map,
                   ParseExtensions(extensions, *descriptor->file()->pool()));
  absl::flat_hash_set<DescriptorWithExtensionMap> converted_descriptors;
  absl::StatusOr<DataSlice> result = absl::InternalError("");
  RETURN_IF_ERROR(
      internal::TrampolineExecutor::Run([&](auto& executor) -> absl::Status {
        ASSIGN_OR_RETURN(result, SchemaFromProtoMessageDescriptor(
                                     db, *descriptor, &extension_map,
                                     converted_descriptors, executor));
        return absl::OkStatus();
      }));
  return result;
}

absl::StatusOr<DataSlice> SchemaFromProto(
    const ::google::protobuf::Descriptor* absl_nonnull descriptor,
    absl::Span<const std::string_view> extensions) {
  auto bag = DataBag::EmptyMutable();
  ASSIGN_OR_RETURN(auto result, SchemaFromProto(bag, descriptor, extensions));
  bag->UnsafeMakeImmutable();
  return result;
}

}  // namespace koladata
