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
#include "koladata/operators/json.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators/strings/strings.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"
#include "nlohmann/json.hpp"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/casting.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/object_factories.h"
#include "koladata/operators/utils.h"
#include "koladata/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {

// TODO: Move to pointwise utils.
template <typename Fn>
absl::Status ForEachDataItem(const DataSlice& slice, Fn fn) {
  if (slice.GetShape().rank() != 1) {
    return absl::InternalError("slice must have rank 1");
  }
  int64_t i = 0;
  for (auto optional_item : slice.slice().AsDataItemDenseArray()) {
    ASSIGN_OR_RETURN(
        auto item,
        DataSlice::Create(optional_item.present ? std::move(optional_item.value)
                                                : internal::DataItem(),
                          slice.GetSchemaImpl(), slice.GetBag()));
    RETURN_IF_ERROR(fn(i, item));
    ++i;
  }
  return absl::OkStatus();
}

// TODO: Move to pointwise utils.
template <typename Fn>
absl::Status ZipCallForEachDataItem(absl::Span<const DataSlice> slices, Fn fn) {
  if (slices.empty()) {
    return absl::OkStatus();
  }
  std::vector<arolla::DenseArray<internal::DataItem>> data_item_arrays;
  data_item_arrays.reserve(slices.size());
  for (const auto& slice : slices) {
    if (slice.GetShape().rank() != 1) {
      return absl::InternalError("slices must have rank 1");
    }
    if (slice.size() != slices[0].size()) {
      return absl::InternalError("slices must all have the same size");
    }
    data_item_arrays.push_back(slice.slice().AsDataItemDenseArray());
  }
  for (int64_t i_item = 0; i_item < slices[0].size(); ++i_item) {
    std::vector<DataSlice> items;
    items.reserve(slices.size());
    for (int64_t i_slice = 0; i_slice < slices.size(); ++i_slice) {
      auto optional_item = data_item_arrays[i_slice][i_item];
      ASSIGN_OR_RETURN(auto item,
                       DataSlice::Create(optional_item.present
                                             ? std::move(optional_item.value)
                                             : internal::DataItem(),
                                         slices[i_slice].GetSchemaImpl(),
                                         slices[i_slice].GetBag()));
      items.push_back(std::move(item));
    }
    RETURN_IF_ERROR(fn(i_item, std::move(items)));
  }
  return absl::OkStatus();
}

}  // namespace

namespace json_internal {

absl::StatusOr<internal::DataItem> JsonBoolToDataItem(
    bool value, const internal::DataItem& schema_impl) {
  if (schema_impl == schema::kBool) {
    return internal::DataItem(value);
  } else if (schema_impl == schema::kMask) {
    return internal::DataItem(arolla::OptionalUnit(value));
  }
  ASSIGN_OR_RETURN(auto result,
                   schema::CastDataTo(internal::DataItem(value), schema_impl),
                   _ << absl::StrFormat("json number %v invalid for %v schema",
                                        value, schema_impl));
  return result;
}

absl::StatusOr<internal::DataItem> JsonNumberToDataItem(
    int64_t value, const internal::DataItem& schema_impl,
    const internal::DataItem& default_number_schema_impl) {
  internal::DataItem effective_schema_impl = schema_impl;
  if (schema_impl == schema::kObject) {
    if (default_number_schema_impl == schema::kObject) {
      // Infer number dtype from value.
      // Note: because of nlohmann number parsing rules, this is only ever
      // called to convert negative integers, so the `<= max` check here is
      // technically redundant.
      if (value <= std::numeric_limits<int32_t>::max() &&
          value >= std::numeric_limits<int32_t>::min()) {
        return internal::DataItem(static_cast<int32_t>(value));
      }
      return internal::DataItem(value);
    } else {
      effective_schema_impl = default_number_schema_impl;
    }
  }
  ASSIGN_OR_RETURN(
      auto result,
      schema::CastDataTo(internal::DataItem(value), effective_schema_impl),
      _ << absl::StrFormat("json number %v invalid for %v schema", value,
                           effective_schema_impl));
  return result;
}

absl::StatusOr<internal::DataItem> JsonNumberToDataItem(
    uint64_t value, const internal::DataItem& schema_impl,
    const internal::DataItem& default_number_schema_impl) {
  internal::DataItem effective_schema_impl = schema_impl;
  if (schema_impl == schema::kObject) {
    if (default_number_schema_impl == schema::kObject) {
      // Infer number dtype from value.
      if (value <= std::numeric_limits<int32_t>::max()) {
        return internal::DataItem(static_cast<int32_t>(value));
      }
      // Note: this will wrap large uint64 values to negative int64 values,
      // matching the behavior of kd.from_py and similar operators.
      return internal::DataItem(static_cast<int64_t>(value));
    } else {
      effective_schema_impl = default_number_schema_impl;
    }
  }
  ASSIGN_OR_RETURN(
      auto result,
      schema::CastDataTo(internal::DataItem(static_cast<int64_t>(value)),
                         effective_schema_impl),
      _ << absl::StrFormat("json number %v invalid for %v schema", value,
                           effective_schema_impl));
  return result;
}

absl::StatusOr<internal::DataItem> JsonNumberToDataItem(
    double value, absl::string_view value_str,
    const internal::DataItem& schema_impl,
    const internal::DataItem& default_number_schema_impl) {
  internal::DataItem effective_schema_impl = schema_impl;
  if (schema_impl == schema::kObject) {
    if (default_number_schema_impl == schema::kObject) {
      // Infer number dtype from value.
      // To match the behavior of `kd.from_py`, we always infer 32-bit
      // floats. Users can opt for additional precision using an explicit
      // schema or `default_number_schema=kd.FLOAT64`
      return internal::DataItem(static_cast<float>(value));
    } else {
      effective_schema_impl = default_number_schema_impl;
    }
  }
  ASSIGN_OR_RETURN(
      auto result,
      schema::CastDataTo(internal::DataItem(value), effective_schema_impl),
      _ << absl::StrFormat("json number %v invalid for %v schema", value,
                           effective_schema_impl));
  return result;
}

absl::StatusOr<internal::DataItem> JsonStringToDataItem(
    std::string value, const internal::DataItem& schema_impl) {
  if (schema_impl == schema::kBytes) {
    std::string bytes_value;
    if (!absl::Base64Unescape(value, &bytes_value)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "json string invalid for %v schema, must be valid base64",
          schema_impl));
    }
    return internal::DataItem(arolla::Bytes(std::move(bytes_value)));
  }
  auto value_text_item = internal::DataItem(arolla::Text(std::move(value)));
  if (schema_impl == schema::kString) {
    return std::move(value_text_item);
  }
  ASSIGN_OR_RETURN(
      auto result, schema::CastDataTo(std::move(value_text_item), schema_impl),
      _ << absl::StrFormat("json string invalid for %v schema", schema_impl));
  return result;
}

absl::StatusOr<internal::DataItem> JsonArrayToList(
    std::vector<internal::DataItem> json_array_values,
    const internal::DataItem& schema_impl,
    const internal::DataItem& value_schema_impl, bool embed_schema,
    const DataBagPtr& bag) {
  ASSIGN_OR_RETURN(
      DataSlice values_slice,
      DataSlice::CreateWithFlatShape(
          internal::DataSliceImpl::Create(std::move(json_array_values)),
          value_schema_impl, bag));
  ASSIGN_OR_RETURN(DataSlice values_list, CreateListsFromLastDimension(
                                              bag, values_slice, std::nullopt));
  if (embed_schema) {
    ASSIGN_OR_RETURN(values_list, values_list.EmbedSchema());
  }
  return values_list.item();
}

absl::StatusOr<internal::DataItem> JsonObjectToEntity(
    std::vector<std::string> json_object_keys,
    std::vector<internal::DataItem> json_object_values,
    const internal::DataItem& schema_impl,
    const std::optional<absl::string_view>& keys_attr,
    const std::optional<absl::string_view>& values_attr, bool embed_schema,
    const DataBagPtr& bag) {
  const bool entity_has_obj_schema = schema_impl == schema::kObject;

  std::optional<DataSlice> entity_schema;  // nullopt iff entity_has_obj_schema
  DataSlice::AttrNamesSet entity_schema_attr_names;
  if (!entity_has_obj_schema) {
    ASSIGN_OR_RETURN(
        entity_schema,
        DataSlice::Create(schema_impl, internal::DataItem(schema::kSchema),
                          bag));
    ASSIGN_OR_RETURN(entity_schema_attr_names, entity_schema->GetAttrNames());
  }

  bool should_add_keys_attr =
      keys_attr.has_value() &&
      (entity_has_obj_schema || entity_schema_attr_names.contains(*keys_attr));
  bool should_add_values_attr =
      values_attr.has_value() &&
      (entity_has_obj_schema ||
       entity_schema_attr_names.contains(*values_attr));
  if (should_add_values_attr && !should_add_keys_attr) {
    return absl::InvalidArgumentError(
        "cannot add json values list attr without adding json keys list attr");
  }

  std::vector<absl::string_view> entity_attr_names;
  std::vector<DataSlice> entity_attr_values;
  size_t max_extra_attrs =
      (should_add_keys_attr ? 1 : 0) + (should_add_values_attr ? 1 : 0);
  entity_attr_names.reserve(json_object_keys.size() + max_extra_attrs);
  entity_attr_values.reserve(json_object_keys.size() + max_extra_attrs);
  std::optional<arolla::DenseArrayBuilder<arolla::Text>> keys_array_builder;
  if (should_add_keys_attr) {
    keys_array_builder.emplace(json_object_keys.size());
  }
  for (int64_t i = 0; i < json_object_keys.size(); ++i) {
    const auto& key = json_object_keys[i];
    if (key != keys_attr && key != values_attr &&
        (entity_has_obj_schema || entity_schema_attr_names.contains(key))) {
      entity_attr_names.emplace_back(key);
      internal::DataItem value_schema_impl;
      if (entity_has_obj_schema) {
        value_schema_impl = internal::DataItem(schema::kObject);
      } else {
        // TODO: Avoid this second entity schema attr lookup to improve
        // performance. We already do this lookup when parsing the value.
        ASSIGN_OR_RETURN(auto value_schema, entity_schema->GetAttr(key));
        value_schema_impl = value_schema.item();
      }
      ASSIGN_OR_RETURN(auto value,
                       DataSlice::Create(json_object_values[i],
                                         std::move(value_schema_impl), bag));
      entity_attr_values.emplace_back(std::move(value));
    }
    if (should_add_keys_attr) {
      keys_array_builder->Add(i, arolla::Text(key));
    }
  }

  if (should_add_keys_attr) {
    ASSIGN_OR_RETURN(auto keys_slice,
                     DataSlice::CreateWithFlatShape(
                         internal::DataSliceImpl::Create(
                             std::move(*keys_array_builder).Build()),
                         internal::DataItem(schema::kString)));
    ASSIGN_OR_RETURN(auto keys_list,
                     CreateListsFromLastDimension(bag, keys_slice));
    entity_attr_names.emplace_back(*keys_attr);
    entity_attr_values.emplace_back(std::move(keys_list));
  }
  if (should_add_values_attr) {
    ASSIGN_OR_RETURN(auto values_slice,
                     DataSlice::CreateWithFlatShape(
                         internal::DataSliceImpl::Create(json_object_values),
                         internal::DataItem(schema::kObject)));
    ASSIGN_OR_RETURN(auto values_list,
                     CreateListsFromLastDimension(bag, values_slice));
    entity_attr_names.emplace_back(*values_attr);
    entity_attr_values.emplace_back(std::move(values_list));
  }

  if (entity_has_obj_schema) {
    ASSIGN_OR_RETURN(auto entity, ObjectCreator::Shaped(
                                      bag, DataSlice::JaggedShape::Empty(),
                                      entity_attr_names, entity_attr_values));
    return entity.item();
  } else {
    ASSIGN_OR_RETURN(auto entity, EntityCreator::Shaped(
                                      bag, DataSlice::JaggedShape::Empty(),
                                      entity_attr_names, entity_attr_values,
                                      std::move(entity_schema), false));
    if (embed_schema) {
      ASSIGN_OR_RETURN(entity, entity.EmbedSchema());
    }
    return entity.item();
  }
}

absl::StatusOr<internal::DataItem> JsonObjectToDict(
    std::vector<std::string> json_object_keys,
    std::vector<internal::DataItem> json_object_values,
    const internal::DataItem& schema_impl, bool embed_schema,
    const DataBagPtr& bag) {
  // JSON objects with schema OBJECT are converted to entities, so this path
  // will never be called with schema_impl == OBJECT.
  DCHECK(schema_impl != schema::kObject);
  ASSIGN_OR_RETURN(
      auto dict_schema,
      DataSlice::Create(schema_impl, internal::DataItem(schema::kSchema), bag));
  DCHECK(dict_schema.IsDictSchema());
  ASSIGN_OR_RETURN(auto key_schema,
                   dict_schema.GetAttr(schema::kDictKeysSchemaAttr));
  ASSIGN_OR_RETURN(auto value_schema,
                   dict_schema.GetAttr(schema::kDictValuesSchemaAttr));
  std::vector<internal::DataItem> keys_items;
  for (auto& key : json_object_keys) {
    ASSIGN_OR_RETURN(auto item,
                     JsonStringToDataItem(std::move(key), key_schema.item()));
    keys_items.emplace_back(std::move(item));
  }
  ASSIGN_OR_RETURN(auto keys, DataSlice::CreateWithFlatShape(
                                  internal::DataSliceImpl::Create(keys_items),
                                  key_schema.item(), bag));
  ASSIGN_OR_RETURN(DataSlice values,
                   DataSlice::CreateWithFlatShape(
                       internal::DataSliceImpl::Create(json_object_values),
                       value_schema.item(), bag));
  ASSIGN_OR_RETURN(
      auto dict,
      CreateDictShaped(bag, DataSlice::JaggedShape::Empty(), std::move(keys),
                       std::move(values), std::move(dict_schema)));
  if (embed_schema) {
    ASSIGN_OR_RETURN(dict, dict.EmbedSchema());
  }
  return dict.item();
}

}  // namespace json_internal

namespace {

// It's necessary to use the nlohmann SAX parser interface to create reasonable
// JSON parse error messages without C++ exceptions enabled, and to avoid either
// on-stack recursion or buffering all of the JSON tokens (which are both softer
// requirements, but good to have).
//
// Unfortunately, the resulting explicit-stack control flow is somewhat hard to
// follow. Read the comments on JsonSaxParser::StackFrame to get a better sense
// of how this class operates.
//
// This class assumes that the nlohmann SAX parser code calls its methods in the
// correct order for a valid JSON parse, and only DCHECKs these assumptions.
class JsonSaxParser final : public nlohmann::json::json_sax_t {
 public:
  JsonSaxParser(DataBagPtr bag, internal::DataItem schema_impl,
                internal::DataItem default_number_schema_impl,
                std::optional<absl::string_view> keys_attr,
                std::optional<absl::string_view> values_attr)
      : bag_(std::move(bag)),
        keys_attr_(std::move(keys_attr)),
        values_attr_(std::move(values_attr)),
        default_number_schema_impl_(std::move(default_number_schema_impl)) {
    // Fake array stack frame used to collect top-level value.
    bool embed_value_schemas = schema_impl == schema::kObject;
    stack_.emplace_back(StackFrame{
        .frame_type = FrameType::kJsonArray,
        .schema_impl = internal::DataItem(),
        .has_constant_value_schema = true,
        .embed_value_schemas = embed_value_schemas,
        .value_schema_impl = std::move(schema_impl),
    });
  }

  // Called when the SAX parser encounters a `null`.
  bool null() override {
    AddValue(internal::DataItem());
    return true;
  }

  // Called when the SAX parser encounters a `true` or `false`.
  bool boolean(bool value) override {
    return AddValueOrSetError(json_internal::JsonBoolToDataItem(
        value, CurrFrame().value_schema_impl));
  }

  // Called when the SAX parser encounters an int64_t number.
  bool number_integer(int64_t value) override {
    return AddValueOrSetError(json_internal::JsonNumberToDataItem(
        value, CurrFrame().value_schema_impl, default_number_schema_impl_));
  }

  // Called when the SAX parser encounters an uint64_t number.
  bool number_unsigned(uint64_t value) override {
    return AddValueOrSetError(json_internal::JsonNumberToDataItem(
        value, CurrFrame().value_schema_impl, default_number_schema_impl_));
  }

  // Called when the SAX parser encounters a number with a decimal point or
  // exponent.
  bool number_float(double value, const std::string& value_str) override {
    return AddValueOrSetError(json_internal::JsonNumberToDataItem(
        value, value_str, CurrFrame().value_schema_impl,
        default_number_schema_impl_));
  }

  // Called when the SAX parser encounters a string (excluding object keys).
  bool string(std::string& value) override {
    return AddValueOrSetError(json_internal::JsonStringToDataItem(
        std::move(value), CurrFrame().value_schema_impl));
  }

  // Called when the SAX parser encounters a `{`.
  bool start_object(size_t) override {
    status_ = StartJsonObject();
    return status_.ok();
  }

  // Called when the SAX parser encounters a `}`.
  bool end_object() override {
    status_ = EndJsonObject();
    return status_.ok();
  }

  // Called when the SAX parser encounters a `[`.
  bool start_array(size_t) override {
    status_ = StartJsonArray();
    return status_.ok();
  }

  // Called when the SAX parser encounters a `]`.
  bool end_array() override {
    status_ = EndJsonArray();
    return status_.ok();
  }

  // Called when the SAX parser encounters an object key.
  bool key(std::string& value) override {
    status_ = AddJsonObjectKeyAndPrepareNextValueSchema(std::move(value));
    return status_.ok();
  }

  bool parse_error(size_t position, const std::string& last_token,
                   const nlohmann::json::exception& exc) override {
    status_ = absl::InvalidArgumentError(
        absl::StrFormat("json parse error at position %d near token \"%s\": %s",
                        position, last_token, exc.what()));
    return false;
  }

  bool binary(nlohmann::json::binary_t& value) override {
    return false;  // Shouldn't be callable from text JSON parsing.
  }

  absl::StatusOr<internal::DataItem> ToDataItem() && {
    if (!status_.ok()) {
      return std::move(status_);
    }
    DCHECK_EQ(stack_.size(), 1);
    return std::move(CurrFrame().values.back());
  }

 private:
  enum class FrameType { kJsonArray, kJsonObject };

  // This "stack frame" represents the child-most *container* (array or object)
  // currently being parsed. Primitives do not get their own stack frames. The
  // first stack frame is always a dummy array, to allow us to uniformly handle
  // inputs that are not containers.
  struct StackFrame {
    // The type of JSON container being parsed at this level of the stack. The
    // top-level container is an implicit length-1 array.
    FrameType frame_type;

    // The schema of the Koda value (list/dict/entity) being built from this
    // container. Missing for the top-level container.
    internal::DataItem schema_impl;

    // True if the Koda value being built has the same schema for all values,
    // stored in value_schema_impl. This is true for all LIST and DICT schemas.
    // False if the value schema needs to be looked up for each object key.
    bool has_constant_value_schema;

    // If true, the schemas of all values must be embedded so that they can be
    // used via OBJECT-schema references. Note that this is a no-op for all
    // primtitives, so only code paths that can produce non-primitives check it.
    bool embed_value_schemas;

    // The schema to be used for the next value in the container. For arrays,
    // this is constant, and for objects, it is updated when the object key is
    // parsed in preparation for the value.
    internal::DataItem value_schema_impl;

    // For objects, the sequence of keys parsed so far. Empty for arrays.
    std::vector<std::string> keys;

    // The sequence of values (w/ schemas) parsed so far.
    std::vector<internal::DataItem> values;
  };

  StackFrame& CurrFrame() { return stack_.back(); }
  const StackFrame& CurrFrame() const { return stack_.back(); }

  absl::Status StartJsonArray() {
    // Determine the value schema for the array using the LIST schema of the
    // values in the current container.
    ASSIGN_OR_RETURN(
        auto item_value_schema_impl,
        [&]() -> absl::StatusOr<internal::DataItem> {
          internal::DataItem schema_impl = CurrFrame().value_schema_impl;
          if (schema_impl == schema::kObject) {
            return internal::DataItem(schema::kObject);
          }
          ASSIGN_OR_RETURN(
              auto schema,
              DataSlice::Create(schema_impl,
                                internal::DataItem(schema::kSchema), bag_));
          if (!schema.IsListSchema()) {
            return absl::InvalidArgumentError(
                absl::StrFormat("json array invalid for non-LIST schema %v",
                                DataSliceRepr(schema)));
          }
          ASSIGN_OR_RETURN(auto item_value_schema,
                           schema.GetAttr(schema::kListItemsSchemaAttr));
          return item_value_schema.item();
        }());

    bool embed_value_schemas = item_value_schema_impl == schema::kObject;
    stack_.emplace_back(StackFrame{
        .frame_type = FrameType::kJsonArray,
        .schema_impl = CurrFrame().value_schema_impl,
        .has_constant_value_schema = true,
        .embed_value_schemas = embed_value_schemas,
        .value_schema_impl = std::move(item_value_schema_impl),
    });
    return absl::OkStatus();
  }

  absl::Status EndJsonArray() {
    DCHECK(CurrFrame().frame_type == FrameType::kJsonArray);
    auto list_schema_impl = std::move(CurrFrame().schema_impl);
    auto value_schema_impl = std::move(CurrFrame().value_schema_impl);
    std::vector<internal::DataItem> values = std::move(CurrFrame().values);
    stack_.pop_back();

    ASSIGN_OR_RETURN(auto list,
                     json_internal::JsonArrayToList(
                         std::move(values), list_schema_impl, value_schema_impl,
                         CurrFrame().embed_value_schemas, bag_));
    AddValue(std::move(list));
    return absl::OkStatus();
  }

  absl::Status StartJsonObject() {
    // Check that the value schema in the current container can be used for a
    // JSON object. Must be OBJECT, DICT[K, V] (for STRING, OBJECT or numeric
    // K), or entity schema.
    internal::DataItem schema_impl = CurrFrame().value_schema_impl;
    if (schema_impl == schema::kObject) {
      stack_.emplace_back(StackFrame{
          .frame_type = FrameType::kJsonObject,
          .schema_impl = internal::DataItem(schema::kObject),
          .has_constant_value_schema = true,
          .embed_value_schemas = true,
          .value_schema_impl = internal::DataItem(schema::kObject),
      });
      return absl::OkStatus();
    }

    ASSIGN_OR_RETURN(
        auto schema,
        DataSlice::Create(schema_impl, internal::DataItem(schema::kSchema),
                          bag_));
    if (schema.IsDictSchema()) {
      ASSIGN_OR_RETURN(auto key_schema,
                       schema.GetAttr(schema::kDictKeysSchemaAttr));
      // Ensure that the key schema of the dict will be something we can convert
      // to when we have the vector of key strings in EndJsonObject.
      if (key_schema.item() != schema::kString &&
          key_schema.item() != schema::kBytes &&
          key_schema.item() != schema::kInt32 &&
          key_schema.item() != schema::kInt64 &&
          key_schema.item() != schema::kFloat32 &&
          key_schema.item() != schema::kFloat64 &&
          key_schema.item() != schema::kObject) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "json object invalid for schema %v", DataSliceRepr(schema)));
      }
      ASSIGN_OR_RETURN(auto value_schema,
                       schema.GetAttr(schema::kDictValuesSchemaAttr));
      stack_.emplace_back(StackFrame{
          .frame_type = FrameType::kJsonObject,
          .schema_impl = std::move(schema_impl),
          .has_constant_value_schema = true,
          .embed_value_schemas = value_schema.item() == schema::kObject,
          .value_schema_impl = value_schema.item(),
      });
      return absl::OkStatus();
    } else if (schema.IsEntitySchema()) {
      bool embed_value_schemas = false;
      if (values_attr_.has_value()) {
        ASSIGN_OR_RETURN(auto values_attr_schema,
                         schema.GetAttrOrMissing(*values_attr_));
        if (!values_attr_schema.IsEmpty()) {
          // If we will be setting the json values attr, values will need
          // embedded schemas.
          embed_value_schemas = true;
        }
      }
      stack_.emplace_back(StackFrame{
          .frame_type = FrameType::kJsonObject,
          .schema_impl = std::move(schema_impl),
          .has_constant_value_schema = false,
          .embed_value_schemas = embed_value_schemas,
      });
      return absl::OkStatus();
    }

    return absl::InvalidArgumentError(absl::StrFormat(
        "json object invalid for schema %v", DataSliceRepr(schema)));
  }

  absl::Status EndJsonObject() {
    DCHECK(CurrFrame().frame_type == FrameType::kJsonObject);
    DCHECK_EQ(CurrFrame().keys.size(), CurrFrame().values.size());
    ASSIGN_OR_RETURN(
        auto schema,
        DataSlice::Create(std::move(CurrFrame().schema_impl),
                          internal::DataItem(schema::kSchema), bag_));
    std::vector<std::string> keys = std::move(CurrFrame().keys);
    std::vector<internal::DataItem> values = std::move(CurrFrame().values);
    stack_.pop_back();

    if (schema.IsDictSchema()) {
      ASSIGN_OR_RETURN(auto dict,
                       json_internal::JsonObjectToDict(
                           std::move(keys), std::move(values), schema.item(),
                           CurrFrame().embed_value_schemas, bag_));
      AddValue(std::move(dict));
      return absl::OkStatus();
    } else {
      ASSIGN_OR_RETURN(auto entity, json_internal::JsonObjectToEntity(
                                        std::move(keys), std::move(values),
                                        schema.item(), keys_attr_, values_attr_,
                                        CurrFrame().embed_value_schemas, bag_));
      AddValue(std::move(entity));
      return absl::OkStatus();
    }
  }

  // Note: For DICT schemas with non-STRING keys, the conversion happens when
  // the dict is constructed, not in this method.
  absl::Status AddJsonObjectKeyAndPrepareNextValueSchema(std::string key) {
    DCHECK(!stack_.empty());
    DCHECK(CurrFrame().frame_type == FrameType::kJsonObject);
    DCHECK_EQ(CurrFrame().keys.size(), CurrFrame().values.size());

    if (!CurrFrame().has_constant_value_schema) {
      ASSIGN_OR_RETURN(
          auto schema,
          DataSlice::Create(CurrFrame().schema_impl,
                            internal::DataItem(schema::kSchema), bag_));
      ASSIGN_OR_RETURN(auto value_schema, schema.GetAttrOrMissing(key));
      if (value_schema.IsEmpty()) {
        CurrFrame().value_schema_impl = internal::DataItem(schema::kObject);
      } else {
        CurrFrame().value_schema_impl = value_schema.item();
      }
    }

    CurrFrame().keys.emplace_back(std::move(key));
    return absl::OkStatus();
  }

  bool AddValueOrSetError(absl::StatusOr<internal::DataItem> value) {
    if (value.ok()) {
      AddValue(std::move(*value));
      return true;
    } else {
      status_ = std::move(value).status();
      return false;
    }
  }

  void AddValue(internal::DataItem value) {
    DCHECK(!stack_.empty());
    CurrFrame().values.emplace_back(std::move(value));
  }

  absl::Status status_ = absl::OkStatus();
  const DataBagPtr bag_;
  const std::optional<absl::string_view> keys_attr_;
  const std::optional<absl::string_view> values_attr_;
  const internal::DataItem default_number_schema_impl_;
  // TODO: Try using std::deque for performance.
  std::vector<StackFrame> stack_;
};

}  // namespace

absl::StatusOr<DataSlice> FromJson(DataSlice x, DataSlice schema,
                                   DataSlice default_number_schema,
                                   DataSlice on_invalid, DataSlice keys_attr,
                                   DataSlice values_attr,
                                   internal::NonDeterministicToken) {
  // Validate `x`.
  RETURN_IF_ERROR(ExpectString("x", x));

  // Validate `schema` and `default_number_schema`.
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  RETURN_IF_ERROR(default_number_schema.VerifyIsSchema());
  if (default_number_schema.item() != schema::kObject &&
      default_number_schema.item() != schema::kInt32 &&
      default_number_schema.item() != schema::kInt64 &&
      default_number_schema.item() != schema::kFloat32 &&
      default_number_schema.item() != schema::kFloat64) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected default_number_schema to be OBJECT or a "
                        "numeric primitive schema, got %v",
                        DataSliceRepr(default_number_schema.GetSchema())));
  }

  // Validate `on_invalid`.
  std::optional<internal::DataItem> on_invalid_item;
  if (on_invalid.is_item()) {
    on_invalid_item.emplace(on_invalid.item());
  } else if (on_invalid.size() == 0) {
    // Special marker value.
  } else {
    return absl::InvalidArgumentError("on_invalid must be a DataItem");
  }

  // Validate `keys_attr` and `values_attr`.
  std::optional<absl::string_view> keys_attr_value;
  std::optional<absl::string_view> values_attr_value;
  if (!keys_attr.IsEmpty()) {
    RETURN_IF_ERROR(
        ExpectPresentScalar("keys_attr", keys_attr, schema::kString));
    keys_attr_value = keys_attr.item().value<arolla::Text>().view();
    if (!values_attr.IsEmpty()) {
      RETURN_IF_ERROR(
          ExpectPresentScalar("values_attr", values_attr, schema::kString));
      values_attr_value = values_attr.item().value<arolla::Text>().view();
    }
  } else if (!values_attr.IsEmpty()) {
    return absl::InvalidArgumentError(
        "values_attr must be None if keys_attr is None");
  }

  internal::DataItem result_schema_impl = schema.item();
  if (on_invalid_item) {
    ASSIGN_OR_RETURN(
        result_schema_impl,
        schema::CommonSchema(result_schema_impl, on_invalid.GetSchemaImpl()));
  }

  DataBagPtr bag = DataBag::EmptyMutable();
  {
    AdoptionQueue adoption_queue;
    adoption_queue.Add(schema);
    // `default_number_schema` is a primitive schema, so no need to adopt.
    adoption_queue.Add(on_invalid);
    RETURN_IF_ERROR(adoption_queue.AdoptInto(*bag));
  }

  const auto& item_from_json =
      [&](absl::string_view value) -> absl::StatusOr<internal::DataItem> {
    JsonSaxParser parser(bag, schema.item(), default_number_schema.item(),
                         keys_attr_value, values_attr_value);
    // TODO: Support strict=false (i.e. recovering from
    // truncated JSON input). Also maybe set ignore_comments if
    // strict=false, although this is maybe too nlohmann-specific.
    nlohmann::json::sax_parse(value, &parser,
                              nlohmann::json::input_format_t::json,
                              /*strict=*/true, /*ignore_comments=*/false);
    auto parsed_item = std::move(parser).ToDataItem();
    if (!parsed_item.ok()) {
      if (on_invalid_item.has_value()) {
        // Swallow parse error and use `on_invalid_item` as marker.
        return *on_invalid_item;
      } else {
        return std::move(parsed_item).status();  // Propagate parse error.
      }
    } else {
      return std::move(*parsed_item);
    }
  };

  if (x.is_item()) {
    internal::DataItem result_item;
    if (x.item().holds_value<arolla::Text>()) {
      ASSIGN_OR_RETURN(result_item,
                       item_from_json(x.item().value<arolla::Text>()));
    }
    bag->UnsafeMakeImmutable();
    return DataSlice::Create(std::move(result_item),
                             std::move(result_schema_impl), std::move(bag),
                             DataSlice::Wholeness::kWhole);
  } else {
    internal::SliceBuilder result_builder(x.size());
    if (x.slice().dtype() == arolla::GetQType<arolla::Text>()) {
      absl::Status status = absl::OkStatus();
      x.slice().values<arolla::Text>().ForEachPresent(
          [&](int64_t i, arolla::view_type_t<arolla::Text> value) {
            if (status.ok()) {
              auto result_item = item_from_json(value);
              if (result_item.ok()) {
                result_builder.InsertGuaranteedNotSetAndUpdateAllocIds(
                    i, *result_item);
              } else {
                status = std::move(result_item).status();
              }
            }
          });
      RETURN_IF_ERROR(std::move(status));
    }
    bag->UnsafeMakeImmutable();
    return DataSlice::Create(std::move(result_builder).Build(), x.GetShape(),
                             std::move(result_schema_impl), std::move(bag),
                             DataSlice::Wholeness::kWhole);
  }
}

namespace {

// Map-like class that preserves insertion order *and* duplicate keys. This is
// used to give us more complete control over the JSON serializer.
//
// NOTE: Implements only parts of the map concept needed to get nlohmann JSON
// serialization to compile.
template <typename K, typename V, typename Compare = std::less<K>,
          typename Allocator = std::allocator<std::pair<const K, V>>>
class FullInsertionOrderMap {
 public:
  using key_type = K;
  using mapped_type = V;
  using value_type = std::pair<const K, V>;
  using size_type = std::vector<value_type>::size_type;
  using difference_type = std::vector<value_type>::difference_type;
  using key_compare = Compare;
  using iterator = std::vector<value_type>::iterator;
  using const_iterator = std::vector<value_type>::const_iterator;

  FullInsertionOrderMap() = default;
  explicit FullInsertionOrderMap(std::vector<value_type> items)
      : items_(std::move(items)) {}

  iterator begin() { return items_.begin(); }
  const_iterator cbegin() const { return items_.cbegin(); }
  iterator end() { return items_.end(); }
  const_iterator cend() const { return items_.cend(); }

  bool empty() { return items_.empty(); }
  size_type size() { return items_.size(); }

  void clear() { items_.clear(); }

  template <typename... Args>
  std::pair<iterator, bool> emplace(Args&&... args) {
    items_.emplace_back(std::forward<Args>(args)...);
    return std::make_tuple(std::prev(items_.end()), true);
  }

 private:
  std::vector<std::pair<const K, V>, Allocator> items_;
};

using SerializableJson = nlohmann::basic_json<FullInsertionOrderMap>;
using SerializableJsonObject = SerializableJson::object_t;

template <typename T>
absl::StatusOr<SerializableJson> PrimitiveValueToSerializableJson(
    const T& value, const internal::DataItem& schema) {
  if constexpr (std::is_same_v<T, internal::ObjectId>) {
    DCHECK((!std::is_same_v<T, internal::ObjectId>));
    return SerializableJson(nullptr);  // Should be unreachable.
  } else if constexpr (std::is_same_v<T, internal::MissingValue>) {
    if (schema == schema::kMask) {
      return SerializableJson(false);  // false
    } else {
      return SerializableJson(nullptr);  // null
    }
  } else if constexpr (std::is_same_v<T, int> || std::is_same_v<T, int64_t>) {
    return SerializableJson(static_cast<int64_t>(value));
  } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
    if (std::isfinite(value)) {
      return SerializableJson(static_cast<double>(value));
    } else if (std::isnan(value)) {
      return SerializableJson("nan");
    } else if (value > 0) {
      return SerializableJson("inf");
    } else {
      return SerializableJson("-inf");
    }
  } else if constexpr (std::is_same_v<T, bool>) {
    return SerializableJson(value);
  } else if constexpr (std::is_same_v<T, arolla::Unit>) {
    return SerializableJson(true);
  } else if constexpr (std::is_same_v<T, arolla::Text>) {
    return SerializableJson(std::string(value.view()));
  } else if constexpr (std::is_same_v<T, arolla::Bytes>) {
    return SerializableJson(absl::Base64Escape(value));
  } else {
    return absl::InvalidArgumentError(absl::StrFormat(
        "unsupported schema %s for json serialization",
        schema::schema_internal::GetQTypeName(arolla::GetQType<T>())));
  }
}

absl::StatusOr<SerializableJson> PrimitiveDataItemToSerializableJson(
    const DataSlice& item) {
  return item.item().VisitValue([&]<typename T>(const T& value) {
    return PrimitiveValueToSerializableJson(value, item.GetSchemaImpl());
  });
}

template <typename T>
absl::StatusOr<std::string> PrimitiveValueToObjectKeyString(const T& value) {
  if constexpr (std::is_same_v<T, internal::MissingValue>) {
    DCHECK(!(std::is_same_v<T, internal::MissingValue>));
    return absl::InvalidArgumentError(
        "missing values not supported for json object key serialization");
  } else if constexpr (std::is_same_v<T, arolla::Text>) {
    return std::string(value.view());
  } else if constexpr (std::is_same_v<T, arolla::Bytes>) {
    return absl::Base64Escape(value);
  } else if constexpr (std::is_same_v<T, int> || std::is_same_v<T, int64_t>) {
    return std::string(arolla::AsTextOp()(value).view());
  } else {
    DCHECK(false);  // Should be unreachable.
    return absl::InvalidArgumentError(absl::StrFormat(
        "unsupported schema %s for json object key serialization",
        schema::schema_internal::GetQTypeName(arolla::GetQType<T>())));
  }
}

absl::StatusOr<SerializableJson> PrimitiveDataItemToObjectKeyString(
    const DataSlice& item) {
  return item.item().VisitValue([&]<typename T>(const T& value) {
    return PrimitiveValueToObjectKeyString(value);
  });
}

// Forward declaration for recursion.
//
// NOTE: nlohmann json serialization uses recursion internally, so there isn't
// much of a robustness benefit to making this function non-recursive, but it
// could be done in a similar way to ToProto/FromProto.
absl::StatusOr<SerializableJson> DataItemToSerializableJson(
    const DataSlice& item,
    absl::flat_hash_set<internal::ObjectId>& path_object_ids,
    const std::optional<absl::string_view>& keys_attr,
    const std::optional<absl::string_view>& values_attr,
    bool include_missing_values);

absl::StatusOr<SerializableJson> ListDataItemToSerializableJson(
    const DataSlice& item,
    absl::flat_hash_set<internal::ObjectId>& path_object_ids,
    const std::optional<absl::string_view>& keys_attr,
    const std::optional<absl::string_view>& values_attr,
    bool include_missing_values) {
  DCHECK(item.is_item());

  ASSIGN_OR_RETURN(auto list_items, item.ExplodeList(0, std::nullopt));
  std::vector<SerializableJson> json_array;
  json_array.reserve(list_items.size());
  RETURN_IF_ERROR(ForEachDataItem(
      list_items, [&](auto, const DataSlice& list_item) -> absl::Status {
        ASSIGN_OR_RETURN(json_array.emplace_back(),
                         DataItemToSerializableJson(
                             list_item, path_object_ids, keys_attr, values_attr,
                             include_missing_values));
        return absl::OkStatus();
      }));
  return SerializableJson(std::move(json_array));
}

absl::StatusOr<SerializableJson> DictDataItemToSerializableJson(
    const DataSlice& item,
    absl::flat_hash_set<internal::ObjectId>& path_object_ids,
    const std::optional<absl::string_view>& keys_attr,
    const std::optional<absl::string_view>& values_attr,
    bool include_missing_values) {
  DCHECK(item.is_item());

  ASSIGN_OR_RETURN(auto keys, item.GetDictKeys());
  ASSIGN_OR_RETURN(auto values, item.GetDictValues());
  DCHECK(!keys.is_item() &&
         keys.slice().present_count() == keys.slice().size());
  if (keys.GetSchemaImpl() != schema::kString &&
      keys.GetSchemaImpl() != schema::kBytes &&
      keys.GetSchemaImpl() != schema::kInt32 &&
      keys.GetSchemaImpl() != schema::kInt64 &&
      keys.GetSchemaImpl() != schema::kObject) {
    return absl::InvalidArgumentError(
        absl::StrFormat("unsupported dict key schema %v for json serialization",
                        keys.GetSchemaImpl()));
  }
  if (keys.IsEmpty()) {
    return SerializableJson(SerializableJsonObject());
  }
  if (keys.dtype() != arolla::GetQType<arolla::Text>() &&
      keys.dtype() != arolla::GetQType<arolla::Bytes>() &&
      keys.dtype() != arolla::GetQType<int32_t>() &&
      keys.dtype() != arolla::GetQType<int64_t>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "unsupported dict key dtype %s for json serialization",
        schema::schema_internal::GetQTypeName(keys.slice().dtype())));
  }

  std::vector<std::pair<std::string, SerializableJson>> mutable_object_items;
  mutable_object_items.reserve(keys.size());
  RETURN_IF_ERROR(ZipCallForEachDataItem(
      {std::move(keys), std::move(values)},
      [&](auto, absl::Span<const DataSlice> values) -> absl::Status {
        DCHECK_EQ(values.size(), 2);  // (key, value)
        ASSIGN_OR_RETURN(auto key,
                         PrimitiveDataItemToObjectKeyString(values[0]));
        ASSIGN_OR_RETURN(auto value,
                         DataItemToSerializableJson(
                             values[1], path_object_ids, keys_attr,
                             values_attr, include_missing_values));
        mutable_object_items.emplace_back(std::move(key), std::move(value));
        return absl::OkStatus();
      }));

  std::sort(
      mutable_object_items.begin(), mutable_object_items.end(),
      [&](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

  std::vector<std::pair<const std::string, SerializableJson>> object_items;
  object_items.reserve(mutable_object_items.size());
  for (auto& item : mutable_object_items) {
    object_items.emplace_back(std::move(item));
  }

  return SerializableJson(SerializableJsonObject(std::move(object_items)));
}

absl::StatusOr<SerializableJson> EntityDataItemToSerializableJson(
    const DataSlice& item,
    absl::flat_hash_set<internal::ObjectId>& path_object_ids,
    const std::optional<absl::string_view>& keys_attr,
    const std::optional<absl::string_view>& values_attr,
    bool include_missing_values) {
  DCHECK(item.is_item());

  ASSIGN_OR_RETURN(auto attr_names, item.GetAttrNames());

  std::vector<std::pair<const std::string, SerializableJson>> object_items;
  if (keys_attr.has_value() && attr_names.contains(*keys_attr)) {
    // Object keys and order are specified by `keys_attr`.
    ASSIGN_OR_RETURN(auto key_list, item.GetAttr(*keys_attr));
    RETURN_IF_ERROR(key_list.GetSchema().VerifyIsListSchema());
    ASSIGN_OR_RETURN(auto key_list_items,
                     key_list.ExplodeList(0, std::nullopt));
    if (key_list_items.GetSchemaImpl() != schema::kString) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected json key list to contain STRING, got %v",
                          key_list_items.GetSchemaImpl()));
    }
    if (!key_list_items.IsEmpty()) {
      if (key_list_items.size() != key_list_items.present_count()) {
        return absl::InvalidArgumentError(
            "expected json key list not to have missing items");
      }

      // Primitive STRING DataSlice must contain only strings.
      DCHECK_EQ(key_list_items.dtype(), arolla::GetQType<arolla::Text>());
      const auto& key_list_values =
          key_list_items.slice().values<arolla::Text>().values;

      if (values_attr.has_value() && attr_names.contains(*values_attr)) {
        // Object values are specified by `values_attr` (supports duplicate
        // keys with differing values).
        ASSIGN_OR_RETURN(auto value_list, item.GetAttr(*values_attr));
        RETURN_IF_ERROR(value_list.GetSchema().VerifyIsListSchema());
        ASSIGN_OR_RETURN(auto value_list_items,
                         value_list.ExplodeList(0, std::nullopt));
        if (value_list_items.size() != key_list_items.size()) {
          return absl::InvalidArgumentError(
              absl::StrFormat("expected json key list and json value list to "
                              "have the same length, got %d and %d",
                              key_list_items.size(), value_list_items.size()));
        }

        RETURN_IF_ERROR(ForEachDataItem(
            value_list_items,
            [&](int64_t i, const DataSlice& value) -> absl::Status {
              if (!include_missing_values && !value.item().has_value()) {
                return absl::OkStatus();
              }
              const auto& key = key_list_values[i];
              ASSIGN_OR_RETURN(auto json_value, DataItemToSerializableJson(
                                                    value, path_object_ids,
                                                    keys_attr, values_attr,
                                                    include_missing_values));
              object_items.emplace_back(key, std::move(json_value));
              return absl::OkStatus();
            }));
      } else {
        // Object values are specified by attr values.
        for (const auto& key : key_list_values) {
          ASSIGN_OR_RETURN(auto attr_value, item.GetAttr(key));
          if (!include_missing_values && !attr_value.item().has_value()) {
            continue;
          }
          ASSIGN_OR_RETURN(
              auto json_attr_value,
              DataItemToSerializableJson(attr_value, path_object_ids, keys_attr,
                                         values_attr, include_missing_values));
          object_items.emplace_back(key, std::move(json_attr_value));
        }
      }
    }
  } else {
    // Object keys are attr names in lexicographic order, and their values are
    // the attr values.
    for (const auto& attr_name : attr_names) {
      if (attr_name == values_attr) {
        // If `values_attr` attribute is set without `keys_attr` attribute,
        // don't include the `values_attr` data, to avoid confusion.
        continue;
      }
      ASSIGN_OR_RETURN(auto attr_value, item.GetAttr(attr_name));
      if (!include_missing_values && !attr_value.item().has_value()) {
        continue;
      }
      ASSIGN_OR_RETURN(auto json_attr_value,
                       DataItemToSerializableJson(attr_value, path_object_ids,
                                                  keys_attr, values_attr,
                                                  include_missing_values));
      object_items.emplace_back(attr_name, std::move(json_attr_value));
    }
  }
  return SerializableJson(SerializableJsonObject(std::move(object_items)));
}

absl::StatusOr<SerializableJson> DataItemToSerializableJson(
    const DataSlice& item,
    absl::flat_hash_set<internal::ObjectId>& path_object_ids,
    const std::optional<absl::string_view>& keys_attr,
    const std::optional<absl::string_view>& values_attr,
    bool include_missing_values) {
  DCHECK(item.is_item());

  if (item.GetSchemaImpl() == schema::kSchema ||
      item.item().holds_value<schema::DType>()) {
    return absl::InvalidArgumentError(
        "unsupported schema SCHEMA for json serialization");
  }

  std::optional<internal::ObjectId> item_object_id;
  if (item.item().holds_value<internal::ObjectId>()) {
    item_object_id = item.item().value<internal::ObjectId>();
  }

  if (item_object_id.has_value()) {
    if (!path_object_ids.insert(*item_object_id).second) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "cycle detected in json serialization at %s", DataSliceRepr(item)));
    }
  }

  SerializableJson json;
  if (item_object_id.has_value()) {
    if (item.GetSchemaImpl() == schema::kItemId) {
      return absl::InvalidArgumentError(
          "unsupported schema ITEMID for json serialization");
    } else if (item_object_id->IsList()) {
      ASSIGN_OR_RETURN(
          json, ListDataItemToSerializableJson(item, path_object_ids, keys_attr,
                                               values_attr,
                                               include_missing_values));
    } else if (item_object_id->IsDict()) {
      ASSIGN_OR_RETURN(
          json, DictDataItemToSerializableJson(item, path_object_ids, keys_attr,
                                               values_attr,
                                               include_missing_values));
    } else {
      ASSIGN_OR_RETURN(
          json, EntityDataItemToSerializableJson(item, path_object_ids,
                                                 keys_attr, values_attr,
                                                 include_missing_values));
    }
  } else {
    ASSIGN_OR_RETURN(json, PrimitiveDataItemToSerializableJson(item));
  }

  if (item_object_id.has_value()) {
    path_object_ids.erase(*item_object_id);
  }

  return json;
}

}  // namespace

absl::StatusOr<DataSlice> ToJson(DataSlice x, DataSlice indent,
                                 DataSlice ensure_ascii, DataSlice keys_attr,
                                 DataSlice values_attr,
                                 DataSlice include_missing_values) {
  int indent_value = 0;
  bool indent_is_none = true;
  if (!indent.IsEmpty()) {
    RETURN_IF_ERROR(ExpectPresentScalar("indent", indent, schema::kInt32));
    indent_value = indent.item().value<int32_t>();
    indent_is_none = false;
  }
  ASSIGN_OR_RETURN(bool ensure_ascii_value,
                   GetBoolArgument(ensure_ascii, "ensure_ascii"));

  bool include_missing_values_value = true;
  if (!include_missing_values.IsEmpty()) {
    ASSIGN_OR_RETURN(
        include_missing_values_value,
        GetBoolArgument(include_missing_values, "include_missing_values"));
  }

  std::optional<absl::string_view> keys_attr_value;
  std::optional<absl::string_view> values_attr_value;
  if (!keys_attr.IsEmpty()) {
    RETURN_IF_ERROR(
        ExpectPresentScalar("keys_attr", keys_attr, schema::kString));
    keys_attr_value = keys_attr.item().value<arolla::Text>().view();
    if (!values_attr.IsEmpty()) {
      RETURN_IF_ERROR(
          ExpectPresentScalar("values_attr", values_attr, schema::kString));
      values_attr_value = values_attr.item().value<arolla::Text>().view();
    }
  }

  ASSIGN_OR_RETURN(auto flat_x, x.Reshape(x.GetShape().FlatFromSize(x.size())));

  arolla::DenseArrayBuilder<arolla::Text> result_builder(x.size());
  RETURN_IF_ERROR(ForEachDataItem(
      flat_x, [&](int64_t i, const DataSlice& item) -> absl::Status {
        if (!item.item().has_value()) {
          return absl::OkStatus();
        }

        absl::flat_hash_set<internal::ObjectId> path_object_ids;
        ASSIGN_OR_RETURN(auto json, DataItemToSerializableJson(
                                        item, path_object_ids,
                                        keys_attr_value, values_attr_value,
                                        include_missing_values_value));
        std::string result =
            json.dump(indent_value, ' ', ensure_ascii_value,
                      nlohmann::detail::error_handler_t::ignore);

        if (indent_is_none) {
          // For compatibility with python json indent=None, we want a mode
          // where we get padding after "," and ":", and nlohmann json doesn't
          // support this natively. Instead, we do some hacky post-processing
          // on its indent=0 output to replicate the padding. indent=0 already
          // has a space after ":" and adds a newline after every comma, so we
          // replace the comma+newline with comma+space and strip the other
          // newlines.
          //
          // This substitution doesn't affect the value of the JSON data because
          // newlines are always escaped inside of JSON strings, so the newline
          // we are substituting out must be whitespace.
          result = absl::StrReplaceAll(result, {{",\n", ", "}, {"\n", ""}});
        }

        result_builder.Add(i, std::move(result));
        return absl::OkStatus();
      }));

  return DataSlice::Create(
      internal::DataSliceImpl::Create(std::move(result_builder).Build()),
      x.GetShape(), internal::DataItem(schema::kString));
}

}  // namespace koladata::ops
