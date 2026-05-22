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
#include "koladata/internal/op_utils/auto_values_update.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_set.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/traverser.h"
#include "koladata/internal/schema_attrs.h"

namespace koladata::internal {

namespace {

// Visitor for auto-assigning values to AUTO_ID and AUTO_REFERENCE attributes.
//
// Fills the `new_databag_` with the attribute updates. To do so, it relies on
// post-order traversal of the databag structure. Thus metadata is visited
// before the schema, and the schema before the objects, etc.
// Note: it is not guaranteed to process correctly if metadata has reference to
// the schema or objects.
class AutoValuesVisitor : public AbstractVisitor {
  static constexpr absl::string_view kAutoIdPrefix =
      AutoIdsUpdateOp::kAutoIdPrefix;

 public:
  explicit AutoValuesVisitor(DataBagImplPtr new_databag)
      : new_databag_(std::move(new_databag)) {}

  // Do nothing on previsit.
  absl::StatusOr<bool> Previsit(
      const DataItem& from_item, const DataItem& from_schema,
      const std::optional<absl::string_view>& from_item_attr_name,
      const DataItem& item, const DataItem& schema) override {
    return true;
  }

  // Do nothing for lists. Note that lists might get overwritten in the
  // AUTO_REFERENCE attributes.
  absl::Status VisitList(const DataItem& list, const DataItem& schema,
                         bool is_object_schema,
                         const DataSliceImpl& items) override {
    return absl::OkStatus();
  }

  // Do nothing for dicts.
  absl::Status VisitDict(const DataItem& dict, const DataItem& schema,
                         bool is_object_schema, const DataSliceImpl& keys,
                         const DataSliceImpl& values) override {
    return absl::OkStatus();
  }

  // Object attribute need handling if (1) it is a metadata object and attribute
  // is a marker of auto-assigned attribute; (2) it was marked as an
  // auto-assigned attribute in the schema's metadata.
  absl::Status VisitObjectAttr(const DataItem& object,
                               const DataItem& schema,
                               const std::optional<ObjectId> schema_metadata,
                               absl::string_view attr_name,
                               const DataItem& attr_value) {
    DCHECK(object.holds_value<ObjectId>());
    if (attr_value.holds_value<ObjectId>()) {
      // Process AUTO_ID attributes in metadata objects.
      if (auto auto_id_name_it =
              auto_id_schema_names_.find(attr_value.value<ObjectId>());
          auto_id_name_it != auto_id_schema_names_.end()) {
        auto_id_attrs_[{.object = object.value<ObjectId>(),
                        .attr_name = CachedName(std::string(attr_name))}] =
            auto_id_name_it->second;
        return absl::OkStatus();
      }
    }
    // AUTO_ID
    if (!schema_metadata.has_value()) {
      return absl::OkStatus();
    }
    if (auto it = auto_id_attrs_.find(
            {.object = *schema_metadata, .attr_name = attr_name});
        it != auto_id_attrs_.end()) {
      auto_id_.push_back(AutoIdItem{
          .object = object.value<ObjectId>(),
          .attr_name = it->first.attr_name,
          .id_name = it->second,
          .existing_value = attr_value,
      });
    }
    return absl::OkStatus();
  }

  // Process object's attributes independently.
  // Expected that schema is visited before object. And if it is a metadata
  // object, it's attributes are visited before the object itself.
  absl::Status VisitObject(
      const DataItem& object, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_values) override {
    auto status = absl::OkStatus();
    if (!schema.holds_value<ObjectId>()) {
      return absl::OkStatus();
    }
    std::optional<ObjectId> schema_metadata = std::nullopt;
    if (auto it = schema_metadata_.find(schema.value<ObjectId>());
        it != schema_metadata_.end()) {
      schema_metadata = it->second;
    }
    RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
        [&](int64_t i, absl::string_view attr_name,
            arolla::OptionalValue<DataItem> attr_value) {
          if (!status.ok() || !attr_value.present) {
            return;
          }
          status = VisitObjectAttr(object, schema, schema_metadata, attr_name,
                                   attr_value.value);
        },
        attr_names, attr_values));
    return status;
  }

  // Process schema's attributes independently.
  // Expected that schema's metadata, if present, is visited before the schema.
  absl::Status VisitSchema(
      const DataItem& item, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_schema) override {
    if (!item.holds_value<ObjectId>()) {
      return absl::InvalidArgumentError(
          absl::StrCat("schema should be an ObjectId: ", item));
    }
    auto status = absl::OkStatus();
    RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
        [&](int64_t i, absl::string_view attr_name,
            arolla::OptionalValue<DataItem> attr) {
          if (!status.ok()) {
            return;
          }
          status = VisitSchemaAttr(item, attr_name, attr.value);
        },
        attr_names, attr_schema));
    return status;
  }

  // All the ObjectIds stay intact.
  absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                    const DataItem& schema) override {
    return item;
  }

  // Assign values in the `new_databag_` to all detected AUTO_ID attributes.
  absl::Status SetIds() {
    std::sort(auto_id_.begin(), auto_id_.end());
    absl::flat_hash_set<absl::string_view> used_ids;
    absl::flat_hash_map<ObjectWithAttrName, absl::string_view,
                        ObjectWithAttrName::Hash>
        assigned_ids;
    for (auto& item : auto_id_) {
      if (item.existing_value.has_value()) {
        return absl::InvalidArgumentError(
            absl::StrFormat("object %v has AUTO_ID attribute %v with name %v "
                            "already assigned to value %v",
                            item.object, item.attr_name, item.id_name,
                            item.existing_value));
      }
    }
    absl::string_view cur_id_name = "";
    int64_t cur_id_counter = 0;
    for (auto &item : auto_id_) {
      if (item.id_name != cur_id_name) {
        cur_id_name = item.id_name;
        cur_id_counter = 0;
      }
      auto [it, inserted] = assigned_ids.try_emplace(
          {.object = item.object, .attr_name = item.id_name}, "");
      if (inserted) {
        std::string id =
            absl::StrFormat("%s_%d", item.id_name, ++cur_id_counter);
        it->second = CachedName(std::move(id));
      }
      ASSIGN_OR_RETURN(
          DataItem existing_value,
          new_databag_->GetAttr(DataItem(item.object), item.attr_name));
      if (existing_value.has_value()) {
        return absl::InvalidArgumentError(
            absl::StrFormat("object %v has AUTO_ID attribute %v with name %v "
                            "already assigned to value %v",
                            item.object, item.attr_name, item.id_name,
                            existing_value));
      }
      RETURN_IF_ERROR(
          new_databag_->SetAttr(DataItem(item.object), item.attr_name,
                                DataItem(arolla::Text(it->second))));
    }
    return absl::OkStatus();
  }

 private:
  struct ObjectWithAttrName {
    ObjectId object;
    absl::string_view attr_name;

    struct Hash {
      size_t operator()(const ObjectWithAttrName& obj) const {
        return absl::HashOf(obj.object, obj.attr_name);
      }
    };

    bool operator==(const ObjectWithAttrName& other) const {
      return std::tie(object, attr_name) ==
             std::tie(other.object, other.attr_name);
    }
  };

  struct AutoIdItem {
    ObjectId object;
    ObjectId schema;
    absl::string_view attr_name;
    absl::string_view id_name;
    DataItem existing_value;

    bool operator<(const AutoIdItem& other) const {
      return std::tie(id_name, object, attr_name) <
             std::tie(other.id_name, other.object, other.attr_name);
    }
  };

  absl::string_view CachedName(std::string name) {
    auto [it, inserted] = names_cache_.emplace(std::move(name));
    return *it;
  }

  absl::Status VisitSchemaNameAttr(const DataItem& schema,
                                   const DataItem& attr) {
    if (!attr.holds_value<arolla::Text>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "%s attribute is not a text, for schema %v, got %v",
          schema::kSchemaNameAttr, schema, attr));
    }
    absl::string_view schema_name = attr.value<arolla::Text>();
    if (schema_name.starts_with(kAutoIdPrefix)) {
      absl::string_view id_name =
          CachedName(std::string(schema_name.substr(kAutoIdPrefix.length())));
      auto_id_schema_names_[schema.value<ObjectId>()] = id_name;
    }
    return absl::OkStatus();
  }

  absl::Status VisitSchemaMetadataAttr(const DataItem& schema,
                                       const DataItem& attr) {
    if (!attr.holds_value<ObjectId>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "%s attribute is not an ObjectId, for schema %v, got %v",
          schema::kSchemaMetadataAttr, schema, attr));
    }
    schema_metadata_[schema.value<ObjectId>()] = attr.value<ObjectId>();
    return absl::OkStatus();
  }

  absl::Status VisitSchemaAttr(const DataItem& schema,
                               absl::string_view attr_name,
                               const DataItem& attr) {
    if (attr_name == schema::kSchemaNameAttr) {
      return VisitSchemaNameAttr(schema, attr);
    }
    if (attr_name == schema::kSchemaMetadataAttr) {
      return VisitSchemaMetadataAttr(schema, attr);
    }
    return absl::OkStatus();
  }

  DataBagImplPtr new_databag_;
  // Cache for strings needs a pointer stability, thus using node_hash_set.
  absl::node_hash_set<std::string> names_cache_;
  // All the string_views in hash maps are pointing to strings in names_cache_.
  absl::flat_hash_map<ObjectId, absl::string_view> auto_id_schema_names_;
  absl::flat_hash_map<ObjectId, ObjectId> schema_metadata_;
  absl::flat_hash_map<ObjectWithAttrName, absl::string_view,
                      ObjectWithAttrName::Hash>
      auto_id_attrs_;
  std::vector<AutoIdItem> auto_id_;
};

}  // namespace

absl::Status AutoIdsUpdateOp::operator()(
    const DataSliceImpl& ds, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks) const {
  auto visitor =
      std::make_shared<AutoValuesVisitor>(DataBagImplPtr::NewRef(new_databag_));
  Traverser<AutoValuesVisitor> traverser(databag, fallbacks, visitor);
  RETURN_IF_ERROR(traverser.TraverseSlice(ds, schema));
  return visitor->SetIds();
}

absl::Status AutoIdsUpdateOp::operator()(
    const DataItem& item, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks) const {
  return AutoIdsUpdateOp::operator()(DataSliceImpl::Create(/*size=*/1, item),
                                     schema, databag, fallbacks);
}

}  // namespace koladata::internal
