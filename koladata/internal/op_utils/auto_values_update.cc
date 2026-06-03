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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_set.h"
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
#include "koladata/internal/dtype.h"
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
  static constexpr absl::string_view kAutoReferencePrefix =
      AutoReferenceUpdateOp::kAutoReferencePrefix;

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
      // Process AUTO_REFERENCE attributes in metadata objects.
      if (process_auto_reference_) {
        if (auto auto_reference_name_it =
                auto_reference_schema_names_.find(attr_value.value<ObjectId>());
            auto_reference_name_it != auto_reference_schema_names_.end()) {
          auto_reference_attrs_[{
              .object = object.value<ObjectId>(),
              .attr_name = CachedName(std::string(attr_name))}] =
              auto_reference_name_it->second;
          return absl::OkStatus();
        }
      }
    }
    if (!schema_metadata.has_value()) {
      return absl::OkStatus();
    }
    // AUTO_ID
    if (auto it = auto_id_attrs_.find(
            {.object = *schema_metadata, .attr_name = attr_name});
        it != auto_id_attrs_.end()) {
      auto_id_.push_back(AutoIdItem{
          .object = object.value<ObjectId>(),
          .schema = schema.value<ObjectId>(),
          .attr_name = it->first.attr_name,
          .id_name = it->second,
          .existing_value = attr_value,
      });
    }
    // AUTO_REFERENCE
    if (process_auto_reference_) {
      if (auto it = auto_reference_attrs_.find(
              {.object = *schema_metadata, .attr_name = attr_name});
          it != auto_reference_attrs_.end()) {
        auto_reference_.push_back(AutoIdItem{
            .object = object.value<ObjectId>(),
            .schema = schema.value<ObjectId>(),
            .attr_name = it->first.attr_name,
            .id_name = it->second,
            .existing_value = attr_value,
        });
      }
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
    absl::flat_hash_map<ObjectWithAttrName, absl::string_view> assigned_ids;
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

  void EnableAutoReferenceProcessing() {
    process_auto_reference_ = true;
  }

  absl::Status SetReferences(const DataBagImpl& databag,
                             DataBagImpl::FallbackSpan fallbacks) {
    absl::flat_hash_map<IdWithAttrName, ObjectIdWithSchema> assigned_ids;
    RETURN_IF_ERROR(CollectAssignedIds(assigned_ids));
    for (const auto& item : auto_reference_) {
      if (item.existing_value.holds_value<arolla::Text>()) {
        RETURN_IF_ERROR(SetSingleReference(item, assigned_ids));
        continue;
      }
      if (!item.existing_value.holds_value<ObjectId>()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "Unexpected existing value %v for auto-reference in namespace %s",
            item.existing_value, item.id_name));
      }
      if (!item.existing_value.value<ObjectId>().IsList()) {
        // Auto reference is already resolved.
        continue;
      }
      DataSliceImpl slice =
          DataSliceImpl::Create(/*size=*/1, item.existing_value);
      ASSIGN_OR_RETURN(auto schema,
                       databag.GetSchemaAttr(DataItem(item.schema),
                                             item.attr_name, fallbacks));
      auto schema_it = namespace_to_schema_.find(item.id_name);
      if (schema_it == namespace_to_schema_.end()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "No schema found for auto-reference namespace %s", item.id_name));
      }
      ObjectId ref_schema = schema_it->second;
      size_t depth = 0;
      for (;; ++depth) {
        if (!schema.holds_value<ObjectId>()) {
          return absl::InvalidArgumentError(absl::StrCat(
              "Unexpected schema type for auto-reference: ", schema));
        }
        ASSIGN_OR_RETURN(auto items_schema,
                         databag.GetSchemaAttrAllowMissing(
                             schema, schema::kListItemsSchemaAttr, fallbacks));
        if (!items_schema.has_value()) {
          // Auto references appear to be already resolved. Or the structure is
          // invalid.
          break;
        }
        ASSIGN_OR_RETURN(
            (auto [items, edge]),
            databag.ExplodeLists(slice, DataBagImpl::ListRange(), fallbacks));
        if (items_schema == DataItem(schema::kString)) {
          ASSIGN_OR_RETURN(auto ref_items,
                           GetRefs(items, item.id_name, assigned_ids));
          RETURN_IF_ERROR(new_databag_->ReplaceInLists(
              slice, DataBagImpl::ListRange(), std::move(ref_items), edge));
          break;
        }
        slice = std::move(items);
        schema = std::move(items_schema);
      }
      ASSIGN_OR_RETURN(auto ref_schema_item,
                       NestedListSchema(DataItem(ref_schema), depth));
      RETURN_IF_ERROR(new_databag_->SetSchemaAttr(
          DataItem(item.schema), item.attr_name, ref_schema_item));
    }
    return absl::OkStatus();
  }

 private:
  struct ObjectWithAttrName {
    ObjectId object;
    absl::string_view attr_name;

    template <typename H>
    friend H AbslHashValue(H h, const ObjectWithAttrName& obj) {
      return H::combine(std::move(h), obj.object, obj.attr_name);
    }

    bool operator==(const ObjectWithAttrName& other) const {
      return std::tie(object, attr_name) ==
             std::tie(other.object, other.attr_name);
    }
  };

  struct IdWithAttrName {
    std::string_view id_name;
    std::string_view attr_name;

    template <typename H>
    friend H AbslHashValue(H h, const IdWithAttrName& obj) {
      return H::combine(std::move(h), obj.id_name, obj.attr_name);
    }

    bool operator==(const IdWithAttrName& other) const {
      return std::tie(id_name, attr_name) ==
             std::tie(other.id_name, other.attr_name);
    }
  };

  struct ObjectIdWithSchema {
    ObjectId object;
    ObjectId schema;
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
    } else if (process_auto_reference_ &&
               schema_name.starts_with(kAutoReferencePrefix)) {
      std::string_view id_name =
          schema_name.substr(kAutoReferencePrefix.length());
      auto_reference_schema_names_[schema.value<ObjectId>()] =
          CachedName(std::string(id_name));
    }
    return absl::OkStatus();
  }

  absl::Status CollectAssignedIds(
      absl::flat_hash_map<IdWithAttrName, ObjectIdWithSchema>& assigned_ids) {
    for (const auto& item : auto_id_) {
      if (item.existing_value.holds_value<arolla::Text>()) {
        assigned_ids[{
            .id_name = item.existing_value.value<arolla::Text>().view(),
            .attr_name = item.id_name}] = {
            .object = item.object,
            .schema = item.schema,
        };
        auto [it, inserted] =
            namespace_to_schema_.emplace(item.id_name, item.schema);
        if (!inserted && it->second != item.schema) {
          return absl::InvalidArgumentError(
              absl::StrFormat("Multiple schemas found for auto-reference "
                              "namespace %s: %v and %v",
                              item.id_name, it->second, item.schema));
        }
      }
    }
    return absl::OkStatus();
  }

  absl::Status SetSingleReference(
      const AutoIdItem& item,
      const absl::flat_hash_map<IdWithAttrName, ObjectIdWithSchema>&
          assigned_ids) {
    auto it = assigned_ids.find(
        {.id_name = item.existing_value.value<arolla::Text>().view(),
         .attr_name = item.id_name});
    if (it == assigned_ids.end()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "No object found for auto-reference: %v in namespace %s",
          item.existing_value, item.id_name));
    }
    RETURN_IF_ERROR(new_databag_->SetAttr(DataItem(item.object), item.attr_name,
                                          DataItem(it->second.object)));
    RETURN_IF_ERROR(new_databag_->SetSchemaAttr(
        DataItem(item.schema), item.attr_name, DataItem(it->second.schema)));
    return absl::OkStatus();
  }

  absl::StatusOr<DataSliceImpl> GetRefs(
      const DataSliceImpl& items, absl::string_view id_name,
      const absl::flat_hash_map<IdWithAttrName, ObjectIdWithSchema>&
          assigned_ids) {
    SliceBuilder ref_bldr(items.size());
    for (int i = 0; i < items.size(); ++i) {
      if (!items[i].has_value()) {
        continue;
      }
      auto it =
          assigned_ids.find({.id_name = items[i].value<arolla::Text>().view(),
                             .attr_name = id_name});
      if (it == assigned_ids.end()) {
        return absl::InvalidArgumentError(absl::StrCat(
            "No object found for auto-reference: %v in namespace %s", items[i],
            id_name));
      }
      ref_bldr.InsertGuaranteedNotSetAndUpdateAllocIds(
          i, DataItem(it->second.object));
    }
    return std::move(ref_bldr).Build();
  }

  absl::StatusOr<DataItem> NestedListSchema(DataItem schema, int depth) {
    for (; depth >= 0; --depth) {
      ASSIGN_OR_RETURN(schema, new_databag_->CreateUuSchemaFromFields(
                                   "__list_schema__", {"__items__"}, {schema}));
    }
    return schema;
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

  absl::Status VisitListItemSchemaAttr(const DataItem& schema,
                                       const DataItem& attr) {
    if (!attr.holds_value<ObjectId>()) {
      return absl::OkStatus();
    }
    if (auto it = auto_reference_schema_names_.find(attr.value<ObjectId>());
        it != auto_reference_schema_names_.end()) {
      // Propagate the AUTO_REFERENCE attribute from the items to the list.
      std::string_view id_name = it->second;
      auto_reference_schema_names_[schema.value<ObjectId>()] = id_name;
    }
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
    if (attr_name == schema::kListItemsSchemaAttr) {
      return VisitListItemSchemaAttr(schema, attr);
    }
    return absl::OkStatus();
  }

  DataBagImplPtr new_databag_;
  bool process_auto_reference_;
  // Cache for strings needs a pointer stability, thus using node_hash_set.
  absl::node_hash_set<std::string> names_cache_;
  // All the string_views in hash maps are pointing to strings in names_cache_.
  absl::flat_hash_map<ObjectId, absl::string_view> auto_id_schema_names_;
  absl::flat_hash_map<ObjectId, absl::string_view> auto_reference_schema_names_;
  absl::flat_hash_map<ObjectId, ObjectId> schema_metadata_;
  absl::flat_hash_map<ObjectWithAttrName, absl::string_view> auto_id_attrs_;
  absl::flat_hash_map<ObjectWithAttrName, absl::string_view>
      auto_reference_attrs_;
  absl::flat_hash_map<absl::string_view, ObjectId> namespace_to_schema_;
  std::vector<AutoIdItem> auto_id_;
  std::vector<AutoIdItem> auto_reference_;
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

absl::Status AutoReferenceUpdateOp::operator()(
    const DataSliceImpl& ds, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks, const DataSliceImpl& input_ds,
    const DataItem& input_schema, const DataBagImpl& input_databag,
    DataBagImpl::FallbackSpan input_fallbacks) const {
  auto visitor =
      std::make_shared<AutoValuesVisitor>(DataBagImplPtr::NewRef(new_databag_));
  Traverser<AutoValuesVisitor> input_traverser(input_databag, input_fallbacks,
                                               visitor);
  RETURN_IF_ERROR(input_traverser.TraverseSlice(input_ds, input_schema));

  Traverser<AutoValuesVisitor> traverser(databag, fallbacks, visitor);
  visitor->EnableAutoReferenceProcessing();
  RETURN_IF_ERROR(traverser.TraverseSlice(ds, schema));
  return visitor->SetReferences(databag, fallbacks);
}
absl::Status AutoReferenceUpdateOp::operator()(
    const DataItem& item, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks, const DataSliceImpl& input_ds,
    const DataItem& input_schema, const DataBagImpl& input_databag,
    DataBagImpl::FallbackSpan input_fallbacks) const {
  return AutoReferenceUpdateOp::operator()(
      DataSliceImpl::Create(/*size=*/1, item), schema, databag, fallbacks,
      input_ds, input_schema, input_databag, input_fallbacks);
}

absl::Status AutoReferenceUpdateOp::operator()(
    const DataSliceImpl& ds, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks, const DataItem& input_item,
    const DataItem& input_schema, const DataBagImpl& input_databag,
    DataBagImpl::FallbackSpan input_fallbacks) const {
  return AutoReferenceUpdateOp::operator()(
      ds, schema, databag, fallbacks,
      DataSliceImpl::Create(/*size=*/1, input_item), input_schema,
      input_databag, input_fallbacks);
}

absl::Status AutoReferenceUpdateOp::operator()(
    const DataItem& item, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks, const DataItem& input_item,
    const DataItem& input_schema, const DataBagImpl& input_databag,
    DataBagImpl::FallbackSpan input_fallbacks) const {
  return AutoReferenceUpdateOp::operator()(
      DataSliceImpl::Create(/*size=*/1, item), schema, databag, fallbacks,
      DataSliceImpl::Create(/*size=*/1, input_item), input_schema,
      input_databag, input_fallbacks);
}

}  // namespace koladata::internal
