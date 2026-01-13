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
#include "koladata/internal/op_utils/deep_uuid.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/traverser.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/internal/uuid_object.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

namespace {

class DeepUuidVisitor : AbstractVisitor {
  constexpr static absl::string_view kSelfReference =
      "__self_reference_placeholder__";

 public:
  explicit DeepUuidVisitor(absl::string_view seed)
      : self_reference_(arolla::Text(kSelfReference)), seed_(seed) {}

  absl::StatusOr<bool> Previsit(
      const DataItem& from_item, const DataItem& from_schema,
      const std::optional<absl::string_view>& from_item_attr_name,
      const DataItem& item, const DataItem& schema) override {
    return true;
  }

  absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                    const DataItem& schema) override {
    if (!item.holds_value<ObjectId>()) {
      return item;
    }
    if (schema == schema::kItemId) {
      return item;
    }
    auto item_it = object_tracker_.find(item);
    if (item_it == object_tracker_.end()) {
      untracked_objects_.insert(item);
      return item;
    }
    return item_it->second;
  }

  absl::StatusOr<DataItem> GetValueSafe(const DataItem& item,
                                        const DataItem& schema) {
    if (!item.holds_value<ObjectId>()) {
      return item;
    }
    if (schema == schema::kItemId) {
      return item;
    }
    auto item_it = object_tracker_.find(item);
    if (item_it == object_tracker_.end()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("object %v is not tracked; note that cyclic "
                          "attributes are not allowed in deep_uuid",
                          item.value<ObjectId>()));
    }
    return item_it->second;
  }

  absl::Status VisitList(const DataItem& list, const DataItem& schema,
                         bool is_object_schema,
                         const DataSliceImpl& items) override {
    DCHECK(list.is_list());
    if (HasUntrackedObjects(items)) {
      return absl::InvalidArgumentError(
          absl::StrFormat("list %v contains untracked objects. Note: cyclic "
                          "attributes are not allowed in deep_uuid",
                          list.value<ObjectId>()));
    }
    DataItem uuid = CreateListUuidFromItemsAndFields(seed_, items, {}, {});
    object_tracker_.emplace(list, std::move(uuid));
    untracked_objects_.erase(list);
    return absl::OkStatus();
  }

  absl::Status VisitDict(const DataItem& dict, const DataItem& schema,
                         bool is_object_schema, const DataSliceImpl& keys,
                         const DataSliceImpl& values) override {
    DCHECK(dict.is_dict());
    DCHECK(keys.size() == values.size());
    if (HasUntrackedObjects(keys)) {
      return absl::InvalidArgumentError(
          absl::StrFormat("list %v contains untracked keys. Note: cyclic "
                          "attributes are not allowed in deep_uuid",
                          dict.value<ObjectId>()));
    }
    if (HasUntrackedObjects(values)) {
      return absl::InvalidArgumentError(
          absl::StrFormat("list %v contains untracked values. Note: cyclic "
                          "attributes are not allowed in deep_uuid",
                          dict.value<ObjectId>()));
    }
    DataItem uuid =
        CreateDictUuidFromKeysValuesAndFields(seed_, keys, values, {}, {});
    object_tracker_.emplace(dict, std::move(uuid));
    untracked_objects_.erase(dict);
    return absl::OkStatus();
  }

  absl::Status VisitObject(
      const DataItem& object, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_values) override {
    DCHECK(object.holds_value<ObjectId>());
    std::vector<std::string_view> attr_names_view;
    attr_names_view.reserve(attr_names.size());
    for (int64_t i = 0; i < attr_names.size(); ++i) {
      attr_names_view.push_back(attr_names[i].value);
    }
    std::vector<std::reference_wrapper<const DataItem>> attr_values_view;
    attr_values_view.reserve(attr_values.size());
    auto status = absl::OkStatus();
    attr_values.ForEach([&](size_t id, bool presence, const DataItem& item) {
      if (untracked_objects_.contains(item)) {
        if (item == object) {
          attr_values_view.push_back(std::cref(self_reference_));
          return;
        }
        status = absl::InvalidArgumentError(
            absl::StrFormat("%v contains untracked attributes. Note: "
                            "cyclic attributes are not allowed in deep_uuid",
                            object.value<ObjectId>()));
      } else {
        attr_values_view.push_back(std::cref(item));
      }
    });
    if (!status.ok()) {
      return status;
    }
    DataItem uuid =
        CreateUuidFromFields(seed_, attr_names_view, attr_values_view);
    object_tracker_.emplace(object, std::move(uuid));
    untracked_objects_.erase(object);
    return absl::OkStatus();
  }

  absl::Status VisitSchema(
      const DataItem& item, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_schema) override {
    std::vector<std::string_view> attr_names_view;
    attr_names_view.reserve(attr_names.size());
    for (int64_t i = 0; i < attr_names.size(); ++i) {
      attr_names_view.push_back(attr_names[i].value);
    }
    std::vector<std::reference_wrapper<const DataItem>> attr_schemas_view;
    attr_schemas_view.reserve(attr_schema.size());
    bool has_untracked_objects = false;
    attr_schema.ForEach(
        [&](size_t id, bool presence, const DataItem& attr_item) {
          if (untracked_objects_.contains(attr_item)) {
            if (attr_item == item) {
              attr_schemas_view.push_back(std::cref(self_reference_));
              return;
            }
            has_untracked_objects = true;
          } else {
            attr_schemas_view.push_back(std::cref(attr_item));
          }
        });
    if (has_untracked_objects) {
      // Uuid for schema is not computed, but we don't return an error unless
      // the schema's uuid is included in the result.
      untracked_objects_.insert(item);
      return absl::OkStatus();
    }
    DataItem uuid =
        CreateSchemaUuidFromFields(seed_, attr_names_view, attr_schemas_view);
    object_tracker_.emplace(item, std::move(uuid));
    untracked_objects_.erase(item);
    return absl::OkStatus();
  }

 private:
  bool HasUntrackedObjects(const DataSliceImpl& items) {
    bool has_untracked_objects = false;
    items.VisitValues([&]<typename T>(arolla::DenseArray<T> array) {
      if constexpr (std::is_same_v<T, ObjectId>) {
        array.ForEachPresent([&](size_t id, const ObjectId& item) {
          if (untracked_objects_.contains(item)) {
            has_untracked_objects = true;
            return;
          }
        });
      }
    });
    return has_untracked_objects;
  }

  DataItem self_reference_;
  absl::string_view seed_;
  absl::flat_hash_map<DataItem, DataItem, DataItem::Hash> object_tracker_;
  absl::flat_hash_set<DataItem, DataItem::Hash> untracked_objects_;
};

}  // namespace

absl::StatusOr<DataSliceImpl> DeepUuidOp::operator()(
    const DataItem& seed, const DataSliceImpl& ds, const DataItem& schema,
    const DataBagImpl& databag, DataBagImpl::FallbackSpan fallbacks) const {
  std::string_view seed_str = "";
  if (seed.has_value()) {
    if (seed.holds_value<arolla::Text>()) {
      seed_str = seed.value<arolla::Text>();
    } else {
      return absl::InvalidArgumentError(
          absl::StrFormat("seed must be a string, got %v", seed));
    }
  }
  auto visitor = std::make_shared<DeepUuidVisitor>(seed_str);
  auto traverse_op = Traverser<DeepUuidVisitor>(databag, fallbacks, visitor);
  RETURN_IF_ERROR(traverse_op.TraverseSlice(ds, schema));
  SliceBuilder result_items(ds.size());
  for (size_t i = 0; i < ds.size(); ++i) {
    ASSIGN_OR_RETURN(auto value,
                     visitor->DeepUuidVisitor::GetValueSafe(ds[i], schema));
    if (!value.holds_value<ObjectId>() && schema != schema::kItemId) {
      if (seed_str.empty()) {
        value = DataItem(CreateUuidObject(value.StableFingerprint()));
      } else {
        arolla::FingerprintHasher hasher(seed_str);
        hasher.Combine(value.StableFingerprint());
        value = DataItem(CreateUuidObject(std::move(hasher).Finish()));
      }
    }
    result_items.InsertIfNotSetAndUpdateAllocIds(i, value);
  }
  return std::move(result_items).Build();
}

absl::StatusOr<DataItem> DeepUuidOp::operator()(
    const DataItem& seed, const DataItem& item, const DataItem& schema,
    const DataBagImpl& databag, DataBagImpl::FallbackSpan fallbacks) const {
  ASSIGN_OR_RETURN(auto result_slice,
                   (*this)(seed, DataSliceImpl::Create(/*size=*/1, item),
                           schema, databag, fallbacks));
  DCHECK_EQ(result_slice.size(), 1);
  return result_slice[0];
}

}  // namespace koladata::internal
