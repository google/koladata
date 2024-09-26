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
#include "koladata/internal/op_utils/deep_clone.h"

#include <cstddef>
#include <memory>
#include <string_view>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/traverser.h"
#include "koladata/internal/schema_utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

namespace {

class DeepCloneVisitor : AbstractVisitor {
 public:
  explicit DeepCloneVisitor(DataBagImplPtr new_databag)
      : new_databag_(std::move(new_databag)), object_tracker_() {}

  absl::Status Previsit(const DataItem& item, const DataItem& schema) override {
    if (schema.holds_value<ObjectId>()) {
      // Entity schema.
      return PrevisitObject(item);
    } else if (schema.holds_value<schema::DType>()) {
      if (schema == schema::kObject) {
        return PrevisitObject(item);
      } else if (schema == schema::kAny) {
        return absl::InternalError(absl::StrFormat(
            "deep_clone does not support %v schema; encountered for object %v",
            schema, item));
      } else if (schema == schema::kSchema) {
        return PrevisitSchema(item);
      }
      return absl::OkStatus();
    }
    return absl::InternalError("unsupported schema type");
  }

  absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                    const DataItem& schema) override {
    if (!item.holds_value<ObjectId>()) {
      return item;
    }
    auto item_it = object_tracker_.find(item);
    if (item_it == object_tracker_.end()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("object %v is not found", item));
    }
    return item_it->second;
  }

  absl::Status VisitList(const DataItem& list, const DataItem& schema,
                         bool is_object_schema,
                         const DataSliceImpl& items) override {
    DCHECK(list.holds_value<ObjectId>() && list.value<ObjectId>().IsList());
    ASSIGN_OR_RETURN(auto new_list, GetValue(list, schema));
    if (is_object_schema) {
      RETURN_IF_ERROR(SetSchemaAttr(new_list, schema));
    }
    RETURN_IF_ERROR(new_databag_->ExtendList(new_list, items));
    return absl::OkStatus();
  }

  absl::Status VisitDict(const DataItem& dict, const DataItem& schema,
                         bool is_object_schema, const DataSliceImpl& keys,
                         const DataSliceImpl& values) override {
    DCHECK(dict.holds_value<ObjectId>() && dict.value<ObjectId>().IsDict());
    DCHECK(keys.size() == values.size());
    ASSIGN_OR_RETURN(auto new_dict, GetValue(dict, schema));
    if (is_object_schema) {
      RETURN_IF_ERROR(SetSchemaAttr(new_dict, schema));
    }
    RETURN_IF_ERROR(new_databag_->SetInDict(
        DataSliceImpl::Create(keys.size(), new_dict), keys, values));
    return absl::OkStatus();
  }

  absl::Status VisitObject(
      const DataItem& object, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_values) override {
    DCHECK(object.holds_value<ObjectId>());
    ASSIGN_OR_RETURN(auto new_object, GetValue(object, schema));
    if (is_object_schema) {
      RETURN_IF_ERROR(SetSchemaAttr(new_object, schema));
    }
    DCHECK(attr_names.size() == attr_values.size());
    DCHECK(attr_names.IsAllPresent());
    for (size_t i = 0; i < attr_names.size(); ++i) {
      if (attr_values.present(i)) {
        auto attr_name = attr_names[i].value;
        const DataItem& value = attr_values[i].value;
        if (schema == schema::kSchema) {
          RETURN_IF_ERROR(
              new_databag_->SetSchemaAttr(new_object, attr_name, value));
        } else {
          RETURN_IF_ERROR(new_databag_->SetAttr(new_object, attr_name, value));
        }
      }
    }
    return absl::OkStatus();
  }

  absl::Status VisitSchema(
      const DataItem& item, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_schema) override {
    return VisitObject(item, schema, is_object_schema, attr_names,
                       attr_schema);
  }

  absl::Status VisitPrimitive(const DataItem& item,
                              const DataItem& schema) override {
    return absl::OkStatus();
  }

 private:
  absl::Status PrevisitObject(const DataItem& item) {
    if (item.holds_value<ObjectId>() && !object_tracker_.contains(item)) {
      DataItem new_item;
      // TODO: Keep ids inside allocations and use mapping between
      // old and new AllocationIds.
      if (item.is_list()) {
        new_item = DataItem(AllocateSingleList());
      } else if (item.is_dict()) {
        new_item = DataItem(AllocateSingleDict());
      } else {
        new_item = DataItem(AllocateSingleObject());
      }
      object_tracker_[item] = std::move(new_item);
    }
    return absl::OkStatus();
  }

  absl::Status PrevisitSchema(const DataItem& schema) {
    if (schema.holds_value<ObjectId>() && !object_tracker_.contains(schema)) {
      auto new_schema = DataItem(AllocateExplicitSchema());
      object_tracker_[schema] = std::move(new_schema);
    }
    return absl::OkStatus();
  }

  absl::Status SetSchemaAttr(const DataItem& item, const DataItem& schema) {
    ASSIGN_OR_RETURN(auto explicit_schema_value,
                     GetValue(schema, DataItem(schema::kSchema)));
    RETURN_IF_ERROR(new_databag_->SetAttr(item, schema::kSchemaAttr,
                                          std::move(explicit_schema_value)));
    return absl::OkStatus();
  }

 private:
  DataBagImplPtr new_databag_;
  absl::flat_hash_map<DataItem, DataItem, DataItem::Hash> object_tracker_;
};

};  // namespace

absl::StatusOr<std::pair<DataSliceImpl, DataItem>> DeepCloneOp::operator()(
    const DataSliceImpl& ds, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks) const {
  auto visitor =
      std::make_shared<DeepCloneVisitor>(DataBagImplPtr::NewRef(new_databag_));
  auto traverse_op = Traverser<DeepCloneVisitor>(databag, fallbacks, visitor);
  RETURN_IF_ERROR(traverse_op.TraverseSlice(ds, schema));
  ASSIGN_OR_RETURN(auto result_schema, visitor->DeepCloneVisitor::GetValue(
                                           schema, DataItem(schema::kSchema)));
  DataSliceImpl::Builder result_items(ds.size());
  for (size_t i = 0; i < ds.size(); ++i) {
    ASSIGN_OR_RETURN(auto value,
                     visitor->DeepCloneVisitor::GetValue(ds[i], result_schema));
    result_items.Insert(i, value);
  }
  return std::make_pair(std::move(result_items).Build(),
                        std::move(result_schema));
}

absl::StatusOr<std::pair<DataItem, DataItem>> DeepCloneOp::operator()(
    const DataItem& item, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks) const {
  ASSIGN_OR_RETURN((auto [result_slice, result_schema]),
                   (*this)(DataSliceImpl::Create(/*size=*/1, item), schema,
                           databag, fallbacks));
  DCHECK_EQ(result_slice.size(), 1);
  return std::make_pair(result_slice[0], std::move(result_schema));
}

}  // namespace koladata::internal
