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
#ifndef KOLADATA_INTERNAL_OP_UTILS_ABSTRACT_VISITOR_H_
#define KOLADATA_INTERNAL_OP_UTILS_ABSTRACT_VISITOR_H_

#include <optional>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/op_utils/traverse_helper.h"

namespace koladata::internal {

// An interface for a visitor that is used in Traverser.
class AbstractVisitor {
 public:
  using TransitionKey = TraverseHelper::TransitionKey;
  using TransitionType = TraverseHelper::TransitionType;

  virtual ~AbstractVisitor() = default;

  // Returns a value for the given item and schema.
  //
  // GetValue would only be called for an (item, schema) after Previsit was
  // called for the same (item, schema). If topological ordering of reachable
  // objects exists, then GetValue would be called for an (item, schema) only
  // after corresponding Visit* method was called for the same (item, schema).
  //
  // Result of GetValue is used in Visit* methods as values for attributes,
  // list items, dict keys and values.
  virtual absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                            const DataItem& schema) = 0;

  // Called for each reachable item and schema before any calls to Visit* or
  // GetValue methods.
  // `transition_key`: if provided - represents the transition key from
  // `from_item` that led to the current item being visited.
  // For objects Previsit is called twice:
  // - first time with schema::kObject.
  // - second time with the schema written in kSchemaAttr attribute.
  // Returns if the item should be traversed further. If false is returned,
  // this item would not be listed for the Visit* methods.
  virtual absl::StatusOr<bool> Previsit(
      const DataItem& from_item, const DataItem& from_schema,
      const std::optional<TransitionKey>& transition_key,
      const DataItem& item, const DataItem& schema) = 0;

  // Called for each reachable list.
  // Args:
  // - list: contains ObjectId of the list.
  // - schema: contains ObjectId of the list schema.
  // - is_object_schema: true iff schema was taken from kSchemaAttr attribute.
  // - items: contains values of the list items.
  // Note: as other Visit* methods, called for each reached (list, schema) pair
  // once. Therefore care should be taken to avoid list content duplication.
  virtual absl::Status VisitList(const DataItem& list, const DataItem& schema,
                                 bool is_object_schema,
                                 const DataSliceImpl& items) = 0;

  // Called for each reachable dict.
  // Args:
  // - dict: contains ObjectId of the dict.
  // - schema: contains ObjectId of the dict schema.
  // - is_object_schema: true iff schema was taken from kSchema attribute.
  // - keys: contains values of the dict keys.
  // - values: contains values of the dict values in the same order as
  // corresponding keys.
  virtual absl::Status VisitDict(const DataItem& dict, const DataItem& schema,
                                 bool is_object_schema,
                                 const DataSliceImpl& keys,
                                 const DataSliceImpl& values) = 0;

  // Called for each reachable object.
  // Args:
  // - list: contains ObjectId.
  // - schema: contains schema.
  // - is_object_schema: true iff schema was taken from kSchema attribute.
  // - attr_names: contains names of the attributes, including special names
  //     like kSchemaAttr, kListItemsSchemaAttr, kDictKeysSchemaAttr,
  //     kDictValuesSchemaAttr.
  // - attr_values: contains values of the attributes in the same order as
  //     corresponding attr_names.
  virtual absl::Status VisitObject(
      const DataItem& object, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_values) = 0;

  // Called for each reachable schema.
  // Args:
  // - item: contains schema.
  // - schema: is DataItem(schema::kSchema).
  // - is_object_schema: true iff schema was taken from kSchema attribute.
  // - attr_names: contains schema attribute names.
  // - attr_schema: contains values for attribute schemas in the same order as
  //     corresponding attr_names.
  virtual absl::Status VisitSchema(
      const DataItem& item, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_schema) = 0;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_ABSTRACT_VISITOR_H_
