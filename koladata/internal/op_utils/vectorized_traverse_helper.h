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
#ifndef KOLADATA_INTERNAL_OP_UTILS_VECTORIZED_TRAVERSE_HELPER_H_
#define KOLADATA_INTERNAL_OP_UTILS_VECTORIZED_TRAVERSE_HELPER_H_

#include <optional>

#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/op_utils/group_by_utils.h"
#include "koladata/internal/op_utils/traverse_helper.h"

namespace koladata::internal::vectorized_traverser_internal {

// Edges from a parent slice to `values` with `schema`. `values` are not
// filtered: they may contain primitives, missing items and duplicates.
struct Transition {
  std::optional<TraverseHelper::TransitionKey> key;
  DataSliceImpl values;
  DataItem schema;
};

using TransitionCallback = absl::FunctionRef<absl::Status(const Transition&)>;

// Iterates over the edges of a DataBag (with fallbacks) graph for
// VectorizedTraverser: the edges from a slice of objects with the same
// transition key and schema are passed to `callback` together as a single
// `Transition`. Transitions with primitive schemas are skipped, since
// primitives have no outgoing edges.
//
// This is a vectorized counterpart of `TraverseHelper::ForEachObject`.
class VectorizedTraverseHelper {
 public:
  VectorizedTraverseHelper(const DataBagImpl& databag,
                           DataBagImpl::FallbackSpan fallbacks)
      : databag_(databag), fallbacks_(fallbacks) {}

  // Calls `callback` for the edges from the virtual root: to the root slice
  // `ds` with `schema`, and to `schema` itself.
  static absl::Status ForEachRootTransition(const DataSliceImpl& ds,
                                            const DataItem& schema,
                                            TransitionCallback callback);

  // Calls `callback` for the outgoing edges of `schemas` (dense ObjectIds from
  // a single allocation).
  absl::Status ForEachChildOfSchemas(const DataSliceImpl& schemas,
                                     TransitionCallback callback);

  // Calls `callback` for the outgoing edges of `entities` with the entity
  // schema `schema`.
  absl::Status ForEachChildOfEntities(const DataSliceImpl& entities,
                                      const DataItem& schema,
                                      TransitionCallback callback);

  // Calls `callback` for the outgoing edges of `objects` with their
  // `__schema__`s `object_schemas` (dense ObjectIds from a single allocation).
  absl::Status ForEachChildOfObjects(const DataSliceImpl& objects,
                                     const DataSliceImpl& object_schemas,
                                     TransitionCallback callback);

 private:
  absl::Status ForEachSchemaAttributeValue(const DataSliceImpl& schemas,
                                           const DataItem& attr_name,
                                           TransitionCallback callback);

  // Groups `objects` by the schema of `attr_name` in `object_schemas`, and
  // calls `callback` for the attribute values of each group.
  absl::Status ForEachObjectsAttributeValue(const DataSliceImpl& objects,
                                            const DataSliceImpl& object_schemas,
                                            const DataItem& attr_name,
                                            TransitionCallback callback);

  // Primitive values have no outgoing edges, so they are skipped (they are
  // validated during the visit).
  absl::Status ForEachAttributeValue(const DataSliceImpl& entities,
                                     const DataItem& attr_name,
                                     const DataItem& attr_schema,
                                     TransitionCallback callback);

  absl::Status ForEachListItem(const DataSliceImpl& lists,
                               const DataItem& item_schema,
                               TransitionCallback callback);

  absl::Status ForEachDictKeyAndValue(const DataSliceImpl& dicts,
                                      const DataItem& keys_schema,
                                      const DataItem& values_schema,
                                      TransitionCallback callback);

  // Groups `values` by `keys` (for ToPyVisitor, which uses string dict keys as
  // dataclass field names) and calls `callback` for the values of each key.
  absl::Status ForEachDictValueGroupedByKey(const DataSliceImpl& keys,
                                            const DataSliceImpl& values,
                                            const DataItem& values_schema,
                                            TransitionCallback callback);

  const DataBagImpl& databag_;
  DataBagImpl::FallbackSpan fallbacks_;
  ObjectsGroupBy group_by_;
};

}  // namespace koladata::internal::vectorized_traverser_internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_VECTORIZED_TRAVERSE_HELPER_H_
