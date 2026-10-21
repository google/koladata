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
#ifndef KOLADATA_INTERNAL_OP_UTILS_VECTORIZED_TRAVERSER_H_
#define KOLADATA_INTERNAL_OP_UTILS_VECTORIZED_TRAVERSER_H_

#include <memory>
#include <optional>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/op_utils/abstract_visitor.h"
#include "koladata/internal/op_utils/group_by_utils.h"
#include "koladata/internal/op_utils/in_degree_tracker.h"
#include "koladata/internal/op_utils/schema_tracker.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/op_utils/vectorized_traverse_helper.h"

namespace koladata::internal {

// Traverses a DataBag starting from the given slice.
//
// Operates on slices of homogeneous items, instead of processing items one by
// one. Reachable objects are split into `QueuedSlice`s right away (see the
// invariants on `QueuedSlice`), so that all items in a `QueuedSlice` are
// homogeneous (share the same token and have the same schema or schemas from
// the same big allocation) and each stage only handles a few kinds of slices.
//
// Traversing is organized in stages:
// 1. PrevisitAndCalculateInDegrees: BFS over reachable slices, invoking
//    `visitor_->PrevisitSlice` and `visitor_->PrevisitObjectSchemas`, checking
//    schema conflicts via `SchemaTracker`, and counting incoming edges via
//    `InDegreeTracker`.
class VectorizedTraverser {
 public:
  VectorizedTraverser(const DataBagImpl& databag,
                      DataBagImpl::FallbackSpan fallbacks,
                      std::shared_ptr<AbstractVisitor> visitor);

  // Traverses all objects reachable from `ds` with `schema`.
  absl::Status TraverseSlice(const DataSliceImpl& ds, const DataItem& schema);

 private:
  // A slice of homogeneous reachable objects that are processed together.
  //
  // Reachable objects are split into such slices right away, so that the
  // slices satisfy the following invariants:
  // - `slice` holds dense (`present_count() == size() > 0`) ObjectIds. Slices
  //   stored in `reachable_slices_` also contain unique ObjectIds.
  // - `schema_item` is one of:
  //   - an entity schema (ObjectId) shared by all objects in `slice`;
  //     `is_object_schema` tells whether it was taken from `__schema__`;
  //   - `schema::kObject`: `object_schemas` (aligned with `slice`) holds
  //     per-object schemas taken from `__schema__`, all from a single
  //     allocation;
  //   - `schema::kSchema`: objects in `slice` are schemas from a single
  //     allocation.
  //   A small allocation is never shared between schemas of a slice (see
  //   `ObjectsGroupBy::SchemaAllocationKeys`), so "a single small allocation"
  //   means "a single schema".
  // - `token` is the token returned by the visitor for all objects in `slice`.
  struct QueuedSlice {
    DataSliceImpl slice;
    DataItem schema_item;
    bool is_object_schema = false;
    DataSliceImpl object_schemas;
    DataItem token;
  };

  using Transition = vectorized_traverser_internal::Transition;
  using TransitionCallback = vectorized_traverser_internal::TransitionCallback;
  using TransitionKey = TraverseHelper::TransitionKey;

  // Calls `visitor_->PrevisitSlice` and validates the returned tokens.
  absl::StatusOr<DataSliceImpl> PrevisitSlice(
      const DataSliceImpl& values, const DataItem& schema,
      const std::optional<TransitionKey>& key, const DataItem& parent_token);

  // Calls `visitor_->PrevisitObjectSchemas` and validates the returned tokens.
  absl::StatusOr<DataSliceImpl> PrevisitObjectSchemas(
      const DataSliceImpl& objects, const DataSliceImpl& schemas,
      const DataItem& token);

  // Returns a slice of `objects` with their `__schema__`s `schemas` (from a
  // single allocation): an entity slice if all `schemas` are equal, and an
  // OBJECT slice otherwise.
  QueuedSlice MakeObjectsSlice(DataSliceImpl objects, DataSliceImpl schemas,
                               const DataItem& token);

  // Records the schema of each ObjectId in `slice` and checks for conflicts
  // (if an ObjectId is reached with different schemas).
  absl::Status TrackSchemas(const QueuedSlice& slice);

  // Records the schemas of `slice` and appends its newly reached objects to
  // `reachable_slices_`. `slice` satisfies the `QueuedSlice` invariants, except
  // that it may contain duplicates.
  absl::Status Enqueue(QueuedSlice slice);

  // Calls `callback` for the outgoing edges of `parent`.
  absl::Status ForEachChild(const QueuedSlice& parent,
                            TransitionCallback callback);

  // Previsits `transition.values` reached from a parent with `parent_token`,
  // and enqueues the objects that should be traversed further.
  absl::Status PrevisitAndEnqueue(const DataItem& parent_token,
                                  const Transition& transition);

  // All `values` are previsited (including DTypes), but only ObjectIds are
  // traversed further.
  absl::Status PrevisitAndEnqueueSchemas(
      const DataItem& parent_token, const std::optional<TransitionKey>& key,
      const DataSliceImpl& values);

  // `objects` are ObjectIds with the entity schema `schema`.
  absl::Status PrevisitAndEnqueueEntities(
      const DataItem& parent_token, const std::optional<TransitionKey>& key,
      const DataSliceImpl& objects, const DataItem& schema);

  // `objects` are ObjectIds with schema OBJECT. They are previsited with
  // OBJECT first, and then with their `__schema__`s (see
  // `PrevisitAndEnqueueObjectSchemas`).
  absl::Status PrevisitAndEnqueueObjects(
      const DataItem& parent_token, const std::optional<TransitionKey>& key,
      const DataSliceImpl& objects);

  // `objects` are dense ObjectIds that were previsited with OBJECT and got
  // `token` from `PrevisitSlice`. Fetches their `__schema__`s, previsits them
  // with `PrevisitObjectSchemas`, and enqueues them.
  absl::Status PrevisitAndEnqueueObjectSchemas(const DataItem& token,
                                               const DataSliceImpl& objects);

  // Stage 1: Previsits all slices reachable from the root `ds` with `schema`,
  // and calculates in-degrees. `reachable_slices_` is used as a BFS queue and
  // holds all reachable slices in the end.
  absl::Status PrevisitAndCalculateInDegrees(const DataSliceImpl& ds,
                                             const DataItem& schema);

  const DataBagImpl& databag_;
  DataBagImpl::FallbackSpan fallbacks_;
  std::shared_ptr<AbstractVisitor> visitor_;
  vectorized_traverser_internal::VectorizedTraverseHelper traverse_helper_;
  InDegreeTracker in_degree_tracker_;
  SchemaTracker schema_tracker_;
  ObjectsGroupBy group_by_;
  // Reachable slices in BFS order; disjoint and satisfying the `QueuedSlice`
  // invariants.
  std::vector<QueuedSlice> reachable_slices_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_VECTORIZED_TRAVERSER_H_
