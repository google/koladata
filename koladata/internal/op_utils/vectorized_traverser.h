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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "absl/functional/function_ref.h"
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
// Traversing is organized in three stages:
// 1. PrevisitAndCalculateInDegrees: BFS over reachable slices, invoking
//    `visitor_->PrevisitSlice` and `visitor_->PrevisitObjectSchemas`, checking
//    schema conflicts via `SchemaTracker`, and counting incoming edges via
//    `InDegreeTracker`.
// 2. BuildTopologicalOrderFromInDegrees: Kahn's algorithm using
//    `InDegreeTracker::DecrementAndGetLast` to produce a topological ordering
//    of disjoint, dense `ObjectId` slices (breaking cycles if any exist).
//    Unlike DFS post-order (which is inherently one-node-at-a-time and needs an
//    explicit recursion stack), Kahn's algorithm processes entire slices of
//    freed objects in batch without materializing the graph in memory.
// 3. VisitInReverseTopologicalOrder: Post-order visit (`VisitSchema`,
//    `VisitList`, `VisitDict`, `VisitObject`) from leaves to roots.
//
// Stages 1 and 2 enumerate edges with the same `VectorizedTraverseHelper` and
// differ only in what they do with each `Transition`: stage 1 previsits and
// enqueues its values, stage 2 decrements their in-degrees.
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
  //   stored in `reachable_slices_` and `topological_order_` also contain
  //   unique ObjectIds.
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
  // - `token` is the token returned by the visitor for all objects in `slice`
  //   (missing in the topological order).
  struct QueuedSlice {
    DataSliceImpl slice;
    DataItem schema_item;
    bool is_object_schema = false;
    DataSliceImpl object_schemas;
    DataItem token;
  };

  // Tells, for each object of a slice with a single schema, whether the schema
  // was taken from `__schema__`.
  struct ObjectSchemaFlags {
    // True iff the schemas of all objects were taken from `__schema__`.
    bool all = false;
    // Otherwise, present for the objects whose schema was taken from
    // `__schema__`.
    DataSliceImpl mask;

    bool operator[](int64_t i) const { return all || mask.present(i); }
  };

  using Transition = vectorized_traverser_internal::Transition;
  using TransitionCallback = vectorized_traverser_internal::TransitionCallback;
  using TransitionKey = TraverseHelper::TransitionKey;
  using QueuedSliceCallback =
      absl::FunctionRef<absl::Status(QueuedSlice slice)>;
  using TokenGroupCallback = absl::FunctionRef<absl::Status(
      const DataItem& token, DataSliceImpl objects)>;

  // Calls `visitor_->PrevisitSlice` and validates the returned tokens.
  absl::StatusOr<DataSliceImpl> PrevisitSlice(
      const DataSliceImpl& values, const DataItem& schema,
      const std::optional<TransitionKey>& key, const DataItem& parent_token);

  // Calls `PrevisitSlice` for `values` and invokes `callback` for each group of
  // ObjectIds with the same present token.
  absl::Status PrevisitByToken(const DataSliceImpl& values,
                               const DataItem& schema,
                               const std::optional<TransitionKey>& key,
                               const DataItem& parent_token,
                               TokenGroupCallback callback);

  // Calls `visitor_->PrevisitObjectSchemas` and validates the returned tokens.
  absl::StatusOr<DataSliceImpl> PrevisitObjectSchemas(
      const DataSliceImpl& objects, const DataSliceImpl& schemas,
      const DataItem& token);

  // Returns a slice of `objects` with their `__schema__`s `schemas` (from a
  // single allocation): an entity slice if all `schemas` are equal, and an
  // OBJECT slice otherwise.
  QueuedSlice MakeObjectsSlice(DataSliceImpl objects, DataSliceImpl schemas,
                               const DataItem& token);

  // Splits `schemas` by schema allocation and calls `callback` for each group.
  absl::Status ForEachSchemaSlice(DataSliceImpl schemas, const DataItem& token,
                                  QueuedSliceCallback callback);

  // Splits `objects` with their `__schema__`s `schemas` by schema allocation
  // and calls `callback` for each group.
  absl::Status ForEachObjectSlice(DataSliceImpl objects, DataSliceImpl schemas,
                                  const DataItem& token,
                                  QueuedSliceCallback callback);

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

  // Splits `values` with `schema` that became free into slices of
  // `topological_order_`, and appends them to `topological_order_`.
  absl::Status AppendToOrder(DataSliceImpl values, const DataItem& schema);

  // Removes edges into `values` with `schema`, and appends objects that became
  // free to `topological_order_`.
  absl::Status DecrementAndAppendToOrder(const DataSliceImpl& values,
                                         const DataItem& schema);

  // Breaks a cycle by removing incoming edges into the next slice in
  // `reachable_slices_` (starting from `slice_idx`) that still has positive
  // in-degrees, and appends the freed objects to `topological_order_`.
  absl::Status BreakCycle(size_t& slice_idx);

  // Stage 2: Builds `topological_order_` (parents before children) of unique,
  // dense ObjectId slices. Replays the root edges, propagates free slices, and
  // breaks the remaining cycles using `reachable_slices_`.
  absl::Status BuildTopologicalOrderFromInDegrees(const DataSliceImpl& ds,
                                                  const DataItem& schema);

  // Returns the value to pass to `Visit*` for a present `item` with `schema`.
  // Primitives are passed as is; other items are converted with
  // `visitor_->GetValue`.
  absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                    const DataItem& schema);

  // Returns the values to pass to `Visit*` for `values` with `schema`.
  absl::StatusOr<DataSliceImpl> GetValues(const DataSliceImpl& values,
                                          const DataItem& schema);

  // Same as above, but with per-item `schemas` (aligned with `values`; items
  // where `schemas` is missing are omitted).
  absl::StatusOr<DataSliceImpl> GetValues(const DataSliceImpl& values,
                                          const DataSliceImpl& schemas);

  absl::Status Visit(const QueuedSlice& slice);

  absl::StatusOr<ObjectSchemaFlags> GetObjectSchemaFlags(
      const QueuedSlice& slice) const;

  // `schemas` are schemas from a single allocation.
  absl::Status VisitSchemas(const DataSliceImpl& schemas);

  // `entities` are dense ObjectIds with the entity schema `schema`.
  absl::Status VisitEntities(const DataSliceImpl& entities,
                             const DataItem& schema,
                             const ObjectSchemaFlags& is_object_schema);

  absl::Status VisitLists(const DataSliceImpl& lists, const DataItem& schema,
                          const DataItem& item_schema,
                          const ObjectSchemaFlags& is_object_schema);

  absl::Status VisitDicts(const DataSliceImpl& dicts, const DataItem& schema,
                          const DataItem& keys_schema,
                          const DataItem& values_schema,
                          const ObjectSchemaFlags& is_object_schema);

  // `objects` are dense ObjectIds with their `__schema__`s `schemas` from a
  // single big allocation.
  absl::Status VisitObjectsWithSchemasInBigAllocation(
      const DataSliceImpl& objects, const DataSliceImpl& schemas);

  // Stage 3: Visits the slices of `topological_order_` from the last to the
  // first, so that children are visited before their parents (unless there are
  // cycles).
  absl::Status VisitInReverseTopologicalOrder();

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
  // Reachable slices in topological order (parents before children); disjoint
  // and satisfying the `QueuedSlice` invariants.
  std::vector<QueuedSlice> topological_order_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_VECTORIZED_TRAVERSER_H_
