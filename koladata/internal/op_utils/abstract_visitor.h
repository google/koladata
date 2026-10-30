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
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/op_utils/traverse_helper.h"

namespace koladata::internal {

// An interface for a visitor that is used in Traverser and VectorizedTraverser.
//
// Tokens (VectorizedTraverser only):
//
// VectorizedTraverser previsits items in slices rather than one by one. To
// decide which items can be previsited together, it asks the visitor to assign
// a token to each previsited item. A token is an ObjectId chosen by the
// visitor. It represents what the visitor needs to know about an item to
// previsit the item's children, e.g. in ToPy, the dataclass the item is
// converted to. The traverser doesn't interpret tokens: it only compares them
// for equality and passes an item's token back as `parent_token` when
// previsiting its children.
//
// - A missing token means the item is not traversed further.
// - Children reached by the same transition from parents with the same token
//   are previsited together, in one PrevisitSlice call (whenever possible).
//   Visitors that need no per-parent information should return the same
//   token for all items (see SameTokenForAllObjects) to keep the traversal
//   fully vectorized.
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

  // Returns values for the present items of `items` with `schema` (aligned with
  // `items`). Called by VectorizedTraverser instead of GetValue.
  //
  // By default, calls `GetValue(items[i], schema)` for each present item.
  virtual absl::StatusOr<DataSliceImpl> GetValues(const DataSliceImpl& items,
                                                  const DataItem& schema);

  // Same as above, but with per-item `schemas` (aligned with `items`, and
  // present wherever `items` is present).
  //
  // By default, delegates to `GetValues(items, *schema)` if all present
  // `schemas` are equal, and calls `GetValue(items[i], schemas[i])` for each
  // present item otherwise.
  virtual absl::StatusOr<DataSliceImpl> GetValues(const DataSliceImpl& items,
                                                  const DataSliceImpl& schemas);

  // Called by Traverser for each reachable item and schema before any calls to
  // Visit* or GetValue methods.
  // `transition_key`: if provided - represents the transition key from
  // `from_item` that led to the current item being visited.
  // For objects Previsit is called twice:
  // - first time with schema::kObject.
  // - second time with the schema written in kSchemaAttr attribute.
  // Returns if the item should be traversed further. If false is returned,
  // this item would not be listed for the Visit* methods.
  //
  // VectorizedTraverser calls PrevisitSlice and PrevisitObjectSchemas instead.
  // By default, traverses all items further.
  virtual absl::StatusOr<bool> Previsit(
      const DataItem& from_item, const DataItem& from_schema,
      const std::optional<TransitionKey>& transition_key, const DataItem& item,
      const DataItem& schema);

  // Called by VectorizedTraverser for each reachable slice before any calls to
  // Visit* or GetValue methods.
  //
  // Previsits the items of `ds`, all reached with `schema` by `transition_key`
  // from parents with the token `parent_token`. As with Previsit, an item is
  // previsited once per edge it is reached by. So the same item may be
  // previsited several times, from different parents or more than once within
  // `ds`. Visitors should return the same token for it each time.
  //
  // Args:
  // - ds: ObjectIds, possibly missing or duplicated. For schema::kSchema, also
  //     DTypes. DTypes and ItemIds (schema::kItemId) are previsited (e.g. to
  //     resolve LIST[INT32]) but never traversed further.
  // - schema: the schema shared by all items of `ds`.
  // - transition_key: nullopt for the root slice and the root schema.
  // - parent_token: the token of the parents of `ds`; missing for the root
  //     slice and the root schema.
  //
  // Returns a slice of tokens aligned with `ds`, where each element is either
  // an ObjectId or missing:
  // - an item with a missing token is not traversed further from this edge,
  //   as when Previsit returns false;
  // - an item with a present token is traversed further, and its token is
  //   passed as `parent_token` when previsiting its children.
  // Tokens of items that are never traversed further (DTypes, ItemIds) are
  // ignored; return missing for them.
  //
  // By default, returns the same token for all ObjectIds of `ds`
  // (SameTokenForAllObjects).
  virtual absl::StatusOr<DataSliceImpl> PrevisitSlice(
      const DataSliceImpl& ds, const DataItem& schema,
      const std::optional<TransitionKey>& transition_key,
      const DataItem& parent_token);

  // Called by VectorizedTraverser for the objects reached with
  // schema::kObject that PrevisitSlice returned present tokens for. Previsits
  // them together with their `__schema__`s, which are only known at this
  // point.
  //
  // Args:
  // - objects: dense ObjectIds.
  // - schemas: the `__schema__` of each object, aligned with `objects`.
  // - token: the token that PrevisitSlice returned for all of `objects`.
  //
  // Returns tokens aligned with `objects`, with the same meaning as in
  // PrevisitSlice. These tokens replace `token`.
  //
  // By default, returns `token` for all objects.
  virtual absl::StatusOr<DataSliceImpl> PrevisitObjectSchemas(
      const DataSliceImpl& objects, const DataSliceImpl& schemas,
      const DataItem& token);

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
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataItem> attr_values) = 0;

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
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataItem> attr_schema) = 0;
};

// Returns the same token (the first present ObjectId of `ds`) for all
// ObjectIds of `ds`, and missing tokens for other items, e.g. DTypes.
//
// A shortcut for PrevisitSlice implementations that traverse all ObjectIds
// further and don't need to tell them apart. The default PrevisitSlice.
absl::StatusOr<DataSliceImpl> SameTokenForAllObjects(const DataSliceImpl& ds);

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_ABSTRACT_VISITOR_H_
