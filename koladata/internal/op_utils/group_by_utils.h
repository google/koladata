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
#ifndef KOLADATA_INTERNAL_OP_UTILS_GROUP_BY_UTILS_H_
#define KOLADATA_INTERNAL_OP_UTILS_GROUP_BY_UTILS_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/dense_group_ops.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/aggregation/group_op_accumulators.h"
#include "arolla/qexpr/operators/dense_array/edge_ops.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/view_types.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {


// Utility class for GroupBy-related operations on data slices.
// Provides methods to create an edge from a slice, and to group or collapse
// objects by edge.
class ObjectsGroupBy {
 public:
  // See ByKeys().
  template <size_t kKeys, size_t kSlices>
  class Grouping;

  ObjectsGroupBy()
      : ctx_(),
        collapse_items_op_(&ctx_.buffer_factory()),
        collapse_objects_op_(&ctx_.buffer_factory()),
        agg_index_op_(&ctx_.buffer_factory()) {}

  // Splits slices of ObjectIds into groups of items with equal `keys` and calls
  // a function for each group:
  //
  //   RETURN_IF_ERROR(group_by.ByKeys(keys_schemas, values_schemas)
  //                       .Split(dicts)
  //                       .ForEach([&](const DataItem& keys_schema,
  //                                    const DataItem& values_schema,
  //                                    DataSliceImpl dicts) {
  //                         return ...;
  //                       }));
  //
  // - `keys` (any values) and the split slices must have the same size.
  // - Items missing in any of them are dropped, so the groups are dense.
  // - Groups are visited in the order of the first occurrence of their keys.
  // - If no split is needed (all items are present and have equal keys), the
  //   function gets the slices as is. Otherwise, allocation ids are recomputed
  //   for each group.
  // - The function may use this ObjectsGroupBy.
  template <class... Keys>
  Grouping<sizeof...(Keys), 0> ByKeys(Keys... keys);

  // Assembles objects into groups defined by the edge.
  absl::StatusOr<std::vector<arolla::DenseArray<ObjectId>>> ByEdge(
      const arolla::DenseArrayEdge& edge,
      const arolla::DenseArray<ObjectId>& objs) {
    ASSIGN_OR_RETURN(auto group_sizes,
                     arolla::DenseArrayEdgeSizesOp()(&ctx_, edge));
    ASSIGN_OR_RETURN(auto indices, agg_index_op_.Apply(edge, objs.ToMask()));
    std::vector<arolla::DenseArrayBuilder<ObjectId>> objs_grouped_bldr;
    objs_grouped_bldr.reserve(group_sizes.size());
    group_sizes.ForEachPresent([&](int64_t idx, int64_t size) {
      objs_grouped_bldr.emplace_back(size);
    });
    RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
        [&](size_t idx, ObjectId new_obj, int64_t group_id, int64_t index) {
          objs_grouped_bldr[group_id].Set(index - 1, new_obj);
        },
        objs, edge.ToMappingEdge().edge_values(), indices));
    std::vector<arolla::DenseArray<ObjectId>> objs_grouped;
    objs_grouped.reserve(group_sizes.size());
    for (auto& bldr : objs_grouped_bldr) {
      objs_grouped.push_back(std::move(bldr).Build());
    }
    return objs_grouped;
  }

  // Choose a single item from each group, defined by the edge.
  absl::StatusOr<arolla::DenseArray<ObjectId>> CollapseByEdge(
      const arolla::DenseArrayEdge& edge,
      const arolla::DenseArray<ObjectId>& schemas) {
    return collapse_objects_op_.Apply(edge, schemas);
  }

  // Choose a single item from each group, defined by the edge.
  absl::StatusOr<DataSliceImpl> CollapseByEdge(
      const arolla::DenseArrayEdge& edge, const DataSliceImpl& schemas) {
    if (schemas.dtype() == arolla::GetQType<ObjectId>()) {
      ASSIGN_OR_RETURN(auto result,
                       CollapseByEdge(edge, schemas.values<ObjectId>()));
      return DataSliceImpl::CreateWithAllocIds(schemas.allocation_ids(),
                                               std::move(result));
    }
    ASSIGN_OR_RETURN(auto result, collapse_items_op_.Apply(
                                      edge, schemas.AsDataItemDenseArray()));
    return DataSliceImpl::Create(result);
  }

  // Creates an edge from the given schemas slice.
  absl::StatusOr<arolla::DenseArrayEdge> EdgeFromSchemasSlice(
      const DataSliceImpl& schemas) {
    if (schemas.dtype() == arolla::GetQType<ObjectId>()) {
      return EdgeFromSchemasArray(schemas.values<ObjectId>());
    }
    ASSIGN_OR_RETURN(auto edge,
                     arolla::DenseArrayEdge::FromSplitPoints(
                         arolla::CreateDenseArray<int64_t>(
                             {0, static_cast<int64_t>(schemas.size())})));
    return SplitEdgeBySchemas(std::move(edge), schemas);
  }

  // Creates an edge from the given schemas array.
  absl::StatusOr<arolla::DenseArrayEdge> EdgeFromSchemasArray(
      const arolla::DenseArray<ObjectId>& schemas) {
    ASSIGN_OR_RETURN(auto edge,
                     arolla::DenseArrayEdge::FromSplitPoints(
                         arolla::CreateDenseArray<int64_t>(
                             {0, static_cast<int64_t>(schemas.size())})));

    return arolla::DenseArrayGroupByOp()(&ctx_, schemas, edge);
  }

  // Returns item if all present values in the slice are equal.
  // Returns missing if slice is empty or has different values.
  static std::optional<DataItem> GetItemIfAllEqual(const DataSliceImpl& ds) {
    if (!ds.is_single_dtype()) {
      return std::nullopt;
    }
    std::optional<DataItem> result_item;
    ds.VisitValues([&]<class T>(const arolla::DenseArray<T>& array) {
      bool all_equal = true;
      std::optional<T> result = std::nullopt;
      array.ForEachPresent([&](size_t idx, arolla::view_type_t<T> attr_schema) {
        // TODO: allow early return if all_equal is false.
        if (result.has_value()) {
          all_equal &= *result == attr_schema;
        } else {
          result = std::move(attr_schema);
        }
      });
      if (all_equal && result.has_value()) {
        result_item = DataItem(std::move(*result));
      }
    });
    return result_item;
  }

  // Returns keys that group `schemas` (ObjectIds, some present) by big
  // allocation: the schema itself if it belongs to a small allocation, and the
  // first ObjectId of its allocation otherwise.
  // NoFollow schemas get keys that differ from the keys of the original
  // schemas.
  // Returns `schemas` as is if they are all equal or all from small
  // allocations.
  // Note: Unlike big allocations, which share an attribute-name dict at offset
  // 0 and store schema attributes as regular attributes across the allocation,
  // each schema in a small allocation stores its attributes in its own dict, so
  // distinct small-allocation schemas cannot be processed together.
  static DataSliceImpl SchemaAllocationKeys(const DataSliceImpl& schemas);

 private:
  using GroupFn = absl::FunctionRef<absl::Status(
      absl::Span<const DataItem> keys, absl::Span<DataSliceImpl> slices)>;

  // Implements ByKeys(keys...).Split(slices...).ForEach(fn).
  absl::Status ForEachGroup(absl::Span<const DataSliceImpl> keys,
                            absl::Span<const DataSliceImpl> slices, GroupFn fn);

  absl::StatusOr<arolla::DenseArrayEdge> SplitEdgeBySchemas(
      const arolla::DenseArrayEdge& edge, const DataSliceImpl& schemas) {
    if (schemas.dtype() == arolla::GetQType<ObjectId>()) {
      return arolla::DenseArrayGroupByOp()(&ctx_, schemas.values<ObjectId>(),
                                           edge);
    } else if (schemas.dtype() == arolla::GetQType<schema::DType>()) {
      return arolla::DenseArrayGroupByOp()(
          &ctx_, schemas.values<schema::DType>(), edge);
    }
    return arolla::DenseArrayGroupByOp()(&ctx_, schemas.AsDataItemDenseArray(),
                                         edge);
  }

  arolla::EvaluationContext ctx_;
  arolla::DenseGroupOps<arolla::CollapseAccumulator<DataItem>>
      collapse_items_op_;
  arolla::DenseGroupOps<arolla::CollapseAccumulator<ObjectId>>
      collapse_objects_op_;
  arolla::DenseGroupOps<arolla::CountPartialAccumulator> agg_index_op_;
};

// Keys and slices to split into groups, see ObjectsGroupBy::ByKeys().
template <size_t kKeys, size_t kSlices>
class ObjectsGroupBy::Grouping {
 public:
  // Sets slices of ObjectIds to split into groups.
  template <class... Slices>
  Grouping<kKeys, sizeof...(Slices)> Split(Slices... slices) && {
    static_assert(kSlices == 0, "Split() can be called only once");
    static_assert(sizeof...(Slices) > 0);
    static_assert((std::is_same_v<Slices, DataSliceImpl> && ...));
    return Grouping<kKeys, sizeof...(Slices)>(group_by_, std::move(keys_),
                                              {std::move(slices)...});
  }

  // Calls `fn(const DataItem& key..., DataSliceImpl slice...) -> absl::Status`
  // for each group. Stops at the first error.
  template <class Fn>
  absl::Status ForEach(Fn&& fn) && {
    static_assert(kSlices > 0, "Split() must be called before ForEach()");
    return group_by_.ForEachGroup(
        absl::MakeConstSpan(keys_), absl::MakeConstSpan(slices_),
        [&fn](absl::Span<const DataItem> keys,
              absl::Span<DataSliceImpl> slices) {
          return Call(fn, keys, slices, std::make_index_sequence<kKeys>(),
                      std::make_index_sequence<kSlices>());
        });
  }

 private:
  friend class ObjectsGroupBy;
  template <size_t, size_t>
  friend class Grouping;

  Grouping(ObjectsGroupBy& group_by, std::array<DataSliceImpl, kKeys> keys,
           std::array<DataSliceImpl, kSlices> slices)
      : group_by_(group_by),
        keys_(std::move(keys)),
        slices_(std::move(slices)) {}

  template <class Fn, size_t... kKeyIdx, size_t... kSliceIdx>
  static absl::Status Call(Fn& fn, absl::Span<const DataItem> keys,
                           absl::Span<DataSliceImpl> slices,
                           std::index_sequence<kKeyIdx...>,
                           std::index_sequence<kSliceIdx...>) {
    return fn(keys[kKeyIdx]..., std::move(slices[kSliceIdx])...);
  }

  ObjectsGroupBy& group_by_;
  std::array<DataSliceImpl, kKeys> keys_;
  std::array<DataSliceImpl, kSlices> slices_;
};

template <class... Keys>
ObjectsGroupBy::Grouping<sizeof...(Keys), 0> ObjectsGroupBy::ByKeys(
    Keys... keys) {
  static_assert(sizeof...(Keys) > 0);
  static_assert((std::is_same_v<Keys, DataSliceImpl> && ...));
  return Grouping<sizeof...(Keys), 0>(*this, {std::move(keys)...}, {});
}

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_GROUP_BY_UTILS_H_
