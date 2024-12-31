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
#ifndef KOLADATA_INTERNAL_OP_UTILS_GROUP_BY_UTILS_H_
#define KOLADATA_INTERNAL_OP_UTILS_GROUP_BY_UTILS_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/dense_group_ops.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/aggregation/group_op_accumulators.h"
#include "arolla/qexpr/operators/dense_array/edge_ops.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {


// Utility class for GroupBy-related operations on data slices.
// Provides methods to create an edge from slice or pair of slices, and to
// group or collapse objects by edge.
class ObjectsGroupBy {
 public:
  ObjectsGroupBy()
      : ctx_(),
        collapse_items_op_(&ctx_.buffer_factory()),
        collapse_objects_op_(&ctx_.buffer_factory()),
        agg_index_op_(&ctx_.buffer_factory()) {}

  // Group DataSlice by Schemas.
  absl::StatusOr<std::pair<DataSliceImpl,
                           std::vector<arolla::DenseArray<ObjectId>>>>
  BySchemas(const DataSliceImpl& items_schemas, const DataSliceImpl& ds) {
    ASSIGN_OR_RETURN(auto edge, EdgeFromSchemasSlice(items_schemas));
    ASSIGN_OR_RETURN(auto group_schemas, CollapseByEdge(edge, items_schemas));
    ASSIGN_OR_RETURN(auto ds_grouped,
                     ByEdge(std::move(edge), ds.values<ObjectId>()));
    return std::make_pair(std::move(group_schemas), std::move(ds_grouped));
  }

  // Creates an edge from pairs of schemas.
  absl::StatusOr<arolla::DenseArrayEdge> EdgeFromSchemaPairs(
      const DataSliceImpl& schemas_a, const DataSliceImpl& schemas_b) {
    DCHECK_EQ(schemas_a.size(), schemas_b.size());
    ASSIGN_OR_RETURN(
        auto edge,
        arolla::DenseArrayEdge::FromSplitPoints(
            arolla::CreateDenseArray<int64_t>({0, schemas_a.size()})));
    ASSIGN_OR_RETURN(edge, SplitEdgeBySchemas(std::move(edge), schemas_a));
    return SplitEdgeBySchemas(std::move(edge), schemas_b);
  }

  // Assembles objects into groups defined by the edge.
  absl::StatusOr<std::vector<arolla::DenseArray<ObjectId>>> ByEdge(
      const arolla::DenseArrayEdge& edge,
      const arolla::DenseArray<ObjectId>& objs) {
    ASSIGN_OR_RETURN(auto group_sizes,
                     arolla::DenseArrayEdgeSizesOp()(&ctx_, edge));
    ASSIGN_OR_RETURN(auto indicies, agg_index_op_.Apply(edge, objs.ToMask()));
    std::vector<arolla::DenseArrayBuilder<ObjectId>> objs_grouped_bldr;
    objs_grouped_bldr.reserve(group_sizes.size());
    group_sizes.ForEachPresent([&](int64_t idx, int64_t size) {
      objs_grouped_bldr.emplace_back(size);
    });
    RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
        [&](size_t idx, ObjectId new_obj, int64_t group_id, int64_t index) {
          objs_grouped_bldr[group_id].Set(index - 1, new_obj);
        },
        objs, edge.ToMappingEdge().edge_values(), indicies));
    std::vector<arolla::DenseArray<ObjectId>> objs_grouped;
    objs_grouped.reserve(group_sizes.size());
    for (auto& bldr : objs_grouped_bldr) {
      objs_grouped.push_back(std::move(bldr).Build());
    }
    return objs_grouped;
  }

  // Choose a single item from each group, defined by th edge.
  absl::StatusOr<arolla::DenseArray<ObjectId>> CollapseByEdge(
      const arolla::DenseArrayEdge& edge,
      const arolla::DenseArray<ObjectId>& schemas) {
    return collapse_objects_op_.Apply(edge, schemas);
  }

  // Choose a single item from each group, defined by th edge.
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
    ASSIGN_OR_RETURN(
        auto edge,
        arolla::DenseArrayEdge::FromSplitPoints(
            arolla::CreateDenseArray<int64_t>({0, schemas.size()})));
    return SplitEdgeBySchemas(std::move(edge), schemas);
  }

  // Creates an edge from the given schemas array.
  absl::StatusOr<arolla::DenseArrayEdge> EdgeFromSchemasArray(
      const arolla::DenseArray<ObjectId>& schemas) {
    ASSIGN_OR_RETURN(
        auto edge, arolla::DenseArrayEdge::FromSplitPoints(
                       arolla::CreateDenseArray<int64_t>({0, schemas.size()})));

    return arolla::DenseArrayGroupByOp()(&ctx_, schemas, edge);
  }

  // Returns item if all present values in the slice are equal.
  // Returns missing if slice is empty or has different values.
  std::optional<DataItem> GetItemIfAllEqual(const DataSliceImpl& ds) {
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
      if (all_equal) {
        result_item = DataItem(std::move(*result));
      }
    });
    return result_item;
  }

 private:
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

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_GROUP_BY_UTILS_H_
