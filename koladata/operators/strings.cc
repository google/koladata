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
#include "koladata/operators/strings.h"

#include <optional>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/arolla_utils.h"
#include "koladata/casting.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/operators/arolla_bridge.h"
#include "koladata/shape_utils.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

absl::StatusOr<DataSlice> AggJoin(const DataSlice& x, const DataSlice& sep) {
  if (sep.GetShape().rank() != 0) {
    return absl::InvalidArgumentError("expected rank(sep) == 0");
  }
  return SimpleAggIntoEval("strings.agg_join", {x, sep});
}

absl::StatusOr<DataSlice> Contains(const DataSlice& x,
                                   const DataSlice& substr) {
  return SimplePointwiseEval(
      "strings.contains", {x, substr},
      /*output_schema=*/internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> Count(const DataSlice& x, const DataSlice& substr) {
  return SimplePointwiseEval("strings.count", {x, substr},
                             internal::DataItem(schema::kInt32));
}

absl::StatusOr<DataSlice> Find(const DataSlice& x, const DataSlice& substr,
                               const DataSlice& start, const DataSlice& end,
                               const DataSlice& failure_value) {
  ASSIGN_OR_RETURN(auto typed_start,
                   CastToNarrow(start, internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(auto typed_end,
                   CastToNarrow(end, internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(
      auto typed_failure_value,
      CastToNarrow(failure_value, internal::DataItem(schema::kInt64)));
  return SimplePointwiseEval(
      "strings.find",
      {x, substr, std::move(typed_start), std::move(typed_end),
       std::move(typed_failure_value)},
      /*output_schema=*/internal::DataItem(schema::kInt64),
      /*primary_operand_indices=*/std::vector<int>({0, 1}));
}

absl::StatusOr<DataSlice> Printf(std::vector<DataSlice> slices) {
  if (slices.empty()) {
    return absl::InvalidArgumentError("expected at least one input");
  }
  const auto& fmt = slices[0];
  ASSIGN_OR_RETURN(auto primitive_schema, GetPrimitiveArollaSchema(fmt));
  // If `fmt` is empty, we avoid calling the implementation altogether. Calling
  // SimplePointwiseEval when `fmt` is empty would resolve it to the type of the
  // first present value, which can be of any type.
  if (!primitive_schema.has_value()) {
    ASSIGN_OR_RETURN(auto common_shape, shape::GetCommonShape(slices));
    return BroadcastToShape(fmt, std::move(common_shape));
  }
  // From here on, we know that at least one input has known schema and we
  // should eval.
  return SimplePointwiseEval("strings.printf", std::move(slices),
                             /*output_schema=*/fmt.GetSchemaImpl());
}

absl::StatusOr<DataSlice> Join(std::vector<DataSlice> slices) {
  if (slices.empty()) {
    return absl::InvalidArgumentError("expected at least one input");
  }
  return SimplePointwiseEval("strings.join", std::move(slices));
}

absl::StatusOr<DataSlice> Length(const DataSlice& x) {
  return SimplePointwiseEval("strings.length", {x},
                             internal::DataItem(schema::kInt32));
}

absl::StatusOr<DataSlice> Lower(const DataSlice& x) {
  // TODO: Add support for BYTES.
  return SimplePointwiseEval("strings.lower", {x},
                             internal::DataItem(schema::kText));
}

absl::StatusOr<DataSlice> Rfind(const DataSlice& x, const DataSlice& substr,
                                const DataSlice& start, const DataSlice& end,
                                const DataSlice& failure_value) {
  ASSIGN_OR_RETURN(auto typed_start,
                   CastToNarrow(start, internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(auto typed_end,
                   CastToNarrow(end, internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(
      auto typed_failure_value,
      CastToNarrow(failure_value, internal::DataItem(schema::kInt64)));
  return SimplePointwiseEval(
      "strings.rfind",
      {x, substr, std::move(typed_start), std::move(typed_end),
       std::move(typed_failure_value)},
      /*output_schema=*/internal::DataItem(schema::kInt64),
      /*primary_operand_indices=*/std::vector<int>({0, 1}));
}

absl::StatusOr<DataSlice> Split(const DataSlice& x, const DataSlice& sep) {
  const auto& x_shape = x.GetShape();
  if (sep.GetShape().rank() != 0) {
    return absl::InvalidArgumentError("expected rank(sep) == 0");
  }
  ASSIGN_OR_RETURN(
      auto common_schema,
      schema::CommonSchema(x.GetSchemaImpl(), sep.GetSchemaImpl()));
  ASSIGN_OR_RETURN(auto x_primitive_schema, GetPrimitiveArollaSchema(x));
  ASSIGN_OR_RETURN(auto sep_primitive_schema, GetPrimitiveArollaSchema(sep));
  // If all inputs are empty-and-unknown, the output will be too.
  if (!x_primitive_schema.has_value() && !sep_primitive_schema.has_value()) {
    ASSIGN_OR_RETURN(auto ds, DataSlice::Create(internal::DataItem(),
                                                std::move(common_schema)));
    ASSIGN_OR_RETURN(
        auto out_edge,
        DataSlice::JaggedShape::Edge::FromUniformGroups(x_shape.size(), 0));
    ASSIGN_OR_RETURN(auto out_shape, x_shape.AddDims({std::move(out_edge)}));
    return BroadcastToShape(ds, std::move(out_shape));
  }
  // Otherwise, we should eval. `strings.split` requires a dense array input, so
  // we flatten to avoid scalar inputs.
  std::vector<arolla::TypedValue> typed_value_holder;
  typed_value_holder.reserve(2);
  ASSIGN_OR_RETURN(auto flat_x,
                   x.Reshape(x_shape.FlattenDims(0, x_shape.rank())));
  ASSIGN_OR_RETURN(auto x_ref,
                   DataSliceToOwnedArollaRef(flat_x, typed_value_holder,
                                             sep_primitive_schema));
  ASSIGN_OR_RETURN(
      auto sep_ref,
      DataSliceToOwnedArollaRef(sep, typed_value_holder, x_primitive_schema));
  ASSIGN_OR_RETURN(
      auto result,
      EvalExpr("strings.split", {std::move(x_ref), std::move(sep_ref)}));
  DCHECK(arolla::IsTupleQType(result.GetType()) && result.GetFieldCount() == 2);
  ASSIGN_OR_RETURN(auto edge_ref,
                   result.GetField(1).As<DataSlice::JaggedShape::Edge>());
  ASSIGN_OR_RETURN(auto out_shape, x_shape.AddDims({std::move(edge_ref)}));
  return DataSliceFromArollaValue(result.GetField(0), std::move(out_shape),
                                  std::move(common_schema));
}

absl::StatusOr<DataSlice> Substr(const DataSlice& x, const DataSlice& start,
                                 const DataSlice& end) {
  ASSIGN_OR_RETURN(auto typed_start,
                   CastToNarrow(start, internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(auto typed_end,
                   CastToNarrow(end, internal::DataItem(schema::kInt64)));
  return SimplePointwiseEval("strings.substr",
                             {x, std::move(typed_start), std::move(typed_end)},
                             /*output_schema=*/x.GetSchemaImpl(),
                             /*primary_operand_indices=*/std::vector<int>({0}));
}

absl::StatusOr<DataSlice> Upper(const DataSlice& x) {
  // TODO: Add support for BYTES.
  return SimplePointwiseEval("strings.upper", {x},
                             internal::DataItem(schema::kText));
}

}  // namespace koladata::ops
