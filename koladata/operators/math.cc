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
#include "koladata/operators/math.h"

#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/arolla_utils.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/operators/arolla_bridge.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {
// This purpose of this function is to handle unbiased argument. If we would
// pass it to SimpleAggIntoEval("math.std", {x, unbiased}) - then the underlying
// SimpleAggEval would use unbiased to infer types, see SimpleAggEval in
// arolla_bridge.cc. P.ex. x (Int32) and unbiased (Boolean) would result into a
// common type Object, while the actual output type should be float32.
absl::StatusOr<DataSlice> AggStatistical(const DataSlice& x,
                                         const DataSlice& unbiased,
                                         std::string_view op_name) {
  const DataSlice::JaggedShape& input_shape = x.GetShape();
  if (input_shape.rank() == 0) {
    return absl::InvalidArgumentError("expected rank(x) > 0");
  }
  DataSlice::JaggedShape result_shape =
      input_shape.RemoveDims(input_shape.rank() - 1);

  ASSIGN_OR_RETURN(const internal::DataItem primitive_schema,
                   GetPrimitiveArollaSchema(x));
  if (!primitive_schema.has_value()) {
    // All empty-and-unknown inputs. We then skip evaluation and broadcast input
    // to the expected shape and keep its schema.
    ASSIGN_OR_RETURN(const DataSlice ds, DataSlice::Create(internal::DataItem(),
                                                           x.GetSchemaImpl()));
    return BroadcastToShape(ds, std::move(result_shape));
  }

  std::vector<arolla::TypedValue> typed_value_holder;
  typed_value_holder.reserve(2);
  // Will store: x, edge, unbiased.
  std::vector<arolla::TypedRef> typed_refs(
      3, arolla::TypedRef::UnsafeFromRawPointer(arolla::GetNothingQType(),
                                                nullptr));
  ASSIGN_OR_RETURN(typed_refs[0],
                   DataSliceToOwnedArollaRef(x, typed_value_holder));
  const arolla::TypedValue edge_tv =
      arolla::TypedValue::FromValue(input_shape.edges().back());
  typed_refs[1] = edge_tv.AsRef();
  ASSIGN_OR_RETURN(typed_refs[2],
                   DataSliceToOwnedArollaRef(unbiased, typed_value_holder));

  ASSIGN_OR_RETURN(const arolla::TypedValue result,
                   EvalExpr(op_name, typed_refs));
  // Get the common schema from both input and output.
  schema::CommonSchemaAggregator schema_agg;
  schema_agg.Add(x.GetSchemaImpl());
  ASSIGN_OR_RETURN(const arolla::QTypePtr output_qtype,
                   arolla::GetScalarQType(result.GetType()));
  ASSIGN_OR_RETURN(const schema::DType result_dtype,
                   schema::DType::FromQType(output_qtype));

  schema_agg.Add(result_dtype);

  ASSIGN_OR_RETURN(internal::DataItem result_schema,
                   std::move(schema_agg).Get());

  return DataSliceFromArollaValue(result.AsRef(), std::move(result_shape),
                                  std::move(result_schema));
}
}  // namespace

absl::StatusOr<DataSlice> Subtract(const DataSlice& x, const DataSlice& y) {
  return SimplePointwiseEval("math.subtract", {x, y});
}

absl::StatusOr<DataSlice> Multiply(const DataSlice& x, const DataSlice& y) {
  return SimplePointwiseEval("math.multiply", {x, y});
}

absl::StatusOr<DataSlice> Divide(const DataSlice& x, const DataSlice& y) {
  return SimplePointwiseEval("math.divide", {x, y});
}

absl::StatusOr<DataSlice> Log(const DataSlice& x) {
  return SimplePointwiseEval("math.log", {x});
}

absl::StatusOr<DataSlice> Exp(const DataSlice& x) {
  return SimplePointwiseEval("math.exp", {x});
}

absl::StatusOr<DataSlice> Abs(const DataSlice& x) {
  return SimplePointwiseEval("math.abs", {x});
}

absl::StatusOr<DataSlice> Ceil(const DataSlice& x) {
  return SimplePointwiseEval("math.ceil", {x});
}

absl::StatusOr<DataSlice> Floor(const DataSlice& x) {
  return SimplePointwiseEval("math.floor", {x});
}

absl::StatusOr<DataSlice> Round(const DataSlice& x) {
  return SimplePointwiseEval("math.round", {x});
}

absl::StatusOr<DataSlice> Pow(const DataSlice& x, const DataSlice& y) {
  return SimplePointwiseEval("math.pow", {x, y});
}

absl::StatusOr<DataSlice> FloorDiv(const DataSlice& x, const DataSlice& y) {
  return SimplePointwiseEval("math.floordiv", {x, y});
}

absl::StatusOr<DataSlice> Mod(const DataSlice& x, const DataSlice& y) {
  return SimplePointwiseEval("math.mod", {x, y});
}

absl::StatusOr<DataSlice> Maximum(const DataSlice& x, const DataSlice& y) {
  return SimplePointwiseEval("math.maximum", {x, y});
}

absl::StatusOr<DataSlice> Minimum(const DataSlice& x, const DataSlice& y) {
  return SimplePointwiseEval("math.minimum", {x, y});
}

absl::StatusOr<DataSlice> AggSum(const DataSlice& x) {
  ASSIGN_OR_RETURN(auto primitive_schema, GetPrimitiveArollaSchema(x));
  // The input has primitive schema or OBJECT/ANY schema with a single primitive
  // dtype.
  if (primitive_schema.has_value()) {
    return SimpleAggIntoEval("math.sum", {x});
  }
  // If the input is fully empty and unknown, we fix the schema to INT32. We
  // cannot skip evaluation even if the input is empty-and-unknown because the
  // output should be full. INT32 is picked as it's the "lowest" schema
  // according to go/koda-type-promotion.
  ASSIGN_OR_RETURN(auto x_int32,
                   x.WithSchema(internal::DataItem(schema::kInt32)));
  // NONE cannot hold values, so we fix the schema to INT32.
  auto output_schema = x.GetSchemaImpl() == schema::kNone
                           ? internal::DataItem(schema::kInt32)
                           : x.GetSchemaImpl();
  return SimpleAggIntoEval("math.sum", {std::move(x_int32)},
                           /*output_schema=*/output_schema);
}

absl::StatusOr<DataSlice> AggMean(const DataSlice& x) {
  return SimpleAggIntoEval("math.mean", {x});
}

absl::StatusOr<DataSlice> AggMedian(const DataSlice& x) {
  return SimpleAggIntoEval("math.median", {x});
}

absl::StatusOr<DataSlice> AggStd(const DataSlice& x,
                                 const DataSlice& unbiased) {
  return AggStatistical(x, unbiased, "math.std");
}

absl::StatusOr<DataSlice> AggMax(const DataSlice& x) {
  return SimpleAggIntoEval("math.max", {x});
}

absl::StatusOr<DataSlice> AggMin(const DataSlice& x) {
  return SimpleAggIntoEval("math.min", {x});
}

}  // namespace koladata::ops
