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

#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/operators/arolla_bridge.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace {

absl::StatusOr<DataSlice> AsScalarBool(const DataSlice& x,
                                       absl::string_view attr_name) {
  if (x.GetShape().rank() != 0 || !x.item().holds_value<bool>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected %s to be a scalar boolean value, got %s",
                        attr_name, arolla::Repr(x)));
  }
  return x.WithSchema(internal::DataItem(schema::kBool));
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

absl::StatusOr<DataSlice> CumMax(const DataSlice& x) {
  return SimpleAggOverEval("math.cum_max", {x});
}

absl::StatusOr<DataSlice> CumMin(const DataSlice& x) {
  return SimpleAggOverEval("math.cum_min", {x});
}

absl::StatusOr<DataSlice> CumSum(const DataSlice& x) {
  return SimpleAggOverEval("math.cum_sum", {x});
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
  ASSIGN_OR_RETURN(auto unbiased_bool, AsScalarBool(unbiased, "unbiased"));
  return SimpleAggIntoEval("math.std", {x, std::move(unbiased_bool)},
                           /*output_schema=*/internal::DataItem(),
                           /*edge_arg_index=*/1,
                           /*primary_operand_indices=*/{{0}});
}

absl::StatusOr<DataSlice> AggVar(const DataSlice& x,
                                 const DataSlice& unbiased) {
  ASSIGN_OR_RETURN(auto unbiased_bool, AsScalarBool(unbiased, "unbiased"));
  return SimpleAggIntoEval("math.var", {x, std::move(unbiased_bool)},
                           /*output_schema=*/internal::DataItem(),
                           /*edge_arg_index=*/1,
                           /*primary_operand_indices=*/{{0}});
}

absl::StatusOr<DataSlice> AggMax(const DataSlice& x) {
  return SimpleAggIntoEval("math.max", {x});
}

absl::StatusOr<DataSlice> AggMin(const DataSlice& x) {
  return SimpleAggIntoEval("math.min", {x});
}

}  // namespace koladata::ops
