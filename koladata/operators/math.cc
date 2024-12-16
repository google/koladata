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

#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/arolla_utils.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/utils.h"
#include "koladata/operators/arolla_bridge.h"
#include "koladata/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

constexpr auto OpError = ::koladata::internal::ToOperatorEvalError;

absl::StatusOr<DataSlice> Subtract(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.subtract"));
  RETURN_IF_ERROR(ExpectNumeric("y", y)).With(OpError("kde.math.subtract"));
  return SimplePointwiseEval("math.subtract", {x, y});
}

absl::StatusOr<DataSlice> Multiply(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.multiply"));
  RETURN_IF_ERROR(ExpectNumeric("y", y)).With(OpError("kde.math.multiply"));
  return SimplePointwiseEval("math.multiply", {x, y});
}

absl::StatusOr<DataSlice> Divide(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.divide"));
  RETURN_IF_ERROR(ExpectNumeric("y", y)).With(OpError("kde.math.divide"));
  return SimplePointwiseEval("math.divide", {x, y});
}

absl::StatusOr<DataSlice> Log(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.log"));
  return SimplePointwiseEval("math.log", {x});
}

absl::StatusOr<DataSlice> Log10(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.log10"));
  return SimplePointwiseEval("math.log10", {x});
}

absl::StatusOr<DataSlice> Sigmoid(
    const DataSlice& x,
    const DataSlice& half,
    const DataSlice& slope) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.sigmoid"));
  RETURN_IF_ERROR(ExpectNumeric("half", half))
      .With(OpError("kde.math.sigmoid"));
  RETURN_IF_ERROR(ExpectNumeric("slope", slope))
      .With(OpError("kde.math.sigmoid"));
  return SimplePointwiseEval("math.sigmoid", {x, half, slope});
}

absl::StatusOr<DataSlice> Exp(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.exp"));
  return SimplePointwiseEval("math.exp", {x});
}

absl::StatusOr<DataSlice> Abs(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.abs"));
  return SimplePointwiseEval("math.abs", {x});
}

absl::StatusOr<DataSlice> Neg(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.neg"));
  return SimplePointwiseEval("math.neg", {x});
}

absl::StatusOr<DataSlice> Sign(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.sign"));
  return SimplePointwiseEval("math.sign", {x});
}

absl::StatusOr<DataSlice> Pos(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.pos"));
  return SimplePointwiseEval("math.pos", {x});
}

absl::StatusOr<DataSlice> Ceil(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.ceil"));
  return SimplePointwiseEval("math.ceil", {x});
}

absl::StatusOr<DataSlice> Floor(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.floor"));
  return SimplePointwiseEval("math.floor", {x});
}

absl::StatusOr<DataSlice> Round(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.round"));
  return SimplePointwiseEval("math.round", {x});
}

absl::StatusOr<DataSlice> Pow(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.pow"));
  RETURN_IF_ERROR(ExpectNumeric("y", y)).With(OpError("kde.math.pow"));
  return SimplePointwiseEval("math.pow", {x, y});
}

absl::StatusOr<DataSlice> FloorDiv(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.floordiv"));
  RETURN_IF_ERROR(ExpectNumeric("y", y)).With(OpError("kde.math.floordiv"));
  return SimplePointwiseEval("math.floordiv", {x, y});
}

absl::StatusOr<DataSlice> Mod(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.mod"));
  RETURN_IF_ERROR(ExpectNumeric("y", y)).With(OpError("kde.math.mod"));
  return SimplePointwiseEval("math.mod", {x, y});
}

absl::StatusOr<DataSlice> Maximum(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.maximum"));
  RETURN_IF_ERROR(ExpectNumeric("y", y)).With(OpError("kde.math.maximum"));
  return SimplePointwiseEval("math.maximum", {x, y});
}

absl::StatusOr<DataSlice> Minimum(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.minimum"));
  RETURN_IF_ERROR(ExpectNumeric("y", y)).With(OpError("kde.math.minimum"));
  return SimplePointwiseEval("math.minimum", {x, y});
}

absl::StatusOr<DataSlice> CumMax(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.cum_max"));
  return SimpleAggOverEval("math.cum_max", {x});
}

absl::StatusOr<DataSlice> CumMin(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.cum_min"));
  return SimpleAggOverEval("math.cum_min", {x});
}

absl::StatusOr<DataSlice> CumSum(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.cum_sum"));
  return SimpleAggOverEval("math.cum_sum", {x});
}

absl::StatusOr<DataSlice> Softmax(const DataSlice& x, const DataSlice& beta) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.softmax"));
  RETURN_IF_ERROR(ExpectNumeric("beta", beta))
      .With(OpError("kde.math.softmax"));
  return SimpleAggOverEval("math.softmax", {x, beta},
                           /*output_schema=*/internal::DataItem(),
                           /*edge_arg_index=*/2);
}

absl::StatusOr<DataSlice> Cdf(const DataSlice& x, const DataSlice& weights) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.floordiv"));
  RETURN_IF_ERROR(ExpectNumeric("weights", weights))
      .With(OpError("kde.math.cdf"));
  return SimpleAggOverEval("math.cdf", {x, weights},
                           /*output_schema=*/internal::DataItem(),
                           /*edge_arg_index=*/2);
}

absl::StatusOr<DataSlice> AggInverseCdf(const DataSlice& x,
                                        const DataSlice& cdf_arg) {
  RETURN_IF_ERROR(ExpectNumeric("x", x))
      .With(OpError("kde.math.agg_inverse_cdf"));
  if (!ToArollaScalar<double>(cdf_arg).ok()) {
    return internal::OperatorEvalError(
        "kde.math.agg_inverse_cdf",
        absl::StrFormat("expected `cdf_arg` argument to contain a scalar float "
                        "value, got %s",
                        DataSliceRepr(cdf_arg)));
  }

  return SimpleAggIntoEval("math.inverse_cdf", {x, cdf_arg},
                           /*output_schema=*/internal::DataItem(),
                           /*edge_arg_index=*/2,
                           /*primary_operand_indices=*/{{0}});
}

absl::StatusOr<DataSlice> AggSum(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.agg_sum"));
  ASSIGN_OR_RETURN(auto primitive_schema, GetPrimitiveArollaSchema(x),
                   internal::OperatorEvalError(std::move(_), "kde.math.agg_sum",
                                               "invalid inputs"));
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
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.agg_mean"));
  return SimpleAggIntoEval("math.mean", {x});
}

absl::StatusOr<DataSlice> AggMedian(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.agg_median"));
  return SimpleAggIntoEval("math.median", {x});
}

absl::StatusOr<DataSlice> AggStd(const DataSlice& x,
                                 const DataSlice& unbiased) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kd.math.agg_std"));
  RETURN_IF_ERROR(ExpectPresentScalar("unbiased", unbiased, schema::kBool))
      .With(OpError("kd.math.agg_std"));
  return SimpleAggIntoEval("math.std", {x, unbiased},
                           /*output_schema=*/internal::DataItem(),
                           /*edge_arg_index=*/1,
                           /*primary_operand_indices=*/{{0}});
}

absl::StatusOr<DataSlice> AggVar(const DataSlice& x,
                                 const DataSlice& unbiased) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kd.math.agg_var"));
  RETURN_IF_ERROR(ExpectPresentScalar("unbiased", unbiased, schema::kBool))
      .With(OpError("kd.math.agg_var"));
  return SimpleAggIntoEval("math.var", {x, unbiased},
                           /*output_schema=*/internal::DataItem(),
                           /*edge_arg_index=*/1,
                           /*primary_operand_indices=*/{{0}});
}

absl::StatusOr<DataSlice> AggMax(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.agg_max"));
  return SimpleAggIntoEval("math.max", {x});
}

absl::StatusOr<DataSlice> AggMin(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x)).With(OpError("kde.math.agg_min"));
  return SimpleAggIntoEval("math.min", {x});
}

}  // namespace koladata::ops
