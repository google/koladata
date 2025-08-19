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
#include "koladata/operators/math.h"

#include <cmath>
#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/qexpr/operators/math/arithmetic.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "koladata/arolla_utils.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/error.h"
#include "koladata/operators/arolla_bridge.h"
#include "koladata/operators/binary_op.h"
#include "koladata/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace {

template <class T1, class T2>
struct NumericArgs {
  constexpr static bool value =
      std::is_arithmetic_v<T1> && std::is_arithmetic_v<T2> &&
      !std::is_same_v<T1, bool> && !std::is_same_v<T2, bool>;
};

template <class T1, class T2>
struct AddableArgs {
  constexpr static bool value =
      NumericArgs<T1, T2>::value ||
      (std::is_same_v<T1, arolla::Text> && std::is_same_v<T2, arolla::Text>) ||
      (std::is_same_v<T1, arolla::Bytes> && std::is_same_v<T2, arolla::Bytes>);
};

absl::Status ExpectNumericArgs(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return ExpectNumeric("y", y);
}

absl::StatusOr<schema::DType> ReturnFloatForInts(schema::DType t1,
                                                 schema::DType t2) {
  if (t1 == schema::kInt32 || t1 == schema::kInt64) t1 = schema::kFloat32;
  if (t2 == schema::kInt32 || t2 == schema::kInt64) t2 = schema::kFloat32;
  if (t1 == schema::kObject || t2 == schema::kNone) {
    return t1;
  } else {
    return t2;
  }
}

struct AddOp {
  using run_on_missing = std::true_type;

  template <class T>
  T operator()(T lhs, T rhs) const
    requires(std::is_arithmetic_v<T> && !std::is_same_v<T, bool>)
  {
    if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
      // Use unsigned type to mitigate signed integer overflow UB.
      using UT = std::make_unsigned_t<T>;
      return static_cast<T>(
          (static_cast<UT>(lhs) + static_cast<UT>(rhs)));
    } else {
      return lhs + rhs;
    }
  }

  arolla::Text operator()(const arolla::Text& lhs, const arolla::Text& rhs) {
    return arolla::Text(absl::StrCat(lhs.view(), rhs.view()));
  }
  arolla::Bytes operator()(const arolla::Bytes& lhs, const arolla::Bytes& rhs) {
    return absl::StrCat(lhs, rhs);
  }

  // Used in batch mode for both Text and Bytes.
  std::string operator()(absl::string_view lhs, absl::string_view rhs) {
    return absl::StrCat(lhs, rhs);
  }
};

struct DivideOp {
  using run_on_missing = std::true_type;

  float operator()(int32_t lhs, int32_t rhs) const {
    return static_cast<float>(lhs) / static_cast<float>(rhs);
  }
  float operator()(int64_t lhs, int64_t rhs) const {
    return static_cast<float>(lhs) / static_cast<float>(rhs);
  }
  float operator()(float lhs, float rhs) const { return lhs / rhs; }
  double operator()(double lhs, double rhs) const { return lhs / rhs; }
};

struct PowOp {
  float operator()(int32_t a, int32_t b) const { return std::pow(a, b); }
  float operator()(int64_t a, int64_t b) const { return std::pow(a, b); }
  float operator()(float a, float b) const { return std::pow(a, b); }
  double operator()(double a, double b) const { return std::pow(a, b); }
};

}  // namespace

absl::StatusOr<DataSlice> Add(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<AddOp, AddableArgs>(
      x, y, [](auto& x, auto& y) -> absl::Status {
        RETURN_IF_ERROR(ExpectCanBeAdded("x", x));
        RETURN_IF_ERROR(ExpectCanBeAdded("y", y));
        return ExpectHaveCommonPrimitiveSchema({"x", "y"}, x, y);
      });
}

absl::StatusOr<DataSlice> Subtract(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::SubtractOp, NumericArgs>(x, y, ExpectNumericArgs);
}

absl::StatusOr<DataSlice> Multiply(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::MultiplyOp, NumericArgs>(x, y, ExpectNumericArgs);
}

absl::StatusOr<DataSlice> Divide(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<DivideOp>(x, y, ExpectNumericArgs, ReturnFloatForInts);
}

absl::StatusOr<DataSlice> Log(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimplePointwiseEval("math.log", {x});
}

absl::StatusOr<DataSlice> Log10(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimplePointwiseEval("math.log10", {x});
}

absl::StatusOr<DataSlice> Sigmoid(
    const DataSlice& x,
    const DataSlice& half,
    const DataSlice& slope) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  RETURN_IF_ERROR(ExpectNumeric("half", half));
  RETURN_IF_ERROR(ExpectNumeric("slope", slope));
  return SimplePointwiseEval("math.sigmoid", {x, half, slope});
}

absl::StatusOr<DataSlice> Exp(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimplePointwiseEval("math.exp", {x});
}

absl::StatusOr<DataSlice> IsNaN(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimplePointwiseEval(
      "math.is_nan", {x}, /*output_schema=*/internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> Abs(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimplePointwiseEval("math.abs", {x});
}

absl::StatusOr<DataSlice> Neg(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimplePointwiseEval("math.neg", {x});
}

absl::StatusOr<DataSlice> Sign(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimplePointwiseEval("math.sign", {x});
}

absl::StatusOr<DataSlice> Pos(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimplePointwiseEval("math.pos", {x});
}

absl::StatusOr<DataSlice> Ceil(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimplePointwiseEval("math.ceil", {x});
}

absl::StatusOr<DataSlice> Floor(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimplePointwiseEval("math.floor", {x});
}

absl::StatusOr<DataSlice> Round(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimplePointwiseEval("math.round", {x});
}

absl::StatusOr<DataSlice> Pow(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<PowOp>(x, y, ExpectNumericArgs, ReturnFloatForInts);
}

absl::StatusOr<DataSlice> FloorDiv(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::FloorDivOp, NumericArgs>(x, y, ExpectNumericArgs);
}

absl::StatusOr<DataSlice> Mod(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::ModOp, NumericArgs>(x, y, ExpectNumericArgs);
}

absl::StatusOr<DataSlice> Maximum(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::MaxOp, NumericArgs>(x, y, ExpectNumericArgs);
}

absl::StatusOr<DataSlice> Minimum(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::MinOp, NumericArgs>(x, y, ExpectNumericArgs);
}

absl::StatusOr<DataSlice> CumMax(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimpleAggOverEval("math.cum_max", {x});
}

absl::StatusOr<DataSlice> CumMin(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimpleAggOverEval("math.cum_min", {x});
}

absl::StatusOr<DataSlice> CumSum(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimpleAggOverEval("math.cum_sum", {x});
}

absl::StatusOr<DataSlice> Softmax(const DataSlice& x, const DataSlice& beta) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  RETURN_IF_ERROR(ExpectNumeric("beta", beta));
  return SimpleAggOverEval("math.softmax", {x, beta},
                           /*output_schema=*/internal::DataItem(),
                           /*edge_arg_index=*/2);
}

absl::StatusOr<DataSlice> Cdf(const DataSlice& x, const DataSlice& weights) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  RETURN_IF_ERROR(ExpectNumeric("weights", weights));
  return SimpleAggOverEval("math.cdf", {x, weights},
                           /*output_schema=*/internal::DataItem(),
                           /*edge_arg_index=*/2);
}

absl::StatusOr<DataSlice> AggInverseCdf(const DataSlice& x,
                                        const DataSlice& cdf_arg) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  if (!ToArollaScalar<double>(cdf_arg).ok()) {
    return absl::InvalidArgumentError(
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
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  ASSIGN_OR_RETURN(auto primitive_schema, GetPrimitiveArollaSchema(x),
                   internal::OperatorEvalError(std::move(_), "kd.math.agg_sum",
                                               "invalid inputs"));
  // The input has primitive schema or OBJECT schema with a single primitive
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
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimpleAggIntoEval("math.mean", {x});
}

absl::StatusOr<DataSlice> AggMedian(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimpleAggIntoEval("math.median", {x});
}

absl::StatusOr<DataSlice> AggStd(const DataSlice& x,
                                 const DataSlice& unbiased) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  RETURN_IF_ERROR(ExpectPresentScalar("unbiased", unbiased, schema::kBool));
  return SimpleAggIntoEval("math.std", {x, unbiased},
                           /*output_schema=*/internal::DataItem(),
                           /*edge_arg_index=*/1,
                           /*primary_operand_indices=*/{{0}});
}

absl::StatusOr<DataSlice> AggVar(const DataSlice& x,
                                 const DataSlice& unbiased) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  RETURN_IF_ERROR(ExpectPresentScalar("unbiased", unbiased, schema::kBool));
  return SimpleAggIntoEval("math.var", {x, unbiased},
                           /*output_schema=*/internal::DataItem(),
                           /*edge_arg_index=*/1,
                           /*primary_operand_indices=*/{{0}});
}

absl::StatusOr<DataSlice> AggMax(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimpleAggIntoEval("math.max", {x});
}

absl::StatusOr<DataSlice> Argmax(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimpleAggIntoEval(
      "math.argmax", {x},
      /*output_schema=*/internal::DataItem(schema::kInt64));
}

absl::StatusOr<DataSlice> AggMin(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimpleAggIntoEval("math.min", {x});
}

absl::StatusOr<DataSlice> Argmin(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectNumeric("x", x));
  return SimpleAggIntoEval(
      "math.argmin", {x},
      /*output_schema=*/internal::DataItem(schema::kInt64));
}

}  // namespace koladata::ops
