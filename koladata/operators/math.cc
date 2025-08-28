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
#include "arolla/qexpr/operators/math/math.h"
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
#include "koladata/operators/unary_op.h"
#include "koladata/operators/utils.h"
#include "koladata/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace {

// Similar to RETURN_IF_ERROR, but doesn't add an extra source location
// to the status.
#define KD_RETURN_AS_IS_IF_ERROR(X)      \
  if (absl::Status st = (X); !st.ok()) { \
    return st;                           \
  }

struct AddableArgs {
  template <class T1, class T2>
  constexpr static bool kIsInvocable =
      NumericArgs::kIsInvocable<T1, T2> ||
      (std::is_same_v<T1, arolla::Text> && std::is_same_v<T2, arolla::Text>) ||
      (std::is_same_v<T1, arolla::Bytes> && std::is_same_v<T2, arolla::Bytes>);

  explicit constexpr AddableArgs(absl::string_view name1,
                                 absl::string_view name2)
      : name1(name1), name2(name2) {}

  absl::Status CheckArgs(const DataSlice& ds1, const DataSlice& ds2) const {
    KD_RETURN_AS_IS_IF_ERROR(ExpectCanBeAdded(name1, ds1));
    KD_RETURN_AS_IS_IF_ERROR(ExpectCanBeAdded(name2, ds2));
    return ExpectHaveCommonPrimitiveSchema({name1, name2}, ds1, ds2);
  }

  const absl::string_view name1, name2;
};

#undef KD_RETURN_AS_IS_IF_ERROR

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

  arolla::Text operator()(const arolla::Text& lhs,
                          const arolla::Text& rhs) const {
    return arolla::Text(absl::StrCat(lhs.view(), rhs.view()));
  }
  arolla::Bytes operator()(const arolla::Bytes& lhs,
                           const arolla::Bytes& rhs) const {
    return absl::StrCat(lhs, rhs);
  }

  // Used in batch mode for both Text and Bytes.
  std::string operator()(absl::string_view lhs, absl::string_view rhs) const {
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

struct ExpOp {
  float operator()(int32_t x) const {
    return arolla::ExpOp{}(static_cast<float>(x));
  }
  float operator()(int64_t x) const {
    return arolla::ExpOp{}(static_cast<float>(x));
  }
  float operator()(float x) const { return arolla::ExpOp{}(x); }
  double operator()(double x) const { return arolla::ExpOp{}(x); }
};

struct LogOp {
  float operator()(int32_t x) const {
    return arolla::LogOp{}(static_cast<float>(x));
  }
  float operator()(int64_t x) const {
    return arolla::LogOp{}(static_cast<float>(x));
  }
  float operator()(float x) const { return arolla::LogOp{}(x); }
  double operator()(double x) const { return arolla::LogOp{}(x); }
};

struct Log10Op {
  float operator()(int32_t x) const {
    return std::log10(static_cast<float>(x));
  }
  float operator()(int64_t x) const {
    return std::log10(static_cast<float>(x));
  }
  float operator()(float x) const { return std::log10(x); }
  double operator()(double x) const { return std::log10(x); }
};

}  // namespace

absl::StatusOr<DataSlice> Add(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<AddOp>(x, y, AddableArgs("x", "y"));
}

absl::StatusOr<DataSlice> Subtract(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::SubtractOp>(x, y, NumericArgs("x", "y"));
}

absl::StatusOr<DataSlice> Multiply(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::MultiplyOp>(x, y, NumericArgs("x", "y"));
}

absl::StatusOr<DataSlice> Divide(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<DivideOp>(x, y, NumericArgs("x", "y"),
                                ReturnFloatForInts);
}

absl::StatusOr<DataSlice> Log(const DataSlice& x) {
  return UnaryOpEval<LogOp>(x, NumericArgs("x"));
}

absl::StatusOr<DataSlice> Log10(const DataSlice& x) {
  return UnaryOpEval<Log10Op>(x, NumericArgs("x"));
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
  return UnaryOpEval<ExpOp>(x, NumericArgs("x"));
}

absl::StatusOr<DataSlice> IsNaN(const DataSlice& x) {
  return UnaryOpEval<arolla::IsNanOp>(x, NumericArgs("x"), schema::kMask);
}

absl::StatusOr<DataSlice> Abs(const DataSlice& x) {
  return UnaryOpEval<arolla::AbsOp>(x, NumericArgs("x"));
}

absl::StatusOr<DataSlice> Neg(const DataSlice& x) {
  return UnaryOpEval<arolla::NegOp>(x, NumericArgs("x"));
}

absl::StatusOr<DataSlice> Sign(const DataSlice& x) {
  return UnaryOpEval<arolla::SignOp>(x, NumericArgs("x"));
}

absl::StatusOr<DataSlice> Pos(const DataSlice& x) {
  return UnaryOpEval<arolla::PosOp>(x, NumericArgs("x"));
}

absl::StatusOr<DataSlice> Ceil(const DataSlice& x) {
  return UnaryOpEval<arolla::CeilOp>(x, NumericArgs("x"));
}

absl::StatusOr<DataSlice> Floor(const DataSlice& x) {
  return UnaryOpEval<arolla::FloorOp>(x, NumericArgs("x"));
}

absl::StatusOr<DataSlice> Round(const DataSlice& x) {
  return UnaryOpEval<arolla::RoundOp>(x, NumericArgs("x"));
}

absl::StatusOr<DataSlice> Pow(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<PowOp>(x, y, NumericArgs("x", "y"), ReturnFloatForInts);
}

absl::StatusOr<DataSlice> FloorDiv(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::FloorDivOp>(x, y, NumericArgs("x", "y"));
}

absl::StatusOr<DataSlice> Mod(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::ModOp>(x, y, NumericArgs("x", "y"));
}

absl::StatusOr<DataSlice> Maximum(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::MaxOp>(x, y, NumericArgs("x", "y"));
}

absl::StatusOr<DataSlice> Minimum(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::MinOp>(x, y, NumericArgs("x", "y"));
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
