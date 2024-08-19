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
#ifndef KOLADATA_OPERATORS_CONVERT_AND_EVAL_H_
#define KOLADATA_OPERATORS_CONVERT_AND_EVAL_H_

#include <cstdint>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata::ops {

// TODO: Accept an operator name instead once
// convert_and_eval_with_shape has been removed.
//
// Evaluates the given operator on the given inputs, using a compilation
// cache.
absl::StatusOr<arolla::TypedValue> EvalExpr(
    const arolla::expr::ExprOperatorPtr& expr_op,
    absl::Span<const arolla::TypedRef> inputs);

// Returns the schema of the data of `x` that is compatible with Arolla.
// * If the schema of `x` is a primitive schema, returns it.
// * If the schema is given by the data (e.g. in the case of `OBJECT`), the
//   type of the data is returned as a primitive schema if possible.
// * If the slice is empty and the type cannot be inferred from its schema,
//   an empty internal::DataItem is returned.
// * Otherwise, an error is returned.
absl::StatusOr<internal::DataItem> GetPrimitiveArollaSchema(const DataSlice& x);

// koda_internal.convert_and_eval_with_shape operator.
//
// Accepts an Arolla ExprOperator as an input that works on core Arolla data
// structures, Converts arguments from DataSlice(s) to core Arolla data
// structures and passes them, along with a common shape of the inputs, to the
// Expr and evaluates it. The provided operator should output a named tuple
// containing "data" and "shape" that is used to create an output DataSlice.
class ConvertAndEvalWithShapeFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// Evaluates the given expression on the given inputs and returns the result.
// The expr_op is expected to be a pointwise operator that should be evaluated
// on the given inputs extracted as Arolla values. The output DataSlice has the
// the common shape and schema of the inputs, or `output_schema` if provided. If
// all inputs are empty-and-unknown, the `expr_op` is not evaluated. In other
// cases the first primitive schema of the present inputs is used to construct
// inputs.
absl::StatusOr<DataSlice> SimplePointwiseEval(
    const arolla::expr::ExprOperatorPtr& expr_op, std::vector<DataSlice> inputs,
    internal::DataItem output_schema = internal::DataItem());

// Evaluates the given expression on the given input and returns the result. The
// expr_op is expected to be an agg-into operator that should be evaluated on
// `x` extracted as an Arolla value and the edge of the last dimension. The
// output DataSlice has the shape of `x` with the last dimension removed and the
// schema of `x`, or `output_schema` if provided. If `x` is empty-and-unknown,
// the `expr_op` is not evaluated.
absl::StatusOr<DataSlice> SimpleAggIntoEval(
    const arolla::expr::ExprOperatorPtr& expr_op, const DataSlice& x,
    internal::DataItem output_schema = internal::DataItem());

// koda_internal.to_arolla_boolean operator.
//
// Attempts to cast the provided DataSlice (only rank=0 is supported) to
// boolean.
absl::StatusOr<bool> ToArollaBoolean(const DataSlice& x);

// koda_internal.to_arolla_int64 operator.
//
// Attempts to cast the provided DataSlice (only rank=0 is supported) to int64.
absl::StatusOr<int64_t> ToArollaInt64(const DataSlice& x);

// koda_internal.to_arolla_float64 operator.
//
// Attempts to cast the provided DataSlice (only rank=0 is supported) to
// float64.
absl::StatusOr<double> ToArollaFloat64(const DataSlice& x);

// koda_internal.to_arolla_dense_array_int64 operator.
//
// Attempts to cast the provided DataSlice to DenseArray<int64>.
absl::StatusOr<arolla::DenseArray<int64_t>> ToArollaDenseArrayInt64(
    const DataSlice& x);

// koda_internal.to_arolla_dense_array_unit operator.
//
// Attempts to cast the provided DataSlice to DenseArray<Unit>.
absl::StatusOr<arolla::DenseArray<arolla::Unit>> ToArollaDenseArrayUnit(
    const DataSlice& x);

// koda_internal.to_arolla_dense_array_unit operator.
//
// Attempts to cast the provided DataSlice to DenseArray<Text>.
absl::StatusOr<arolla::DenseArray<arolla::Text>> ToArollaDenseArrayText(
    const DataSlice& x);

// koda_internal._to_data_slice operator.
//
// Attempts to cast the provided value to DataSlice.
struct ToDataSliceOp {
  // Impl for Scalars.
  template <typename T>
  absl::StatusOr<DataSlice> operator()(T x) const {
    return DataSlice::Create(internal::DataItem{std::move(x)},
                             internal::DataItem{schema::GetDType<T>()});
  }

  // Impl for Optionals.
  template <typename T>
  absl::StatusOr<DataSlice> operator()(arolla::OptionalValue<T> x) const {
    return DataSlice::Create(internal::DataItem{std::move(x)},
                             internal::DataItem{schema::GetDType<T>()});
  }

  // Impl for DenseArrays.
  template <typename T>
  absl::StatusOr<DataSlice> operator()(arolla::DenseArray<T> x) const {
    auto slice_impl = internal::DataSliceImpl::Create(std::move(x));
    auto shape = DataSlice::JaggedShape::FlatFromSize(slice_impl.size());
    return DataSlice::Create(std::move(slice_impl), std::move(shape),
                             internal::DataItem{schema::GetDType<T>()});
  }
};

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_CONVERT_AND_EVAL_H_
