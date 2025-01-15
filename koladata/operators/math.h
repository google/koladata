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
#ifndef KOLADATA_OPERATORS_MATH_H_
#define KOLADATA_OPERATORS_MATH_H_

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"

namespace koladata::ops {

// kd.math.add.
absl::StatusOr<DataSlice> Add(const DataSlice& x, const DataSlice& y);
// kd.math.subtract.
absl::StatusOr<DataSlice> Subtract(const DataSlice& x, const DataSlice& y);

// kd.math.multiply.
absl::StatusOr<DataSlice> Multiply(const DataSlice& x, const DataSlice& y);

// kd.math.divide.
absl::StatusOr<DataSlice> Divide(const DataSlice& x, const DataSlice& y);

// kd.math.log.
absl::StatusOr<DataSlice> Log(const DataSlice& x);

// kd.math.log10.
absl::StatusOr<DataSlice> Log10(const DataSlice& x);

// kd.math.exp.
absl::StatusOr<DataSlice> Exp(const DataSlice& x);


// kd.math.sigmoid.
absl::StatusOr<DataSlice> Sigmoid(
    const DataSlice& x,
    const DataSlice& half,
    const DataSlice& slope);

// kd.math.abs.
absl::StatusOr<DataSlice> Abs(const DataSlice& x);

// kd.math.neg.
absl::StatusOr<DataSlice> Neg(const DataSlice& x);

// kd.math.sign.
absl::StatusOr<DataSlice> Sign(const DataSlice& x);

// kd.math.pos.
absl::StatusOr<DataSlice> Pos(const DataSlice& x);

// kd.math.ceil.
absl::StatusOr<DataSlice> Ceil(const DataSlice& x);

// kd.math.floor.
absl::StatusOr<DataSlice> Floor(const DataSlice& x);

// kd.math.round.
absl::StatusOr<DataSlice> Round(const DataSlice& x);

// kd.math.pow.
absl::StatusOr<DataSlice> Pow(const DataSlice& x, const DataSlice& y);

// kd.math.floordiv.
absl::StatusOr<DataSlice> FloorDiv(const DataSlice& x, const DataSlice& y);

// kd.math.mod.
absl::StatusOr<DataSlice> Mod(const DataSlice& x, const DataSlice& y);

// kd.math.maximum.
absl::StatusOr<DataSlice> Maximum(const DataSlice& x, const DataSlice& y);

// kd.math.cum_max.
absl::StatusOr<DataSlice> CumMax(const DataSlice& x);

// kd.math.minimum.
absl::StatusOr<DataSlice> Minimum(const DataSlice& x, const DataSlice& y);

// kd.math.cum_min.
absl::StatusOr<DataSlice> CumMin(const DataSlice& x);

// kd.math._agg_sum.
absl::StatusOr<DataSlice> AggSum(const DataSlice& x);

// kd.math._cum_sum.
absl::StatusOr<DataSlice> CumSum(const DataSlice& x);

// kd.math.softmax
absl::StatusOr<DataSlice> Softmax(const DataSlice& x, const DataSlice& beta);

// kd.math._cdf.
absl::StatusOr<DataSlice> Cdf(const DataSlice& x, const DataSlice& weights);

// kd.math._agg_inverse_df.
absl::StatusOr<DataSlice> AggInverseCdf(const DataSlice& x,
                                        const DataSlice& cdf_arg);

// kd.math._agg_mean.
absl::StatusOr<DataSlice> AggMean(const DataSlice& x);

// kd.math._agg_median.
absl::StatusOr<DataSlice> AggMedian(const DataSlice& x);

// kd.math._agg_std.
absl::StatusOr<DataSlice> AggStd(const DataSlice& x, const DataSlice& unbiased);

// kd.math._agg_var.
absl::StatusOr<DataSlice> AggVar(const DataSlice& x, const DataSlice& unbiased);

// kd.math._agg_max.
absl::StatusOr<DataSlice> AggMax(const DataSlice& x);

// kd.math._agg_min.
absl::StatusOr<DataSlice> AggMin(const DataSlice& x);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_MATH_H_
