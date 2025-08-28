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
#include "koladata/operators/bitwise.h"

#include "absl/status/statusor.h"
#include "arolla/qexpr/operators/bitwise/bitwise.h"
#include "koladata/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/operators/binary_op.h"
#include "koladata/operators/unary_op.h"
#include "koladata/operators/utils.h"

namespace koladata::ops {

absl::StatusOr<DataSlice> BitwiseAnd(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::BitwiseAndOp>(x, y, IntegerArgs("x", "y"));
}

absl::StatusOr<DataSlice> BitwiseOr(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::BitwiseOrOp>(x, y, IntegerArgs("x", "y"));
}

absl::StatusOr<DataSlice> BitwiseXor(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::BitwiseXorOp>(x, y, IntegerArgs("x", "y"));
}

absl::StatusOr<DataSlice> BitwiseInvert(const DataSlice& x) {
  return UnaryOpEval<arolla::InvertOp>(x, IntegerArgs("x", "y"));
}

absl::StatusOr<DataSlice> BitwiseCount(const DataSlice& x) {
  return UnaryOpEval<arolla::BitwiseCountOp>(x, IntegerArgs("x", "y"),
                                             schema::kInt32);
}

}  // namespace koladata::ops
