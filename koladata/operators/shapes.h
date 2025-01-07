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
#ifndef KOLADATA_OPERATORS_SHAPES_H_
#define KOLADATA_OPERATORS_SHAPES_H_

#include <cstdint>
#include <utility>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"

namespace koladata::ops {

// kde.shapes.get_shape operator.
inline DataSlice::JaggedShape GetShape(const DataSlice& x) {
  return x.GetShape();
}

// kde.shapes._reshape operator.
inline absl::StatusOr<DataSlice> Reshape(const DataSlice& x,
                                         DataSlice::JaggedShape shape) {
  return x.Reshape(std::move(shape));
}

// kde.shapes._expand_to_shape operator.
absl::StatusOr<DataSlice> ExpandToShape(const DataSlice& x,
                                        DataSlice::JaggedShape shape,
                                        int64_t ndim);

// kde.shapes.new operator.
class JaggedShapeCreateOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.shapes._new_with_size operator.
class JaggedShapeCreateWithSizeOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_SHAPES_H_
