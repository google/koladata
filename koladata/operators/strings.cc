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

#include <memory>
#include <vector>

#include "absl/status/statusor.h"
#include "koladata/arolla_utils.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/shape_utils.h"
#include "koladata/operators/convert_and_eval.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

absl::StatusOr<DataSlice> Substr(const DataSlice& x, const DataSlice& start,
                                 const DataSlice& end) {
  // If `x` is empty-and-unknown, the output will be too. In all other cases,
  // we evaluate M.strings.substr on the provided inputs.
  ASSIGN_OR_RETURN(auto primitive_schema, GetPrimitiveArollaSchema(x));
  if (!primitive_schema.has_value()) {
    ASSIGN_OR_RETURN(auto common_shape, shape::GetCommonShape({x, start, end}));
    return BroadcastToShape(x, std::move(common_shape));
  }
  ASSIGN_OR_RETURN((auto [aligned_ds, aligned_shape]),
                   shape::AlignNonScalars({x, start, end}));
  std::vector<arolla::TypedValue> typed_value_holder;
  typed_value_holder.reserve(3);
  ASSIGN_OR_RETURN(
      auto x_ref, DataSliceToOwnedArollaRef(aligned_ds[0], typed_value_holder));
  ASSIGN_OR_RETURN(auto start_ref, DataSliceToOwnedArollaRef(
                                       aligned_ds[1], typed_value_holder,
                                       internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(auto end_ref, DataSliceToOwnedArollaRef(
                                     aligned_ds[2], typed_value_holder,
                                     internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(
      auto result,
      EvalExpr(
          std::make_shared<arolla::expr::RegisteredOperator>("strings.substr"),
          {std::move(x_ref), std::move(start_ref), std::move(end_ref)}));
  return DataSliceFromArollaValue(result.AsRef(), std::move(aligned_shape),
                                  x.GetSchemaImpl());
}

}  // namespace koladata::ops
