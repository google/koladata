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
#ifndef KOLADATA_OPERATORS_ASSERTION_H_
#define KOLADATA_OPERATORS_ASSERTION_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/dtype.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

// kd.assertion.assert_ds_has_primitives_of.
inline absl::StatusOr<DataSlice> AssertDsHasPrimitivesOf(
    const DataSlice& ds, const DataSlice& dtype, const arolla::Text& message) {
  RETURN_IF_ERROR(dtype.VerifyIsPrimitiveSchema());
  const auto& ds_schema = ds.GetSchemaImpl();
  if (ds_schema != schema::kAny && ds_schema != schema::kObject &&
      ds_schema != dtype.item()) {
    return absl::FailedPreconditionError(message.view());
  }
  if (ds.present_count() > 0 &&
      ds.dtype() != dtype.item().value<schema::DType>().qtype()) {
    return absl::FailedPreconditionError(message.view());
  }
  return ds;
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_ASSERTION_H_
