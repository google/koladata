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
#ifndef KOLADATA_OPERATORS_SCHEMA_H_
#define KOLADATA_OPERATORS_SCHEMA_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

// kde.core.get_primitive_schema.
inline absl::StatusOr<DataSlice> GetPrimitiveSchema(const DataSlice& ds) {
  if (!schema::DType::VerifyQTypeSupported(ds.dtype())) {
    return absl::FailedPreconditionError(
        "DataSlice does not have any items or has non-primitive items or has "
        "items of mixed primitive dtypes.");
  }
  ASSIGN_OR_RETURN(auto dtype, schema::DType::FromQType(ds.dtype()));
  return DataSlice::Create(internal::DataItem(dtype),
                           internal::DataItem(schema::kSchema));
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_SCHEMA_H_
