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

#include "absl/status/statusor.h"
#include "arolla/util/text.h"
#include "koladata/arolla_utils.h"
#include "koladata/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

// kd.assertion.assert_primitive.
inline absl::StatusOr<DataSlice> AssertPrimitive(const DataSlice& arg_name,
                                                 const DataSlice& ds,
                                                 const DataSlice& dtype) {
  RETURN_IF_ERROR(dtype.VerifyIsPrimitiveSchema());
  ASSIGN_OR_RETURN(auto arg_name_text, ToArollaScalar<arolla::Text>(arg_name));
  RETURN_IF_ERROR(ExpectDType(arg_name_text.view(), ds,
                              dtype.item().value<schema::DType>()));
  return ds;
}

// kd.assertion.assert_present_scalar.
inline absl::StatusOr<DataSlice> AssertPresentScalar(const DataSlice& arg_name,
                                                 const DataSlice& ds,
                                                 const DataSlice& dtype) {
  RETURN_IF_ERROR(dtype.VerifyIsPrimitiveSchema());
  ASSIGN_OR_RETURN(auto arg_name_text, ToArollaScalar<arolla::Text>(arg_name));
  RETURN_IF_ERROR(ExpectPresentScalar(arg_name_text.view(), ds,
                                      dtype.item().value<schema::DType>()));
  return ds;
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_ASSERTION_H_
