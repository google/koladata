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
#ifndef KOLADATA_OPERATORS_CURVES_H_
#define KOLADATA_OPERATORS_CURVES_H_

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"

namespace koladata::ops {

// kd.curves.log_pwl_curve
absl::StatusOr<DataSlice> LogPwlCurve(const DataSlice& x,
                                      const DataSlice& adjustments);

// kd.curves.pwl_curve
absl::StatusOr<DataSlice> PwlCurve(const DataSlice& x,
                                   const DataSlice& adjustments);

// kd.curves.log_p1_pwl_curve
absl::StatusOr<DataSlice> LogP1PwlCurve(const DataSlice& x,
                                        const DataSlice& adjustments);

// kd.curves.symmetric_log_p1_pwl_curve
absl::StatusOr<DataSlice> SymmetricLogP1PwlCurve(const DataSlice& x,
                                                 const DataSlice& adjustments);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_CURVES_H_
