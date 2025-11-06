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
#ifndef KOLADATA_FUNCTOR_PARALLEL_CREATE_TRANSFORM_CONFIG_H_
#define KOLADATA_FUNCTOR_PARALLEL_CREATE_TRANSFORM_CONFIG_H_

#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/functor/parallel/transform_config.h"

namespace koladata::functor::parallel {

// Creates a ParallelTransformConfig from the given config_src -- a scalar
// DataSlice with a structure corresponding to ParallelTransformConfigProto.
absl::StatusOr<ParallelTransformConfigPtr absl_nonnull>
CreateParallelTransformConfig(DataSlice config_src);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_CREATE_TRANSFORM_CONFIG_H_
