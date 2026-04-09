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
#ifndef KOLADATA_FUNCTOR_PARALLEL_TRANSFORM_CONFIG_REGISTRY_H_
#define KOLADATA_FUNCTOR_PARALLEL_TRANSFORM_CONFIG_REGISTRY_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "koladata/functor/parallel/transform_config.h"
#include "koladata/functor/parallel/transform_config.pb.h"

namespace koladata::functor::parallel {

// Returns the parallel transform config complete with all extensions.
absl::StatusOr<ParallelTransformConfigPtr> GetDefaultParallelTransformConfig(
    bool allow_runtime_transforms = false);

// Extends the default parallel transform config with new replacements.
absl::Status ExtendDefaultParallelTransformConfig(absl::string_view text_proto);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_TRANSFORM_CONFIG_REGISTRY_H_
