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
#ifndef KOLADATA_OPERATORS_JSON_STREAM_H_
#define KOLADATA_OPERATORS_JSON_STREAM_H_

#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::ops {

// kd.json_stream._salvage_stream
absl::StatusOr<koladata::functor::parallel::StreamPtr> JsonStreamSalvageStream(
    functor::parallel::ExecutorPtr absl_nonnull executor,
    const koladata::functor::parallel::StreamPtr absl_nonnull& input_stream,
    const DataSlice& allow_nan, const DataSlice& ensure_ascii,
    const DataSlice& max_depth);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_JSON_STREAM_H_
