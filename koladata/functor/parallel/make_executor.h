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
#ifndef KOLADATA_FUNCTOR_PARALLEL_MAKE_EXECUTOR_H_
#define KOLADATA_FUNCTOR_PARALLEL_MAKE_EXECUTOR_H_

#include <cstddef>

#include "absl/base/nullability.h"
#include "koladata/functor/parallel/executor.h"

namespace koladata::functor::parallel {

// Returns a new executor with a limited number of threads.
//
// If `thread_limit` is 0, number of threads is selected automatically.
//
// Note: The `thread_limit` limits the concurrency; however, the executor may
// have no dedicated threads, and the actual concurrency limit might be lower.
ExecutorPtr /*absl_nonnull*/ MakeExecutor(size_t thread_limit);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_MAKE_EXECUTOR_H_
