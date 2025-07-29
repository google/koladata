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
#ifndef KOLADATA_FUNCTOR_PARALLEL_GET_DEFAULT_EXECUTOR_H_
#define KOLADATA_FUNCTOR_PARALLEL_GET_DEFAULT_EXECUTOR_H_

#include <optional>

#include "absl/base/nullability.h"
#include "koladata/functor/parallel/executor.h"

namespace koladata::functor::parallel {

// Returns the default executor.
const ExecutorPtr absl_nonnull& GetDefaultExecutor() noexcept;

// If the current executor is not set, this helper sets the default executor as
// the current.
class ProvideDefaultCurrentExecutorScopeGuard final {
 public:
  ProvideDefaultCurrentExecutorScopeGuard() noexcept;

 private:
  std::optional<CurrentExecutorScopeGuard> guard_;
};

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_GET_DEFAULT_EXECUTOR_H_
