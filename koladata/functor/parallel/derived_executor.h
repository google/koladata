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
#ifndef KOLADATA_FUNCTOR_PARALLEL_DERIVED_EXECUTOR_H_
#define KOLADATA_FUNCTOR_PARALLEL_DERIVED_EXECUTOR_H_

#include <memory>
#include <string>

#include "absl/base/nullability.h"
#include "koladata/functor/parallel/executor.h"

namespace koladata::functor::parallel {

// An executor that adds an extra ContextGuard to scheduled tasks.
class DerivedExecutor final : public Executor {
 public:
  DerivedExecutor(absl_nonnull ExecutorPtr base_executor,
                  ContextGuardInitializer extra_context_initializer);

  std::string Repr() const noexcept final;

 private:
  void DoSchedule(TaskFn task_fn) noexcept final;

  ExecutorPtr base_executor_;
};

using DerivedExecutorPtr = std::shared_ptr<DerivedExecutor>;

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_DERIVED_EXECUTOR_H_
