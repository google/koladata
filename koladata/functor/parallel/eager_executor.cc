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
#include "koladata/functor/parallel/eager_executor.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "koladata/functor/parallel/executor.h"

namespace koladata::functor::parallel {

namespace {

class EagerExecutorImpl final : public Executor {
 public:
  void DoSchedule(TaskFn task_fn) noexcept final { std::move(task_fn)(); }

  std::string Repr() const noexcept final { return "eager_executor"; }
};

}  // namespace

const ExecutorPtr absl_nonnull& GetEagerExecutor() noexcept {
  // It is useful to have a singleton eager executor, so that identical exprs
  // using it would have the same fingerprint and therefore potentially avoid
  // double computation.
  static absl::NoDestructor<ExecutorPtr> executor(
      std::make_shared<EagerExecutorImpl>());
  return *executor;
}

}  // namespace koladata::functor::parallel
