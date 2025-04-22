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
#include "koladata/functor/parallel/eager_executor.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "koladata/functor/parallel/executor.h"

namespace koladata::functor::parallel {

namespace {

class EagerExecutorImpl : public Executor {
 public:
  absl::Status Schedule(TaskFn task_fn) override {
    std::move(task_fn)();
    return absl::OkStatus();
  }

  std::string Repr() const override { return "eager_executor"; }
};

}  // namespace

ExecutorPtr GetEagerExecutor() {
  // It is useful to have a singleton eager executor, so that identical exprs
  // using it would have the same fingerprint and therefore potentially avoid
  // double computation.
  static absl::NoDestructor<ExecutorPtr> executor(
      std::make_shared<EagerExecutorImpl>());
  return *executor;
}

}  // namespace koladata::functor::parallel
