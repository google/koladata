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
#include "koladata/functor/parallel/derived_executor.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/strings/str_cat.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "koladata/functor/parallel/executor.h"

namespace koladata::functor::parallel {

// The task of setting up the extra ContextGuard is delegated to the base class.
DerivedExecutor::DerivedExecutor(
    absl_nonnull ExecutorPtr base_executor,
    ContextGuardInitializer extra_context_initializer)
    : Executor(std::move(extra_context_initializer)),
      base_executor_(std::move(base_executor)) {}

std::string DerivedExecutor::Repr() const noexcept {
  int derived_depth = 1;

  Executor* executor = base_executor_.get();
  DerivedExecutor* derived_executor;
  while (
      (derived_executor = arolla::fast_dynamic_downcast_final<DerivedExecutor*>(
           executor)) != nullptr) {
    ++derived_depth;
    executor = derived_executor->base_executor_.get();
  }

  // executor points to the first non-derived executor in the chain.
  return absl::StrCat("derived_executor[", executor->Repr(), ", ",
                      derived_depth, "]");
}

void DerivedExecutor::DoSchedule(TaskFn task_fn) noexcept {
  base_executor_->Schedule(std::move(task_fn));
}

}  // namespace koladata::functor::parallel
