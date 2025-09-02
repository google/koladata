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
#include "koladata/functor/parallel/executor.h"

#include <utility>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace koladata::functor::parallel {

thread_local constinit CurrentExecutorScopeGuard::ThreadLocalData
    CurrentExecutorScopeGuard::thread_local_data_;

bool IsExecutorTask() {
  return CurrentExecutorScopeGuard::thread_local_data_.is_executor_task;
}

absl::StatusOr<ExecutorPtr absl_nonnull> CurrentExecutor() noexcept {
  if (auto* result = CurrentExecutorScopeGuard::current_executor()) {
    return result->shared_from_this();
  }
  return absl::InvalidArgumentError("current executor is not set");
}

CurrentExecutorScopeGuard::CurrentExecutorScopeGuard(
    ExecutorPtr absl_nullable executor) noexcept
    : executor_(std::move(executor)),
      previous_executor_(std::exchange(thread_local_data_.current_executor,
                                       executor_.get())) {}

CurrentExecutorScopeGuard::~CurrentExecutorScopeGuard() {
  auto& data = thread_local_data_;
  CHECK_EQ(data.current_executor, executor_.get())
      << "CurrentExecutorScopeGuard: Scope nesting invariant "
         "violated!";
  data.current_executor = previous_executor_;
}

Executor* absl_nullable CurrentExecutorScopeGuard::current_executor() noexcept {
  return thread_local_data_.current_executor;
}

}  // namespace koladata::functor::parallel

namespace arolla {

void FingerprintHasherTraits<koladata::functor::parallel::ExecutorPtr>::
operator()(FingerprintHasher* hasher,
           const koladata::functor::parallel::ExecutorPtr& value) const {
  if (value != nullptr) {
    hasher->Combine(value->uuid());
  }
}

ReprToken ReprTraits<koladata::functor::parallel::ExecutorPtr>::operator()(
    const koladata::functor::parallel::ExecutorPtr& value) const {
  if (value == nullptr) {
    return ReprToken{"executor{nullptr}"};
  }
  return ReprToken{value->Repr()};
}

AROLLA_DEFINE_SIMPLE_QTYPE(EXECUTOR, koladata::functor::parallel::ExecutorPtr);

}  // namespace arolla
