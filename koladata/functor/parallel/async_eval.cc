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
#include "koladata/functor/parallel/async_eval.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/cancellation.h"
#include "koladata/expr/expr_eval.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/future.h"
#include "koladata/functor/parallel/future_qtype.h"

namespace koladata::functor::parallel {

namespace {
// This class is used to track the number of remaining tasks and to schedule
// the evaluation of the expression when all tasks are ready.
class AsyncCountdown {
 public:
  AsyncCountdown(int64_t remaining_tasks, ExecutorPtr::weak_type executor,
                 arolla::expr::ExprOperatorPtr op,
                 std::vector<std::optional<arolla::TypedValue>> input_values,
                 FutureWriter result_writer)
      : remaining_tasks_(remaining_tasks),
        executor_(std::move(executor)),
        op_(std::move(op)),
        input_values_(std::move(input_values)),
        result_writer_(std::move(result_writer)) {}

  void SetInput(size_t index, absl::StatusOr<arolla::TypedValue> value) {
    if (!value.ok()) {
      {
        absl::MutexLock lock(&lock_);
        if (error_reported_) {
          return;
        }
        error_reported_ = true;
      }
      std::move(result_writer_).SetValue(value.status());
      return;
    }
    if (IsFutureQType(value->GetType())) {
      // This branch is currently not tested since there's no way to create
      // a future to a future in Python. However, keeping it here for
      // future-proofing.
      LOG(FATAL) << "futures to futures are not supported in async eval";
    }
    bool should_schedule = false;
    {
      absl::MutexLock lock(&lock_);
      if (index < 0 || index >= input_values_.size()) {
        LOG(FATAL) << "index " << index << " is out of bounds [0, "
                   << input_values_.size() << ").";
      }
      if (input_values_[index].has_value()) {
        LOG(FATAL) << "Setting input " << index << " twice.";
      }
      input_values_[index] = std::move(*value);
      --remaining_tasks_;
      if (remaining_tasks_ == 0 && !error_reported_) {
        should_schedule = true;
      }
    }
    // Note that even though we don't hold the lock here, if should_schedule
    // is true, remaining_tasks_ is 0 and therefore SetInput will not be
    // called again, so error_reported_ cannot become true.
    if (should_schedule) {
      Schedule();
    }
  }

  void Schedule() {
    std::vector<std::optional<arolla::TypedValue>> input_values;
    {
      absl::MutexLock lock(&lock_);
      input_values.swap(input_values_);
    }

    auto executor = executor_.lock();
    if (executor == nullptr) {
      std::move(result_writer_)
          .SetValue(
              absl::InvalidArgumentError("the executor was destroyed while the "
                                         "async eval was still pending"));
      return;
    }

    executor->Schedule([op = op_, input_values = std::move(input_values),
                        result_writer = std::move(result_writer_),
                        cancelation_context =
                            std::move(cancellation_context_)]() mutable {
      arolla::CancellationContext::ScopeGuard cancellation_scope(
          std::move(cancelation_context));
      std::vector<arolla::TypedRef> input_refs;
      input_refs.reserve(input_values.size());
      for (const auto& value : input_values) {
        if (!value) {
          std::move(result_writer)
              .SetValue(absl::InternalError(
                  "all inputs are done but one is still not set"));
          return;
        }
        input_refs.push_back(value->AsRef());
      }
      absl::StatusOr<arolla::TypedValue> value =
          expr::EvalOpWithCompilationCache(op, input_refs);
      std::move(result_writer).SetValue(std::move(value));
    });
  }

  // Disable copy and move.
  AsyncCountdown(const AsyncCountdown&) = delete;
  AsyncCountdown(AsyncCountdown&&) = delete;
  AsyncCountdown& operator=(const AsyncCountdown&) = delete;
  AsyncCountdown& operator=(AsyncCountdown&&) = delete;

 private:
  int64_t remaining_tasks_ ABSL_GUARDED_BY(lock_);
  bool error_reported_ ABSL_GUARDED_BY(lock_) = false;
  // This is a weak pointer to avoid the following circular reference:
  // - executor has a list of pending/executing tasks
  // - any task has a shared pointer to the Future it writes to.
  // - the Future has a list of consumers
  // - a consumer has a shared pointer to the AsyncCountdown
  // - the AsyncCountdown has a shared pointer to the executor.
  // Therefore if the executor is destroyed while this task is still pending,
  // we will get an error since we can no longer schedule it.
  ExecutorPtr::weak_type executor_;
  arolla::expr::ExprOperatorPtr op_;
  std::vector<std::optional<arolla::TypedValue>> input_values_
      ABSL_GUARDED_BY(lock_);
  FutureWriter result_writer_;
  absl::Mutex lock_;
  arolla::CancellationContextPtr /*absl_nullable*/ cancellation_context_ =
      arolla::CurrentCancellationContext();
};

// We need reference counting here since multiple futures use the countdown
// which may complete in arbitrary order.
using AsyncCountdownPtr = std::shared_ptr<AsyncCountdown>;

}  // namespace

absl::StatusOr<FuturePtr> AsyncEvalWithCompilationCache(
    const ExecutorPtr& executor, const arolla::expr::ExprOperatorPtr& op,
    absl::Span<const arolla::TypedRef> input_values,
    arolla::QTypePtr result_qtype) {
  auto [result, result_writer] = MakeFuture(result_qtype);
  int64_t input_count = input_values.size();
  int64_t remaining_tasks = 0;
  // It is important for the countdown to not have pointers to the futures
  // that have it registered as a consumer, to avoid circular references.
  // Therefore we replace the futures with nullopt here.
  std::vector<std::optional<arolla::TypedValue>> ready_input_values;
  ready_input_values.reserve(input_count);
  for (const auto& input_value : input_values) {
    if (IsFutureQType(input_value.GetType())) {
      ++remaining_tasks;
      ready_input_values.push_back(std::nullopt);
    } else {
      ready_input_values.push_back(arolla::TypedValue(input_value));
    }
  }
  // How ownership works: suppose future2 depends on future1.
  // - the executor, in its list of pending/executing tasks, stores
  //   the callback computing future1.
  // - the callback computing future1 has a shared pointer to future1.
  // - future1, in its list of consumers, has a shared pointer to countdown.
  // - countdown stores a shared pointer to future2.
  // This guarantees that whenever future1's consumers are invoked, the
  // downstream futures still exist and can consume. At the same time, there
  // are no cycles, so if for example executor is interrupted, the futures
  // will be destroyed automatically via the shared pointers.
  AsyncCountdownPtr countdown = std::make_shared<AsyncCountdown>(
      remaining_tasks, executor, op, std::move(ready_input_values),
      std::move(result_writer));

  if (remaining_tasks == 0) {
    countdown->Schedule();
  } else {
    for (int64_t i = 0; i < input_count; ++i) {
      if (IsFutureQType(input_values[i].GetType())) {
        const FuturePtr& future = input_values[i].UnsafeAs<FuturePtr>();
        future->AddConsumer(
            [countdown, i](absl::StatusOr<arolla::TypedValue> value) {
              countdown->SetInput(i, std::move(value));
            });
      }
    }
  }
  return result;
}

}  // namespace koladata::functor::parallel
