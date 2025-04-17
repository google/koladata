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

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "koladata/expr/expr_eval.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/future.h"
#include "koladata/functor/parallel/future_qtype.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {

namespace {
// This class is used to track the number of remaining tasks and to schedule
// the evaluation of the expression when all tasks are ready.
class AsyncCountdown {
 public:
  AsyncCountdown(int64_t remaining_tasks, ExecutorPtr::weak_type executor,
                 arolla::expr::ExprOperatorPtr op,
                 std::vector<std::optional<arolla::TypedValue>> input_values,
                 FuturePtr result)
      : remaining_tasks_(remaining_tasks),
        executor_(std::move(executor)),
        op_(std::move(op)),
        input_values_(std::move(input_values)),
        result_(std::move(result)) {}

  absl::Status SetInput(size_t index,
                        absl::StatusOr<arolla::TypedValue> value) {
    if (!value.ok()) {
      absl::MutexLock lock(&lock_);
      if (error_reported_) {
        return absl::OkStatus();
      }
      error_reported_ = true;
      return result_->SetValue(value.status());
    }
    if (IsFutureQType(value->GetType())) {
      // This branch is currently not tested since there's no way to create
      // a future to a future in Python. However, keeping it here for
      // future-proofing.
      return absl::InvalidArgumentError(
          "futures to futures are not supported in async eval");
    }
    bool should_schedule = false;
    {
      absl::MutexLock lock(&lock_);
      if (index < 0 || index >= input_values_.size()) {
        return absl::InvalidArgumentError(
            absl::StrCat("index ", index, " is out of bounds [0, ",
                         input_values_.size(), ")."));
      }
      if (input_values_[index].has_value()) {
        return absl::InvalidArgumentError(
            absl::StrCat("Setting input ", index, " twice."));
      }
      input_values_[index] = std::move(*value);
      --remaining_tasks_;
      if (remaining_tasks_ == 0 && !error_reported_) {
        should_schedule = true;
      }
    }
    if (should_schedule) {
      return Schedule();
    } else {
      return absl::OkStatus();
    }
  }

  absl::Status Schedule() {
    std::vector<std::optional<arolla::TypedValue>> input_values;
    {
      absl::MutexLock lock(&lock_);
      input_values.swap(input_values_);
    }

    auto executor = executor_.lock();
    if (executor == nullptr) {
      return result_->SetValue(absl::InvalidArgumentError(
          "the executor was destroyed while the async eval was still pending"));
    }

    auto execute = [op = op_, input_values = std::move(input_values),
                    result = result_]() {
      std::vector<arolla::TypedRef> input_refs;
      input_refs.reserve(input_values.size());
      for (const auto& value : input_values) {
        if (!value) {
          // TODO: Propagate this error instead of ignoring.
          result
              ->SetValue(absl::InternalError(
                  "all inputs are done but one is still not set"))
              .IgnoreError();
          return;
        }
        input_refs.push_back(value->AsRef());
      }
      absl::StatusOr<arolla::TypedValue> value =
          expr::EvalOpWithCompilationCache(op, input_refs);
      if (!value.ok() || !IsFutureQType(value->GetType())) {
        // TODO: Propagate this error instead of ignoring.
        result->SetValue(std::move(value)).IgnoreError();
        return;
      }
      std::move(value)->UnsafeAs<FuturePtr>()->AddConsumer(
          [result](absl::StatusOr<arolla::TypedValue> value) {
            // TODO: Propagate this error instead of ignoring.
            result->SetValue(std::move(value)).IgnoreError();
          });
    };

    return executor->Schedule(std::move(execute));
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
  FuturePtr result_;
  absl::Mutex lock_;
};

// We need reference counting here since multiple futures use the countdown
// which may complete in arbitrary order.
using AsyncCountdownPtr = std::shared_ptr<AsyncCountdown>;

}  // namespace

absl::StatusOr<FuturePtr> AsyncEvalWithCompilationCache(
    const ExecutorPtr& executor, const arolla::expr::ExprOperatorPtr& op,
    absl::Span<const arolla::TypedRef> input_values,
    arolla::QTypePtr result_qtype) {
  FuturePtr result = std::make_shared<Future>(result_qtype);
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
      remaining_tasks, executor, op, std::move(ready_input_values), result);

  if (remaining_tasks == 0) {
    RETURN_IF_ERROR(countdown->Schedule());
  } else {
    for (int64_t i = 0; i < input_count; ++i) {
      if (IsFutureQType(input_values[i].GetType())) {
        const FuturePtr& future = input_values[i].UnsafeAs<FuturePtr>();
        future->AddConsumer(
            [countdown, i](absl::StatusOr<arolla::TypedValue> value) {
              // TODO: Propagate this error instead of ignoring.
              countdown->SetInput(i, std::move(value)).IgnoreError();
            });
      }
    }
  }
  return result;
}

}  // namespace koladata::functor::parallel
