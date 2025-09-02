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
#ifndef KOLADATA_FUNCTOR_PARALLEL_EXECUTOR_H_
#define KOLADATA_FUNCTOR_PARALLEL_EXECUTOR_H_

#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/statusor.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace koladata::functor::parallel {

// An abstract class for executors. It assigns a random fingerprint to each
// executor instance, but otherwise delegates the behavior to the derived
// classes.
//
// Note that there is no error propagation logic in the executor, since
// the errors can be specific to one of many tasks executed by the same
// executor. The error propagation should be done inside the task function.
//
class Executor : public std::enable_shared_from_this<Executor> {
 public:
  using TaskFn = absl::AnyInvocable<void() &&>;

  // Default constructor.
  Executor() noexcept = default;

  // Disable copy and move semantics.
  Executor(const Executor&) = delete;
  Executor& operator=(const Executor&) = delete;

  // Returns the uuid of the executor. This is a randomly generated
  // fingerprint, unique for each executor instance, that is used to compute
  // the fingerprint of the executor QValue.
  const arolla::Fingerprint& uuid() const noexcept { return uuid_; }

  // Runs a given task on the executor. This method is thread-safe.
  //
  // Note: The executor can discard tasks without execution if it's in
  // the process of shutting down. This may happen immediately or in the future.
  template <typename Task>
  auto Schedule(Task&& task) noexcept
      -> std::enable_if_t<std::is_constructible_v<TaskFn, Task&&>>;

  // Returns the string representation of the executor.
  virtual std::string Repr() const noexcept = 0;

  // Note: Scheduled tasks may maintain ownership of the executor, in which case
  // the destructor won't be called until the scheduled tasks are deleted (for
  // example after completion).
  //
  // While the base executor interface doesn't provide a mechanism for early
  // task deletion, a specific executor implementation might provide such
  // a mechanism and corresponding guarantees.
  virtual ~Executor() noexcept = default;

 protected:
  // An override point for the schedule method.
  virtual void DoSchedule(TaskFn task) noexcept = 0;

 private:
  arolla::Fingerprint uuid_ = arolla::RandomFingerprint();
};

using ExecutorPtr = std::shared_ptr<Executor>;

// Checks if the current control flow is a task running on an executor.
//
// Note: When this function returns `true`, there is no guarantee that
// `CurrentExecutor()` will return the executor the task is running on.
// This is because the result of `CurrentExecutor()` can be overridden
// using `CurrentExecutorScopeGuard`.
//
// IMPORTANT: The implementation uses thread_local storage.
bool IsExecutorTask();

// Returns the current executor.
//
// For tasks running on an executor, this function returns the executor.
// The current executor can be overridden using `CurrentExecutorScopeGuard`.
//
// If there is no current executor, this function returns an error.
//
// IMPORTANT: The implementation uses thread_local storage.
absl::StatusOr<ExecutorPtr absl_nonnull> CurrentExecutor() noexcept;

// A RAII-based guard for overriding the current executor. The constructor of
// CurrentExecutorScopeGuard sets the specified executor as the current one,
// and its destructor restores the previous executor.
//
// The current executor can be retrieved using `CurrentExecutor()`.
//
// IMPORTANT:
//  * The scope guards must be destroyed in the reverse order of their
//    construction.
//  * Construction and destruction must occur on the same thread, as
//    they use thread_local storage.
//
class [[nodiscard]] CurrentExecutorScopeGuard final {
 public:
  explicit CurrentExecutorScopeGuard(ExecutorPtr
                                     absl_nullable executor) noexcept;
  ~CurrentExecutorScopeGuard() noexcept;

  // Disable copy and move semantics.
  CurrentExecutorScopeGuard(const CurrentExecutorScopeGuard&) = delete;
  CurrentExecutorScopeGuard& operator=(const CurrentExecutorScopeGuard&) =
      delete;

  // Returns a raw pointer to the current executor.
  //
  // Note: For common cases, prefer using the free function `CurrentExecutor()`,
  // which never returns `nullptr`.
  static Executor* absl_nullable current_executor() noexcept;

 private:
  ExecutorPtr absl_nullable const executor_;
  Executor* absl_nullable const previous_executor_;

  struct ThreadLocalData {
    Executor* absl_nullable current_executor = nullptr;
    bool is_executor_task = false;
  };

  // Note: `constinit` is essential for performance. Without it, a hidden guard
  // variable for initialization might be introduced.
  static thread_local constinit ThreadLocalData thread_local_data_;

  friend class Executor;
  friend bool IsExecutorTask();
};

// Note: Using a template parameter rather than a fixed `TaskFn` to have
// potential to improve inlining and avoid extra allocations during the
// scheduling.
template <typename Task>
auto Executor::Schedule(Task&& task) noexcept
    -> std::enable_if_t<std::is_constructible_v<TaskFn, Task&&>> {
  return DoSchedule([executor = shared_from_this(),
                     task = std::forward<Task>(task)]() mutable {
    auto& data = CurrentExecutorScopeGuard::thread_local_data_;
    const auto previous_data = data;
    data = {.current_executor = executor.get(), .is_executor_task = true};
    std::move(task)();
    data = previous_data;
  });
}

}  // namespace koladata::functor::parallel

namespace arolla {

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(
    koladata::functor::parallel::ExecutorPtr);
AROLLA_DECLARE_REPR(koladata::functor::parallel::ExecutorPtr);
AROLLA_DECLARE_SIMPLE_QTYPE(EXECUTOR, koladata::functor::parallel::ExecutorPtr);

}  // namespace arolla

#endif  // KOLADATA_FUNCTOR_PARALLEL_EXECUTOR_H_
