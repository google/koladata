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
#include "koladata/functor/parallel/basic_routine.h"

#include <atomic>
#include <memory>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/thread_safe_shared_ptr.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {
namespace {

// Implementation note: `BasicRoutineState` is managed by a `std::shared_ptr`,
// allowing us to use a `std::weak_ptr` for cancellation subscriptions. While
// shared ownership is generally discouraged, this particular `shared_ptr` is
// fully contained within this file.
//
// Technically, we could merge `BasicRoutineHooks` and `BasicRoutineState` into
// a single type, but that would risk exposing the shared-ownership mechanism
// beyond the scope of this file.

struct BasicRoutineState {
  const ExecutorPtr absl_nonnull executor;
  std::unique_ptr<BasicRoutineHooks> absl_nonnull hooks;
  StreamReaderPtr absl_nullable parked_reader;

  const arolla::CancellationContextPtr absl_nonnull cancellation_context =
      arolla::CurrentCancellationContext();
  arolla::CancellationContext::Subscription cancellation_subscription;

  // Set when the routine is waiting for a stream reader to be ready.
  arolla::ThreadSafeSharedPtr<BasicRoutineState> waiting_for_reader;

  // Set if `hooks->Interrupted()` returns true.
  std::atomic_flag interrupted;

  ~BasicRoutineState() {
    cancellation_subscription = {};
    parked_reader.reset();
    executor->Schedule([hooks = std::move(hooks)]() mutable { hooks.reset(); });
  }
};

using BasicRoutineStatePtr = std::shared_ptr<BasicRoutineState>;

// A wrapper for `hooks->Interrupted()` that guarantees that once it returns
// true, it always returns true afterwards.
[[nodiscard]] bool Interrupted(BasicRoutineState& state) {
  if (state.interrupted.test()) [[unlikely]] {
    return true;
  }
  if (state.hooks->Interrupted()) [[unlikely]] {
    state.interrupted.test_and_set();
    return true;
  }
  return false;
}

// Handles the cancellation of the routine.
void OnCancel(BasicRoutineState& state) {
  auto status = state.cancellation_context->GetStatus();
  DCHECK(!status.ok());
  if (status.ok()) {
    return;
  }
  state.hooks->OnCancel(std::move(status));
  if (Interrupted(state)) {
    state.waiting_for_reader.store(nullptr);
  }
}

void ScheduleRun(BasicRoutineStatePtr absl_nonnull&& state,
                 StreamReaderPtr absl_nullable reader);

void Start(BasicRoutineStatePtr absl_nonnull&& state) {
  arolla::CancellationContext::ScopeGuard cancellation_scope(
      state->cancellation_context);
  if (Interrupted(*state)) [[unlikely]] {
    return;
  }
  auto reader = state->hooks->Start();
  ScheduleRun(std::move(state), std::move(reader));
}

void Run(BasicRoutineStatePtr absl_nonnull&& state) {
  arolla::CancellationContext::ScopeGuard cancellation_scope(
      state->cancellation_context);
  DCHECK(state->parked_reader != nullptr);
  if (state->parked_reader == nullptr) [[unlikely]] {
    return;
  }
  if (Interrupted(*state)) [[unlikely]] {
    return;
  }
  auto reader = state->hooks->Resume(std::move(state->parked_reader));
  ScheduleRun(std::move(state), std::move(reader));
}

void ScheduleRun(BasicRoutineStatePtr absl_nonnull&& state,
                 StreamReaderPtr absl_nullable reader) {
  if (reader == nullptr) {
    return;
  }
  // While the routine is waiting on the reader, `state->waiting_for_reader`
  // is responsible for keeping its `state` alive. The reader subscription holds
  // only a weak pointer, allowing `OnCancel()` to release the state before
  // the reader becomes ready.
  state->waiting_for_reader.store(state);
  if (Interrupted(*state)) [[unlikely]] {
    // Handle a potential race if `OnCancel()` happened just before we stored
    // the state in `state->waiting_for_reader`.
    state->waiting_for_reader.store(nullptr);
    return;
  }
  state->parked_reader = std::move(reader);
  state->parked_reader->SubscribeOnce(
      [weak_state = std::weak_ptr(state)]() mutable {
        auto state = weak_state.lock();
        if (state == nullptr) {
          return;
        }
        state->waiting_for_reader.store(nullptr);
        if (Interrupted(*state)) [[unlikely]] {
          return;
        }
        state->executor->Schedule(
            [state = std::move(state)]() mutable { Run(std::move(state)); });
      });
}

}  // namespace

void StartBasicRoutine(
    ExecutorPtr absl_nonnull executor,
    std::unique_ptr<BasicRoutineHooks> absl_nonnull routine_hooks) {
  auto state = std::make_shared<BasicRoutineState>(std::move(executor),
                                                   std::move(routine_hooks));
  if (state->cancellation_context != nullptr) {
    // Note: Use a weak pointer since the subscription for the cancellation
    // notification using owning pointers is discouraged.
    state->cancellation_subscription = state->cancellation_context->Subscribe(
        [weak_state = std::weak_ptr(state)]() {
          if (auto state = weak_state.lock()) {
            OnCancel(*state);
          }
        });
  }
  if (Interrupted(*state)) [[unlikely]] {
    return;
  }
  state->executor->Schedule(
      [state = std::move(state)]() mutable { Start(std::move(state)); });
}

}  // namespace koladata::functor::parallel
