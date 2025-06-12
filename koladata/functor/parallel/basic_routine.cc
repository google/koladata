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

#include <memory>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "arolla/util/cancellation.h"
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
  const std::unique_ptr<BasicRoutineHooks> absl_nonnull hooks;
  StreamReaderPtr absl_nullable reader;

  const arolla::CancellationContextPtr absl_nonnull cancellation_context =
      arolla::CurrentCancellationContext();
  arolla::CancellationContext::Subscription cancellation_subscription;
};

using BasicRoutineStatePtr = std::shared_ptr<BasicRoutineState>;

[[nodiscard]] bool Interrupted(const BasicRoutineState& state) {
  return (state.cancellation_context != nullptr &&
          state.cancellation_context->Cancelled()) ||
         state.hooks->Interrupted();
}

void Run(BasicRoutineStatePtr absl_nonnull&& state) {
  arolla::CancellationContext::ScopeGuard cancellation_scope(
      state->cancellation_context);
  if (state->reader == nullptr) {
    state->reader = state->hooks->Start();
  } else {
    state->reader = state->hooks->Resume(std::move(state->reader));
  }
  if (state->reader == nullptr) {
    return;
  }
  if (Interrupted(*state)) {
    return;
  }
  state->reader->SubscribeOnce([state = std::move(state)]() mutable {
    if (Interrupted(*state)) {
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
        [weak_state = std::weak_ptr<BasicRoutineState>(state)]() {
          if (auto state = weak_state.lock()) {
            auto status = state->cancellation_context->GetStatus();
            DCHECK(!status.ok());
            if (!status.ok()) {
              state->hooks->OnCancel(std::move(status));
            }
          }
        });
  }
  if (Interrupted(*state)) {
    return;
  }
  state->executor->Schedule(
      [state = std::move(state)]() mutable { Run(std::move(state)); });
}

}  // namespace koladata::functor::parallel
