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
#include "koladata/functor/parallel/stream_reduce.h"

#include <memory>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/cancellation.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {
namespace {

using Functor = absl::AnyInvocable<absl::StatusOr<arolla::TypedValue>(
    arolla::TypedRef, arolla::TypedRef) const>;

struct State {
  const arolla::CancellationContextPtr cancellation_context =
      arolla::CurrentCancellationContext();
  const ExecutorPtr /*absl_nonnull*/ executor;
  const StreamReaderPtr /*absl_nonnull*/ reader;
  const StreamWriterPtr /*absl_nonnull*/ writer;
  const Functor functor;
  arolla::TypedValue value;

  arolla::CancellationContext::Subscription cancellation_subscription;

  State(ExecutorPtr /*absl_nonnull*/ executor, StreamReaderPtr /*absl_nonnull*/ reader,
        StreamWriterPtr writer, Functor functor,
        arolla::TypedValue initial_value)
      : executor(std::move(executor)),
        reader(std::move(reader)),
        writer(std::move(writer)),
        functor(std::move(functor)),
        value(std::move(initial_value)) {}
};

using StatePtr = std::shared_ptr<State>;

[[nodiscard]] bool Interrupted(const State& state) {
  if (state.cancellation_context != nullptr &&
      state.cancellation_context->Cancelled()) [[unlikely]] {
    state.writer->TryClose(state.cancellation_context->GetStatus());
    return true;
  }
  if (state.writer->Orphaned()) [[unlikely]] {
    return true;
  }
  return false;
}

void Process(StatePtr /*absl_nonnull*/ state) {
  arolla::CancellationContext::ScopeGuard cancellation_scope(
      state->cancellation_context);
  if (Interrupted(*state)) {
    return;
  }
  auto try_read_result = state->reader->TryRead();
  while (arolla::TypedRef* item = try_read_result.item()) {
    auto value = state->functor(state->value.AsRef(), *item);
    if (!value.ok()) {
      state->writer->TryClose(std::move(value).status());
      return;
    }
    if (value->GetType() != state->value.GetType()) {
      state->writer->TryClose(absl::InvalidArgumentError(absl::StrFormat(
          "functor returned a value of the wrong type: expected %s, got %s",
          state->value.GetType()->name(), value->GetType()->name())));
      return;
    }
    state->value = *std::move(value);
    if (Interrupted(*state)) {
      return;
    }
    try_read_result = state->reader->TryRead();
  }
  if (absl::Status* status = try_read_result.close_status()) {
    if (!status->ok() || state->writer->TryWrite(state->value.AsRef())) {
      state->writer->TryClose(std::move(*status));
    }
    return;
  }
  state->reader->SubscribeOnce([state = std::move(state)]() mutable {
    if (Interrupted(*state)) {
      return;
    }
    state->executor->Schedule(
        [state = std::move(state)]() mutable { Process(std::move(state)); });
  });
}

}  // namespace

StreamPtr /*absl_nonnull*/ StreamReduce(ExecutorPtr /*absl_nonnull*/ executor,
                                    arolla::TypedValue initial_value,
                                    const StreamPtr /*absl_nonnull*/& input_stream,
                                    Functor functor) {
  DCHECK(functor != nullptr);
  auto [result, writer] = MakeStream(initial_value.GetType(), 1);
  auto state = std::make_shared<State>(
      std::move(executor), input_stream->MakeReader(), std::move(writer),
      std::move(functor), std::move(initial_value));
  if (state->cancellation_context != nullptr) {
    // Note: Use a weak pointer to the state since the subscription for
    // the cancellation notification with owning pointers is discouraged.
    state->cancellation_subscription = state->cancellation_context->Subscribe(
        [weak_state = std::weak_ptr<State>(state)]() {
          if (auto state = weak_state.lock()) {
            state->writer->TryClose(state->cancellation_context->GetStatus());
          }
        });
  }
  // Note: Consider spinning the first iteration of processing without the
  // executor.
  state->executor->Schedule(
      [state = std::move(state)]() mutable { Process(std::move(state)); });
  return std::move(result);
}

}  // namespace koladata::functor::parallel
