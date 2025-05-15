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
#include "koladata/functor/parallel/stream_map.h"

#include <atomic>
#include <cstddef>
#include <deque>
#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/cancellation.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {
namespace {

// A helper interface that generalizes the construction of output streams for
// the `stream.map` and `stream.map_unordered` operators.
//
// Notes:
//
//  * `offset` always refers to a position in the input stream.
//  * For a given `offset`, at most one `Push*` method can be invoked.
//  * `PushStatus(offset, status)` can be called multiple times, with
//    different offsets, indicating that the input stream or the functor call
//    returned an error.
//  * A call `PushStatus(offset, absl::OkStatus())` always corresponds to
//    the end of the input stream; notably, there might not be further offsets
//    beyond this point. Such a call may not happen, if there were earlier
//    errors.
//
class Sink {
 public:
  // Performs a fast "pre-flight" check to determine if the sink is waiting for
  // information at the given `offset`.
  //
  // Returns `false` if the sink does NOT need information for the given
  // `offset` or any further offsets beyond it.
  virtual bool Accepts(size_t offset) const = 0;

  // Pushes an `item` to the sink.
  //
  // Returns `false` if the sink does NOT need information further beyond
  // the given `offset`.
  virtual bool PushItem(size_t offset, arolla::TypedValue&& item) = 0;

  // Pushes a `status` to the sink. It's assumed that the sink will
  // automatically need no information further beyond the given `offset`.
  virtual void PushStatus(size_t offset, absl::Status&& status) = 0;

  // Forwards a cancellation event to the sink.
  //
  // This operation should preferably be fast, as multiple subsystems
  // may be waiting for a cancellation notification.
  virtual void Cancel(absl::Status&& status) = 0;

  Sink() = default;
  virtual ~Sink() = default;

  // Disallow copy and move.
  Sink(const Sink&) = delete;
  Sink& operator=(const Sink&) = delete;
};

using Functor = absl::AnyInvocable<  // clang-format hint
    absl::StatusOr<arolla::TypedValue>(arolla::TypedRef) const>;

// A structure with the state shared between different routines implementing
// the stream.map* operations. Only the routines in this file have access to it.
template <typename SinkT>
struct State {
  const arolla::CancellationContextPtr /*absl_nullable*/ cancellation_context =
      arolla::CurrentCancellationContext();
  arolla::CancellationContext::Subscription cancellation_subscription;

  const ExecutorPtr /*absl_nonnull*/ executor;
  const Functor functor;

  // Note: There is only one reading routine, thus no extra synchronization
  // needed.
  const StreamReaderPtr /*absl_nonnull*/ reader;
  size_t read_count = 0;
  std::atomic_flag stop_reader;

  const arolla::QTypePtr /*absl_nonnull*/ value_qtype;
  SinkT sink;

  // Note: A constructor for compatibility with std::make_shared.
  State(ExecutorPtr /*absl_nonnull*/ executor, Functor functor,
        StreamReaderPtr /*absl_nonnull*/ reader,
        StreamWriterPtr /*absl_nonnull*/ writer)
      : executor(std::move(executor)),
        functor(std::move(functor)),
        reader(std::move(reader)),
        value_qtype(writer->value_qtype()),
        sink(std::move(writer)) {}
};

// Calls the functor on the given item, and forwards the result to the sink.
template <typename StateT>
void ProcessItem(std::shared_ptr<StateT> /*absl_nonnull*/ state, size_t offset,
                 arolla::TypedRef item) {
  if (!state->sink.Accepts(offset)) {
    return;
  }
  // Set up the current cancellation context for the functor execution (and
  // potentially downstream computations).
  arolla::CancellationContext::ScopeGuard cancellation_scope(
      state->cancellation_context);
  absl::StatusOr<arolla::TypedValue> result = state->functor(item);
  if (!result.ok()) [[unlikely]] {
    state->sink.PushStatus(offset, std::move(result).status());
    state->stop_reader.test_and_set(std::memory_order_relaxed);
  } else if (result->GetType() != state->value_qtype) [[unlikely]] {
    state->sink.PushStatus(
        offset,
        absl::InvalidArgumentError(absl::StrFormat(
            "functor returned a value of the wrong type: expected %s, got %s",
            state->value_qtype->name(), result->GetType()->name())));
    state->stop_reader.test_and_set(std::memory_order_relaxed);
  } else if (!state->sink.PushItem(offset, *std::move(result))) [[unlikely]] {
    state->stop_reader.test_and_set(std::memory_order_relaxed);
  }
}

// Reads the items from the stream and schedules their processing.
template <typename StateT>
void ReadItems(std::shared_ptr<StateT> /*absl_nonnull*/ state) {
  if (state->stop_reader.test(std::memory_order_relaxed)) {
    return;
  }
  // Handle available items.
  auto try_read_result = state->reader->TryRead();
  while (arolla::TypedRef* item = try_read_result.item()) {
    state->executor->Schedule(
        [state, offset = state->read_count, item = *item]() mutable {
          ProcessItem(std::move(state), offset, item);
        });
    state->read_count += 1;
    if (state->stop_reader.test(std::memory_order_relaxed)) {
      return;
    }
    try_read_result = state->reader->TryRead();
  }
  // Handle the case where the stream has been fully read.
  if (absl::Status* status = try_read_result.close_status()) {
    // When `!status.ok()`, it indicates the stream was closed due to an error.
    // `kOk` signifies that the stream just ended.
    //
    // Note: We could also set the `stop_reader` flag, but the reader routine
    // ends here and won't ever look at it.
    state->sink.PushStatus(state->read_count, std::move(*status));
    return;
  }
  // Wait for more items to be available.
  state->reader->SubscribeOnce([state = std::move(state)]() mutable {
    if (state->stop_reader.test(std::memory_order_relaxed)) {
      return;
    }
    state->executor->Schedule(
        [state = std::move(state)]() mutable { ReadItems(std::move(state)); });
  });
}

template <typename StateT>
StreamPtr /*absl_nonnull*/ StreamMapImpl(
    ExecutorPtr /*absl_nonnull*/ executor,
    const StreamPtr /*absl_nonnull*/& input_stream,
    arolla::QTypePtr /*absl_nonnull*/ return_value_type,  // clang-format hint
    Functor functor) {
  DCHECK(functor != nullptr);
  auto [stream, writer] = MakeStream(return_value_type);
  auto state =
      std::make_shared<StateT>(std::move(executor), std::move(functor),
                               input_stream->MakeReader(), std::move(writer));
  if (state->cancellation_context != nullptr) {
    // Note: Use a weak pointer to the state since the subscription for
    // the cancellation notification with owning pointers is discouraged.
    state->cancellation_subscription = state->cancellation_context->Subscribe(
        [weak_state = std::weak_ptr<StateT>(state)] {
          auto state = weak_state.lock();
          state->stop_reader.test_and_set(std::memory_order_relaxed);
          auto status = state->cancellation_context->GetStatus();
          DCHECK(!status.ok());
          state->sink.Cancel(std::move(status));
        });
  }
  // Trigger the reading routine for the first time (further reading will be
  // scheduled on the executor).
  ReadItems(std::move(state));
  return std::move(stream);
}

class OrderedSink final : public Sink {
 public:
  explicit OrderedSink(StreamWriterPtr /*absl_nonnull*/ writer)
      : writer_(std::move(writer)) {}

  bool Accepts(size_t offset) const final {
    return offset < final_offset_.load(std::memory_order_relaxed);
  }

  bool PushItem(size_t offset, arolla::TypedValue&& item) final {
    absl::MutexLock lock(&mutex_);
    DCHECK_LE(write_count_, offset);
    if (!Accepts(offset)) {
      return false;
    }
    const size_t i = offset - write_count_;
    if (buffer_.size() <= i) {
      buffer_.resize(i + 1);
    }
    buffer_[i] = std::move(item);
    while (!buffer_.empty() && buffer_.front().has_value()) {
      if (!writer_->TryWrite(buffer_.front()->AsRef())) {
        final_offset_ = 0;  // Orphaned or already closed.
        return false;
      }
      write_count_ += 1;
      buffer_.pop_front();
    }
    DCHECK_LE(write_count_, final_offset_);
    if (write_count_ == final_offset_) {
      writer_->TryClose(std::move(final_status_));
      final_offset_ = 0;
      return false;
    }
    return true;
  }

  void PushStatus(size_t offset, absl::Status&& status) final {
    absl::MutexLock lock(&mutex_);
    DCHECK_LE(write_count_, offset);
    if (!Accepts(offset)) {
      return;
    }
    DCHECK_LT(offset, final_offset_);
    final_offset_ = offset;
    if (offset == write_count_) {
      writer_->TryClose(std::move(status));
    } else {
      final_status_ = std::move(status);
    }
  }

  void Cancel(absl::Status&& status) final {
    writer_->TryClose(std::move(status));
  }

 private:
  const StreamWriterPtr /*absl_nonnull*/ writer_;
  // Note: While `final_offset_` is not protected by the `mutex_`,
  // writing to it always happens under the mutex.
  std::atomic<size_t> final_offset_ = std::numeric_limits<size_t>::max();
  absl::Status final_status_ ABSL_GUARDED_BY(mutex_);
  size_t write_count_ ABSL_GUARDED_BY(mutex_) = 0;
  std::deque<std::optional<arolla::TypedValue>> buffer_ ABSL_GUARDED_BY(mutex_);
  absl::Mutex mutex_;
};

class UnorderedSink final : public Sink {
 public:
  explicit UnorderedSink(StreamWriterPtr /*absl_nonnull*/ writer)
      : writer_(std::move(writer)) {}

  ~UnorderedSink() final {
    if (!failed_.test_and_set(std::memory_order_relaxed) &&
        write_count_ == input_stream_size_) {
      std::move(*writer_).Close(absl::OkStatus());
    }
  }

  bool Accepts(size_t /*offset*/) const final {
    return !failed_.test(std::memory_order_relaxed);
  }

  bool PushItem(size_t /*offset*/, arolla::TypedValue&& item) final {
    if (!failed_.test(std::memory_order_relaxed)) [[likely]] {
      if (writer_->TryWrite(item.AsRef())) [[likely]] {
        write_count_ += 1;
        return true;
      }
      failed_.test_and_set(std::memory_order_relaxed);
    }
    return false;
  }

  void PushStatus(size_t offset, absl::Status&& status) final {
    if (status.ok()) {
      // Note: We don't need any extra synchronization here, since
      // `absl::OkStatus` indicates the end of the input stream, which
      // can only occur once.
      DCHECK(!input_stream_size_.has_value());
      input_stream_size_ = offset;
    } else if (!failed_.test_and_set(std::memory_order_relaxed)) {
      std::move(*writer_).Close(std::move(status));
    }
  }

  void Cancel(absl::Status&& status) final {
    DCHECK(!status.ok());
    if (!failed_.test_and_set(std::memory_order_relaxed)) {
      std::move(*writer_).Close(std::move(status));
    }
  }

 private:
  const StreamWriterPtr /*absl_nonnull*/ writer_;
  std::atomic_flag failed_;
  std::atomic<size_t> write_count_ = 0;
  std::optional<size_t> input_stream_size_;
};

}  // namespace

StreamPtr /*absl_nonnull*/ StreamMap(
    ExecutorPtr /*absl_nonnull*/ executor,
    const StreamPtr /*absl_nonnull*/& input_stream,
    arolla::QTypePtr /*absl_nonnull*/ return_value_type,  // clang-format hint
    Functor functor) {
  return StreamMapImpl<State<OrderedSink>>(
      std::move(executor), input_stream, return_value_type, std::move(functor));
}

StreamPtr /*absl_nonnull*/ StreamMapUnordered(
    ExecutorPtr /*absl_nonnull*/ executor,
    const StreamPtr /*absl_nonnull*/& input_stream,
    arolla::QTypePtr /*absl_nonnull*/ return_value_type,  // clang-format hint
    Functor functor) {
  return StreamMapImpl<State<UnorderedSink>>(
      std::move(executor), input_stream, return_value_type, std::move(functor));
}

}  // namespace koladata::functor::parallel
