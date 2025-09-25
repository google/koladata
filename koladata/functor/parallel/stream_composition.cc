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
#include "koladata/functor/parallel/stream_composition.h"

#include <memory>
#include <queue>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "arolla/qtype/typed_ref.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {

class StreamInterleave::Scheduler {
 public:
  explicit Scheduler(StreamWriterPtr absl_nonnull writer)
      : writer_(std::move(writer)) {}

  ~Scheduler() { writer_->TryClose(absl::OkStatus()); }

  bool Orphaned() const { return writer_->Orphaned(); }

  void AddItem(arolla::TypedRef item) { (void)writer_->TryWrite(item); }

  void AddError(absl::Status status) {
    DCHECK(!status.ok());
    if (!status.ok()) {
      writer_->TryClose(std::move(status));
    }
  }

  static void ProcessInput(std::shared_ptr<Scheduler> self,
                           StreamReaderPtr reader) {
    auto try_read_result = reader->TryRead();
    while (auto* item = try_read_result.item()) {
      if (!self->writer_->TryWrite(*item)) {
        return;  // Closed or orphaned.
      }
      try_read_result = reader->TryRead();
    }
    if (auto* status = try_read_result.close_status()) {
      if (!status->ok()) {
        self->writer_->TryClose(std::move(*status));
      }
      return;
    }
    // Note: Consider adding a subscription method that automatically passes
    // the reader as the first argument to the callback.
    auto* reader_ptr = reader.get();
    reader_ptr->SubscribeOnce(
        [self = std::move(self), reader = std::move(reader)]() mutable {
          ProcessInput(std::move(self), std::move(reader));
        });
  }

 private:
  const StreamWriterPtr absl_nonnull writer_;
};

StreamInterleave::StreamInterleave(StreamWriterPtr absl_nonnull writer)
    : scheduler_(std::make_shared<Scheduler>(std::move(writer))) {}

bool StreamInterleave::Orphaned() const { return scheduler_->Orphaned(); }

void StreamInterleave::Add(const StreamPtr absl_nonnull& stream) {
  DCHECK(stream != nullptr);
  DCHECK(scheduler_ != nullptr);
  if (stream != nullptr && scheduler_ != nullptr) {
    Scheduler::ProcessInput(scheduler_, stream->MakeReader());
  }
}

void StreamInterleave::AddItem(arolla::TypedRef item) {
  scheduler_->AddItem(item);
}

void StreamInterleave::AddError(absl::Status status) {
  scheduler_->AddError(std::move(status));
}

// This class holds the execution state of the stream chain, and the callbacks
// hold a pointer to it. It is thread-safe.
class StreamChain::Scheduler : public std::enable_shared_from_this<Scheduler> {
 public:
  explicit Scheduler(StreamWriterPtr absl_nonnull writer)
      : writer_(std::move(writer)) {}

  bool Orphaned() const { return writer_->Orphaned(); }

  // Adds one more input stream to the chain.
  void AddInput(StreamReaderPtr input) {
    bool must_process_pending_inputs = false;
    {
      absl::MutexLock lock(mutex_);
      if (error_reported_) {
        return;
      }
      if (pending_inputs_.empty()) {
        must_process_pending_inputs = true;
      }
      pending_inputs_.push(std::move(input));
    }
    // Since we release the lock before this line, other threads may have added
    // more inputs at this point, however, since there is no callback registered
    // and this pending input is still in the queue, none of them will have
    // called ProcessPendingInputs, so our invariants will be maintained.
    if (must_process_pending_inputs) {
      ProcessPendingInputs();
    }
  }

  void AddItem(arolla::TypedRef item) {
    DCHECK(item.GetType() == writer_->value_qtype());
    auto [stream, writer] = MakeStream(writer_->value_qtype());
    writer->Write(item);
    std::move(*writer).Close();
    AddInput(stream->MakeReader());
  }

  void AddError(absl::Status status) {
    DCHECK(!status.ok());
    if (!status.ok()) {
      auto [stream, writer] = MakeStream(writer_->value_qtype());
      std::move(*writer).Close(std::move(status));
      AddInput(stream->MakeReader());
    }
  }

  // Will be executed once no callbacks exist and the corresponding
  // StreamChain is gone, so no new inputs can be added.
  ~Scheduler() {
    CHECK(pending_inputs_.empty());
    if (!error_reported_) {
      std::move(*writer_).Close();
    }
  }

  // Disallow copying, moving.
  Scheduler(const Scheduler&) = delete;
  Scheduler& operator=(const Scheduler&) = delete;
  Scheduler(Scheduler&&) = delete;
  Scheduler& operator=(Scheduler&&) = delete;

 private:
  // Processes the pending input(s) until there is one that is not ready,
  // and subscribes to it.
  void ProcessPendingInputs() {
    StreamReader* active_input = nullptr;
    {
      absl::MutexLock lock(mutex_);
      CHECK(!pending_inputs_.empty());
      CHECK(!error_reported_);
      active_input = pending_inputs_.front().get();
    }
    for (;;) {
      auto result = ProcessInput(*active_input);
      if (result == ProcessInputResult::kPending) {
        return;
      }
      if (result == ProcessInputResult::kError) {
        absl::MutexLock lock(mutex_);
        pending_inputs_ = {};
        error_reported_ = true;
        return;
      }
      {
        absl::MutexLock lock(mutex_);
        pending_inputs_.pop();
        if (pending_inputs_.empty()) {
          return;
        } else {
          active_input = pending_inputs_.front().get();
        }
      }
    }
  }

  // Processes one pending input.
  enum class ProcessInputResult {
    kPending,
    kDone,
    kError,
  };
  ProcessInputResult ProcessInput(StreamReader& input) {
    auto try_read_result = input.TryRead();
    while (auto* item = try_read_result.item()) {
      writer_->Write(*item);
      try_read_result = input.TryRead();
    }
    if (auto* status = try_read_result.close_status()) {
      if (status->ok()) {
        return ProcessInputResult::kDone;
      } else {
        std::move(*writer_).Close(std::move(*status));
        return ProcessInputResult::kError;
      }
    }
    // Since we release the lock before this line, we might have other pending
    // inputs added in the meantime. However, since no other callback is
    // registered and we have our pending input as the first in the queue,
    // nothing could have happened to it.
    // We need to release the lock since SubscribeOnce might execute the
    // callback immediately, and the callback will try to acquire the lock.
    input.SubscribeOnce(
        [self = shared_from_this()] { self->ProcessPendingInputs(); });
    return ProcessInputResult::kPending;
  }

  absl::Mutex mutex_;

  // writer_ is only invoked by the thread that is executing
  // ProcessPendingInputs, which is always at most one, so we don't need to
  // protect it with a mutex. If we did, we should have used a separate mutex
  // since we don't want to deadlock when downstream processing of the writer
  // tries to add one more stream to the chain.
  //
  // Note: We also access writer_->Orphaned() from the StreamChain::Orphaned()
  // method. This is deadlock-safe because writer_->Orphaned() involves no
  // internal locking.
  const StreamWriterPtr absl_nonnull writer_;

  // There are three possible states when mutex_ is not held:
  // - the queue is empty.
  // - there is exactly one callback scheduled with the first pending input.
  // - there is no callback scheduled, but there is exactly one thread that
  //   will eventually schedule a callback with the first pending input.
  std::queue<StreamReaderPtr> pending_inputs_ ABSL_GUARDED_BY(mutex_);
  bool error_reported_ ABSL_GUARDED_BY(mutex_) = false;
};

StreamChain::StreamChain(StreamWriterPtr absl_nonnull writer)
    : scheduler_(std::make_shared<Scheduler>(std::move(writer))) {}

bool StreamChain::Orphaned() const { return scheduler_->Orphaned(); }

void StreamChain::Add(const StreamPtr absl_nonnull& stream) {
  DCHECK(stream != nullptr);
  DCHECK(scheduler_ != nullptr);
  if (stream != nullptr && scheduler_ != nullptr) {
    scheduler_->AddInput(stream->MakeReader());
  }
}

void StreamChain::AddItem(arolla::TypedRef item) { scheduler_->AddItem(item); }

void StreamChain::AddError(absl::Status status) {
  DCHECK(!status.ok());
  DCHECK(scheduler_ != nullptr);
  if (!status.ok() && scheduler_ != nullptr) {
    scheduler_->AddError(std::move(status));
  }
}

}  // namespace koladata::functor::parallel
