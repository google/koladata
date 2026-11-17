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

#include <atomic>
#include <memory>
#include <queue>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/once_callbacks.h"
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
    AddInput(MakeSingleItemReader(arolla::TypedValue(item)));
  }

  void AddError(absl::Status status) {
    DCHECK(!status.ok());
    if (!status.ok()) {
      AddInput(MakeErrorReader(std::move(status)));
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

class ProxyStreamWriter::Impl final
    : public std::enable_shared_from_this<ProxyStreamWriter::Impl>,
      public Stream {
 public:
  explicit Impl(arolla::QTypePtr absl_nonnull value_qtype)
      : Stream(value_qtype) {}

  StreamReaderPtr absl_nonnull MakeReader() final ABSL_LOCKS_EXCLUDED(mutex_) {
    StreamPtr attached_stream;
    {
      absl::MutexLock lock(mutex_);
      attached_stream = stream_;
    }
    if (attached_stream != nullptr) {
      return attached_stream->MakeReader();
    }
    return std::make_unique<Reader>(shared_from_this());
  }

  bool IsAttached() const { return attached_.test(std::memory_order_relaxed); }

  void TryAttach(StreamPtr absl_nonnull stream) ABSL_LOCKS_EXCLUDED(mutex_) {
    if (attached_.test_and_set(std::memory_order_relaxed)) {
      return;
    }
    std::shared_ptr<StreamReader> subscription_reader;
    {
      absl::MutexLock lock(mutex_);
      DCHECK(stream_ == nullptr);
      stream_ = std::move(stream);
      if (callbacks_.empty()) {
        return;
      }
      subscription_reader = stream_->MakeReader();
      subscription_reader_ = subscription_reader;
    }
    subscription_reader->SubscribeOnce([weak_self = weak_from_this()] {
      auto self = weak_self.lock();
      if (self == nullptr) {
        return;
      }
      std::shared_ptr<StreamReader> subscription_reader;  // Destroyed and
      arolla::OnceCallbacks callbacks;  // invoked outside of the lock.
      {
        absl::MutexLock lock(self->mutex_);
        subscription_reader = std::move(self->subscription_reader_);
        swap(self->callbacks_, callbacks);
      }
      std::move(callbacks).Notify();
    });
  }

 private:
  class Reader final : public StreamReader {
   public:
    explicit Reader(std::shared_ptr<Impl> absl_nonnull proxy_stream)
        : proxy_stream_(std::move(proxy_stream)) {}

    ~Reader() final {
      // NOTE: `subscription_` can be safely read without acquiring
      // `proxy_stream_->mutex_` since the destructor guarantees exclusive
      // access.
      if (!subscription_) [[likely]] {
        return;
      }
      std::shared_ptr<StreamReader> subscription_reader;  // Destroyed outside
      arolla::OnceCallbacks callbacks;                    // of the lock.
      absl::MutexLock lock(proxy_stream_->mutex_);
      if (!proxy_stream_->callbacks_.empty()) {
        callbacks = proxy_stream_->callbacks_.Remove(std::move(subscription_));
        if (proxy_stream_->callbacks_.empty()) {
          subscription_reader = std::move(proxy_stream_->subscription_reader_);
        }
      }
    }

    TryReadResult TryRead() final {
      if (auto* reader = underlying_reader_.load(std::memory_order_acquire);
          reader != nullptr) [[likely]] {
        return reader->TryRead();
      } else if (auto* reader = ResolveOrSubscribe()) {
        return reader->TryRead();
      }
      return {};
    }

    void SubscribeOnce(arolla::OnceCallback absl_nonnull callback) final {
      if (auto* reader = underlying_reader_.load(std::memory_order_acquire);
          reader != nullptr) [[likely]] {
        reader->SubscribeOnce(std::move(callback));
      } else if (auto* reader = ResolveOrSubscribe(&callback)) {
        reader->SubscribeOnce(std::move(callback));
      }
    }

   private:
    StreamReader* absl_nullable ResolveOrSubscribe(
        arolla::OnceCallback absl_nonnull* absl_nullable callback = nullptr) {
      absl::MutexLock lock(proxy_stream_->mutex_);
      if (underlying_reader_holder_ != nullptr) {
        return underlying_reader_holder_.get();
      }
      if (proxy_stream_->stream_ == nullptr) {
        if (callback != nullptr) {
          subscription_ = proxy_stream_->callbacks_.Add(
              std::move(*callback), std::move(subscription_));
        }
        return nullptr;
      }
      if (proxy_stream_->callbacks_.empty()) {
        subscription_ = {};
      }
      underlying_reader_holder_ = proxy_stream_->stream_->MakeReader();
      underlying_reader_.store(underlying_reader_holder_.get(),
                               std::memory_order_release);
      return underlying_reader_holder_.get();
    }

    const std::shared_ptr<Impl> absl_nonnull proxy_stream_;
    std::atomic<StreamReader* absl_nullable> underlying_reader_ = nullptr;
    StreamReaderPtr absl_nullable underlying_reader_holder_
        ABSL_GUARDED_BY(proxy_stream_->mutex_);
    arolla::OnceCallbacks::Token subscription_
        ABSL_GUARDED_BY(proxy_stream_->mutex_);
  };

  std::atomic_flag attached_;
  absl::Mutex mutex_;
  StreamPtr absl_nullable stream_ ABSL_GUARDED_BY(mutex_);
  std::shared_ptr<StreamReader> absl_nullable subscription_reader_
      ABSL_GUARDED_BY(mutex_);
  arolla::OnceCallbacks callbacks_ ABSL_GUARDED_BY(mutex_);
};

ProxyStreamWriter::ProxyStreamWriter(Impl& impl)
    : value_qtype_(impl.value_qtype()), impl_(impl.weak_from_this()) {}

ProxyStreamWriter::~ProxyStreamWriter() {
  if (auto proxy_stream = impl_.lock();
      proxy_stream != nullptr && !proxy_stream->IsAttached()) {
    proxy_stream->TryAttach(
        MakeErrorStream(proxy_stream->value_qtype(), OrphanedError()));
  }
}

ProxyStreamWriter& ProxyStreamWriter::operator=(
    ProxyStreamWriter&& other) noexcept {
  if (this != &other) {
    ProxyStreamWriter tmp(std::move(*this));
    value_qtype_ = std::move(other.value_qtype_);
    impl_ = std::move(other.impl_);
  }
  return *this;
}

bool ProxyStreamWriter::Orphaned() const {
  auto proxy_stream = impl_.lock();
  return proxy_stream == nullptr || proxy_stream->IsAttached();
}

void ProxyStreamWriter::TryAttach(StreamPtr absl_nonnull stream) {
  if (stream == nullptr) [[unlikely]] {
    LOG(FATAL) << "expected a non-null stream";
  }
  if (stream->value_qtype() != value_qtype_) [[unlikely]] {
    LOG(FATAL) << "expected a stream with value_qtype " << value_qtype_->name()
               << ", got " << stream->value_qtype()->name();
  }
  if (auto proxy_stream = impl_.lock()) {
    proxy_stream->TryAttach(std::move(stream));
  }
}

std::pair<StreamPtr absl_nonnull, ProxyStreamWriter> MakeProxyStream(
    arolla::QTypePtr absl_nonnull value_qtype) {
  auto stream = std::make_shared<ProxyStreamWriter::Impl>(value_qtype);
  ProxyStreamWriter writer(*stream);
  return {std::move(stream), std::move(writer)};
}

}  // namespace koladata::functor::parallel
