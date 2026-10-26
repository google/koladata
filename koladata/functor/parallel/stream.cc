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
#include "koladata/functor/parallel/stream.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <new>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/memory.h"
#include "arolla/util/repr.h"

namespace koladata::functor::parallel {
namespace {

using ::arolla::AlignedAllocN;
using ::arolla::AlignedPtr;
using ::arolla::GetNothingQType;
using ::arolla::QTypePtr;
using ::arolla::TypedRef;

uint32_t GetDefaultChunkCapacity(QTypePtr absl_nonnull value_qtype) {
  constexpr size_t kDefaultByteCapacity = 512;
  const size_t bytesize = value_qtype->type_layout().AllocSize();
  return bytesize ? (kDefaultByteCapacity + bytesize - 1) / bytesize
                  : kDefaultByteCapacity;
}

// A non-thread-safe, fixed-capacity storage block for stream items.
//
// For trivially copyable types, values are copied directly into uninitialized
// storage. Other types require slot initialization (default construction)
// before they can be written to.
//
// If 'lazy_init' is false, all slots are initialized upfront. If 'lazy_init'
// is true, slots are initialized on-write (see the implementation for details),
// which can be more efficient for short streams.
class Chunk {
 public:
  Chunk(QTypePtr absl_nonnull value_qtype, size_t capacity, bool lazy_init);
  ~Chunk();

  // Disallow copy and move.
  Chunk(const Chunk&) = delete;
  Chunk& operator=(const Chunk&) = delete;

  size_t capacity() const { return capacity_; }

  TypedRef GetRef(size_t i) const;
  void AppendRef(size_t i, TypedRef ref);

 private:
  const QTypePtr absl_nonnull value_qtype_;
  const size_t capacity_;
  const size_t value_bytesize_;
  const AlignedPtr storage_;
  // The number of initialized slots. Always 0 for trivially copyable types.
  size_t initialized_size_ = 0;
};

// Combined implementation of Stream and StreamWriter.
class StreamImpl final : public std::enable_shared_from_this<StreamImpl>,
                         public Stream {
 public:
  class Writer;
  class Reader;

  StreamImpl(QTypePtr absl_nonnull value_qtype, size_t initial_capacity);

  StreamReaderPtr absl_nonnull MakeReader() final ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  enum class [[nodiscard]] InternalWriteResult { kOk, kClosed };
  InternalWriteResult InternalWrite(arolla::TypedRef value);

  void InternalClose(absl::Status&& status);

  // Returns the number of chunks.
  size_t chunk_count() const ABSL_SHARED_LOCKS_REQUIRED(mutex_) {
    return first_chunk_.has_value() + other_chunks_.size();
  }

  // Returns the i-th chunk.
  Chunk& chunk(size_t i) ABSL_SHARED_LOCKS_REQUIRED(mutex_) {
    DCHECK_LT(i, chunk_count());
    return i == 0 ? *first_chunk_ : other_chunks_[i - 1];
  }

  absl::Mutex mutex_;
  // Note: The first chunk is stored inline to avoid extra allocations for
  // short streams, or streams with known capacity.
  std::optional<Chunk> first_chunk_ ABSL_GUARDED_BY(mutex_);
  std::deque<Chunk> other_chunks_ ABSL_GUARDED_BY(mutex_);
  // Points to the last chunk, or nullptr if there are no chunks.
  Chunk* absl_nullable last_chunk_ ABSL_GUARDED_BY(mutex_) = nullptr;
  // The number of values in `last_chunk_`.
  size_t last_chunk_size_ ABSL_GUARDED_BY(mutex_) = 0;
  // The capacity of `last_chunk_`, or 0 if there are no chunks.
  size_t last_chunk_capacity_ ABSL_GUARDED_BY(mutex_) = 0;
  std::optional<absl::Status> status_ ABSL_GUARDED_BY(mutex_);
  std::vector<absl::AnyInvocable<void() &&>> callbacks_ ABSL_GUARDED_BY(mutex_);

  const uint32_t default_chunk_capacity_;
};

// A stream writer implementation.
class StreamImpl::Writer final : public StreamWriter {
 public:
  explicit Writer(const std::shared_ptr<StreamImpl> absl_nonnull& stream);
  ~Writer() final;

  bool Orphaned() const final;

  void Write(TypedRef value) final;

  void Close(absl::Status status) && final;

  bool TryWrite(TypedRef value) final;

  void TryClose(absl::Status status) final;

 private:
  const std::weak_ptr<StreamImpl> weak_stream_;
  std::atomic_flag closed_ = false;
};

// A stream reader implementation.
class StreamImpl::Reader final : public StreamReader {
 public:
  explicit Reader(std::shared_ptr<StreamImpl> absl_nonnull stream);

  TryReadResult TryRead() final;

  void SubscribeOnce(absl::AnyInvocable<void() &&>&& callback) final;

 private:
  void Update() ABSL_EXCLUSIVE_LOCKS_REQUIRED(stream_->mutex_);

  const std::shared_ptr<StreamImpl> absl_nonnull stream_;

  size_t next_chunk_index_ = 0;
  Chunk* absl_nonnull chunk_;
  size_t offset_ = 0;
  size_t known_size_ = 0;
};

Chunk::Chunk(QTypePtr absl_nonnull value_qtype, size_t capacity, bool lazy_init)
    : value_qtype_(value_qtype),
      capacity_(capacity),
      value_bytesize_(value_qtype->type_layout().AllocSize()),
      storage_(AlignedAllocN(
          std::align_val_t{value_qtype->type_layout().AllocAlignment()},
          value_bytesize_, capacity)) {
  if (storage_ == nullptr) {
    LOG(FATAL) << "Failed to allocate chunk storage: alignment="
               << value_qtype->type_layout().AllocAlignment()
               << ", capacity=" << capacity
               << ", element_size=" << value_bytesize_;
  }
  if (!lazy_init && !value_qtype->is_trivially_copyable()) {
    value_qtype->type_layout().InitializeAlignedAllocN(storage_.get(),
                                                       capacity_);
    initialized_size_ = capacity_;
  }
}

Chunk::~Chunk() {
  // Note: Assuming that no concurrent access can happen to the chunk during
  // the destruction.
  value_qtype_->type_layout().DestroyAllocN(storage_.get(), initialized_size_);
}

TypedRef Chunk::GetRef(size_t i) const {
  DCHECK_LT(i, capacity_);
  return TypedRef::UnsafeFromRawPointer(
      value_qtype_,
      reinterpret_cast<const char*>(storage_.get()) + i * value_bytesize_);
}

void Chunk::AppendRef(size_t i, TypedRef ref) {
  DCHECK_LT(i, capacity_);
  DCHECK_EQ(ref.GetType(), value_qtype_);
  char* slot = reinterpret_cast<char*>(storage_.get()) + i * value_bytesize_;
  if (value_qtype_->is_trivially_copyable()) {
    // Trivially copyable values need neither initialization nor destruction.
    std::memcpy(slot, ref.GetRawPointer(), value_bytesize_);
    return;
  }
  DCHECK_LE(i, initialized_size_);
  if (i == initialized_size_) [[unlikely]] {
    // Initialize the next batch of slots, doubling the number of initialized
    // slots (1, 1, 2, 4, ...) within the capacity.
    const size_t batch_size = std::min(capacity_ - initialized_size_,
                                       std::max<size_t>(1, initialized_size_));
    value_qtype_->type_layout().InitializeAlignedAllocN(slot, batch_size);
    initialized_size_ += batch_size;
  }
  value_qtype_->UnsafeCopy(ref.GetRawPointer(), slot);
}

StreamImpl::StreamImpl(QTypePtr absl_nonnull value_qtype,
                       size_t initial_capacity)
    : Stream(value_qtype),
      default_chunk_capacity_(GetDefaultChunkCapacity(value_qtype)) {
  if (initial_capacity > 0) {
    // The `initial_capacity` is expected to match the stream size (see
    // MakeStream()), so we initialize the chunk upfront.
    last_chunk_ = &first_chunk_.emplace(value_qtype, initial_capacity,
                                        /*lazy_init=*/false);
    last_chunk_capacity_ = last_chunk_->capacity();
  }
}

StreamReaderPtr absl_nonnull StreamImpl::MakeReader() {
  return std::make_unique<Reader>(shared_from_this());
}

StreamImpl::InternalWriteResult StreamImpl::InternalWrite(
    arolla::TypedRef value) {
  DCHECK_EQ(value.GetType(), value_qtype());  // Tested by the public method.
  std::vector<absl::AnyInvocable<void() &&>> callbacks;
  {
    absl::MutexLock lock(mutex_);
    if (status_.has_value()) {
      return InternalWriteResult::kClosed;
    }
    if (last_chunk_size_ == last_chunk_capacity_) [[unlikely]] {
      // Only the first chunk is initialized lazily; a stream that has
      // filled a chunk is likely to fill the next one too.
      if (last_chunk_ == nullptr) {
        last_chunk_ = &first_chunk_.emplace(
            value_qtype(), default_chunk_capacity_, /*lazy_init=*/true);
      } else {
        last_chunk_ = &other_chunks_.emplace_back(
            value_qtype(), default_chunk_capacity_, /*lazy_init=*/false);
      }
      last_chunk_capacity_ = last_chunk_->capacity();
      last_chunk_size_ = 0;
    }
    DCHECK(last_chunk_ != nullptr);
    last_chunk_->AppendRef(last_chunk_size_++, value);
    callbacks_.swap(callbacks);
  }
  for (auto& callback : callbacks) {
    std::move(callback)();
  }
  return InternalWriteResult::kOk;
}

void StreamImpl::InternalClose(absl::Status&& status) {
  std::vector<absl::AnyInvocable<void() &&>> callbacks;
  {
    absl::MutexLock lock(mutex_);
    DCHECK(!status_.has_value());  // Protected by the atomic flag `closed_`
    if (status_.has_value()) {     // in the writer.
      return;
    }
    status_.emplace(std::move(status));
    callbacks_.swap(callbacks);
  }
  for (auto& callback : callbacks) {
    std::move(callback)();
  }
}

StreamImpl::Writer::Writer(
    const std::shared_ptr<StreamImpl> absl_nonnull& stream)
    : StreamWriter(stream->value_qtype()), weak_stream_(stream) {}

bool StreamImpl::Writer::Orphaned() const {
  return closed_.test(std::memory_order_relaxed) || weak_stream_.expired();
}

void StreamImpl::Writer::Write(TypedRef value) {
  if (value.GetType() != value_qtype()) {
    LOG(FATAL) << "expected a value of type " << value_qtype()->name()
               << ", got " << value.GetType()->name();
  }
  auto stream = weak_stream_.lock();
  if (stream == nullptr) {
    return;  // The stream object is gone.
  }
  switch (stream->InternalWrite(value)) {
    case InternalWriteResult::kOk:
      return;
    case InternalWriteResult::kClosed:
      LOG(FATAL) << "writing to a closed stream";
      return;
  }
  ABSL_UNREACHABLE();
}

bool StreamImpl::Writer::TryWrite(arolla::TypedRef value) {
  if (value.GetType() != value_qtype()) {
    LOG(FATAL) << "expected a value of type " << value_qtype()->name()
               << ", got " << value.GetType()->name();
  }
  auto stream = weak_stream_.lock();
  if (stream == nullptr) {
    return false;  // The stream object is gone.
  }
  switch (stream->InternalWrite(value)) {
    case InternalWriteResult::kOk:
      return true;
    case InternalWriteResult::kClosed:
      return false;
  }
  ABSL_UNREACHABLE();
}

void StreamImpl::Writer::Close(absl::Status status) && {
  if (closed_.test_and_set(std::memory_order_relaxed)) {
    LOG(FATAL) << "closing a closed stream";
  }
  if (auto stream = weak_stream_.lock()) {
    stream->InternalClose(std::move(status));
  }
}

void StreamImpl::Writer::TryClose(absl::Status status) {
  if (closed_.test_and_set(std::memory_order_relaxed)) {
    return;  // The stream is already closed.
  }
  if (auto stream = weak_stream_.lock()) {
    stream->InternalClose(std::move(status));
  }
}

StreamImpl::Writer::~Writer() {
  if (closed_.test_and_set(std::memory_order_relaxed)) {
    return;
  }
  if (auto stream = weak_stream_.lock()) {
    stream->InternalClose(absl::CancelledError("orphaned"));
  }
}

StreamImpl::Reader::Reader(std::shared_ptr<StreamImpl> absl_nonnull stream)
    : stream_(std::move(stream)) {
  static absl::NoDestructor<Chunk> kEmptyUnitChunk(GetNothingQType(), 0,
                                                   /*lazy_init=*/true);
  chunk_ = kEmptyUnitChunk.get();
}

StreamReader::TryReadResult StreamImpl::Reader::TryRead() {
  if (offset_ < known_size_) [[likely]] {
    return chunk_->GetRef(offset_++);
  }
  absl::MutexLock lock(stream_->mutex_);
  Update();
  TryReadResult result;
  if (offset_ < known_size_) {
    result = chunk_->GetRef(offset_++);
    Update();
  } else if (stream_->status_.has_value()) {
    result = *stream_->status_;
  }
  return result;
}

void StreamImpl::Reader::Update() {
  const size_t chunk_count = stream_->chunk_count();
  if (offset_ == chunk_->capacity() && next_chunk_index_ < chunk_count) {
    chunk_ = &stream_->chunk(next_chunk_index_++);
    offset_ = 0;
  }
  if (next_chunk_index_ == chunk_count) {
    known_size_ = stream_->last_chunk_size_;
  } else {
    known_size_ = chunk_->capacity();
  }
}

void StreamImpl::Reader::SubscribeOnce(
    absl::AnyInvocable<void() &&>&& callback) {
  {
    absl::MutexLock lock(stream_->mutex_);
    if (!stream_->status_.has_value()) {
      Update();
      if (offset_ == known_size_) {
        stream_->callbacks_.push_back(std::move(callback));
        return;
      }
    }
  }
  std::move(callback)();
}

}  // namespace

std::pair<StreamPtr absl_nonnull, StreamWriterPtr absl_nonnull> MakeStream(
    arolla::QTypePtr absl_nonnull value_qtype, size_t initial_capacity) {
  auto stream_impl =
      std::make_shared<StreamImpl>(value_qtype, initial_capacity);
  std::pair<StreamPtr, StreamWriterPtr> result;
  result.second = std::make_unique<StreamImpl::Writer>(stream_impl);
  result.first = std::move(stream_impl);
  return result;
}

}  // namespace koladata::functor::parallel

namespace arolla {

void FingerprintHasherTraits<koladata::functor::parallel::StreamPtr>::
operator()(FingerprintHasher* hasher,
           const koladata::functor::parallel::StreamPtr& value) const {
  if (value != nullptr) {
    hasher->Combine(value->uuid());
  }
}

ReprToken ReprTraits<koladata::functor::parallel::StreamPtr>::operator()(
    const koladata::functor::parallel::StreamPtr& value) const {
  if (value == nullptr) {
    return ReprToken{"stream{nullptr}"};
  }
  return ReprToken{absl::StrCat("stream[", value->value_qtype()->name(), "]")};
}

}  // namespace arolla
