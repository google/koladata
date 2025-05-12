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
#include "koladata/functor/parallel/stream.h"

#include <atomic>
#include <cstddef>
#include <deque>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
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

using ::arolla::AlignedAlloc;
using ::arolla::GetNothingQType;
using ::arolla::MallocPtr;
using ::arolla::QTypePtr;
using ::arolla::TypedRef;

size_t GetDefaultChunkCapacity(QTypePtr value_qtype) {
  constexpr size_t kDefaultByteCapacity = 512;
  const size_t bytesize = value_qtype->type_layout().AllocSize();
  return bytesize ? (kDefaultByteCapacity + bytesize - 1) / bytesize
                  : kDefaultByteCapacity;
}

class Chunk {
 public:
  Chunk(QTypePtr value_qtype, size_t capacity);
  ~Chunk();

  // Disallow copy and move.
  Chunk(const Chunk&) = delete;
  Chunk& operator=(const Chunk&) = delete;

  size_t capacity() const { return capacity_; }

  TypedRef GetRef(size_t i) const;
  void SetRef(size_t i, TypedRef ref);

 private:
  const QTypePtr value_qtype_;
  const size_t capacity_;
  const size_t value_bytesize_;
  const MallocPtr storage_;
};

// Combined implementation of Stream and StreamWriter.
class StreamImpl final : public std::enable_shared_from_this<StreamImpl>,
                         public Stream {
 public:
  class Writer;
  class Reader;

  StreamImpl(QTypePtr value_qtype, size_t initial_capacity);

  StreamReaderPtr MakeReader() final ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  enum class [[nodiscard]] InternalWriteResult { kOk, kClosed };
  InternalWriteResult InternalWrite(arolla::TypedRef value);

  void InternalClose(absl::Status&& status);

  const size_t default_chunk_capacity_;

  absl::Mutex mutex_;
  std::deque<Chunk> chunks_ ABSL_GUARDED_BY(mutex_);
  size_t last_chunk_size_ ABSL_GUARDED_BY(mutex_) = 0;
  std::optional<absl::Status> status_ ABSL_GUARDED_BY(mutex_);
  std::vector<absl::AnyInvocable<void() &&>> callbacks_ ABSL_GUARDED_BY(mutex_);
};

// A stream writer implementation.
class StreamImpl::Writer final : public StreamWriter {
 public:
  explicit Writer(const std::shared_ptr<StreamImpl>& stream);
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
  explicit Reader(std::shared_ptr<StreamImpl> stream);

  TryReadResult TryRead() final;

  void SubscribeOnce(absl::AnyInvocable<void() &&>&& callback) final;

 private:
  void Update() ABSL_EXCLUSIVE_LOCKS_REQUIRED(stream_->mutex_);

  const std::shared_ptr<StreamImpl> stream_;

  size_t next_chunk_index_ = 0;
  Chunk* chunk_;
  size_t offset_ = 0;
  size_t known_size_ = 0;
};

Chunk::Chunk(QTypePtr value_qtype, size_t capacity)
    : value_qtype_(value_qtype),
      capacity_(capacity),
      value_bytesize_(value_qtype->type_layout().AllocSize()),
      storage_(AlignedAlloc(value_qtype->type_layout().AllocAlignment(),
                            capacity * value_bytesize_)) {
  value_qtype->type_layout().InitializeAlignedAllocN(storage_.get(), capacity_);
}

Chunk::~Chunk() {
  // Note: Assuming that no concurrent access can happen to the chunk during
  // the destruction.
  value_qtype_->type_layout().DestroyAllocN(storage_.get(), capacity_);
}

TypedRef Chunk::GetRef(size_t i) const {
  DCHECK_LT(i, capacity_);
  return TypedRef::UnsafeFromRawPointer(
      value_qtype_,
      reinterpret_cast<const char*>(storage_.get()) + i * value_bytesize_);
}

void Chunk::SetRef(size_t i, TypedRef ref) {
  DCHECK_LT(i, capacity_);
  DCHECK_EQ(ref.GetType(), value_qtype_);
  value_qtype_->UnsafeCopy(
      ref.GetRawPointer(),
      reinterpret_cast<char*>(storage_.get()) + i * value_bytesize_);
}

StreamImpl::StreamImpl(QTypePtr value_qtype, size_t initial_capacity)
    : Stream(value_qtype),
      default_chunk_capacity_(GetDefaultChunkCapacity(value_qtype)) {
  if (initial_capacity > 0) {
    chunks_.emplace_back(value_qtype, initial_capacity);
  }
}

StreamReaderPtr StreamImpl::MakeReader() {
  return std::make_unique<Reader>(shared_from_this());
}

StreamImpl::InternalWriteResult StreamImpl::InternalWrite(
    arolla::TypedRef value) {
  DCHECK_EQ(value.GetType(), value_qtype());  // Tested by the public method.
  std::vector<absl::AnyInvocable<void() &&>> callbacks;
  {
    absl::MutexLock lock(&mutex_);
    if (status_.has_value()) {
      return InternalWriteResult::kClosed;
    }
    if (chunks_.empty() || chunks_.back().capacity() == last_chunk_size_) {
      chunks_.emplace_back(value_qtype(), default_chunk_capacity_);
      last_chunk_size_ = 0;
    }
    chunks_.back().SetRef(last_chunk_size_++, value);
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
    absl::MutexLock lock(&mutex_);
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

StreamImpl::Writer::Writer(const std::shared_ptr<StreamImpl>& stream)
    : StreamWriter(stream->value_qtype()), weak_stream_(stream) {}

bool StreamImpl::Writer::Orphaned() const { return weak_stream_.expired(); }

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

StreamImpl::Reader::Reader(std::shared_ptr<StreamImpl> stream)
    : stream_(std::move(stream)) {
  static absl::NoDestructor<Chunk> kEmptyUnitChunk(GetNothingQType(), 0);
  chunk_ = kEmptyUnitChunk.get();
}

StreamReader::TryReadResult StreamImpl::Reader::TryRead() {
  if (offset_ < known_size_) [[likely]] {
    return chunk_->GetRef(offset_++);
  }
  absl::MutexLock lock(&stream_->mutex_);
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
  if (offset_ == chunk_->capacity() &&
      next_chunk_index_ < stream_->chunks_.size()) {
    chunk_ = &stream_->chunks_[next_chunk_index_++];
    offset_ = 0;
  }
  if (next_chunk_index_ == stream_->chunks_.size()) {
    known_size_ = stream_->last_chunk_size_;
  } else {
    known_size_ = chunk_->capacity();
  }
}

void StreamImpl::Reader::SubscribeOnce(
    absl::AnyInvocable<void() &&>&& callback) {
  {
    absl::MutexLock lock(&stream_->mutex_);
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

std::pair<StreamPtr, StreamWriterPtr> MakeStream(arolla::QTypePtr value_qtype,
                                                 size_t initial_capacity) {
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
