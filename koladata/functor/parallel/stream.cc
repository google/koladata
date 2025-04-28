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

#include <cstddef>
#include <deque>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/memory.h"

namespace koladata::functor::parallel {
namespace {

using ::arolla::AlignedAlloc;
using ::arolla::MallocPtr;
using ::arolla::QTypePtr;
using ::arolla::TypedRef;

constexpr size_t kChunkDefaultByteCapacity = 512;

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
                         public Stream,
                         public StreamWriter {
 public:
  class Reader;

  StreamImpl(QTypePtr value_qtype, size_t initial_capacity);

  StreamReaderPtr MakeReader() final ABSL_LOCKS_EXCLUDED(mutex_);

  void Write(TypedRef value) final ABSL_LOCKS_EXCLUDED(mutex_);

  void Close(absl::Status status) final ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  const size_t default_capacity_;
  absl::Mutex mutex_;
  std::deque<Chunk> chunks_ ABSL_GUARDED_BY(mutex_);
  size_t last_chunk_offset_ ABSL_GUARDED_BY(mutex_) = 0;
  std::optional<absl::Status> status_ ABSL_GUARDED_BY(mutex_);
  std::vector<absl::AnyInvocable<void() &&>> callbacks_ ABSL_GUARDED_BY(mutex_);
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

  size_t next_chunk_index_ = 1;
  Chunk* chunk_ = nullptr;
  size_t offset_ = 0;
  size_t known_size_ = 0;
};

Chunk::Chunk(QTypePtr value_qtype, size_t capacity)
    : value_qtype_(value_qtype),
      capacity_(capacity),
      value_bytesize_(value_qtype->type_layout().AllocSize()),
      storage_(AlignedAlloc(value_qtype->type_layout().AllocAlignment(),
                            capacity * value_bytesize_)) {
  value_qtype_->type_layout().InitializeAlignedAllocN(storage_.get(),
                                                      capacity_);
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
      default_capacity_((kChunkDefaultByteCapacity +
                         value_qtype->type_layout().AllocSize() - 1) /
                        value_qtype->type_layout().AllocSize()) {
  chunks_.emplace_back(
      value_qtype, initial_capacity > 0 ? initial_capacity : default_capacity_);
}

void StreamImpl::Write(TypedRef value) {
  if (value.GetType() != value_qtype()) {
    LOG(FATAL) << "expected a value of type " << value_qtype()->name()
               << ", got " << value.GetType()->name();
  }
  std::vector<absl::AnyInvocable<void() &&>> callbacks;
  {
    absl::MutexLock lock(&mutex_);
    if (status_.has_value()) {
      LOG(FATAL) << "writing to a closed stream";
    }
    if (chunks_.back().capacity() == last_chunk_offset_) {
      chunks_.emplace_back(value_qtype(), default_capacity_);
      last_chunk_offset_ = 0;
    }
    chunks_.back().SetRef(last_chunk_offset_++, value);
    callbacks_.swap(callbacks);
  }
  for (auto& callback : callbacks) {
    std::move(callback)();
  }
}

void StreamImpl::Close(absl::Status status) {
  std::vector<absl::AnyInvocable<void() &&>> callbacks;
  {
    absl::MutexLock lock(&mutex_);
    if (status_.has_value()) {
      LOG(FATAL) << "closing a closed stream";
    }
    status_.emplace(std::move(status));
    callbacks_.swap(callbacks);
  }
  for (auto& callback : callbacks) {
    std::move(callback)();
  }
}

StreamReaderPtr StreamImpl::MakeReader() {
  return std::make_shared<Reader>(shared_from_this());
}

StreamImpl::Reader::Reader(std::shared_ptr<StreamImpl> stream)
    : stream_(std::move(stream)) {
  absl::MutexLock lock(&stream_->mutex_);
  chunk_ = &stream_->chunks_.front();
  if (next_chunk_index_ == stream_->chunks_.size()) {
    known_size_ = stream_->last_chunk_offset_;
  } else {
    known_size_ = chunk_->capacity();
  }
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
    known_size_ = stream_->last_chunk_offset_;
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
  auto tmp = std::make_shared<StreamImpl>(value_qtype, initial_capacity);
  std::pair<StreamPtr, StreamWriterPtr> result;
  result.second = tmp;
  result.first = std::move(tmp);
  return result;
}

}  // namespace koladata::functor::parallel
