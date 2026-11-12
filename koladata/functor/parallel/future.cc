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
#include "koladata/functor/parallel/future.h"

#include <memory>
#include <optional>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/once_callbacks.h"
#include "arolla/util/repr.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {

void Future::AddConsumer(ConsumerFn&& consumer) {
  {
    absl::MutexLock l(lock_);
    if (!value_.has_value()) {
      callbacks_.Add([this, consumer = std::move(consumer)]() mutable {
        std::move(consumer)(*value_);
      });
      return;
    }
  }
  // Important to not call this while holding the lock.
  std::move(consumer)(*value_);
}

void Future::SetValue(absl::StatusOr<arolla::TypedValue> value) {
  if (value.ok() && value->GetType() != value_qtype()) {
    LOG(FATAL) << "value type " << value->GetType()->name()
               << " does not match future type " << value_qtype()->name();
  }
  arolla::OnceCallbacks callbacks;
  {
    absl::MutexLock l(lock_);
    if (value_) {
      LOG(FATAL) << "future already has a value";
    }
    value_ = std::move(value);
    swap(callbacks_, callbacks);
  }
  // Important to not call this while holding the lock.
  std::move(callbacks).Notify();
}

std::optional<absl::StatusOr<const arolla::TypedValue&>> Future::PeekValue() {
  absl::MutexLock l(lock_);
  if (!value_.has_value()) {
    return std::nullopt;
  }
  if (!value_->ok()) {
    return value_->status();
  }
  return **value_;
}

absl::StatusOr<arolla::TypedValue> Future::GetValueForTesting() {
  absl::MutexLock l(lock_);
  if (!value_.has_value()) {
    return absl::InvalidArgumentError("future has no value");
  }
  return *value_;
}

void FutureWriter::SetValue(absl::StatusOr<arolla::TypedValue> value) && {
  if (future_ == nullptr) {
    LOG(FATAL) << "Trying to set value on a moved-from FutureWriter";
  }
  future_->SetValue(std::move(value));
  future_.reset();
}

FutureWriter::~FutureWriter() {
  if (future_ != nullptr) {
    future_->SetValue(absl::CancelledError("orphaned"));
  }
}

std::pair<FuturePtr, FutureWriter> MakeFuture(arolla::QTypePtr value_qtype) {
  auto future =
      std::make_shared<Future>(Future::PrivateConstructorTag(), value_qtype);
  auto future_writer = FutureWriter(future);
  return {std::move(future), std::move(future_writer)};
}

class FutureStreamReader final : public StreamReader {
 public:
  explicit FutureStreamReader(FuturePtr future) : future_(std::move(future)) {}

  ~FutureStreamReader() final {
    if (!token_) [[likely]] {
      return;
    }
    arolla::OnceCallbacks callbacks;  // Destroyed outside of the lock.
    {
      absl::MutexLock l(future_->lock_);
      if (!future_->value_.has_value()) {
        callbacks = future_->callbacks_.Remove(std::move(token_));
      }
    }
  }

  TryReadResult TryRead() final {
    if (!read_) {
      absl::MutexLock l(future_->lock_);
      if (!future_->value_.has_value()) {
        return TryReadResult{};  // Not ready yet.
      }
      token_ = {};
      read_ = true;
      if (future_->value_->status().ok()) {
        // We return a reference to the value stored inside the future.
        return (**future_->value_).AsRef();
      }
      status_ = future_->value_->status();
    }
    return status_;
  }

  void SubscribeOnce(arolla::OnceCallback absl_nonnull callback) final {
    if (!read_) {
      absl::MutexLock l(future_->lock_);
      if (!future_->value_.has_value()) {
        token_ =
            future_->callbacks_.Add(std::move(callback), std::move(token_));
        return;
      }
      token_ = {};
    }
    std::move(callback).Notify();
  }

 private:
  const FuturePtr future_;
  arolla::OnceCallbacks::Token token_ ABSL_GUARDED_BY(future_->lock_);
  absl::Status status_ = absl::OkStatus();
  bool read_ = false;
};

StreamReaderPtr absl_nonnull Future::MakeReader() {
  return std::make_unique<FutureStreamReader>(shared_from_this());
}

}  // namespace koladata::functor::parallel

namespace arolla {

void FingerprintHasherTraits<koladata::functor::parallel::FuturePtr>::
operator()(FingerprintHasher* hasher,
           const koladata::functor::parallel::FuturePtr& value) const {
  if (value != nullptr) {
    hasher->Combine(value->uuid());
  }
}

ReprToken ReprTraits<koladata::functor::parallel::FuturePtr>::operator()(
    const koladata::functor::parallel::FuturePtr& value) const {
  if (value == nullptr) {
    return ReprToken{"future{nullptr}"};
  }
  // Maybe include the contents of the future here in the future?
  return ReprToken{absl::StrCat("future[", value->value_qtype()->name(), "]")};
}

}  // namespace arolla
