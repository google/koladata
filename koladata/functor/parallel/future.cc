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
#include <vector>

#include "absl/base/nullability.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {

void Future::AddConsumer(ConsumerFn&& consumer) {
  std::optional<absl::StatusOr<arolla::TypedValue>> value;
  {
    absl::MutexLock l(lock_);
    if (!value_) {
      consumers_.push_back(std::move(consumer));
      return;
    }
    value = value_;
  }
  // Important to not call this while holding the lock.
  std::move(consumer)(std::move(*value));
}

void Future::SetValue(absl::StatusOr<arolla::TypedValue> value) {
  if (value.ok() && value->GetType() != value_qtype_) {
    LOG(FATAL) << "value type " << value->GetType()->name()
               << " does not match future type " << value_qtype_->name();
  }
  std::vector<ConsumerFn> consumers;
  {
    absl::MutexLock l(lock_);
    if (value_) {
      LOG(FATAL) << "future already has a value";
    }
    value_ = value;
    consumers.swap(consumers_);
  }
  for (auto& consumer : consumers) {
    // Important to not call this while holding the lock.
    std::move(consumer)(value);
  }
}

absl::StatusOr<arolla::TypedValue> Future::GetValueForTesting() {
  absl::MutexLock l(lock_);
  if (!value_) {
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

StreamPtr absl_nonnull StreamFromFuture(const FuturePtr absl_nonnull& future) {
  // Note: Consider implementing the stream interface directly over the future
  // to avoid copying values into the stream buffer.
  auto [result, writer] = MakeStream(future->value_qtype(), 1);
  future->AddConsumer([writer = std::move(writer)](
                          absl::StatusOr<arolla::TypedValue> value) mutable {
    if (value.ok()) {
      writer->Write(value->AsRef());
      std::move(*writer).Close();
    } else {
      std::move(*writer).Close(std::move(value).status());
    }
  });
  return std::move(result);
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
