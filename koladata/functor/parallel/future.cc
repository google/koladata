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
#include "koladata/functor/parallel/future.h"

#include <optional>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace koladata::functor::parallel {

void Future::AddConsumer(ConsumerFn consumer) {
  std::optional<absl::StatusOr<arolla::TypedValue>> value;
  {
    absl::MutexLock l(&lock_);
    if (!value_) {
      consumers_.push_back(std::move(consumer));
      return;
    }
    value = value_;
  }
  // Important to not call this while holding the lock.
  consumer(std::move(*value));
}

absl::Status Future::SetValue(absl::StatusOr<arolla::TypedValue> value) {
  if (value.ok() && value->GetType() != value_qtype_) {
    return absl::InvalidArgumentError(
        absl::StrCat("value type ", value->GetType()->name(),
                     " does not match future type ", value_qtype_->name()));
  }
  std::vector<ConsumerFn> consumers;
  {
    absl::MutexLock l(&lock_);
    if (value_) {
      return absl::InvalidArgumentError("future already has a value");
    }
    value_ = value;
    consumers.swap(consumers_);
  }
  for (auto& consumer : consumers) {
    // Important to not call this while holding the lock.
    consumer(value);
  }
  return absl::OkStatus();
}

absl::StatusOr<arolla::TypedValue> Future::GetValueForTesting() {
  absl::MutexLock l(&lock_);
  if (!value_) {
    return absl::InvalidArgumentError("future has no value");
  }
  return *value_;
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
