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
#ifndef KOLADATA_FUNCTOR_PARALLEL_FUTURE_H_
#define KOLADATA_FUNCTOR_PARALLEL_FUTURE_H_

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {

class Future;
class FutureWriter;

using FuturePtr = std::shared_ptr<Future>;

// This class is a future in the cooperative execution environment. One can
// register consumers to be notified when the value is ready.
//
// The value is either an arolla::TypedValue or an error
// (represented as absl::StatusOr).
//
// This class is thread-safe. Once the value is set, it is immutable.
// It must be created through MakeFuture.
class Future {
  struct PrivateConstructorTag {};

 public:
  using ConsumerFn =
      absl::AnyInvocable<void(absl::StatusOr<arolla::TypedValue>) &&>;

  // Creates a future with the given value qtype, without a value.
  Future(PrivateConstructorTag, arolla::QTypePtr value_qtype)
      : value_qtype_(value_qtype) {};

  // Adds a consumer to be notified when the value is ready. If the value is
  // already set, the consumer is called immediately.
  void AddConsumer(ConsumerFn&& consumer);

  // Gets the value of the future for testing purposes. Returns an error if the
  // future is not ready yet. Real code should rely on AddConsumer instead.
  absl::StatusOr<arolla::TypedValue> GetValueForTesting();

  // Returns the qtype of the value of the future.
  arolla::QTypePtr value_qtype() const { return value_qtype_; }

  // Returns a randomly generated unique identifier of the future.
  arolla::Fingerprint uuid() const { return uuid_; }

  // Disable copy and move.
  Future(const Future&) = delete;
  Future(Future&&) = delete;
  Future& operator=(const Future&) = delete;
  Future& operator=(Future&&) = delete;

 private:
  // Sets the value of the future. Notifies all consumers. Must be called
  // at most once.
  void SetValue(absl::StatusOr<arolla::TypedValue> value);

  arolla::QTypePtr value_qtype_;
  arolla::Fingerprint uuid_ = arolla::RandomFingerprint();
  absl::Mutex lock_;
  std::optional<absl::StatusOr<arolla::TypedValue>> value_
      ABSL_GUARDED_BY(lock_) = std::nullopt;
  // This will be empty after the value is set.
  std::vector<ConsumerFn> consumers_ ABSL_GUARDED_BY(lock_);

  friend class FutureWriter;
  friend std::pair<FuturePtr, FutureWriter> MakeFuture(
      arolla::QTypePtr value_qtype);
};

// This class can be used to set the value of a future. It is created together
// with the future and is destroyed when SetValue is called, to guarantee that
// we only set the value at most once.
// It must be created through MakeFuture.
// This is called a Promise in C++, so we should probably rename it to Promise
// if we ever make this API public.
class FutureWriter {
 public:
  void SetValue(absl::StatusOr<arolla::TypedValue> value) &&;

  // If the writer is destroyed before SetValue is called, we write an
  // "orphaned" error to the future.
  ~FutureWriter();

  // Returns the qtype of the value of the future.
  arolla::QTypePtr value_qtype() const { return future_->value_qtype(); }

  // Allow move but not copy.
  FutureWriter(FutureWriter&&) = default;
  FutureWriter& operator=(FutureWriter&&) = default;
  FutureWriter(const FutureWriter&) = delete;
  FutureWriter& operator=(const FutureWriter&) = delete;

 private:
  explicit FutureWriter(FuturePtr absl_nonnull future)
      : future_(std::move(future)) {}

  FuturePtr future_;

  friend std::pair<FuturePtr, FutureWriter> MakeFuture(
      arolla::QTypePtr value_qtype);
};

// Creates a future and a writer for it.
std::pair<FuturePtr, FutureWriter> MakeFuture(arolla::QTypePtr value_qtype);

// Returns a stream from the given future.
StreamPtr absl_nonnull StreamFromFuture(const FuturePtr absl_nonnull& future);

}  // namespace koladata::functor::parallel

namespace arolla {
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(
    koladata::functor::parallel::FuturePtr);
AROLLA_DECLARE_REPR(koladata::functor::parallel::FuturePtr);
}  // namespace arolla

#endif  // KOLADATA_FUNCTOR_PARALLEL_FUTURE_H_
