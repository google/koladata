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
#ifndef KOLADATA_FUNCTOR_PARALLEL_EXECUTOR_H_
#define KOLADATA_FUNCTOR_PARALLEL_EXECUTOR_H_

#include <memory>
#include <string>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace koladata::functor::parallel {

// An abstract class for executors. It assigns a random fingerprint to each
// executor instance, but otherwise delegates the behavior to the derived
// classes.
class Executor {
 public:
  // Note that there is no error propagation logic in the executor, since
  // the errors can be specific to one of many tasks executed by the same
  // executor. The error propagation should be done inside the task function.
  using TaskFn = absl::AnyInvocable<void() &&>;

  // Returns the uuid of the executor. This is a randomly generated
  // fingerprint, unique for each executor instance, that is used to compute
  // the fingerprint of the executor QValue.
  const arolla::Fingerprint& uuid() const { return uuid_; }

  // Runs a given task on the executor. This method is thread-safe.
  virtual absl::Status Schedule(TaskFn task_fn) = 0;

  // Returns the string representation of the executor.
  virtual std::string Repr() const = 0;

  virtual ~Executor() = default;

 private:
  arolla::Fingerprint uuid_ = arolla::RandomFingerprint();
};

using ExecutorPtr = std::shared_ptr<Executor>;

}  // namespace koladata::functor::parallel

namespace arolla {

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(
    koladata::functor::parallel::ExecutorPtr);
AROLLA_DECLARE_REPR(koladata::functor::parallel::ExecutorPtr);
AROLLA_DECLARE_SIMPLE_QTYPE(EXECUTOR, koladata::functor::parallel::ExecutorPtr);

}  // namespace arolla

#endif  // KOLADATA_FUNCTOR_PARALLEL_EXECUTOR_H_
