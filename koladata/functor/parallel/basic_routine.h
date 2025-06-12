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
#ifndef KOLADATA_FUNCTOR_PARALLEL_BASIC_ROUTINE_H_
#define KOLADATA_FUNCTOR_PARALLEL_BASIC_ROUTINE_H_

#include <memory>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {

// An auxiliary interface that simplifies the creation of basic routines,
// structured around the following cycle:
//
//   auto reader = routine_hooks->Start();
//   while (reader != nullptr) {
//     WaitFor(reader);  // Waits until `reader` is ready.
//     reader = routine_hooks->Resume(std::move(reader));
//   }
//
// Use `StartBasicRoutine(executor, routine_hooks)` to start the routine.
//
// `Start` and `Resume` encapsulate the routine's useful work. When either
// method returns a stream reader, the routine pauses and resumes execution once
// that reader is ready. (Performance tip: to minimise dispatching overhead, do
// as much work as possible while the reader is already ready.)
//
// Importantly, there's no strong guarantee that `Resume` will be invoked (e.g.,
// if the computation is interrupted or the executor shuts down), so resource
// management must not depend on it.
//
// Threading:
// * `Start` and `Resume` are always invoked on the executor. Their calls are
//   serialized, meaning one won't begin until the previous call has returned.
// * `Interrupted` and `OnCancel` are invoked on unspecified threads.
//
class BasicRoutineHooks {
 public:
  BasicRoutineHooks() = default;
  virtual ~BasicRoutineHooks() = default;

  // Disallow copy and move.
  BasicRoutineHooks(BasicRoutineHooks&&) = delete;
  BasicRoutineHooks& operator=(BasicRoutineHooks&&) = delete;

  // The routine calls this method before doing any scheduling to ensure
  // the computation is still needed.
  //
  // Returning `true` means the computation is no longer needed and the routine
  // should stop.
  virtual bool Interrupted() const = 0;

  // The routine calls this method when it receives a cancellation signal.
  // The provided `status` will always indicate an error (i.e., `status.ok()` is
  // false).
  //
  // Note: This method should promptly return control, since more subsystems
  // may wait for the cancellation signal.
  virtual void OnCancel(absl::Status&& status) = 0;

  // The methods `Start` and `Resume` encapsulate the useful work performed by
  // the routine. The routine calls `Start` the first time, and `Resume` every
  // time the previously returned stream reader becomes ready. Returning
  // `nullptr` indicates that the routine should stop.
  virtual StreamReaderPtr absl_nullable Start() = 0;

  // The routine calls this method when previously returned stream reader
  // becomes ready.
  virtual StreamReaderPtr absl_nullable Resume(  // clang-format hint
      StreamReaderPtr absl_nonnull reader) = 0;
};

// Starts a basic routine on the given executor within the current cancellation
// context.
void StartBasicRoutine(
    ExecutorPtr absl_nonnull executor,
    std::unique_ptr<BasicRoutineHooks> absl_nonnull routine_hooks);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_BASIC_ROUTINE_H_
