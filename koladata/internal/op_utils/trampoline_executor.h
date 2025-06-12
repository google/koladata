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
#ifndef KOLADATA_INTERNAL_OP_UTILS_TRAMPOLINE_EXECUTOR_H_
#define KOLADATA_INTERNAL_OP_UTILS_TRAMPOLINE_EXECUTOR_H_

#include <utility>
#include <vector>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

// A helper class that queues up callbacks to run in order after a function
// returns (and callbacks from those callbacks, etc.). This gives us a way to
// write recursive code without on-stack recursion, in a similar structure to
// the naive recursive code.
//
// This is an extension of the pattern described in
// https://en.wikipedia.org/wiki/Trampoline_(computing)#High-level_programming
// where we can enqueue multiple callbacks ("thunks") instead of returning one.
// This is not fundamentally more powerful for removing on-stack recursion, but
// does make it possible to call multiple subroutines that enqueue their own
// callbacks (or conditionally enqueue callbacks), without adding extra layers
// of callbacks to force the intended execution order.
//
// Example:
//
// === Recursive Program ===
//
// absl::StatusOr<int> f(int x) {
//   if (x > 0) {
//     ASSIGN_OR_RETURN(int y, f(x - 1));
//     return x * y;
//   } else {
//     return 1;
//   }
// }
//
// === TrampolineExecutor Program ===
//
// absl::Status f(int x, TrampolineExecutor& executor, int& result) {
//   if (x > 0) {
//     auto y = std::make_unique<int>();
//     executor.Enqueue([&executor, x, y_ptr = y.get()]() {
//       // Perform the call to `f` in a callback to avoid on-stack recursion.
//       return f(x - 1, executor, *y_ptr);
//     });
//     executor.Enqueue([x, y = std::move(y), &result]() {
//       // This will be run after the previous callback returns, so we can use
//       // the contents of `*y`.
//       result = x * (*y);
//       return absl::OkStatus();
//     });
//   } else {
//     result = 1;
//   }
//   return absl::OkStatus();
// }
//
class TrampolineExecutor final {
 public:
  using Callback = absl::AnyInvocable<absl::Status() &&>;

  // Creates a TrampolineExecutor, runs `root_callback` with it as an argument,
  // and then executes any enqueued callbacks (which may enqueue more callbacks)
  // until all callbacks have returned or some callback has returned a non-OK
  // status.
  template <typename F>
  static absl::Status Run(F root_callback) {
    TrampolineExecutor self;
    RETURN_IF_ERROR(std::move(root_callback)(self));
    return std::move(self).Run();
  }

  // Disable copy and assign.
  TrampolineExecutor(const TrampolineExecutor&) = delete;
  TrampolineExecutor& operator=(const TrampolineExecutor&) = delete;
  TrampolineExecutor(TrampolineExecutor&&) = delete;
  TrampolineExecutor& operator=(TrampolineExecutor&&) = delete;

  // Enqueues a callback (typically a lambda), which must be movable, and must
  // be callable with `absl::Status() &&`.
  //
  // If this is called from the currently-executing callback, the enqueued
  // callback will be called exactly once after both the currently-executing
  // callback has returned and any callbacks previously enqueued by the
  // currently-executing callback have returned. If any callbacks return non-OK
  // status before this callback is called, it is destroyed without being
  // called.
  void Enqueue(Callback callback);

 private:
  TrampolineExecutor() = default;

  absl::Status Run() &&;
  void FlushBuffer();

  std::vector<Callback> stack_;
  std::vector<Callback> buffer_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_TRAMPOLINE_EXECUTOR_H_
