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
#ifndef KOLADATA_INTERNAL_OP_UTILS_ERROR_H_
#define KOLADATA_INTERNAL_OP_UTILS_ERROR_H_

#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "arolla/util/status.h"

namespace koladata::internal {

// Returns a status with a Koda error payload containing the given error message
// and the given status as the cause.
//
// By default, the error message is taken from the status, but can be overridden
// by passing a custom error_message.
absl::Status OperatorEvalError(absl::Status status,
                               absl::string_view operator_name);
absl::Status OperatorEvalError(absl::Status status,
                               absl::string_view operator_name,
                               absl::string_view error_message);

// Returns a absl::InvalidArgumentError status with a Koda error payload
// containing the given error message.
absl::Status OperatorEvalError(absl::string_view operator_name,
                               absl::string_view error_message);

// Wraps the given function, so all its errors are converted into
// OperatorEvalError.
template <typename Ret, typename... Args>
class ReturnsOperatorEvalError {
 public:
  ReturnsOperatorEvalError(std::string name, Ret (*func)(Args...))
      : name_(std::move(name)), func_(func) {}

  Ret operator()(const Args&... args) const {
    if constexpr (arolla::IsStatusOrT<Ret>::value) {
      auto result = func_(args...);
      if (!result.ok()) {
        return OperatorEvalError(result.status(), name_);
      }
      return result;
    } else {
      return func_(args...);
    }
  }

 private:
  std::string name_;
  Ret (*func_)(Args...);
};

template <typename Ret, typename... Args>
ReturnsOperatorEvalError(std::string name, Ret (*func)(Args...))
    -> ReturnsOperatorEvalError<Ret, Args...>;

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_ERROR_H_
