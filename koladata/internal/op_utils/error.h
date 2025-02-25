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
#include <type_traits>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "arolla/util/meta.h"
#include "arolla/util/status.h"

namespace koladata::internal {

// Turns the given `status` into a Koda error and amends the error message with
// the operator name.
//
// By default the function does not put the original `status` as a cause, and
// just amends its error message. However, if `error_message` is provided, or if
// `status` has a non-Koda payload, it will create an new layer of Koda error,
// keeping the original `status` as a cause.
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
template <typename Fn, typename Ret, typename ArgsList>
class ReturnsOperatorEvalError;

template <typename Fn, typename Ret, typename... Args>
class ReturnsOperatorEvalError<Fn, Ret, arolla::meta::type_list<Args...>> {
 public:
  ReturnsOperatorEvalError(std::string name, Fn func)
      : name_(std::move(name)), func_(std::move(func)) {}

  // NOTE: Unlike for a regularly deduced template function, `Args...` resemble
  // the original function's signature and so may contain reference types.
  // Because of that, we are using `std::forward` to perfectly forward the
  // arguments.
  Ret operator()(Args... args) const {
    if constexpr (arolla::IsStatusOrT<Ret>::value) {
      auto result = func_(std::forward<Args>(args)...);
      if (!result.ok()) {
        return OperatorEvalError(result.status(), name_);
      }
      return result;
    } else if constexpr (std::is_same_v<Ret, absl::Status>) {
      auto status = func_(std::forward<Args>(args)...);
      if (!status.ok()) {
        return OperatorEvalError(status, name_);
      }
      return status;
    } else {
      return func_(std::forward<Args>(args)...);
    }
  }

 private:
  std::string name_;
  Fn func_;
};

template <typename Fn>
ReturnsOperatorEvalError(std::string name, Fn func) -> ReturnsOperatorEvalError<
    Fn, typename arolla::meta::function_traits<Fn>::return_type,
    typename arolla::meta::function_traits<Fn>::arg_types>;

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_ERROR_H_
