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
#ifndef KOLADATA_INTERNAL_OP_UTILS_QEXPR_H_
#define KOLADATA_INTERNAL_OP_UTILS_QEXPR_H_

#include <memory>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/util/meta.h"
#include "arolla/util/status.h"
#include "arolla/util/traceme.h"
#include "koladata/internal/op_utils/error.h"

namespace koladata {

// Wraps the given function with Koda improvements:
// * All errors are converted into OperatorEvalError.
// * Evaluation is traced using TraceMe.
template <typename Fn, typename Ret, typename ArgsList>
class KodaOperatorWrapper;

template <typename Fn, typename Ret, typename... Args>
class KodaOperatorWrapper<Fn, Ret, arolla::meta::type_list<Args...>> {
 public:
  KodaOperatorWrapper(std::string name, Fn func)
      : name_(std::move(name)), func_(std::move(func)) {}

  // NOTE: Unlike for a regularly deduced template function, `Args...` resemble
  // the original function's signature and so may contain reference types.
  // Because of that, we are using `std::forward` to perfectly forward the
  // arguments. We avoid `Args&&... args` since this object will be fed into
  // arolla::meta::function_traits later which won't work.
  Ret operator()(Args... args) const {
    arolla::profiling::TraceMe t([&] { return absl::StrCat("<Op> ", name_); });
    if constexpr (arolla::IsStatusOrT<Ret>::value) {
      auto result = func_(std::forward<Args>(args)...);
      if (!result.ok()) {
        return internal::OperatorEvalError(result.status(), name_);
      }
      return result;
    } else if constexpr (std::is_same_v<Ret, absl::Status>) {
      auto status = func_(std::forward<Args>(args)...);
      if (!status.ok()) {
        return internal::OperatorEvalError(status, name_);
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
KodaOperatorWrapper(std::string name, Fn func) -> KodaOperatorWrapper<
    Fn, typename arolla::meta::function_traits<Fn>::return_type,
    typename arolla::meta::function_traits<Fn>::arg_types>;

// Creates a bound operator implemented by the provided functor. All the errors
// returned by the functor are wrapped into OperatorEvalError with the given
// name.
template <typename Functor>
std::unique_ptr<arolla::BoundOperator> MakeBoundOperator(std::string name,
                                                         Functor functor) {
  static_assert(std::is_same_v<decltype(functor(
                                   std::declval<arolla::EvaluationContext*>(),
                                   std::declval<arolla::FramePtr>())),
                               absl::Status>,
                "functor(ctx, frame) must return absl::Status");
  return arolla::MakeBoundOperator(
      KodaOperatorWrapper(std::move(name), std::move(functor)));
}

}  // namespace koladata

#endif  // KOLADATA_INTERNAL_OP_UTILS_QEXPR_H_
