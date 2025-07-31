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

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/optools.h"
#include "arolla/util/meta.h"
#include "arolla/util/status.h"
#include "arolla/util/traceme.h"
#include "koladata/internal/op_utils/error.h"

namespace koladata {

// Compile-time flags for KodaOperatorWrapper.
struct KodaOperatorWrapperFlags {
  // No additional logic.
  static constexpr int kNone = 0;
  // Errors are converted into OperatorEvalError.
  static constexpr int kWrapError = 1 << 0;
  // Evaluation is traced using TraceMe.
  static constexpr int kProfile = 1 << 1;
  // All of the above.
  static constexpr int kAll = ~0;
};

// Wraps the given function with Koda improvements, including error wrapping and
// profiling support. See KodaOperatorWrapperFlags for flags.
template <int flags, typename Fn, typename Ret, typename ArgsList>
class KodaOperatorWrapper;

template <int flags, typename Fn, typename Ret, typename... Args>
class KodaOperatorWrapper<flags, Fn, Ret, arolla::meta::type_list<Args...>> {
  static constexpr bool kWrapError =
      flags & KodaOperatorWrapperFlags::kWrapError;
  static constexpr bool kProfile = flags & KodaOperatorWrapperFlags::kProfile;

  // Safety measure to prevent bugs where the error is believed to be wrapped
  // when it is in fact not.
  static_assert(
      !(kWrapError &&
        std::is_same_v<arolla::meta::type_list<Args...>,
                       arolla::meta::type_list<arolla::EvaluationContext*,
                                               arolla::FramePtr>> &&
        !std::is_same_v<Ret, absl::Status>),
      "functor(ctx, frame) must return absl::Status");

 public:
  KodaOperatorWrapper(std::string name, Fn func)
      : name_(std::move(name)), func_(std::move(func)) {}

  // NOTE: Unlike for a regularly deduced template function, `Args...` resemble
  // the original function's signature and so may contain reference types.
  // Because of that, we are using `std::forward` to perfectly forward the
  // arguments. We avoid `Args&&... args` since this object will be fed into
  // arolla::meta::function_traits later which won't work.
  Ret operator()(Args... args) const {
    if constexpr (kProfile) {
      arolla::profiling::TraceMe traceme(
          [&] { return absl::StrCat("<Op> ", name_); });
    }
    if constexpr (kWrapError && arolla::IsStatusOrT<Ret>::value) {
      auto result = func_(std::forward<Args>(args)...);
      if (!result.ok()) {
        return internal::OperatorEvalError(result.status(), name_);
      }
      return result;
    } else if constexpr (kWrapError && std::is_same_v<Ret, absl::Status>) {
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

// Wraps the given function with Koda improvements, including error wrapping and
// profiling support. See KodaOperatorWrapperFlags for flags.
template <int flags = KodaOperatorWrapperFlags::kAll, typename Fn>
auto MakeKodaOperatorWrapper(std::string name, Fn func) {
  return KodaOperatorWrapper<
      flags, Fn, typename arolla::meta::function_traits<Fn>::return_type,
      typename arolla::meta::function_traits<Fn>::arg_types>(std::move(name),
                                                             std::move(func));
}

// Creates a bound operator implemented by the provided functor with Koda
// improvements, including error wrapping and profiling support. See
// KodaOperatorWrapperFlags for flags.
template <int flags = KodaOperatorWrapperFlags::kAll, typename Functor>
std::unique_ptr<arolla::BoundOperator> MakeBoundOperator(std::string name,
                                                         Functor functor) {
  return arolla::MakeBoundOperator(MakeKodaOperatorWrapper<flags, Functor>(
      std::move(name), std::move(functor)));
}

namespace macro_internal {

template <typename Fn, typename... Args>
auto OperatorMacroImpl(absl::string_view name, Fn func) {
  return MakeKodaOperatorWrapper(std::string(name), std::move(func));
}

template <typename Fn, typename... Args>
auto OperatorMacroImpl(absl::string_view name, Fn func,
                       absl::string_view display_name) {
  DCHECK_NE(name, display_name) << "remove excessive display_name argument";
  return MakeKodaOperatorWrapper(std::string(display_name), std::move(func));
}

}  // namespace macro_internal

}  // namespace koladata

// Registers the provided operator / function wrapped with
// MakeKodaOperatorWrapper. See AROLLA_REGISTER_QEXPR_OPERATOR for details.
#define KODA_QEXPR_OPERATOR(name, ...) \
  AROLLA_REGISTER_QEXPR_OPERATOR(      \
      name, ::koladata::macro_internal::OperatorMacroImpl(name, __VA_ARGS__))

// Registers the provided operator / function wrapped with
// MakeKodaOperatorWrapper. Allows the signature to be manually specified.
//
// TODO: Support derived qtypes via automatic casting.
// Use for operators we need to provide explicit signatures for, due to, for
// example, the use of derived qtypes. See AROLLA_REGISTER_QEXPR_OPERATOR for
// details.
#define KODA_QEXPR_OPERATOR_WITH_SIGNATURE(name, signature, ...)              \
  AROLLA_REGISTER_QEXPR_OPERATOR(                                             \
      name, ::koladata::macro_internal::OperatorMacroImpl(name, __VA_ARGS__), \
      signature)

// Registers the provided operator family. See
// AROLLA_REGISTER_QEXPR_OPERATOR_FAMILY for details.
#define KODA_QEXPR_OPERATOR_FAMILY AROLLA_REGISTER_QEXPR_OPERATOR_FAMILY

#endif  // KOLADATA_INTERNAL_OP_UTILS_QEXPR_H_
