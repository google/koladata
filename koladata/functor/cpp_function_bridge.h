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
#ifndef KOLADATA_FUNCTOR_CPP_FUNCTION_BRIDGE_H_
#define KOLADATA_FUNCTOR_CPP_FUNCTION_BRIDGE_H_

#include <functional>
#include <source_location>  // NOLINT(build/c++20): needed for OSS logging.
#include <utility>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operators/std_function_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/meta.h"
#include "arolla/util/status.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"

namespace koladata::functor {

// Creates a functor with the given C++ implementation.
// Types of the inputs are not restricted, the output type has to be specified
// explicitly.
absl::StatusOr<DataSlice> CreateFunctorFromStdFunction(
    std::function<
        absl::StatusOr<arolla::TypedValue>(absl::Span<const arolla::TypedRef>)>
        fn,
    absl::string_view name,
    const arolla::expr::ExprOperatorSignature& signature,
    arolla::QTypePtr output_type,
    std::source_location loc = std::source_location::current());

// Creates a functor with the given C++ implementation.
// Types of the inputs are not restricted, the output type has to be specified
// explicitly.
absl::StatusOr<DataSlice> CreateFunctorFromStdFunction(
    std::function<
        absl::StatusOr<arolla::TypedValue>(absl::Span<const arolla::TypedRef>)>
        fn,
    absl::string_view name, absl::string_view signature_spec,
    arolla::QTypePtr output_type,
    std::source_location loc = std::source_location::current());

// Creates a functor with the given C++ implementation.
// Types for the inputs and outputs are deduced from the implementation.
template <typename Fn>
absl::StatusOr<DataSlice> CreateFunctorFromFunction(
    Fn&& fn, absl::string_view name, absl::string_view signature_spec,
    std::source_location loc = std::source_location::current()) {
  using ResT = arolla::strip_statusor_t<
      typename arolla::meta::function_traits<Fn>::return_type>;
  return CreateFunctorFromStdFunction(
      arolla::expr_operators::WrapAsEvalFn(std::forward<Fn>(fn)), name,
      signature_spec, arolla::GetQType<ResT>(), loc);
}

// Creates a functor with the given C++ implementation.
// Types for the inputs and outputs are deduced from the implementation.
template <typename Fn>
absl::StatusOr<DataSlice> CreateFunctorFromFunction(
    Fn&& fn, absl::string_view name,
    const arolla::expr::ExprOperatorSignature& signature,
    std::source_location loc = std::source_location::current()) {
  using ResT = arolla::strip_statusor_t<
      typename arolla::meta::function_traits<Fn>::return_type>;
  return CreateFunctorFromStdFunction(
      arolla::expr_operators::WrapAsEvalFn(std::forward<Fn>(fn)), name,
      signature, arolla::GetQType<ResT>(), loc);
}

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_CPP_FUNCTION_BRIDGE_H_
