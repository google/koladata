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
#ifndef KOLADATA_INTERNAL_OP_UTILS_QEXPR_H_
#define KOLADATA_INTERNAL_OP_UTILS_QEXPR_H_

#include <memory>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "koladata/internal/op_utils/error.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"

namespace koladata {

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
      internal::ReturnsOperatorEvalError(std::move(name), std::move(functor)));
}

}  // namespace koladata

#endif  // KOLADATA_INTERNAL_OP_UTILS_QEXPR_H_
