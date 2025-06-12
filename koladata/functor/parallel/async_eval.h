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
#ifndef KOLADATA_FUNCTOR_PARALLEL_ASYNC_EVAL_H_
#define KOLADATA_FUNCTOR_PARALLEL_ASYNC_EVAL_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/future.h"

namespace koladata::functor::parallel {

// Evaluates the given expression via the given executor. If any of the inputs
// is a future, will wait for it to be ready before evaluating the expression.
// If the expression returns a future, we will copy the value from that future
// to the output future when it is ready, otherwise we will write the expression
// result directly to the output future.
//
// This takes the inner expression as an operator (typically a lambda) since we
// have an easy way to infer attributes for an operator, but we seem to have
// no public API to infer attributes for an expression given the input
// attributes. Since this is intended for internal use, this seems fine.
absl::StatusOr<FuturePtr> AsyncEvalWithCompilationCache(
    const ExecutorPtr& executor, const arolla::expr::ExprOperatorPtr& op,
    absl::Span<const arolla::TypedRef> input_values,
    arolla::QTypePtr result_qtype);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_ASYNC_EVAL_H_
