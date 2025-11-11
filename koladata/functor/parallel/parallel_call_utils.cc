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
#include "koladata/functor/parallel/parallel_call_utils.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_slice.h"
#include "koladata/expr/expr_eval.h"
#include "koladata/functor/call.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/transform.h"
#include "koladata/functor/parallel/transform_config.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

absl::StatusOr<arolla::TypedValue> FutureFromParallel(const ExecutorPtr
                                                      absl_nonnull& executor,
                                                      arolla::TypedRef value) {
  static const absl::NoDestructor<arolla::expr::ExprOperatorPtr> kOp(
      std::make_shared<arolla::expr::RegisteredOperator>(
          "koda_internal.parallel.future_from_parallel"));
  return koladata::expr::EvalOpWithCompilationCache(
      *kOp, {arolla::TypedRef::FromValue(executor), value});
}

}  // namespace

absl::StatusOr<arolla::TypedValue> AsParallel(arolla::TypedRef value) {
  static const absl::NoDestructor<arolla::expr::ExprOperatorPtr> kOp(
      std::make_shared<arolla::expr::RegisteredOperator>(
          "koda_internal.parallel.as_parallel"));
  return koladata::expr::EvalOpWithCompilationCache(*kOp, {value});
}

absl::StatusOr<ParallelTransformConfigPtr absl_nonnull>
GetDefaultParallelTransformConfig() {
  static const absl::NoDestructor<arolla::expr::ExprOperatorPtr> kOp(
      std::make_shared<arolla::expr::RegisteredOperator>(
          "koda_internal.parallel.get_default_transform_config"));
  ASSIGN_OR_RETURN(auto qvalue,
                   koladata::expr::EvalOpWithCompilationCache(*kOp, {}));
  ASSIGN_OR_RETURN(auto config, qvalue.As<ParallelTransformConfigPtr>());
  if (config == nullptr) {
    return absl::InternalError(
        "koda_internal.parallel.get_default_transform_config() returned "
        "nullptr");
  }
  return config;
}

absl::StatusOr<CallFn> TransformToParallelCallFn(
    const ParallelTransformConfigPtr& config, DataSlice functor) {
  ASSIGN_OR_RETURN(auto parallel_functor,
                   TransformToParallel(config, std::move(functor)));
  return CallFn([parallel_functor = std::move(parallel_functor)](
                    absl::Span<const arolla::TypedRef> args,
                    absl::Span<const std::string> kwnames)
                    -> absl::StatusOr<arolla::TypedValue> {
    return CallFunctorWithCompilationCache(parallel_functor, args, kwnames);
  });
}

absl::StatusOr<CallFn> TransformToSimpleParallelCallFn(
    DataSlice functor, bool allow_runtime_transforms) {
  ASSIGN_OR_RETURN(auto config, GetDefaultParallelTransformConfig());
  // A small optimization: Check the default config and create a new one
  // only if `allow_runtime_transforms` does not match the requirement.
  if (config->allow_runtime_transforms() != allow_runtime_transforms) {
    config = std::make_shared<ParallelTransformConfig>(
        allow_runtime_transforms, config->operator_replacements());
  }
  ASSIGN_OR_RETURN(auto parallel_functor_fn,
                   TransformToParallelCallFn(config, std::move(functor)));
  return CallFn([parallel_functor_fn = std::move(parallel_functor_fn)](
                    absl::Span<const arolla::TypedRef> args,
                    absl::Span<const std::string> kwnames)
                    -> absl::StatusOr<arolla::TypedValue> {
    ASSIGN_OR_RETURN(auto executor, CurrentExecutor());
    std::vector<arolla::TypedRef> parallel_args;
    parallel_args.reserve(1 + args.size());
    parallel_args.push_back(arolla::TypedRef::FromValue(executor));
    std::vector<arolla::TypedValue> holder;
    holder.reserve(args.size());
    for (const auto& arg : args) {
      ASSIGN_OR_RETURN(auto parallel_arg, AsParallel(arg));
      parallel_args.push_back(parallel_arg.AsRef());
      holder.push_back(std::move(parallel_arg));
    }
    ASSIGN_OR_RETURN(auto parallel_result,
                     parallel_functor_fn(parallel_args, kwnames));
    return FutureFromParallel(executor, parallel_result.AsRef());
  });
}

}  // namespace koladata::functor::parallel
