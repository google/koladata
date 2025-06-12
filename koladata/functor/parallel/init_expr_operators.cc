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
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/functor/parallel/eager_executor.h"
#include "koladata/functor/parallel/expr_operators.h"
#include "koladata/functor/parallel/get_default_executor.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {

using ::arolla::TypedValue;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::LambdaOperator;
using ::arolla::expr::RegisterOperator;
using ::koladata::expr::MakeLiteral;

AROLLA_INITIALIZER(
        .name = "arolla_operators/koda_functor_parallel",
        .reverse_deps = {arolla::initializer_dep::kOperators},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(
              RegisterOperator(
                  "koda_internal.parallel.get_eager_executor",
                  LambdaOperator::Make(
                      "kd.parallel.get_eager_executor", ExprOperatorSignature{},
                      MakeLiteral(TypedValue::FromValue(GetEagerExecutor())),
                      "Returns the eager executor."))
                  .status());
          RETURN_IF_ERROR(
              RegisterOperator(
                  "koda_internal.parallel.get_default_executor",
                  LambdaOperator::Make(
                      "kd.parallel.get_default_executor",
                      ExprOperatorSignature{},
                      MakeLiteral(TypedValue::FromValue(GetDefaultExecutor())),
                      "Returns the default executor."))
                  .status());
          RETURN_IF_ERROR(arolla::expr::RegisterOperator<AsyncEvalOp>(
                              "koda_internal.parallel.async_eval")
                              .status());
          return absl::OkStatus();
        })

}  // namespace koladata::functor::parallel
