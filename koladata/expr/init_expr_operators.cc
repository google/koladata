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
#include <memory>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/util/init_arolla.h"
#include "koladata/data_slice_qtype.h"  // IWYU pragma: keep
#include "koladata/expr/expr_operators.h"
#include "koladata/internal/ellipsis.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::expr {

AROLLA_INITIALIZER(
        .name = "arolla_operators/koda_internal",
        .reverse_deps = {arolla::initializer_dep::kOperators},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(arolla::expr::RegisterOperator<InputOperator>(
                              "koda_internal.input")
                              .status());
          RETURN_IF_ERROR(
              RegisterOperator("koda_internal.ellipsis",
                               arolla::expr::MakeLambdaOperator(
                                   arolla::expr::ExprOperatorSignature{},
                                   arolla::expr::Literal(internal::Ellipsis{})))
                  .status());
          RETURN_IF_ERROR(
              arolla::expr::RegisterOperator(
                  "koda_internal.with_name",
                  std::make_shared<arolla::expr::NameAnnotation>(
                      /*aux_policy=*/"_koladata_annotation_with_name"))
                  .status());
          RETURN_IF_ERROR(arolla::expr::RegisterOperator(
                              "koda_internal.to_arolla_int64",
                              std::make_shared<ToArollaInt64Operator>())
                              .status());
          RETURN_IF_ERROR(arolla::expr::RegisterOperator(
                              "koda_internal.to_arolla_text",
                              std::make_shared<ToArollaTextOperator>())
                              .status());
          RETURN_IF_ERROR(
              arolla::expr::RegisterOperator<NonDeterministicOperator>(
                  "koda_internal.non_deterministic").status());
          return absl::OkStatus();
        })

}  // namespace koladata::expr
