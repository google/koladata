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
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/util/init_arolla.h"
#include "koladata/iterables/expr_operators.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::iterables {

AROLLA_INITIALIZER(
        .name = "arolla_operators/koda_iterables",
        .reverse_deps = {arolla::initializer_dep::kOperators},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(arolla::expr::RegisterOperator<GetIterableQTypeOp>(
                              "koda_internal.iterables.get_iterable_qtype")
                              .status());
          return absl::OkStatus();
        })

}  // namespace koladata::iterables
