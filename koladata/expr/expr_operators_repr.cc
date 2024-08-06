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
#include <optional>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "koladata/expr/expr_operators.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/operator_repr_functions.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/repr.h"
#include "arolla/util/string.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::expr {

namespace {

// koda_internal.input(container_name, input_key) repr. Printed as:
//   - koda_internal.input('V', 'foo') is printed as V.foo.
//   - koda_internal.input('V', 'foo.bar') is printed as V['foo.bar'].
//   - as a special case, kode_internal.input('I', 'self') is printed as S.
std::optional<arolla::ReprToken> KodaInputOpRepr(
    const arolla::expr::ExprNodePtr& node,
    const absl::flat_hash_map<arolla::Fingerprint, arolla::ReprToken>&) {
  absl::string_view container_name =
      node->node_deps()[0]->qvalue().value().UnsafeAs<arolla::Text>().view();
  absl::string_view input_key =
      node->node_deps()[1]->qvalue().value().UnsafeAs<arolla::Text>().view();
  if (container_name == "I" && input_key == "self") {
    return arolla::ReprToken{"S"};
  }
  return arolla::ReprToken{
      absl::StrCat(container_name, arolla::ContainerAccessString(input_key))};
}

std::optional<arolla::ReprToken> KodaLiteralOpRepr(
    const arolla::expr::ExprNodePtr& node,
    const absl::flat_hash_map<arolla::Fingerprint, arolla::ReprToken>&) {
  ASSIGN_OR_RETURN(auto decayed_op,
                   arolla::expr::DecayRegisteredOperator(node->op()),
                   std::nullopt);
  if (const auto* op =
          arolla::fast_dynamic_downcast_final<const LiteralOperator*>(
              decayed_op.get())) {
    return op->value().GenReprToken();
  } else {
    return std::nullopt;
  }
}

}  // namespace

AROLLA_INITIALIZER(.init_fn = [] {
  arolla::expr::RegisterOpReprFnByByRegistrationName("koda_internal.input",
                                                     KodaInputOpRepr);
  arolla::expr::RegisterOpReprFnByQValueSpecializationKey(
      "::koladata::expr::LiteralOperator", KodaLiteralOpRepr);
})

}  // namespace koladata::expr
