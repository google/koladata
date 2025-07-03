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
#include "koladata/functor/cpp_function_bridge.h"

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operators/std_function_operator.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_slice.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/signature_utils.h"
#include "koladata/signature.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

absl::StatusOr<koladata::DataSlice> CreateFunctorFromStdFunction(
    std::function<
        absl::StatusOr<arolla::TypedValue>(absl::Span<const arolla::TypedRef>)>
        fn,
    absl::string_view name, absl::string_view signature_spec,
    arolla::QTypePtr output_type) {
  ASSIGN_OR_RETURN(auto signature,
                   arolla::expr::ExprOperatorSignature::Make(signature_spec));
  auto get_output_type =
      [output_type](absl::Span<const arolla::QType* const> input_qtypes) {
        return output_type;
      };
  auto arolla_op =
      std::make_shared<arolla::expr_operators::StdFunctionOperator>(
          name, signature, "", get_output_type, std::move(fn));

  ASSIGN_OR_RETURN(expr::InputContainer input_container,
                   expr::InputContainer::Create("I"));
  std::vector<absl::StatusOr<arolla::expr::ExprNodePtr>> args;
  std::vector<Signature::Parameter> koda_params;
  args.reserve(signature.parameters.size());
  koda_params.reserve(signature.parameters.size());
  for (const auto& p : signature.parameters) {
    // Note: here we don't check default values because `Make(signature_spec)`
    // never sets any.
    if (p.kind != arolla::expr::ExprOperatorSignature::Parameter::Kind::
                      kPositionalOrKeyword) {
      return absl::FailedPreconditionError(
          "only positionalOrKeyword arguments supported");
    }
    args.push_back(input_container.CreateInput(p.name));
    Signature::Parameter koda_param;
    koda_param.name = p.name;
    koda_param.kind = Signature::Parameter::Kind::kPositionalOrKeyword;
    koda_params.push_back(std::move(koda_param));
  }
  ASSIGN_OR_RETURN(auto op_expr,
                   arolla::expr::CallOp(std::move(arolla_op), std::move(args)));
  auto op_slice =
      DataSlice::CreateFromScalar(arolla::expr::ExprQuote(std::move(op_expr)));
  ASSIGN_OR_RETURN(auto koda_signature, Signature::Create(koda_params));
  ASSIGN_OR_RETURN(auto koda_signature_slice,
                   CppSignatureToKodaSignature(koda_signature));
  return CreateFunctor(std::move(op_slice), std::move(koda_signature_slice),
                       {}, {});
}


}  // namespace koladata::functor
