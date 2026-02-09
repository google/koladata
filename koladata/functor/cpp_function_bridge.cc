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
#include <optional>
#include <source_location>  // NOLINT(build/c++20): needed for OSS logging.
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operators/std_function_operator.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/signature_utils.h"
#include "koladata/functor_storage.h"
#include "koladata/signature.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

absl::StatusOr<koladata::DataSlice> CreateFunctorFromStdFunction(
    std::function<
        absl::StatusOr<arolla::TypedValue>(absl::Span<const arolla::TypedRef>)>
        fn,
    absl::string_view name,
    const arolla::expr::ExprOperatorSignature& signature,
    arolla::expr_operators::StdFunctionOperator::OutputQTypeFn output_qtype_fn,
    std::source_location loc) {
  auto arolla_op =
      std::make_shared<arolla::expr_operators::StdFunctionOperator>(
          name, signature, "", std::move(output_qtype_fn), std::move(fn));

  ASSIGN_OR_RETURN(expr::InputContainer input_container,
                   expr::InputContainer::Create("I"));
  std::vector<absl::StatusOr<arolla::expr::ExprNodePtr>> args;
  std::vector<Signature::Parameter> koda_params;
  args.reserve(signature.parameters.size());
  koda_params.reserve(signature.parameters.size());
  for (const auto& p : signature.parameters) {
    if (p.kind != arolla::expr::ExprOperatorSignature::Parameter::Kind::
                      kPositionalOrKeyword) {
      return absl::InvalidArgumentError(
          "only positionalOrKeyword arguments supported");
    }
    args.push_back(input_container.CreateInput(p.name));
    Signature::Parameter koda_param;
    koda_param.name = p.name;
    koda_param.kind = Signature::Parameter::Kind::kPositionalOrKeyword;
    if (p.default_value.has_value()) {
      if (p.default_value->GetType() != arolla::GetQType<DataSlice>()) {
        return absl::InvalidArgumentError(
            "only DataItem default argument values are supported");
      }
      koda_param.default_value = p.default_value->UnsafeAs<DataSlice>();
    }
    koda_params.push_back(std::move(koda_param));
  }
  ASSIGN_OR_RETURN(auto op_expr,
                   arolla::expr::CallOp(std::move(arolla_op), std::move(args)));
  auto op_slice =
      DataSlice::CreatePrimitive(arolla::expr::ExprQuote(std::move(op_expr)));
  ASSIGN_OR_RETURN(auto koda_signature, Signature::Create(koda_params));
  ASSIGN_OR_RETURN(auto koda_signature_slice,
                   CppSignatureToKodaSignature(koda_signature));
  std::string module = absl::StrCat("<c++: ", loc.file_name(), ">");
  return CreateFunctor(
      std::move(op_slice), std::move(koda_signature_slice),
      {functor::kModuleAttrName, functor::kQualnameAttrName},
      {DataSlice::CreatePrimitive(arolla::Text(std::move(module))),
       DataSlice::CreatePrimitive(arolla::Text(name))});
}

absl::StatusOr<koladata::DataSlice> CreateFunctorFromStdFunction(
    std::function<
        absl::StatusOr<arolla::TypedValue>(absl::Span<const arolla::TypedRef>)>
        fn,
    absl::string_view name,
    const arolla::expr::ExprOperatorSignature& signature,
    arolla::QTypePtr output_type, std::source_location loc) {
  auto output_qtype_fn =
      [output_type](absl::Span<const arolla::QType* const> input_qtypes) {
        return output_type;
      };
  return CreateFunctorFromStdFunction(std::move(fn), name, signature,
                                      std::move(output_qtype_fn), loc);
}

absl::StatusOr<DataSlice> CreateFunctorFromStdFunction(
    std::function<
        absl::StatusOr<arolla::TypedValue>(absl::Span<const arolla::TypedRef>)>
        fn,
    absl::string_view name, absl::string_view signature_spec,
    arolla::expr_operators::StdFunctionOperator::OutputQTypeFn output_qtype_fn,
    std::source_location loc) {
  ASSIGN_OR_RETURN(auto signature,
                   arolla::expr::ExprOperatorSignature::Make(signature_spec));
  return CreateFunctorFromStdFunction(std::move(fn), name, signature,
                                      std::move(output_qtype_fn), loc);
}

absl::StatusOr<DataSlice> CreateFunctorFromStdFunction(
    std::function<
        absl::StatusOr<arolla::TypedValue>(absl::Span<const arolla::TypedRef>)>
        fn,
    absl::string_view name, absl::string_view signature_spec,
    arolla::QTypePtr output_type, std::source_location loc) {
  ASSIGN_OR_RETURN(auto signature,
                   arolla::expr::ExprOperatorSignature::Make(signature_spec));
  return CreateFunctorFromStdFunction(std::move(fn), name, signature,
                                      output_type, loc);
}

}  // namespace koladata::functor
