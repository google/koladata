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
#include "koladata/functor/bind_operator.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/quote.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/expr/non_determinism.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/signature_utils.h"
#include "koladata/internal/op_utils/qexpr.h"
#include "koladata/operators/utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

namespace {

using ::arolla::Text;
using ::arolla::TypedRef;
using ::arolla::TypedValue;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::Literal;

absl::Status ValidateFn(const DataSlice& returns) {
  if (!returns.is_item()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("fn must be a data item, but has shape: %s",
                        arolla::Repr(returns.GetShape())));
  }
  if (!returns.item().has_value()) {
    return absl::InvalidArgumentError("fn must be present");
  }
  return absl::OkStatus();
}

ExprNodePtr MakeArgumentNamesTuple(
    absl::Span<const absl::string_view> variable_names) {
  std::vector<TypedValue> names;
  names.reserve(variable_names.size());
  for (const auto& name : variable_names) {
    names.push_back(TypedValue::FromValue(Text(name)));
  }
  return Literal(arolla::MakeTuple(names));
}

absl::StatusOr<ExprNodePtr> MakeTupleOfVariables(
    absl::Span<const std::string> variable_names) {
  ASSIGN_OR_RETURN(auto v_container, expr::InputContainer::Create("V"));
  std::vector<ExprNodePtr> variable_values;
  variable_values.reserve(variable_names.size());
  for (const auto& name : variable_names) {
    ASSIGN_OR_RETURN(variable_values.emplace_back(),
                     v_container.CreateInput(name));
  }
  return ::arolla::expr::BindOp("core.make_tuple", std::move(variable_values),
                                {});
}

absl::StatusOr<ExprNodePtr> MakeNamedTupleOfVariables(
    absl::Span<const absl::string_view> variable_names) {
  ASSIGN_OR_RETURN(auto v_container, expr::InputContainer::Create("V"));
  std::vector<ExprNodePtr> variable_values;
  variable_values.reserve(variable_names.size() + 1);
  variable_values.push_back(MakeArgumentNamesTuple(variable_names));
  for (const auto& name : variable_names) {
    ASSIGN_OR_RETURN(variable_values.emplace_back(),
                     v_container.CreateInput(name));
  }
  return ::arolla::expr::BindOp("namedtuple.make", std::move(variable_values),
                                {});
}

absl::StatusOr<ExprNodePtr> MakeCapturingLambdaExpr(
    TypedRef return_type_as, absl::Span<const std::string> aux_arg_names,
    absl::Span<const absl::string_view> kwargs_names) {
  ASSIGN_OR_RETURN(auto i_container, expr::InputContainer::Create("I"));
  ASSIGN_OR_RETURN(auto v_container, expr::InputContainer::Create("V"));
  std::vector<ExprNodePtr> call_args;
  absl::flat_hash_map<std::string, ExprNodePtr> call_kwargs;

  ASSIGN_OR_RETURN(call_args.emplace_back(),
                   v_container.CreateInput("_aux_fn"));

  ASSIGN_OR_RETURN(ExprNodePtr args_tuple, MakeTupleOfVariables(aux_arg_names));
  ASSIGN_OR_RETURN(call_kwargs["args"],
                   ::arolla::expr::CallOp("core.concat_tuples",
                                          {std::move(args_tuple),
                                           i_container.CreateInput("args")}));

  ASSIGN_OR_RETURN(auto variables_named_tuple,
                   MakeNamedTupleOfVariables(kwargs_names));
  ASSIGN_OR_RETURN(call_kwargs["kwargs"],
                   ::arolla::expr::CallOp("namedtuple.union",
                                          {std::move(variables_named_tuple),
                                           i_container.CreateInput("kwargs")}));

  ASSIGN_OR_RETURN(call_kwargs[expr::kNonDeterministicParamName],
                   expr::GenNonDeterministicToken());
  call_kwargs["return_type_as"] = Literal(TypedValue(return_type_as));

  return ::arolla::expr::BindOp("kd.functor.call", std::move(call_args),
                                std::move(call_kwargs));
}

absl::StatusOr<DataSlice> CreateBind(
    const DataSlice& fn, TypedRef return_type_as,
    std::vector<DataSlice> args_values,
    std::vector<absl::string_view> kwargs_names,
    std::vector<DataSlice> kwargs_values) {
  RETURN_IF_ERROR(ValidateFn(fn));

  int64_t num_args = args_values.size();
  std::vector<absl::string_view> variable_names = kwargs_names;
  std::vector<DataSlice> variable_values = std::move(kwargs_values);
  variable_names.reserve(variable_names.size() + num_args + 1);
  variable_values.reserve(variable_values.size() + num_args + 1);

  std::vector<std::string> aux_arg_names;
  aux_arg_names.reserve(num_args);
  for (int64_t i = 0; i < num_args; ++i) {
    aux_arg_names.push_back(absl::StrFormat("_aux_fn_arg_%d", i));
  }
  for (int64_t i = 0; i < num_args; ++i) {
    variable_names.push_back(aux_arg_names[i]);
    variable_values.push_back(std::move(args_values[i]));
  }

  ASSIGN_OR_RETURN(
      auto capturing_lambda_expr,
      MakeCapturingLambdaExpr(return_type_as, aux_arg_names, kwargs_names));
  auto capturing_lambda_slice = DataSlice::CreateFromScalar(
      arolla::expr::ExprQuote(std::move(capturing_lambda_expr)));

  variable_names.push_back("_aux_fn");
  variable_values.push_back(fn);

  ASSIGN_OR_RETURN(auto result,
                   functor::CreateFunctor(
                       std::move(capturing_lambda_slice),
                       /*signature=*/KodaArgsKwargsSignature(),
                       std::move(variable_names), std::move(variable_values)));
  return result;
}

class BindOperator : public arolla::QExprOperator {
 public:
  explicit BindOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(input_types, arolla::GetQType<DataSlice>()) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "kd.functor.bind",
        [fn_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         args_slot = input_slots[1], return_type_as_slot = input_slots[2],
         kwargs_slot = input_slots[3],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          const auto& fn = frame.Get(fn_slot);
          std::vector<absl::string_view> kwargs_names =
              ops::GetFieldNames(kwargs_slot);
          std::vector<DataSlice> kwargs_values =
              ops::GetValueDataSlices(kwargs_slot, frame);
          std::vector<DataSlice> args_values =
              ops::GetValueDataSlices(args_slot, frame);
          ASSIGN_OR_RETURN(
              auto result,
              CreateBind(fn, TypedRef::FromSlot(return_type_as_slot, frame),
                         std::move(args_values), std::move(kwargs_names),
                         std::move(kwargs_values)));
          frame.Set(output_slot, std::move(result));
          return absl::OkStatus();
        });
  }
};

}  // namespace

// kd.functor.expr_fn.
absl::StatusOr<arolla::OperatorPtr> BindOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 5) {
    return absl::InvalidArgumentError("requires exactly 5 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be DataSlice");
  }
  RETURN_IF_ERROR(ops::VerifyTuple(input_types[1]));
  // input_types[2] is the return type, which is not used to verify the
  // signature.
  RETURN_IF_ERROR(ops::VerifyNamedTuple(input_types[3]));
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[4]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<BindOperator>(input_types), input_types, output_type);
}

}  // namespace koladata::functor
