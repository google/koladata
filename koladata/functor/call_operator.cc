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
#include "koladata/functor/call_operator.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/call.h"
#include "koladata/functor/functor.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {
namespace {

class CallOperator : public arolla::QExprOperator {
 public:
  explicit CallOperator(absl::Span<const arolla::QTypePtr> input_types,
                        arolla::QTypePtr output_type)
      : QExprOperator("kde.functor.call", arolla::QExprOperatorSignature::Get(
                                              input_types, output_type)) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [fn_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         args_slot = input_slots[1], kwargs_slot = input_slots[3],
         output_slot](arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          const auto& fn_data_slice = frame.Get(fn_slot);

          std::vector<arolla::TypedRef> arg_refs;
          arg_refs.reserve(args_slot.SubSlotCount());
          for (int i = 0; i < args_slot.SubSlotCount(); ++i) {
            arg_refs.push_back(
                arolla::TypedRef::FromSlot(args_slot.SubSlot(i), frame));
          }

          auto kwargs_qtype = kwargs_slot.GetType();
          auto kwarg_names = arolla::GetFieldNames(kwargs_qtype);
          std::vector<std::pair<std::string, arolla::TypedRef>> kwarg_refs;
          kwarg_refs.reserve(kwargs_slot.SubSlotCount());
          for (int i = 0; i < kwargs_slot.SubSlotCount(); ++i) {
            kwarg_refs.push_back(
                {kwarg_names[i],
                 arolla::TypedRef::FromSlot(kwargs_slot.SubSlot(i), frame)});
          }
          ASSIGN_OR_RETURN(auto result,
                           functor::CallFunctorWithCompilationCache(
                               fn_data_slice, arg_refs, kwarg_refs),
                           ctx->set_status(std::move(_)));
          if (result.GetType() != output_slot.GetType()) {
            ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
                "the functor was called with `%s` as the output type, but the"
                " computation resulted in type `%s` instead",
                output_slot.GetType()->name(), result.GetType()->name())));
            return;
          }
          RETURN_IF_ERROR(result.CopyToSlot(output_slot, frame))
              .With(ctx->set_status());
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> CallOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 4) {
    return absl::InvalidArgumentError("requires exactly 4 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be DataSlice");
  }
  if (!arolla::IsTupleQType(input_types[1])) {
    return absl::InvalidArgumentError("requires second argument to be Tuple");
  }
  // Argument 2 is used to infer the return type, which is handled on the Expr
  // operator level, so we just ignore it here.
  if (!arolla::IsNamedTupleQType(input_types[3])) {
    return absl::InvalidArgumentError(
        "requires third argument to be NamedTuple");
  }
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<CallOperator>(input_types, output_type), input_types,
      output_type);
}

absl::StatusOr<DataSlice> MaybeCall(const DataSlice& maybe_fn,
                                    const DataSlice& arg) {
  ASSIGN_OR_RETURN(bool is_functor, IsFunctor(maybe_fn));
  if (is_functor) {
    ASSIGN_OR_RETURN(auto result,
                     functor::CallFunctorWithCompilationCache(
                         maybe_fn, {arolla::TypedRef::FromValue(arg)}, {}));
    if (result.GetType() != arolla::GetQType<DataSlice>()) {
      return absl::InternalError(absl::StrFormat(
          "the functor is expected to be evaluated to a DataSlice"
          ", but the result has type `%s` instead",
          result.GetType()->name()));
    }
    return result.As<DataSlice>();
  }
  return maybe_fn;
}

}  // namespace koladata::functor
