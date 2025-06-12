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
#include "koladata/functor/call_operator.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/call.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/stack_trace.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/operators/utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {
namespace {

class CallOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [fn_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         args_slot = input_slots[1],
         stack_trace_frame_slot = input_slots[3].UnsafeToSlot<DataSlice>(),
         kwargs_slot = input_slots[4],
         output_slot](arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          const auto& fn_data_slice = frame.Get(fn_slot);

          std::vector<arolla::TypedRef> arg_refs;
          arg_refs.reserve(args_slot.SubSlotCount() +
                           kwargs_slot.SubSlotCount());
          for (int64_t i = 0; i < args_slot.SubSlotCount(); ++i) {
            arg_refs.push_back(
                arolla::TypedRef::FromSlot(args_slot.SubSlot(i), frame));
          }
          for (int64_t i = 0; i < kwargs_slot.SubSlotCount(); ++i) {
            arg_refs.push_back(
                arolla::TypedRef::FromSlot(kwargs_slot.SubSlot(i), frame));
          }
          auto kwnames = arolla::GetFieldNames(kwargs_slot.GetType());
          auto result = functor::CallFunctorWithCompilationCache(
              fn_data_slice, arg_refs, kwnames);
          if (!result.ok()) {
            ctx->set_status(MaybeAddStackTraceFrame(
                std::move(result).status(), frame.Get(stack_trace_frame_slot)));
            return;
          }
          if (result->GetType() != output_slot.GetType()) {
            ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
                "The functor was called with `%s` as the output type, but the"
                " computation resulted in type `%s` instead. You can specify"
                " the expected output type via the `return_type_as=` parameter"
                " to the functor call.",
                output_slot.GetType()->name(), result->GetType()->name())));
            return;
          }
          RETURN_IF_ERROR(result->CopyToSlot(output_slot, frame))
              .With(ctx->set_status());
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> CallOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 6) {
    return absl::InvalidArgumentError("requires exactly 6 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("requires 1st argument to be DataSlice");
  }
  if (!arolla::IsTupleQType(input_types[1])) {
    return absl::InvalidArgumentError("requires 2nd argument to be Tuple");
  }
  // Argument 2 is used to infer the return type, which is handled on the Expr
  // operator level, so we just ignore it here.
  if (input_types[3] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("requires 4th argument to be DataSlice");
  }
  if (!arolla::IsNamedTupleQType(input_types[4])) {
    return absl::InvalidArgumentError("requires 5th argument to be NamedTuple");
  }
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[5]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<CallOperator>(input_types, output_type), input_types,
      output_type);
}

namespace {

class CallAndUpdateNamedTupleOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    auto original_field_names = arolla::GetFieldNames(input_slots[2].GetType());
    absl::flat_hash_map<absl::string_view, int64_t> field_name_to_index;
    field_name_to_index.reserve(original_field_names.size());
    for (int64_t i = 0; i < original_field_names.size(); ++i) {
      field_name_to_index[original_field_names[i]] = i;
    }

    return arolla::MakeBoundOperator(
        [fn_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         args_slot = input_slots[1], namedtuple_to_update_slot = input_slots[2],
         kwargs_slot = input_slots[3], output_slot, original_field_names,
         field_name_to_index = std::move(field_name_to_index)](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          const auto& fn_data_slice = frame.Get(fn_slot);

          std::vector<arolla::TypedRef> arg_refs;
          arg_refs.reserve(args_slot.SubSlotCount() +
                           kwargs_slot.SubSlotCount());
          for (int64_t i = 0; i < args_slot.SubSlotCount(); ++i) {
            arg_refs.push_back(
                arolla::TypedRef::FromSlot(args_slot.SubSlot(i), frame));
          }
          for (int64_t i = 0; i < kwargs_slot.SubSlotCount(); ++i) {
            arg_refs.push_back(
                arolla::TypedRef::FromSlot(kwargs_slot.SubSlot(i), frame));
          }
          auto kwnames = arolla::GetFieldNames(kwargs_slot.GetType());
          ASSIGN_OR_RETURN(auto result,
                           functor::CallFunctorWithCompilationCache(
                               fn_data_slice, arg_refs, kwnames),
                           ctx->set_status(std::move(_)));
          if (!IsNamedTupleQType(result.GetType())) {
            ctx->set_status(absl::InvalidArgumentError(
                absl::StrFormat("the functor must"
                                " return a namedtuple, but it returned `%s`",
                                result.GetType()->name())));
            return;
          }
          // Now we replace the fields in "namedtuple_to_update_slot" with
          // the corresponding fields from result.
          std::vector<arolla::TypedRef> new_namedtuple_fields;
          new_namedtuple_fields.reserve(
              namedtuple_to_update_slot.SubSlotCount());
          for (int64_t i = 0; i < namedtuple_to_update_slot.SubSlotCount();
               ++i) {
            new_namedtuple_fields.push_back(arolla::TypedRef::FromSlot(
                namedtuple_to_update_slot.SubSlot(i), frame));
          }
          auto update_field_names = arolla::GetFieldNames(result.GetType());
          for (int64_t i = 0; i < result.GetFieldCount(); ++i) {
            auto it = field_name_to_index.find(update_field_names[i]);
            if (it == field_name_to_index.end()) {
              ctx->set_status(absl::InvalidArgumentError(
                  absl::StrFormat("the functor returned a namedtuple with "
                                  "field `%s`, but the original"
                                  " namedtuple does not have such a field",
                                  update_field_names[i])));
              return;
            }
            int64_t original_index = it->second;
            arolla::TypedRef result_field = result.GetField(i);
            if (new_namedtuple_fields[original_index].GetType() !=
                result_field.GetType()) {
              ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
                  "the functor returned a namedtuple with field `%s` of type "
                  "`%s`, but the original namedtuple has type `%s` for it",
                  update_field_names[i], result_field.GetType()->name(),
                  new_namedtuple_fields[original_index].GetType()->name())));
              return;
            }
            new_namedtuple_fields[original_index] = result_field;
          }
          ASSIGN_OR_RETURN(arolla::TypedValue updated_namedtuple,
                           arolla::MakeNamedTuple(original_field_names,
                                                  new_namedtuple_fields),
                           ctx->set_status(std::move(_)));
          RETURN_IF_ERROR(updated_namedtuple.CopyToSlot(output_slot, frame))
              .With(ctx->set_status());
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr>
CallAndUpdateNamedTupleOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 5) {
    return absl::InvalidArgumentError("requires exactly 5 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be DataSlice");
  }
  if (!arolla::IsTupleQType(input_types[1])) {
    return absl::InvalidArgumentError("requires second argument to be Tuple");
  }
  if (!arolla::IsNamedTupleQType(input_types[2])) {
    return absl::InvalidArgumentError(
        "requires third argument to be NamedTuple");
  }
  if (!arolla::IsNamedTupleQType(input_types[3])) {
    return absl::InvalidArgumentError(
        "requires fourth argument to be NamedTuple");
  }
  if (input_types[2] != output_type) {
    return absl::InvalidArgumentError(
        "requires output type to be the same as the third argument");
  }
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[4]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<CallAndUpdateNamedTupleOperator>(input_types,
                                                        output_type),
      input_types, output_type);
}

absl::StatusOr<DataSlice> MaybeCall(arolla::EvaluationContext* ctx,
                                    const DataSlice& maybe_fn,
                                    const DataSlice& arg,
                                    internal::NonDeterministicToken) {
  ASSIGN_OR_RETURN(bool is_functor, IsFunctor(maybe_fn));
  if (is_functor) {
    ASSIGN_OR_RETURN(auto result,
                     functor::CallFunctorWithCompilationCache(
                         maybe_fn, /*args=*/{arolla::TypedRef::FromValue(arg)},
                         /*kwargs=*/{}));
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
