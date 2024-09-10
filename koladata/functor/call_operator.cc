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
  explicit CallOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator("kde.functor.call",
                      arolla::QExprOperatorSignature::Get(
                          input_types, arolla::GetQType<DataSlice>())) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [fn_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         args_slot = input_slots[1], kwargs_slot = input_slots[2],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
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
          if (result.GetType() != arolla::GetQType<DataSlice>()) {
            ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
                "Each Koda functor must evaluate to a DataSlice, but got"
                " type `%s` instead",
                result.GetType()->name())));
            return;
          }
          frame.Set(output_slot, result.UnsafeAs<DataSlice>());
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> CallOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 3) {
    return absl::InvalidArgumentError("requires exactly 3 arguments");
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
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<CallOperator>(input_types), input_types, output_type);
}

}  // namespace koladata::functor