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
#include "koladata/functor/with_assertion_operator.h"

#include <cstdint>
#include <memory>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/arolla_utils.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/call.h"
#include "koladata/functor/functor_storage.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {
namespace{

class WithAssertionOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [input_slot = input_slots[0],
         condition_slot = input_slots[1].UnsafeToSlot<DataSlice>(),
         message_or_fn_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         args_slot = input_slots[3],
         output_slot](arolla::EvaluationContext* ctx,
                      arolla::FramePtr frame) -> absl::Status {
          auto raise = [](const DataSlice& message_slice) -> absl::Status {
            ASSIGN_OR_RETURN(auto message,
                             ToArollaScalar<arolla::Text>(message_slice));
            return absl::FailedPreconditionError(message.view());
          };
          ASSIGN_OR_RETURN(auto condition, ToArollaOptionalScalar<arolla::Unit>(
                                               frame.Get(condition_slot)));
          if (condition.present) {
            input_slot.CopyTo(frame, output_slot, frame);
            return absl::OkStatus();
          }
          const DataSlice& message_or_fn = frame.Get(message_or_fn_slot);
          ASSIGN_OR_RETURN(bool is_fn, IsFunctor(message_or_fn));
          if (!is_fn) {
            if (args_slot.SubSlotCount()) {
              return absl::InvalidArgumentError(
                  "expected `args` to be empty when `message_or_fn` is a "
                  "STRING message");
            }
            return raise(message_or_fn);
          }
          // Otherwise we need to eval it to get the output.
          std::vector<arolla::TypedRef> arg_refs;
          arg_refs.reserve(args_slot.SubSlotCount());
          for (int64_t i = 0; i < args_slot.SubSlotCount(); ++i) {
            arg_refs.push_back(
                arolla::TypedRef::FromSlot(args_slot.SubSlot(i), frame));
          }
          ASSIGN_OR_RETURN(auto message_tv,
                           functor::CallFunctorWithCompilationCache(
                               message_or_fn, arg_refs, {}));
          if (message_tv.GetType() != arolla::GetQType<DataSlice>()) {
            return absl::InvalidArgumentError(absl::StrFormat(
                "expected %s as the functor output type, but got %s",
                arolla::GetQType<DataSlice>()->name(),
                message_tv.GetType()->name()));
          }
          return raise(message_tv.UnsafeAs<DataSlice>());
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> WithAssertionOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 4) {
    return absl::InvalidArgumentError("requires exactly 4 arguments");
  }
  if (input_types[1] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("requires argument 1 to be DataSlice");
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError("requires argument 2 to be DataSlice");
  }
  if (!arolla::IsTupleQType(input_types[3])) {
    return absl::InvalidArgumentError("requires argument 3 to be Tuple");
  }
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<WithAssertionOperator>(input_types, output_type),
      input_types, output_type);
}

}  // namespace koladata::functor
