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
#include "koladata/functor/aggregate_operator.h"

#include <cstdint>
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
#include "koladata/iterables/iterable_qtype.h"
#include "koladata/operators/utils.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/sequence/sequence.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {
namespace {

class AggregateOperator : public arolla::QExprOperator {
 public:
  explicit AggregateOperator(absl::Span<const arolla::QTypePtr> input_types,
                             arolla::QTypePtr output_type)
      : QExprOperator(
            "kd.functor.aggregate",
            arolla::QExprOperatorSignature::Get(input_types, output_type)) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [fn_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         seq_input_slot = input_slots[1].UnsafeToSlot<arolla::Sequence>(),
         output_slot](arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          const auto& fn_data_slice = frame.Get(fn_slot);
          const arolla::Sequence& seq_input = frame.Get(seq_input_slot);
          const int64_t seq_size = seq_input.size();
          std::vector<arolla::TypedRef> arg_refs;
          arg_refs.reserve(seq_size);
          for (int64_t i = 0; i < seq_size; ++i) {
            arg_refs.push_back(seq_input.GetRef(i));
          }
          ASSIGN_OR_RETURN(auto result,
                           functor::CallFunctorWithCompilationCache(
                               fn_data_slice, std::move(arg_refs), {}),
                           ctx->set_status(std::move(_)));
          if (result.GetType() != output_slot.GetType()) {
            ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
                "The functor was called with `%s` as the output type, but the"
                " computation resulted in type `%s` instead. You can specify"
                " the expected output type via the `return_type_as=` parameter"
                " to the functor call.",
                output_slot.GetType()->name(), result.GetType()->name())));
            return;
          }
          RETURN_IF_ERROR(result.CopyToSlot(output_slot, frame))
              .With(ctx->set_status());
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> AggregateOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 4) {
    return absl::InvalidArgumentError("requires exactly 4 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be DataSlice");
  }

  if (!iterables::IsIterableQType(input_types[1])) {
    return absl::InvalidArgumentError(
        "requires second argument to be an iterable");
  }

  // Argument 2 is used to infer the return type, which is handled on the Expr
  // operator level, so we just ignore it here.

  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[3]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<AggregateOperator>(input_types, output_type),
      input_types, output_type);
}

}  // namespace koladata::functor
