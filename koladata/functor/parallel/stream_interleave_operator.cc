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
#include "koladata/functor/parallel/stream_interleave_operator.h"

#include <cstdint>
#include <memory>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_composition.h"
#include "koladata/functor/parallel/stream_qtype.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"

namespace koladata::functor::parallel {
namespace {

class StreamInterleaveOp : public arolla::QExprOperator {
 public:
  explicit StreamInterleaveOp(absl::Span<const arolla::QTypePtr> input_types,
                              arolla::QTypePtr output_type)
      : QExprOperator(
            arolla::QExprOperatorSignature::Get(input_types, output_type)) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [input_slot = input_slots[0],
         value_qtype = output_slot.GetType()->value_qtype(),
         output_slot = output_slot.UnsafeToSlot<StreamPtr>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          const int64_t arg_count = input_slot.SubSlotCount();
          if (arg_count == 1) {
            frame.Set(
                output_slot,
                frame.Get(input_slot.SubSlot(0).UnsafeToSlot<StreamPtr>()));
            return;
          }
          auto [stream, writer] = MakeStream(value_qtype);
          frame.Set(output_slot, std::move(stream));
          StreamInterleave interleave_helper(std::move(writer));
          for (int64_t i = 0; i < arg_count; ++i) {
            interleave_helper.Add(
                frame.Get(input_slot.SubSlot(i).UnsafeToSlot<StreamPtr>()));
          }
        });
  }
};

}  // namespace

// stream_interleave(TUPLE[STREAM[T], ...], T, NON_DETERMINISTIC) -> STREAM[T]
absl::StatusOr<arolla::OperatorPtr>
StreamInterleaveOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 3) {
    return absl::InvalidArgumentError("invalid number of arguments");
  }
  if (!IsStreamQType(output_type)) {
    return absl::InvalidArgumentError("unexpected output type");
  }
  if (!arolla::IsTupleQType(input_types[0])) {
    return absl::InvalidArgumentError("unexpected first argument type");
  }
  for (const auto& field : input_types[0]->type_fields()) {
    if (field.GetType() != output_type) {
      return absl::InvalidArgumentError("unexpected first argument type");
    }
  }
  return std::make_shared<StreamInterleaveOp>(input_types, output_type);
}

}  // namespace koladata::functor::parallel
