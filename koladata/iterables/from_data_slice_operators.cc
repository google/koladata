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
#include "koladata/iterables/from_data_slice_operators.h"

#include <memory>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/sequence/mutable_sequence.h"
#include "arolla/sequence/sequence.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::iterables {

namespace {

absl::StatusOr<arolla::Sequence> SequenceFrom1DSlice(const DataSlice& x) {
  if (x.GetShape().rank() != 1) {
    return absl::InvalidArgumentError(absl::StrCat(
        "expected a 1D data slice, got ", x.GetShape().rank(), " dimensions"));
  }
  ASSIGN_OR_RETURN(auto res,
                   arolla::MutableSequence::Make(arolla::GetQType<DataSlice>(),
                                                 x.GetShape().size()));
  auto span = res.UnsafeSpan<DataSlice>();
  for (int i = 0; i < span.size(); ++i) {
    ASSIGN_OR_RETURN(span[i], DataSlice::Create(x.slice()[i], x.GetSchemaImpl(),
                                                x.GetBag()));
  }
  return std::move(res).Finish();
}

struct SequenceFrom1DSliceOp final : public arolla::QExprOperator {
  explicit SequenceFrom1DSliceOp()
      : QExprOperator(arolla::QExprOperatorSignature::Get(
            {arolla::GetQType<DataSlice>()},
            arolla::GetSequenceQType(arolla::GetQType<DataSlice>()))) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [x_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         output_slot = output_slot.UnsafeToSlot<arolla::Sequence>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          ASSIGN_OR_RETURN(auto res, SequenceFrom1DSlice(frame.Get(x_slot)),
                           ctx->set_status(std::move(_)));
          frame.Set(output_slot, std::move(res));
        });
  }
};

}  // namespace

// Sequence has no QTypeTraits, since it corresponds to many possible QTypes.
// Therefore we cannot use the automatic creation of an operator from the C++
// function.
absl::StatusOr<arolla::OperatorPtr> SequenceFrom1DSliceOpFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1 ||
      input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "unexpected argument types: %s", JoinTypeNames(input_types)));
  }
  return EnsureOutputQTypeMatches(
      arolla::OperatorPtr(new SequenceFrom1DSliceOp()), input_types,
      output_type);
}

}  // namespace koladata::iterables
