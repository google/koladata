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
#include "koladata/iterables/sequence_operators.h"

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/sequence/mutable_sequence.h"
#include "arolla/sequence/sequence.h"
#include "arolla/sequence/sequence_qtype.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/operators/slices.h"
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
      : QExprOperator({arolla::GetQType<DataSlice>()},
                      arolla::GetSequenceQType(arolla::GetQType<DataSlice>())) {
  }

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

namespace {

absl::StatusOr<DataSlice> SequenceTo1DSlice(const arolla::Sequence& x) {
  DataSlice is_stack = DataSlice::CreateFromScalar(true);
  DataSlice ndim = DataSlice::CreateFromScalar(0);
  std::vector<const DataSlice*> stack_args;
  stack_args.reserve(2 + x.size());
  stack_args.push_back(&is_stack);
  stack_args.push_back(&ndim);
  for (size_t i = 0; i < x.size(); ++i) {
    ASSIGN_OR_RETURN(const DataSlice& data_slice, x.GetRef(i).As<DataSlice>());
    if (data_slice.GetShape().rank() != 0) {
      return absl::InvalidArgumentError(
          absl::StrCat("All inputs must be DataItems, got ",
                       data_slice.GetShape().rank(), " dimensions"));
    }
    stack_args.push_back(&data_slice);
  }
  return ops::ConcatOrStack(stack_args);
}

struct SequenceTo1DSliceOp final : public arolla::QExprOperator {
  explicit SequenceTo1DSliceOp()
      : QExprOperator({arolla::GetSequenceQType(arolla::GetQType<DataSlice>())},
                      arolla::GetQType<DataSlice>()) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [x_slot = input_slots[0].UnsafeToSlot<arolla::Sequence>(),
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          ASSIGN_OR_RETURN(auto res, SequenceTo1DSlice(frame.Get(x_slot)),
                           ctx->set_status(std::move(_)));
          frame.Set(output_slot, std::move(res));
        });
  }
};

}  // namespace

// Sequence has no QTypeTraits, since it corresponds to many possible QTypes.
// Therefore we cannot use the automatic creation of an operator from the C++
// function.
absl::StatusOr<arolla::OperatorPtr> SequenceTo1DSliceOpFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1 ||
      input_types[0] !=
          arolla::GetSequenceQType(arolla::GetQType<DataSlice>())) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "unexpected argument types: %s", JoinTypeNames(input_types)));
  }
  return EnsureOutputQTypeMatches(
      arolla::OperatorPtr(new SequenceTo1DSliceOp()), input_types, output_type);
}

namespace {

struct SequenceChainOp final : public arolla::QExprOperator {
  explicit SequenceChainOp(arolla::QTypePtr output_type)
      : QExprOperator({GetSequenceQType(output_type)}, output_type) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    if (input_slots.size() != 1) {
      return absl::InternalError("expected exactly one input");
    }
    return arolla::MakeBoundOperator(
        [input_slot = input_slots[0].UnsafeToSlot<arolla::Sequence>(),
         output_slot = output_slot](arolla::EvaluationContext* ctx,
                                    arolla::FramePtr frame) {
          const auto& seq_of_seq = frame.Get(input_slot);
          size_t total_size = 0;
          for (size_t i = 0; i < seq_of_seq.size(); ++i) {
            const auto& seq = seq_of_seq.GetRef(i).UnsafeAs<arolla::Sequence>();
            total_size += seq.size();
          }
          ASSIGN_OR_RETURN(
              auto res,
              arolla::MutableSequence::Make(
                  output_slot.GetType()->value_qtype(), total_size),
              ctx->set_status(std::move(_)));
          size_t offset = 0;
          for (size_t i = 0; i < seq_of_seq.size(); ++i) {
            const auto& seq = seq_of_seq.GetRef(i).UnsafeAs<arolla::Sequence>();
            for (size_t i = 0; i < seq.size(); ++i) {
              res.UnsafeSetRef(offset, seq.GetRef(i));
              ++offset;
            }
          }
          frame.Set(output_slot.UnsafeToSlot<arolla::Sequence>(),
                    std::move(res).Finish());
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> SequenceChainOpFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("expects exactly one input");
  }
  arolla::QTypePtr input_type = input_types[0];
  if (!arolla::IsSequenceQType(input_type)) {
    return absl::InvalidArgumentError("input type must be a sequence");
  }
  if (!arolla::IsSequenceQType(output_type)) {
    return absl::InvalidArgumentError("output type must be a sequence");
  }
  if (input_type != GetSequenceQType(output_type)) {
    return absl::InvalidArgumentError(
        "input type must be a sequence of sequences (of output type)");
  }
  return EnsureOutputQTypeMatches(
      arolla::OperatorPtr(new SequenceChainOp(output_type)), input_types,
      output_type);
}

}  // namespace koladata::iterables
