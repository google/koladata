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
#include "koladata/functor/parallel/future_operators.h"

#include <memory>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/future.h"
#include "koladata/functor/parallel/future_qtype.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/operators/core/utility_operators.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

class AsFutureOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [input_slot = input_slots[0], output_slot](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          arolla::TypedValue input_value =
              arolla::TypedValue::FromSlot(input_slot, frame);
          auto [future, writer] = MakeFuture(input_slot.GetType());
          std::move(writer).SetValue(std::move(input_value));
          frame.Set(output_slot.UnsafeToSlot<FuturePtr>(), std::move(future));
          return absl::OkStatus();
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> AsFutureOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("requires exactly 1 argument");
  }
  if (IsFutureQType(input_types[0])) {
    if (input_types[0] != output_type) {
      return absl::InvalidArgumentError(
          "output future qtype must match input type");
    }
    return arolla::EnsureOutputQTypeMatches(arolla::MakeCopyOp(output_type),
                                            input_types, output_type);
  } else {
    if (!IsFutureQType(output_type) ||
        output_type->value_qtype() != input_types[0]) {
      return absl::InvalidArgumentError(
          "output qtype must be a future of the input type");
    }
    return arolla::EnsureOutputQTypeMatches(
        std::make_shared<AsFutureOperator>(input_types, output_type),
        input_types, output_type);
  }
}

namespace {

class GetFutureValueForTestingOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [input_slot = input_slots[0].UnsafeToSlot<FuturePtr>(), output_slot](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          const auto& future = frame.Get(input_slot);
          if (future == nullptr) {
            return absl::InvalidArgumentError("future is null");
          }
          ASSIGN_OR_RETURN(arolla::TypedValue value,
                           future->GetValueForTesting());
          return value.CopyToSlot(output_slot, frame);
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr>
GetFutureValueForTestingOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("requires exactly 1 argument");
  }
  if (input_types[0] != GetFutureQType(output_type)) {
    return absl::InvalidArgumentError(
        "argument must be a future of the output type");
  }
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<GetFutureValueForTestingOperator>(input_types,
                                                         output_type),
      input_types, output_type);
}

}  // namespace koladata::functor::parallel
