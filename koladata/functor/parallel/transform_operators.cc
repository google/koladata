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
#include "koladata/functor/parallel/transform_operators.h"

#include <memory>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/execution_context.h"
#include "koladata/functor/parallel/transform.h"
#include "koladata/internal/op_utils/qexpr.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

class TransformManyOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "koda_internal.parallel.transform_many",
        [context_slot = input_slots[0].UnsafeToSlot<ExecutionContextPtr>(),
         fn_slot = input_slots[1],
         output_slot = output_slot](arolla::EvaluationContext* ctx,
                                    arolla::FramePtr frame) -> absl::Status {
          const auto& context = frame.Get(context_slot);
          arolla::TypedValue fn = arolla::TypedValue::FromSlot(fn_slot, frame);
          ASSIGN_OR_RETURN(arolla::TypedValue result,
                           TransformManyToParallel(context, std::move(fn)));
          RETURN_IF_ERROR(result.CopyToSlot(output_slot, frame));
          return absl::OkStatus();
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> TransformManyOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 2) {
    return absl::InvalidArgumentError("requires exactly 2 arguments");
  }
  if (input_types[0] != arolla::GetQType<ExecutionContextPtr>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be ExecutionContext");
  }
  if (input_types[1] != arolla::GetQType<DataSlice>() &&
      input_types[1] != arolla::GetUnspecifiedQType()) {
    return absl::InvalidArgumentError(
        "requires second argument to be a DataSlice or unspecified");
  }
  if (output_type != input_types[1]) {
    return absl::InvalidArgumentError(
        "the output type must be the same as the second argument");
  }
  return std::make_shared<TransformManyOperator>(input_types, output_type);
}

}  // namespace koladata::functor::parallel
