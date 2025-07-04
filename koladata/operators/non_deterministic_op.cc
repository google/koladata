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
#include "koladata/operators/non_deterministic_op.h"

#include <memory>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/internal/op_utils/qexpr.h"

namespace koladata::ops {
namespace {

using ::arolla::BoundOperator;
using ::arolla::EvaluationContext;
using ::arolla::FramePtr;
using ::arolla::GetQType;
using ::arolla::OperatorPtr;
using ::arolla::QExprOperator;
using ::arolla::QTypePtr;
using ::arolla::TypedSlot;

class NonDeterministicIdentityOp final : public QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final {
    return MakeBoundOperator("koda_internal.non_deterministic_identity",
                             [input_slot = input_slots[0], output_slot](
                                 EvaluationContext*, FramePtr frame) {
                               input_slot.CopyTo(frame, output_slot, frame);
                               return absl::OkStatus();
                             });
  }
};

}  // namespace

absl::StatusOr<OperatorPtr> NonDeterministicIdentityOpFamily::DoGetOperator(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) const {
  if (input_types.size() != 2 || input_types[0] != output_type ||
      input_types[1] != GetQType<internal::NonDeterministicToken>()) {
    return absl::InvalidArgumentError(
        "expected types: (T, NON_DETERMINISTIC_TOKEN) -> T");
  }
  return std::make_shared<NonDeterministicIdentityOp>(input_types, output_type);
}

}  // namespace koladata::ops
