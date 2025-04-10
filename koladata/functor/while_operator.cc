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
#include "koladata/functor/while_operator.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/while.h"
#include "koladata/iterables/iterable_qtype.h"
#include "koladata/operators/utils.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {
namespace {

class WhileOperator final : public arolla::QExprOperator {
 public:
  WhileOperator(absl::Span<const arolla::QTypePtr> input_types,
                arolla::QTypePtr output_type)
      : arolla::QExprOperator(
            "kd.functor._while",
            arolla::QExprOperatorSignature::Get(input_types, output_type)) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const override {
    std::optional<arolla::TypedSlot> output_var_slot;
    std::string_view output_var_name;
    bool is_returns_mode = false;
    if (input_slots[2].GetType() != arolla::GetUnspecifiedQType()) {
      output_var_slot = input_slots[2];
      output_var_name = "returns";
      is_returns_mode = true;
    } else if (input_slots[3].GetType() != arolla::GetUnspecifiedQType()) {
      output_var_slot = input_slots[3];
      output_var_name = "yields";
    } else {
      DCHECK(input_slots[4].GetType() != arolla::GetUnspecifiedQType());
      output_var_slot = input_slots[4];
      output_var_name = "yields_interleaved";
    }

    return arolla::MakeBoundOperator(
        [condition_fn_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         body_fn_slot = input_slots[1].UnsafeToSlot<DataSlice>(),
         output_var_slot = output_var_slot.value(), output_var_name,
         is_returns_mode, initial_state_slot = input_slots[5],
         output_slot](arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          const auto& condition_fn = frame.Get(condition_fn_slot);
          const auto& body_fn = frame.Get(body_fn_slot);

          auto initial_state_names =
              arolla::GetFieldNames(initial_state_slot.GetType());
          std::vector<std::string> var_names;
          var_names.reserve(initial_state_names.size() + 1);
          std::vector<arolla::TypedValue> var_values;
          var_values.reserve(initial_state_names.size() + 1);
          var_names.emplace_back(output_var_name);
          var_values.emplace_back(
              arolla::TypedValue::FromSlot(output_var_slot, frame));
          for (int64_t i = 0; i < initial_state_names.size(); ++i) {
            var_names.emplace_back(initial_state_names[i]);
            var_values.emplace_back(arolla::TypedValue::FromSlot(
                initial_state_slot.SubSlot(i), frame));
          }

          ASSIGN_OR_RETURN(var_values,
                           WhileWithCompilationCache(
                               condition_fn, body_fn, std::move(var_names),
                               std::move(var_values), is_returns_mode ? 0 : 1),
                           ctx->set_status(std::move(_)));
          RETURN_IF_ERROR(var_values[0].CopyToSlot(output_slot, frame))
              .With(ctx->set_status());
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> WhileOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  // Args: condition_fn, body_fn, returns, yields, yields_interleaved,
  // initial_state, _non_deterministic_token
  if (input_types.size() != 7) {
    return absl::InvalidArgumentError("requires exactly 7 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires first argument `condition_fn` to be DataSlice");
  }
  if (input_types[1] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires second argument `body_fn` to be DataSlice");
  }
  if (((input_types[2] != arolla::GetUnspecifiedQType()) +
       (input_types[3] != arolla::GetUnspecifiedQType()) +
       (input_types[4] != arolla::GetUnspecifiedQType())) != 1) {
    return absl::InvalidArgumentError(
        "requires exactly one of `returns`, `yields`, or `yields_interleaved` "
        "to be specified");
  }
  if (input_types[2] != arolla::GetUnspecifiedQType()) {
    if (input_types[2] != output_type) {
      return absl::InvalidArgumentError(
          "requires third argument `returns` to have the same type as the "
          "output if specified");
    }
  }
  if (input_types[3] != arolla::GetUnspecifiedQType()) {
    if (!iterables::IsIterableQType(input_types[3])) {
      return absl::InvalidArgumentError(
          "requires fourth argument `yields` to be an iterable if specified");
    }
    if (!arolla::IsSequenceQType(output_type)) {
      return absl::InvalidArgumentError(
          "requires output type to be a sequence if `yields` is specified");
    }
    if (input_types[3] != output_type->value_qtype()) {
      return absl::InvalidArgumentError(
          "requires fourth argument `yields` to have the same type as the "
          "output if specified");
    }
  }
  if (input_types[4] != arolla::GetUnspecifiedQType()) {
    if (!iterables::IsIterableQType(input_types[4])) {
      return absl::InvalidArgumentError(
          "requires fifth argument `yields_interleaved` to be an iterable if "
          "specified");
    }
    if (!arolla::IsSequenceQType(output_type)) {
      return absl::InvalidArgumentError(
          "requires output type to be a sequence if `yields_interleave` is "
          "specified");
    }
    if (input_types[4] != output_type->value_qtype()) {
      return absl::InvalidArgumentError(
          "requires fifth argument `yields_interleaved` to have the same type "
          "as the output if specified");
    }
  }
  if (!arolla::IsNamedTupleQType(input_types[5])) {
    return absl::InvalidArgumentError(
        "requires sixth argument `initial_state` to be a namedtuple");
  }
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[6]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<WhileOperator>(input_types, output_type), input_types,
      output_type);
}

}  // namespace koladata::functor
