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
#include "koladata/functor/parallel/async_eval_operator.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/async_eval.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/future.h"
#include "koladata/functor/parallel/future_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

class AsyncEvalOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [executor_slot = input_slots[0].UnsafeToSlot<ExecutorPtr>(),
         op_slot = input_slots[1].UnsafeToSlot<arolla::expr::ExprOperatorPtr>(),
         op_input_slots =
             std::vector(input_slots.begin() + 2, input_slots.end()),
         output_slot](arolla::EvaluationContext* ctx,
                      arolla::FramePtr frame) -> absl::Status {
          const auto& executor = frame.Get(executor_slot);
          const auto& op = frame.Get(op_slot);
          arolla::QTypePtr output_qtype = output_slot.GetType()->value_qtype();

          std::vector<arolla::TypedRef> arg_refs;
          arg_refs.reserve(op_input_slots.size());
          for (int64_t i = 0; i < op_input_slots.size(); ++i) {
            arg_refs.push_back(
                arolla::TypedRef::FromSlot(op_input_slots[i], frame));
          }
          ASSIGN_OR_RETURN(auto result,
                           AsyncEvalWithCompilationCache(executor, op, arg_refs,
                                                         output_qtype));
          if (GetFutureQType(result->value_qtype()) != output_slot.GetType()) {
            return absl::InternalError("async eval returned unexpected type");
          }
          frame.Set(output_slot.UnsafeToSlot<FuturePtr>(), std::move(result));
          return absl::OkStatus();
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> AsyncEvalOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() < 2) {
    return absl::InvalidArgumentError("requires at least 2 arguments");
  }
  if (input_types[0] != arolla::GetQType<ExecutorPtr>()) {
    return absl::InvalidArgumentError("requires first argument to be Executor");
  }
  if (input_types[1] != arolla::GetQType<arolla::expr::ExprOperatorPtr>()) {
    return absl::InvalidArgumentError(
        "requires second argument to be operator");
  }
  if (!IsFutureQType(output_type)) {
    return absl::InvalidArgumentError("requires output type to be future");
  }
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<AsyncEvalOperator>(input_types, output_type),
      input_types, output_type);
}

namespace {

class AsyncUnpackTupleOperator : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [input_slot = input_slots[0].UnsafeToSlot<FuturePtr>(), output_slot](
            arolla::EvaluationContext* /*ctx*/,
            arolla::FramePtr frame) -> absl::Status {
          const FuturePtr& input = frame.Get(input_slot);
          arolla::QTypePtr output_qtype = output_slot.GetType();
          DCHECK(arolla::IsTupleQType(output_qtype));
          int64_t num_fields = output_qtype->type_fields().size();
          std::vector<arolla::TypedValue> output_futures;
          std::vector<FutureWriter> output_writers;
          output_futures.reserve(num_fields);
          output_writers.reserve(num_fields);
          for (int64_t idx = 0; idx < num_fields; ++idx) {
            arolla::QTypePtr field_qtype =
                output_qtype->type_fields()[idx].GetType();
            DCHECK(IsFutureQType(field_qtype));
            auto [future, writer] = MakeFuture(field_qtype->value_qtype());
            output_futures.push_back(MakeFutureQValue(std::move(future)));
            output_writers.push_back(std::move(writer));
          }
          input->AddConsumer(
              [output_writers = std::move(output_writers)](
                  absl::StatusOr<arolla::TypedValue> value) mutable {
                for (int64_t idx = 0; idx < output_writers.size(); ++idx) {
                  std::move(output_writers[idx])
                      .SetValue(value.ok()
                                    ? arolla::TypedValue(value->GetField(idx))
                                    : value);
                }
              });
          auto output_tuple = arolla::MakeTuple(std::move(output_futures));
          return output_tuple.CopyToSlot(output_slot, frame);
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr>
AsyncUnpackTupleOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("requires exactly 1 argument");
  }
  if (!IsFutureQType(input_types[0])) {
    return absl::InvalidArgumentError("requires first argument to be a future");
  }
  arolla::QTypePtr input_value_qtype = input_types[0]->value_qtype();
  if (!IsTupleQType(input_value_qtype) &&
      !IsNamedTupleQType(input_value_qtype)) {
    return absl::InvalidArgumentError(
        "requires the future to contain a tuple or a namedtuple");
  }
  if (!IsTupleQType(output_type)) {
    return absl::InvalidArgumentError("requires output type to be a tuple");
  }
  if (input_value_qtype->type_fields().size() !=
      output_type->type_fields().size()) {
    return absl::InvalidArgumentError("tuple fields size mismatch");
  }
  for (int64_t idx = 0; idx < input_value_qtype->type_fields().size(); ++idx) {
    arolla::QTypePtr output_field_qtype =
        output_type->type_fields()[idx].GetType();
    if (!IsFutureQType(output_field_qtype)) {
      return absl::InvalidArgumentError(
          "requires output type to be a tuple of futures");
    }
    if (output_field_qtype->value_qtype() !=
        input_value_qtype->type_fields()[idx].GetType()) {
      return absl::InvalidArgumentError("tuple field qtype mismatch");
    }
  }
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<AsyncUnpackTupleOperator>(input_types, output_type),
      input_types, output_type);
}

}  // namespace koladata::functor::parallel
