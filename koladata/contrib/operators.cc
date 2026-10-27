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
#include "arolla/qexpr/operators.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "koladata/contrib/cascade.h"
#include "koladata/contrib/flatten_cyclic_references.h"
#include "koladata/contrib/get_attr_or_item.h"
#include "koladata/contrib/sanitize_names.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/call.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_qtype.h"
#include "koladata/functor/parallel/transform_config_registry.h"
#include "koladata/internal/op_utils/qexpr.h"
#include "koladata/operators/utils.h"

namespace koladata::contrib {
namespace {

using ::koladata::functor::parallel::ExecutorPtr;
using ::koladata::functor::parallel::IsStreamQType;
using ::koladata::functor::parallel::StreamPtr;

class StreamBatchedCascadeOp final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "kd_ext.contrib._stream_batched_cascade",
        [executor_slot = input_slots[0].UnsafeToSlot<ExecutorPtr>(),
         input_stream_slot = input_slots[1].UnsafeToSlot<StreamPtr>(),
         step_fn_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         yields_param_name_slot = input_slots[3].UnsafeToSlot<arolla::Text>(),
         initial_yields_slot = input_slots[4], state_slot = input_slots[5],
         output_slot = output_slot.UnsafeToSlot<StreamPtr>()](
            arolla::EvaluationContext* /*ctx*/,
            arolla::FramePtr frame) -> absl::Status {
          auto step_fn = [step_fn_ds = frame.Get(step_fn_slot)](
                             absl::Span<const arolla::TypedRef> args,
                             absl::Span<const std::string> kwnames)
              -> absl::StatusOr<arolla::TypedValue> {
            ASSIGN_OR_RETURN(auto result,
                             functor::CallFunctorWithCompilationCache(
                                 step_fn_ds, args, kwnames),
                             _ << "error occurred while calling `step_fn`");
            return result;
          };
          ASSIGN_OR_RETURN(
              auto stream,
              StreamBatchedCascade(
                  frame.Get(executor_slot), frame.Get(input_stream_slot),
                  std::move(step_fn), frame.Get(yields_param_name_slot).view(),
                  arolla::TypedRef::FromSlot(initial_yields_slot, frame),
                  arolla::TypedRef::FromSlot(state_slot, frame)));
          frame.Set(output_slot, std::move(stream));
          return absl::OkStatus();
        });
  }
};

// kd_ext.contrib._stream_batched_cascade(
//     executor: EXECUTOR,
//     stream: STREAM[T],
//     step_fn: DATA_SLICE,
//     yields_param_name: TEXT,
//     initial_yields: STREAM[U],
//     initial_state: NAMEDTUPLE,
//     NON_DETERMINISTIC_TOKEN,
// ) -> STREAM[STREAM[U]]
class StreamBatchedCascadeOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final {
    if (!IsStreamQType(output_type)) {
      return absl::InvalidArgumentError("output type must be a stream");
    }
    if (input_types.size() != 7) {
      return absl::InvalidArgumentError("requires exactly 7 arguments");
    }
    if (input_types[0] != arolla::GetQType<ExecutorPtr>()) {
      return absl::InvalidArgumentError("1st argument must be an executor");
    }
    if (!IsStreamQType(input_types[1])) {
      return absl::InvalidArgumentError("2nd argument must be a stream");
    }
    if (input_types[2] != arolla::GetQType<DataSlice>()) {
      return absl::InvalidArgumentError("3rd argument must be a functor");
    }
    if (input_types[3] != arolla::GetQType<arolla::Text>()) {
      return absl::InvalidArgumentError("4th argument must be a TEXT");
    }
    if (input_types[4] != output_type->value_qtype()) {
      return absl::InvalidArgumentError(
          "5th argument must be the same as the value type of the output");
    }
    if (!arolla::IsNamedTupleQType(input_types[5])) {
      return absl::InvalidArgumentError("6th argument must be a namedtuple");
    }
    RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[6]));
    return std::make_shared<StreamBatchedCascadeOp>(input_types, output_type);
  }
};

KODA_QEXPR_OPERATOR_FAMILY(
    "kd_ext.contrib._stream_batched_cascade",
    std::make_unique<StreamBatchedCascadeOperatorFamily>());

KODA_QEXPR_OPERATOR("kd_ext.contrib._flatten_cyclic_references",
                    FlattenCyclicReferences,
                    "kd_ext.contrib.flatten_cyclic_references");

KODA_QEXPR_OPERATOR("kd_ext.contrib.get_attr_or_item", GetAttrOrItem);

KODA_QEXPR_OPERATOR("kd_ext.contrib.sanitize_names", SanitizeNames);

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kOperators}, .init_fn = [] {
          return functor::parallel::ExtendDefaultParallelTransformConfig(R"pb(
            from_op: "kd_ext.contrib.batched_cascade"
            to_op: "kd_ext.contrib._parallel_batched_cascade"
            argument_transformation {
              arguments: EXECUTOR
              arguments: ORIGINAL_ARGUMENTS
              functor_argument_indices: 1
            }
          )pb");
        });

}  // namespace
}  // namespace koladata::contrib
