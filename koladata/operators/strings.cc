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
#include "koladata/operators/strings.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/arolla_utils.h"
#include "koladata/casting.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/operators/arolla_bridge.h"
#include "koladata/operators/utils.h"
#include "koladata/shape_utils.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace {

absl::StatusOr<DataSlice> EvalFormatOp(absl::string_view op_name,
                                       const DataSlice& fmt,
                                       std::vector<DataSlice> slices) {
  ASSIGN_OR_RETURN(auto primitive_schema, GetPrimitiveArollaSchema(fmt));
  // If `fmt` is empty, we avoid calling the implementation altogether. Calling
  // SimplePointwiseEval when `fmt` is empty would resolve it to the type of the
  // first present value, which can be of any type.
  if (!primitive_schema.has_value()) {
    ASSIGN_OR_RETURN(auto common_shape, shape::GetCommonShape(slices));
    return BroadcastToShape(fmt, std::move(common_shape));
  }
  // From here on, we know that at least one input has known schema and we
  // should eval.
  return SimplePointwiseEval(op_name, std::move(slices), fmt.GetSchemaImpl());
}

class FormatOperator : public arolla::QExprOperator {
 public:
  explicit FormatOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(arolla::QExprOperatorSignature::Get(
            input_types, arolla::GetQType<DataSlice>())) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    auto named_tuple_slot = input_slots[1];
    auto attr_names = GetAttrNames(named_tuple_slot);
    ASSIGN_OR_RETURN(
        auto arg_names_slice,
        DataSlice::Create(
            internal::DataItem(arolla::Text(absl::StrJoin(attr_names, ","))),
            internal::DataItem(schema::kText)));
    return arolla::MakeBoundOperator(
        [arg_names_slice = std::move(arg_names_slice),
         attr_names = std::move(attr_names),
         format_spec_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         named_tuple_slot, output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          auto values = GetValueDataSlices(named_tuple_slot, frame);

          const DataSlice& format_spec = frame.Get(format_spec_slot);
          values.insert(values.begin(), {format_spec, arg_names_slice});
          ASSIGN_OR_RETURN(
              auto result,
              EvalFormatOp("strings.format", format_spec, std::move(values)),
              ctx->set_status(std::move(_)));
          frame.Set(output_slot, std::move(result));
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> FormatOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 2) {
    return absl::InvalidArgumentError("requires exactly 2 arguments");
  }
  RETURN_IF_ERROR(VerifyNamedTuple(input_types[1]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<FormatOperator>(input_types), input_types, output_type);
}

absl::StatusOr<DataSlice> AggJoin(const DataSlice& x, const DataSlice& sep) {
  if (sep.GetShape().rank() != 0) {
    return absl::InvalidArgumentError("expected rank(sep) == 0");
  }
  return SimpleAggIntoEval("strings.agg_join", {x, sep});
}

absl::StatusOr<DataSlice> Contains(const DataSlice& x,
                                   const DataSlice& substr) {
  return SimplePointwiseEval(
      "strings.contains", {x, substr},
      /*output_schema=*/internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> Count(const DataSlice& x, const DataSlice& substr) {
  return SimplePointwiseEval("strings.count", {x, substr},
                             internal::DataItem(schema::kInt32));
}

absl::StatusOr<DataSlice> Find(const DataSlice& x, const DataSlice& substr,
                               const DataSlice& start, const DataSlice& end,
                               const DataSlice& failure_value) {
  ASSIGN_OR_RETURN(auto typed_start,
                   CastToNarrow(start, internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(auto typed_end,
                   CastToNarrow(end, internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(
      auto typed_failure_value,
      CastToNarrow(failure_value, internal::DataItem(schema::kInt64)));
  return SimplePointwiseEval(
      "strings.find",
      {x, substr, std::move(typed_start), std::move(typed_end),
       std::move(typed_failure_value)},
      /*output_schema=*/internal::DataItem(schema::kInt64),
      /*primary_operand_indices=*/{{0, 1}});
}

absl::StatusOr<DataSlice> Printf(std::vector<DataSlice> slices) {
  if (slices.empty()) {
    return absl::InvalidArgumentError("expected at least one input");
  }
  const auto& fmt = slices[0];
  return EvalFormatOp("strings.printf", fmt, std::move(slices));
}

// This operator is only used for koda_operator_coverage_test.
absl::StatusOr<DataSlice> TestOnlyFormatWrapper(std::vector<DataSlice> slices) {
  if (slices.size() < 2) {
    return absl::InvalidArgumentError("expected at least two inputs");
  }
  const auto& arg_name_slice = slices[1];
  absl::string_view arg_names = arg_name_slice.item().value<arolla::Text>();
  std::vector<arolla::TypedRef> arg_values;
  arg_values.reserve(slices.size() - 2);
  for (size_t i = 2; i < slices.size(); ++i) {
    arg_values.push_back(arolla::TypedRef::FromValue(slices[i]));
  }
  std::vector<std::string> arg_names_split;
  if (!arg_names.empty()) {
    arg_names_split = absl::StrSplit(arg_names, ',');
  }
  ASSIGN_OR_RETURN(auto kwargs,
                   arolla::MakeNamedTuple(arg_names_split, arg_values));
  ASSIGN_OR_RETURN(
      auto result,
      EvalExpr("kde.strings.format",
               {arolla::TypedRef::FromValue(slices[0]), kwargs.AsRef()}));
  return result.As<DataSlice>();
}

absl::StatusOr<DataSlice> Join(std::vector<DataSlice> slices) {
  if (slices.empty()) {
    return absl::InvalidArgumentError("expected at least one input");
  }
  return SimplePointwiseEval("strings.join", std::move(slices));
}

absl::StatusOr<DataSlice> Length(const DataSlice& x) {
  return SimplePointwiseEval("strings.length", {x},
                             internal::DataItem(schema::kInt32));
}

absl::StatusOr<DataSlice> Lower(const DataSlice& x) {
  // TODO: Add support for BYTES.
  return SimplePointwiseEval("strings.lower", {x},
                             internal::DataItem(schema::kText));
}

absl::StatusOr<DataSlice> Replace(const DataSlice& s,
                                  const DataSlice& old_substr,
                                  const DataSlice& new_substr,
                                  const DataSlice& max_subs) {
  ASSIGN_OR_RETURN(auto typed_max_subs,
                   CastToNarrow(max_subs, internal::DataItem(schema::kInt32)));
  return SimplePointwiseEval(
      "strings.replace", {s, old_substr, new_substr, std::move(typed_max_subs)},
      /*output_schema=*/s.GetSchemaImpl(),
      /*primary_operand_indices=*/{{0, 1, 2}});
}

absl::StatusOr<DataSlice> Rfind(const DataSlice& x, const DataSlice& substr,
                                const DataSlice& start, const DataSlice& end,
                                const DataSlice& failure_value) {
  ASSIGN_OR_RETURN(auto typed_start,
                   CastToNarrow(start, internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(auto typed_end,
                   CastToNarrow(end, internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(
      auto typed_failure_value,
      CastToNarrow(failure_value, internal::DataItem(schema::kInt64)));
  return SimplePointwiseEval(
      "strings.rfind",
      {x, substr, std::move(typed_start), std::move(typed_end),
       std::move(typed_failure_value)},
      /*output_schema=*/internal::DataItem(schema::kInt64),
      /*primary_operand_indices=*/{{0, 1}});
}

absl::StatusOr<DataSlice> Split(const DataSlice& x, const DataSlice& sep) {
  const auto& x_shape = x.GetShape();
  if (sep.GetShape().rank() != 0) {
    return absl::InvalidArgumentError("expected rank(sep) == 0");
  }
  ASSIGN_OR_RETURN(
      auto common_schema,
      schema::CommonSchema(x.GetSchemaImpl(), sep.GetSchemaImpl()));
  ASSIGN_OR_RETURN(auto x_primitive_schema, GetPrimitiveArollaSchema(x));
  ASSIGN_OR_RETURN(auto sep_primitive_schema, GetPrimitiveArollaSchema(sep));
  // If all inputs are empty-and-unknown, the output will be too.
  if (!x_primitive_schema.has_value() && !sep_primitive_schema.has_value()) {
    ASSIGN_OR_RETURN(auto ds, DataSlice::Create(internal::DataItem(),
                                                std::move(common_schema)));
    ASSIGN_OR_RETURN(
        auto out_edge,
        DataSlice::JaggedShape::Edge::FromUniformGroups(x_shape.size(), 0));
    ASSIGN_OR_RETURN(auto out_shape, x_shape.AddDims({std::move(out_edge)}));
    return BroadcastToShape(ds, std::move(out_shape));
  }
  // Otherwise, we should eval. `strings.split` requires a dense array input, so
  // we flatten to avoid scalar inputs.
  std::vector<arolla::TypedValue> typed_value_holder;
  typed_value_holder.reserve(2);
  ASSIGN_OR_RETURN(auto flat_x,
                   x.Reshape(x_shape.FlattenDims(0, x_shape.rank())));
  ASSIGN_OR_RETURN(auto x_ref,
                   DataSliceToOwnedArollaRef(flat_x, typed_value_holder,
                                             sep_primitive_schema));
  ASSIGN_OR_RETURN(
      auto sep_ref,
      DataSliceToOwnedArollaRef(sep, typed_value_holder, x_primitive_schema));
  ASSIGN_OR_RETURN(
      auto result,
      EvalExpr("strings.split", {std::move(x_ref), std::move(sep_ref)}));
  DCHECK(arolla::IsTupleQType(result.GetType()) && result.GetFieldCount() == 2);
  ASSIGN_OR_RETURN(auto edge_ref,
                   result.GetField(1).As<DataSlice::JaggedShape::Edge>());
  ASSIGN_OR_RETURN(auto out_shape, x_shape.AddDims({std::move(edge_ref)}));
  return DataSliceFromArollaValue(result.GetField(0), std::move(out_shape),
                                  std::move(common_schema));
}

absl::StatusOr<DataSlice> Strip(const DataSlice& s, const DataSlice& chars) {
  return SimplePointwiseEval("strings.strip", {s, chars});
}

absl::StatusOr<DataSlice> Substr(const DataSlice& x, const DataSlice& start,
                                 const DataSlice& end) {
  ASSIGN_OR_RETURN(auto typed_start,
                   CastToNarrow(start, internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(auto typed_end,
                   CastToNarrow(end, internal::DataItem(schema::kInt64)));
  return SimplePointwiseEval("strings.substr",
                             {x, std::move(typed_start), std::move(typed_end)},
                             /*output_schema=*/x.GetSchemaImpl(),
                             /*primary_operand_indices=*/{{0}});
}

absl::StatusOr<DataSlice> Upper(const DataSlice& x) {
  // TODO: Add support for BYTES.
  return SimplePointwiseEval("strings.upper", {x},
                             internal::DataItem(schema::kText));
}

}  // namespace koladata::ops
