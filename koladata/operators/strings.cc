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
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/arolla_utils.h"
#include "koladata/casting.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/qexpr.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/operators/arolla_bridge.h"
#include "koladata/operators/utils.h"
#include "koladata/pointwise_utils.h"
#include "koladata/schema_utils.h"
#include "koladata/shape_utils.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"
#include "arolla/util/string.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {

// A wrapper around CastToNarrow with nicer error message.
absl::StatusOr<DataSlice> NarrowToInt64(const DataSlice& arg,
                                        absl::string_view arg_name) {
  RETURN_IF_ERROR(ExpectInteger(arg_name, arg));
  return CastToNarrow(arg, internal::DataItem(schema::kInt64));
}

// Returns OK if all the arguments can be formatted. If arg_names is not empty,
// it will be used for error reporting.
absl::Status ExpectCanBeFormatted(absl::Span<const absl::string_view> arg_names,
                                  absl::Span<const DataSlice> args) {
  for (size_t i = 0; i < args.size(); ++i) {
    auto narrowed_schema = GetNarrowedSchema(args[i]);
    bool can_be_formatted =
        schema::IsImplicitlyCastableTo(narrowed_schema,
                                       internal::DataItem(schema::kFloat64)) ||
        narrowed_schema == schema::kBytes ||
        narrowed_schema == schema::kString || narrowed_schema == schema::kBool;
    if (!can_be_formatted) {
      // NOTE: here we are "overfitting" the function for the two existing use
      // cases. If we need to support more, we should consider a more generic
      // solution.
      std::string arg_name = arg_names.empty()
                                 ? absl::StrCat(i + 1)
                                 : absl::StrCat("`", arg_names[i], "`");
      return absl::InvalidArgumentError(
          absl::StrFormat("cannot format argument %s of type %s", arg_name,
                          DescribeSliceSchema(args[i])));
    }
  }
  return absl::OkStatus();
}

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
      : QExprOperator(input_types, arolla::GetQType<DataSlice>()) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    auto named_tuple_slot = input_slots[1];
    auto attr_names = GetFieldNames(named_tuple_slot);
    ASSIGN_OR_RETURN(
        auto arg_names_slice,
        DataSlice::Create(
            internal::DataItem(arolla::Text(absl::StrJoin(attr_names, ","))),
            internal::DataItem(schema::kString)));
    return MakeBoundOperator(
        "kd.strings.format",
        [arg_names_slice = std::move(arg_names_slice),
         attr_names = std::move(attr_names),
         format_spec_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         named_tuple_slot, output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          auto values = GetValueDataSlices(named_tuple_slot, frame);
          RETURN_IF_ERROR(ExpectCanBeFormatted(attr_names, values));
          const DataSlice& format_spec = frame.Get(format_spec_slot);
          RETURN_IF_ERROR(ExpectConsistentStringOrBytes("fmt", format_spec));
          values.insert(values.begin(), {format_spec, arg_names_slice});
          ASSIGN_OR_RETURN(
              auto result,
              EvalFormatOp("strings.format", format_spec, std::move(values)));
          frame.Set(output_slot, std::move(result));
          return absl::OkStatus();
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
  RETURN_IF_ERROR(ExpectConsistentStringOrBytes({"x", "sep"}, x, sep));
  if (sep.GetShape().rank() != 0) {
    return absl::InvalidArgumentError("expected rank(sep) == 0");
  }
  return SimpleAggIntoEval("strings.agg_join", {x, sep});
}

absl::StatusOr<DataSlice> Contains(const DataSlice& x,
                                   const DataSlice& substr) {
  RETURN_IF_ERROR(ExpectConsistentStringOrBytes({"x", "substr"}, x, substr));
  return SimplePointwiseEval(
      "strings.contains", {x, substr},
      /*output_schema=*/internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> Count(const DataSlice& x, const DataSlice& substr) {
  RETURN_IF_ERROR(ExpectConsistentStringOrBytes({"x", "substr"}, x, substr));
  return SimplePointwiseEval("strings.count", {x, substr},
                             internal::DataItem(schema::kInt64));
}

absl::StatusOr<DataSlice> DecodeBase64(const DataSlice& x,
                                       bool missing_if_invalid) {
  RETURN_IF_ERROR(ExpectConsistentStringOrBytes("x", x));
  return ApplyUnaryPointwiseFn(
      x,
      [&]<typename T>(arolla::meta::type<T>, const auto& value_view)
          -> absl::StatusOr<arolla::OptionalValue<arolla::Bytes>> {
        if constexpr (std::is_same_v<T, arolla::Text> ||
                      std::is_same_v<T, arolla::Bytes>) {
          std::string dst;
          if (!absl::Base64Unescape(value_view, &dst)) {
            if (missing_if_invalid) {
              return std::nullopt;
            }
            return absl::InvalidArgumentError(absl::StrFormat(
                "invalid base64 string: %s",
                arolla::Truncate(
                    arolla::Repr(internal::DataItem(T(value_view))), 200)));
          }
          return arolla::Bytes(std::move(dst));
        } else {
          return absl::InvalidArgumentError(
              absl::StrFormat("expected bytes or string, got %s",
                              arolla::GetQType<T>()->name()));
        }
      },
      internal::DataItem(schema::kBytes));
}

absl::StatusOr<DataSlice> EncodeBase64(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectBytes("x", x));
  return ApplyUnaryPointwiseFn(
      x,
      [&]<typename T>(arolla::meta::type<T>,
                      const auto& value_view) -> absl::StatusOr<arolla::Text> {
        if constexpr (std::is_same_v<T, arolla::Bytes>) {
          std::string dst;
          absl::Base64Escape(value_view, &dst);
          return arolla::Text(std::move(dst));
        } else {
          return absl::InvalidArgumentError(absl::StrFormat(
              "expected bytes, got %s", arolla::GetQType<T>()->name()));
        }
      },
      internal::DataItem(schema::kString));
}

absl::StatusOr<DataSlice> Find(const DataSlice& x, const DataSlice& substr,
                               const DataSlice& start, const DataSlice& end) {
  RETURN_IF_ERROR(ExpectConsistentStringOrBytes({"x", "substr"}, x, substr));
  ASSIGN_OR_RETURN(auto typed_start, NarrowToInt64(start, "start"));
  ASSIGN_OR_RETURN(auto typed_end, NarrowToInt64(end, "end"));
  return SimplePointwiseEval(
      "strings.find", {x, substr, std::move(typed_start), std::move(typed_end)},
      /*output_schema=*/internal::DataItem(schema::kInt64),
      /*primary_operand_indices=*/{{0, 1}});
}

absl::StatusOr<DataSlice> Printf(std::vector<DataSlice> slices) {
  if (slices.empty()) {
    return absl::InvalidArgumentError("expected at least one input");
  }
  const auto& fmt = slices[0];
  RETURN_IF_ERROR(ExpectConsistentStringOrBytes("fmt", fmt));
  RETURN_IF_ERROR(ExpectCanBeFormatted({}, absl::MakeSpan(slices).subspan(1)));
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
      EvalExpr("kd.strings.format",
               {arolla::TypedRef::FromValue(slices[0]), kwargs.AsRef()}));
  return result.As<DataSlice>();
}

absl::StatusOr<DataSlice> Join(std::vector<DataSlice> slices) {
  if (slices.empty()) {
    return absl::InvalidArgumentError("expected at least one input");
  }
  for (size_t i = 1; i < slices.size(); ++i) {
    RETURN_IF_ERROR(ExpectConsistentStringOrBytes(
        {"slices[0]", absl::StrCat("slices[", i, "]")}, slices[0], slices[i]));
  }
  return SimplePointwiseEval("strings.join", std::move(slices));
}

absl::StatusOr<DataSlice> Length(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectConsistentStringOrBytes("x", x));
  return SimplePointwiseEval("strings.length", {x},
                             internal::DataItem(schema::kInt64));
}

absl::StatusOr<DataSlice> Lower(const DataSlice& x) {
  // TODO: Add support for BYTES.
  RETURN_IF_ERROR(ExpectString("x", x));
  return SimplePointwiseEval("strings.lower", {x},
                             internal::DataItem(schema::kString));
}

absl::StatusOr<DataSlice> Lstrip(const DataSlice& s, const DataSlice& chars) {
  RETURN_IF_ERROR(ExpectConsistentStringOrBytes({"s", "chars"}, s, chars));
  return SimplePointwiseEval("strings.lstrip", {s, chars});
}

absl::StatusOr<DataSlice> RegexExtract(const DataSlice& text,
                                       const DataSlice& regex) {
  RETURN_IF_ERROR(ExpectString("text", text));
  ASSIGN_OR_RETURN(absl::string_view regex_view,
                   GetStringArgument(regex, "regex"));
  ASSIGN_OR_RETURN(auto text_schema, GetPrimitiveArollaSchema(text));
  if (!text_schema.has_value()) {
    // text is empty-and-unknown. We then skip evaluation.
    // Note that we did not check whether `regex` is a well-formed regular
    // expression (and that it has exactly one capturing group). We only checked
    // its rank and schema.
    ASSIGN_OR_RETURN(DataSlice ds, DataSlice::Create(internal::DataItem(),
                                                     text.GetSchemaImpl()));
    return BroadcastToShape(std::move(ds), text.GetShape());
  }

  std::vector<arolla::TypedValue> typed_value_holder;
  ASSIGN_OR_RETURN(
      arolla::TypedRef text_ref,
      DataSliceToOwnedArollaRef(text, typed_value_holder,
                                internal::DataItem(schema::kString)));
  arolla::TypedValue typed_regex =
      arolla::TypedValue::FromValue(arolla::Text(regex_view));
  ASSIGN_OR_RETURN(arolla::TypedValue result,
                   EvalExpr("strings.extract_regex",
                            {std::move(text_ref), typed_regex.AsRef()}));
  return DataSliceFromArollaValue(result.AsRef(), text.GetShape(),
                                  text.GetSchemaImpl());
}

absl::StatusOr<DataSlice> RegexMatch(const DataSlice& text,
                                     const DataSlice& regex) {
  RETURN_IF_ERROR(ExpectString("text", text));
  ASSIGN_OR_RETURN(absl::string_view regex_view,
                   GetStringArgument(regex, "regex"));
  ASSIGN_OR_RETURN(auto text_schema, GetPrimitiveArollaSchema(text));
  if (!text_schema.has_value()) {
    // text is empty-and-unknown. We then skip evaluation.
    ASSIGN_OR_RETURN(DataSlice ds,
                     DataSlice::Create(internal::DataItem(),
                                       internal::DataItem(schema::kMask)));
    return BroadcastToShape(std::move(ds), text.GetShape());
  }

  std::vector<arolla::TypedValue> typed_value_holder;
  ASSIGN_OR_RETURN(
      arolla::TypedRef text_ref,
      DataSliceToOwnedArollaRef(text, typed_value_holder,
                                internal::DataItem(schema::kString)));
  arolla::TypedValue typed_regex =
      arolla::TypedValue::FromValue(arolla::Text(regex_view));
  ASSIGN_OR_RETURN(arolla::TypedValue result,
                   EvalExpr("strings.contains_regex",
                            {std::move(text_ref), typed_regex.AsRef()}));
  return DataSliceFromArollaValue(result.AsRef(), text.GetShape(),
                                  internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> Replace(const DataSlice& s,
                                  const DataSlice& old_substr,
                                  const DataSlice& new_substr,
                                  const DataSlice& max_subs) {
  RETURN_IF_ERROR(ExpectConsistentStringOrBytes(
      {"s", "old_substr", "new_substr"}, s, old_substr, new_substr));
  RETURN_IF_ERROR(ExpectInteger("max_subs", max_subs));
  ASSIGN_OR_RETURN(auto typed_max_subs, NarrowToInt64(max_subs, "max_subs"));
  return SimplePointwiseEval(
      "strings.replace", {s, old_substr, new_substr, std::move(typed_max_subs)},
      /*output_schema=*/s.GetSchemaImpl(),
      /*primary_operand_indices=*/{{0, 1, 2}});
}

absl::StatusOr<DataSlice> Rfind(const DataSlice& x, const DataSlice& substr,
                                const DataSlice& start, const DataSlice& end) {
  RETURN_IF_ERROR(ExpectConsistentStringOrBytes({"x", "substr"}, x, substr));
  ASSIGN_OR_RETURN(auto typed_start, NarrowToInt64(start, "start"));
  ASSIGN_OR_RETURN(auto typed_end, NarrowToInt64(end, "end"));
  return SimplePointwiseEval(
      "strings.rfind",
      {x, substr, std::move(typed_start), std::move(typed_end)},
      /*output_schema=*/internal::DataItem(schema::kInt64),
      /*primary_operand_indices=*/{{0, 1}});
}

absl::StatusOr<DataSlice> Rstrip(const DataSlice& s, const DataSlice& chars) {
  RETURN_IF_ERROR(ExpectConsistentStringOrBytes({"s", "chars"}, s, chars));
  return SimplePointwiseEval("strings.rstrip", {s, chars});
}

absl::StatusOr<DataSlice> Split(const DataSlice& x, const DataSlice& sep) {
  RETURN_IF_ERROR(ExpectConsistentStringOrBytes({"x", "sep"}, x, sep));
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
  RETURN_IF_ERROR(ExpectConsistentStringOrBytes({"s", "chars"}, s, chars));
  return SimplePointwiseEval("strings.strip", {s, chars});
}

absl::StatusOr<DataSlice> Substr(const DataSlice& x, const DataSlice& start,
                                 const DataSlice& end) {
  RETURN_IF_ERROR(ExpectConsistentStringOrBytes("x", x));
  ASSIGN_OR_RETURN(auto typed_start, NarrowToInt64(start, "start"));
  ASSIGN_OR_RETURN(auto typed_end, NarrowToInt64(end, "end"));
  return SimplePointwiseEval("strings.substr",
                             {x, std::move(typed_start), std::move(typed_end)},
                             /*output_schema=*/x.GetSchemaImpl(),
                             /*primary_operand_indices=*/{{0}});
}

absl::StatusOr<DataSlice> Upper(const DataSlice& x) {
  // TODO: Add support for BYTES.
  RETURN_IF_ERROR(ExpectString("x", x));
  return SimplePointwiseEval("strings.upper", {x},
                             internal::DataItem(schema::kString));
}

}  // namespace koladata::ops
