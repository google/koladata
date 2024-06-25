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
#include "koladata/operators/slice.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <variant>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/ellipsis.h"
#include "koladata/internal/op_utils/at.h"
#include "koladata/object_factories.h"
#include "koladata/operators/convert_and_eval.h"
#include "arolla/dense_array/edge.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operator_errors.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/slice_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace {

struct Slice {
  int64_t start;
  std::optional<int64_t> stop;
};

using SlicingArgType = std::variant<Slice, const DataSlice*>;

// TODO: remove this to Expr operator constraints.
absl::Status IsSliceQTypeValid(const arolla::QTypePtr& qtype, int64_t curr_pos,
                               std::optional<int64_t>& ellipsis_pos) {
  if (qtype == arolla::GetQType<DataSlice>()) {
    return absl::OkStatus();
  } else if (qtype == arolla::GetQType<internal::Ellipsis>()) {
    if (ellipsis_pos.has_value()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "ellipsis ... can appear at most once in the slicing arguments, "
          "found at least two at positions: %d and %d",
          *ellipsis_pos, curr_pos));
    }
    ellipsis_pos = curr_pos;
    return absl::OkStatus();
  } else if (arolla::IsSliceQType(qtype)) {
    const auto& subfields = qtype->type_fields();
    DCHECK_EQ(subfields.size(), 3);

    auto start_qtype = subfields[0].GetType();
    if (start_qtype != arolla::GetQType<int32_t>() &&
        start_qtype != arolla::GetQType<int64_t>() &&
        start_qtype != arolla::GetUnspecifiedQType()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "'start' argument of a Slice must be integer or unspecified, got: ",
          start_qtype->name()));
    }
    auto end_qtype = subfields[1].GetType();
    if (end_qtype != arolla::GetQType<int32_t>() &&
        end_qtype != arolla::GetQType<int64_t>() &&
        end_qtype != arolla::GetUnspecifiedQType()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "'end' argument of a Slice must be integer or unspecified, got: ",
          end_qtype->name()));
    }
    auto step_qtype = subfields[2].GetType();
    if (step_qtype != arolla::GetUnspecifiedQType()) {
      return absl::InvalidArgumentError(
          absl::StrCat("'step' argument of a Slice is not supported, got: ",
                       end_qtype->name()));
    }
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("unsupported input type: ", qtype->name()));
  ;
}

std::optional<int64_t> GetSliceArg(const arolla::TypedSlot& field,
                                   arolla::FramePtr frame) {
  if (field.GetType() == arolla::GetUnspecifiedQType()) {
    return std::nullopt;
  } else if (field.GetType() == arolla::GetQType<int32_t>()) {
    return frame.Get(field.UnsafeToSlot<int32_t>());
  } else if (field.GetType() == arolla::GetQType<int64_t>()) {
    return frame.Get(field.UnsafeToSlot<int64_t>());
  } else {
    DCHECK(false);
    return std::nullopt;
  }
}

absl::StatusOr<std::vector<SlicingArgType>> ExtractSlicingArgs(
    const std::vector<arolla::TypedSlot>& slots, arolla::FramePtr frame,
    const int64_t x_rank) {
  std::vector<SlicingArgType> slices;
  std::optional<int64_t> ellipsis_pos;
  for (auto i = 0; i < slots.size(); ++i) {
    const auto qtype = slots[i].GetType();
    if (qtype == arolla::GetQType<DataSlice>()) {
      slices.push_back(&frame.Get(slots[i].UnsafeToSlot<DataSlice>()));
    } else if (arolla::IsSliceQType(qtype)) {
      auto start = GetSliceArg(slots[i].SubSlot(0), frame);
      auto end = GetSliceArg(slots[i].SubSlot(1), frame);
      slices.emplace_back(Slice{start.has_value() ? *start : 0, end});
    } else if (qtype == arolla::GetQType<koladata::internal::Ellipsis>()) {
      ellipsis_pos = i;
    }
  }

  if (ellipsis_pos.has_value()) {
    if (slices.size() > x_rank) {
      return absl::InvalidArgumentError(
          absl::StrFormat("cannot subslice DataSlice 'x' as the number of "
                          "provided non-ellipsis slicing arguments is larger "
                          "than x.ndim: %d > %d",
                          slices.size(), x_rank));
    }
    // Insert full slices (e.g. slice(0, None)) so that slices have the same
    // size as x_rank.
    // There is an optimization: when ellipsis is the first slicing argument,
    // only implode and explode the last N dimensions where N is the number of
    // non-ellipsis slicing arguments.
    if (*ellipsis_pos != 0) {
      slices.insert(slices.begin() + *ellipsis_pos, x_rank - slices.size(),
                    Slice{0, std::nullopt});
    }
  } else if (slices.size() != x_rank) {
    return absl::InvalidArgumentError(
        absl::StrFormat("cannot subslice DataSlice 'x' as the number of "
                        "provided slicing arguments is different from x.ndim: "
                        "%d != %d",
                        slices.size(), x_rank));
  }
  return slices;
}

class SubsliceOperator : public arolla::InlineOperator {
 public:
  SubsliceOperator(absl::Span<const arolla::QTypePtr> types)
      : InlineOperator("kde.core.subslice",
                       arolla::QExprOperatorSignature::Get(
                           types, arolla::GetQType<DataSlice>())) {}

 private:
  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const override {
    return arolla::MakeBoundOperator(
        [x_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         slice_slots = std::vector(input_slots.begin() + 1, input_slots.end()),
         result_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          const auto& x = frame.Get(x_slot);
          ASSIGN_OR_RETURN(
              auto slice_args,
              ExtractSlicingArgs(slice_slots, frame, x.GetShape().rank()),
              ctx->set_status(std::move(_)));

          // TODO: improve the performance by avoiding list
          // creation.
          auto temp_db = DataBag::Empty();
          auto new_x = x.WithDb(temp_db);
          for (size_t i = 0; i < slice_args.size(); ++i) {
            ASSIGN_OR_RETURN(new_x,
                             CreateListsFromLastDimension(temp_db, new_x),
                             ctx->set_status(std::move(_)));
          }

          for (const auto& slice_arg : slice_args) {
            if (std::holds_alternative<const DataSlice*>(slice_arg)) {
              ASSIGN_OR_RETURN(
                  new_x,
                  new_x.GetFromList(*std::get<const DataSlice*>(slice_arg)),
                  ctx->set_status(std::move(_)));
            } else {
              auto slice = std::get<Slice>(slice_arg);
              ASSIGN_OR_RETURN(new_x,
                               new_x.ExplodeList(slice.start, slice.stop),
                               ctx->set_status(std::move(_)));
            }
          }
          frame.Set(result_slot, new_x.WithDb(x.GetDb()));
        });
  }
};

absl::StatusOr<DataSlice> AtImpl(const DataSlice& x, const DataSlice& indices) {
  const auto& x_shape = x.GetShapePtr();
  const auto& indices_shape = indices.GetShapePtr();
  // If ndim(indices) == ndim(x) - 1, insert a unit dimension to the end,
  // which is needed by internal::AtOp().
  // If ndim(indices) > ndim(x) - 1, flatten the last ndim(indices) - ndim(x)
  // + 1 dimensions.
  // The flattened_shape always has the same rank and the same N-1 dimensions
  // as the shape of x.
  auto flattened_shape =
      indices_shape->FlattenDims(x_shape->rank() - 1, indices_shape->rank());

  std::optional<arolla::DenseArrayEdge> indices_to_common =
      flattened_shape->edges().empty()
          ? std::nullopt
          : std::make_optional(flattened_shape->edges().back());
  auto x_to_common = x_shape->edges().back();
  ASSIGN_OR_RETURN(auto index_array, ToArollaDenseArrayInt64(indices));

  return DataSlice::Create(
      internal::AtOp(x.slice(), index_array, x_to_common, indices_to_common),
      indices_shape, x.GetSchemaImpl(), x.GetDb());
}

}  // namespace

absl::StatusOr<arolla::OperatorPtr> SubsliceOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.empty()) {
    return OperatorNotDefinedError("kde.core.subslice", input_types,
                                   "expected at least 1 argument");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return OperatorNotDefinedError("kde.core.subslice", input_types,
                                   "'x' must be a DataSlice");
  }

  std::optional<int64_t> ellipsis_pos_for_error;
  for (size_t i = 1; i < input_types.size(); ++i) {
    auto input_type = input_types[i];
    if (auto status =
            IsSliceQTypeValid(input_type, i - 1, ellipsis_pos_for_error);
        !status.ok()) {
      return OperatorNotDefinedError(
          "kde.core.subslice", input_types,
          absl::StrFormat("slicing argument at position %d is invalid: %s",
                          i - 1, status.message()));
    }
  }
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<SubsliceOperator>(input_types), input_types,
      output_type);
}

absl::StatusOr<DataSlice> At(const DataSlice& x, const DataSlice& indices) {
  const auto& x_shape = x.GetShapePtr();
  if (x_shape->rank() == 0) {
    return absl::InvalidArgumentError("kd.at is not supported for DataItem.");
  }
  const auto shape_for_expansion = x_shape->RemoveDims(x_shape->rank() - 1);
  const auto& indices_shape = indices.GetShapePtr();
  if (indices_shape->rank() >= shape_for_expansion->rank()) {
    if (!shape_for_expansion->IsBroadcastableTo(*indices_shape)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "DataSlice with shape=%s cannot be expanded to shape=%s; kd.at "
          "requires shape(x)[:-1] to be broadcastable to shape(indices) when "
          "ndim(x) <= ndim(indices)",
          arolla::Repr(*indices_shape), arolla::Repr(*shape_for_expansion)));
    }
    return AtImpl(x, indices);
  } else {
    // Expand indices if rank(indices_shape) < rank(shape_for_expansion).
    ASSIGN_OR_RETURN(
        auto expanded_indices, BroadcastToShape(indices, shape_for_expansion),
        _ << "kd.at requires shape(indices) to be broadcastable to "
          << "shape(x)[:-1] when ndim(x) - 1 > ndim(indices)");
    return AtImpl(x, expanded_indices);
  }
}

}  // namespace koladata::ops
