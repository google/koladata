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
#include "koladata/operators/jagged_shape.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_slice.h"
#include "koladata/object_factories.h"
#include "koladata/operators/arolla_bridge.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/operators/dense_array/edge_ops.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace {

// Creates an edge from a DataSlice of sizes, or a DataItem of a single size.
// When `slice` is a DataItem, it specifies a uniform child_size of the edge,
// and the `parent_size` specifies the parent count.
//
// NOTE: The `parent_size` is _not_ validated in the case of a DataSlice.
absl::StatusOr<DataSlice::JaggedShape::Edge> GetEdgeFromSizes(
    const DataSlice& slice, int64_t parent_size,
    arolla::EvaluationContext* ctx) {
  using Edge = DataSlice::JaggedShape::Edge;
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<Edge> {
    using T = std::decay_t<decltype(impl)>;
    if constexpr (std::is_same_v<T, internal::DataSliceImpl>) {
      if (slice.GetShape().rank() != 1) {
        return absl::InvalidArgumentError(absl::StrCat(
            "unsupported DataSlice rank: ", slice.GetShape().rank()));
      }
      ASSIGN_OR_RETURN(auto sizes, ToArollaDenseArrayInt64(slice));
      return arolla::DenseArrayEdgeFromSizesOp()(ctx, sizes);
    } else {
      ASSIGN_OR_RETURN(auto child_size, ToArollaInt64(slice));
      return Edge::FromUniformGroups(parent_size, child_size);
    }
  });
}

// Creates a JaggedShape from the provided dimensions.
//
// The inputs are a combination of Edges and DataSlices representing the
// dimensions of the JaggedShape. Edges are used as is, while DataSlices are
// treated as sizes. DataItems (of ints) are interpreted as uniform dimensions
// which have the same child size for all parent elements.  DataSlices (of ints)
// are interpreted as a list of sizes, where `ds[i]` is the child size of parent
// `i`. Only rank-0 or rank-1 int DataSlices are supported.
class JaggedShapeCreateOperator : public arolla::InlineOperator {
 public:
  JaggedShapeCreateOperator(absl::Span<const arolla::QTypePtr> types)
      : InlineOperator("kde.shapes.create",
                       arolla::QExprOperatorSignature::Get(
                           types, arolla::GetQType<DataSlice::JaggedShape>())) {
    for (const auto& input_type : types) {
      DCHECK(input_type == arolla::GetQType<DataSlice>() ||
             input_type == arolla::GetQType<DataSlice::JaggedShape::Edge>());
    }
  }

 private:
  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const override {
    using Edge = DataSlice::JaggedShape::Edge;
    using EdgeOrSliceSlot = std::variant<Slot<Edge>, Slot<DataSlice>>;

    std::vector<EdgeOrSliceSlot> edge_or_slice_slots;
    edge_or_slice_slots.reserve(input_slots.size());
    for (size_t i = 0; i < input_slots.size(); ++i) {
      if (input_slots[i].GetType() == arolla::GetQType<Edge>()) {
        edge_or_slice_slots.push_back(input_slots[i].UnsafeToSlot<Edge>());
      } else {
        edge_or_slice_slots.push_back(input_slots[i].UnsafeToSlot<DataSlice>());
      }
    }
    Slot<DataSlice::JaggedShape> shape_slot =
        output_slot.UnsafeToSlot<DataSlice::JaggedShape>();
    return arolla::MakeBoundOperator(
        [edge_or_slice_slots = std::move(edge_or_slice_slots),
         shape_slot = std::move(shape_slot)](arolla::EvaluationContext* ctx,
                                             arolla::FramePtr frame) {
          DataSlice::JaggedShape::EdgeVec edges;
          edges.reserve(edge_or_slice_slots.size());
          for (const auto& input_slot : edge_or_slice_slots) {
            if (std::holds_alternative<Slot<Edge>>(input_slot)) {
              edges.push_back(frame.Get(std::get<Slot<Edge>>(input_slot)));
            } else {
              ASSIGN_OR_RETURN(
                  auto edge,
                  GetEdgeFromSizes(
                      frame.Get(std::get<Slot<DataSlice>>(input_slot)),
                      edges.empty() ? 1 : edges.back().child_size(), ctx),
                  ctx->set_status(std::move(_)));
              edges.push_back(std::move(edge));
            }
          }
          ASSIGN_OR_RETURN(auto jagged_shape,
                           DataSlice::JaggedShape::FromEdges(std::move(edges)),
                           ctx->set_status(std::move(_)));
          frame.Set(shape_slot, std::move(jagged_shape));
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr>
JaggedShapeCreateOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  for (const auto& input_type : input_types) {
    if (input_type != arolla::GetQType<DataSlice>() &&
        input_type != arolla::GetQType<DataSlice::JaggedShape::Edge>()) {
      return absl::InvalidArgumentError(
          absl::StrCat("unsupported input type: ", input_type->name()));
    }
  }
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<JaggedShapeCreateOperator>(input_types), input_types,
      output_type);
}

absl::StatusOr<DataSlice> ExpandToShape(const DataSlice& x,
                                        DataSlice::JaggedShape shape,
                                        int64_t ndim) {
  if (ndim == 0) {
    return BroadcastToShape(x, std::move(shape));
  }

  if (ndim < 0 || ndim > x.GetShape().rank()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "ndim must be a positive integer and <= x.ndim, got %d", ndim));
  }

  auto temp_db = DataBag::Empty();
  auto new_x = x.WithDb(temp_db);
  for (int64_t i = 0; i < ndim; ++i) {
    ASSIGN_OR_RETURN(new_x, CreateListsFromLastDimension(temp_db, new_x));
  }
  ASSIGN_OR_RETURN(
      new_x, BroadcastToShape(new_x, shape),
      _ << absl::StrFormat(
          "Cannot expand 'x' imploded with the last %d dimension(s) to "
          "'shape' due to incompatible shapes. Got 'x' shape: %s, imploded "
          "'x' shape: %s, 'shape' to expand: %s",
          ndim, arolla::Repr(x.GetShape()), arolla::Repr(new_x.GetShape()),
          arolla::Repr(shape)));
  for (int64_t i = 0; i < ndim; ++i) {
    ASSIGN_OR_RETURN(new_x, new_x.ExplodeList(0, std::nullopt));
  }
  return new_x.WithDb(x.GetDb());
}

}  // namespace koladata::ops
