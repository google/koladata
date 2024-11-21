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
#include "koladata/operators/shapes.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "koladata/arolla_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_slice.h"
#include "koladata/object_factories.h"
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

using Edge = DataSlice::JaggedShape::Edge;

// Creates an edge from a DataSlice of sizes, or a DataItem of a single size.
// When `slice` is a DataItem, it specifies a uniform group_size of the edge,
// and the `adjacent_size` specifies the parent count, or the child count
// depending on `is_parent_size`.
//
// NOTE: The `adjacent_size` is _not_ validated in the case of a DataSlice.
template <bool is_parent_size>
absl::StatusOr<DataSlice::JaggedShape::Edge> GetEdgeFromSizes(
    const DataSlice& slice, int64_t adjacent_size,
    arolla::EvaluationContext* ctx) {
  using Edge = DataSlice::JaggedShape::Edge;
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<Edge> {
    using T = std::decay_t<decltype(impl)>;
    if constexpr (std::is_same_v<T, internal::DataSliceImpl>) {
      if (slice.GetShape().rank() != 1) {
        return absl::InvalidArgumentError(absl::StrCat(
            "unsupported DataSlice rank: ", slice.GetShape().rank()));
      }
      ASSIGN_OR_RETURN(auto sizes, ToArollaDenseArray<int64_t>(slice));
      return arolla::DenseArrayEdgeFromSizesOp()(ctx, sizes);
    } else {
      ASSIGN_OR_RETURN(auto group_size, ToArollaScalar<int64_t>(slice));
      if constexpr (is_parent_size) {
        return Edge::FromUniformGroups(adjacent_size, group_size);
      } else {
        if (group_size == 0) {
          // In case group_size is 0, the parent size could be anything. In some
          // cases, it's possible to figure out the parent size, but not in the
          // general case, so we raise for now. Consider improving this.
          return absl::InvalidArgumentError(
              "expected a non-zero group size following a placeholder "
              "dimension (-1)");
        }
        return Edge::FromUniformGroups(adjacent_size / group_size, group_size);
      }
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
class JaggedShapeCreateOperator : public arolla::QExprOperator {
 public:
  explicit JaggedShapeCreateOperator(absl::Span<const arolla::QTypePtr> types)
      : QExprOperator(arolla::QExprOperatorSignature::Get(
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
    Slot<DataSlice::JaggedShape> shape_slot =
        output_slot.UnsafeToSlot<DataSlice::JaggedShape>();
    return arolla::MakeBoundOperator(
        [edge_or_slice_slots =
             std::vector(input_slots.begin(), input_slots.end()),
         shape_slot = std::move(shape_slot)](arolla::EvaluationContext* ctx,
                                             arolla::FramePtr frame) {
          DataSlice::JaggedShape::EdgeVec edges;
          edges.reserve(edge_or_slice_slots.size());
          for (const auto& input_slot : edge_or_slice_slots) {
            if (input_slot.GetType() == arolla::GetQType<Edge>()) {
              edges.push_back(frame.Get(input_slot.UnsafeToSlot<Edge>()));
            } else {
              ASSIGN_OR_RETURN(
                  auto edge,
                  GetEdgeFromSizes</*is_parent_size=*/true>(
                      frame.Get(input_slot.UnsafeToSlot<DataSlice>()),
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

// Returns the index of the placeholder dimension, or -1 if no placeholder
// dimension is found.
absl::StatusOr<int> GetPlaceholderDimension(
    arolla::FramePtr frame, absl::Span<const arolla::TypedSlot> input_slots) {
  int dim = -1;
  for (int i = 0; i < input_slots.size(); ++i) {
    const auto& input_slot = input_slots[i];
    if (input_slot.GetType() == arolla::GetQType<Edge>()) {
      continue;
    }
    const auto& slice = frame.Get(input_slot.UnsafeToSlot<DataSlice>());
    if (slice.is_item()) {
      ASSIGN_OR_RETURN(int64_t value, ToArollaScalar<int64_t>(slice));
      if (value == -1) {
        if (dim != -1) {
          return absl::InvalidArgumentError(
              "only one dimension can be a placeholder");
        }
        dim = i;
      }
    }
  }
  return dim;
}

class JaggedShapeCreateWithSizeOperator : public arolla::QExprOperator {
 public:
  explicit JaggedShapeCreateWithSizeOperator(
      absl::Span<const arolla::QTypePtr> types)
      : QExprOperator(arolla::QExprOperatorSignature::Get(
            types, arolla::GetQType<DataSlice::JaggedShape>())) {}

 private:
  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const override {
    Slot<DataSlice> size_slot = input_slots[0].UnsafeToSlot<DataSlice>();
    Slot<DataSlice::JaggedShape> shape_slot =
        output_slot.UnsafeToSlot<DataSlice::JaggedShape>();
    return arolla::MakeBoundOperator([size_slot = std::move(size_slot),
                                      edge_or_slice_slots =
                                          std::vector(input_slots.begin() + 1,
                                                      input_slots.end()),
                                      shape_slot = std::move(shape_slot)](
                                         arolla::EvaluationContext* ctx,
                                         arolla::FramePtr frame) {
      // To support the placeholder dimension (-1), we look to its adjacent
      // dimensions to figure out the size. The LHS edges are computed
      // through a "forward pass" using prior edges (lower dimensions) to
      // resolve uniform dimension specifications. Conversely, the RHS edges
      // are computed through a "backward pass" using higher dimensions
      // starting with the provided size. The placeholder dimension is then
      // formed through the child size of the previous dimension and the
      // parent size of the next dimension.
      ASSIGN_OR_RETURN(int64_t size,
                       ToArollaScalar<int64_t>(frame.Get(size_slot)),
                       ctx->set_status(std::move(_)));
      if (size < 0) {
        ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
            "size must be a non-negative integer, got: %d", size)));
        return;
      }
      DataSlice::JaggedShape::EdgeVec edges(edge_or_slice_slots.size());
      auto previous_child_size = [&](int i) {
        return i == 0 ? 1 : edges[i - 1].child_size();
      };
      auto next_parent_size = [&](int i) {
        return i == edges.size() - 1 ? size : edges[i + 1].parent_size();
      };
      ASSIGN_OR_RETURN(int placeholder_dim_index,
                       GetPlaceholderDimension(frame, edge_or_slice_slots),
                       ctx->set_status(std::move(_)));
      int iterto =
          placeholder_dim_index >= 0 ? placeholder_dim_index : edges.size();
      // Forward pass.
      for (int i = 0; i < iterto; ++i) {
        const auto& input_slot = edge_or_slice_slots[i];
        if (input_slot.GetType() == arolla::GetQType<Edge>()) {
          edges[i] = frame.Get(input_slot.UnsafeToSlot<Edge>());
        } else {
          ASSIGN_OR_RETURN(auto edge,
                           GetEdgeFromSizes</*is_parent_size=*/true>(
                               frame.Get(input_slot.UnsafeToSlot<DataSlice>()),
                               previous_child_size(i), ctx),
                           ctx->set_status(std::move(_)));
          edges[i] = std::move(edge);
        }
      }
      // Backward pass.
      for (int i = edges.size() - 1; i > iterto; --i) {
        const auto& input_slot = edge_or_slice_slots[i];
        if (input_slot.GetType() == arolla::GetQType<Edge>()) {
          edges[i] = frame.Get(input_slot.UnsafeToSlot<Edge>());
        } else {
          ASSIGN_OR_RETURN(auto edge,
                           GetEdgeFromSizes</*is_parent_size=*/false>(
                               frame.Get(input_slot.UnsafeToSlot<DataSlice>()),
                               next_parent_size(i), ctx),
                           ctx->set_status(std::move(_)));
          edges[i] = std::move(edge);
        }
      }
      if (placeholder_dim_index != -1) {
        int64_t parent_size = previous_child_size(placeholder_dim_index);
        int64_t child_size = next_parent_size(placeholder_dim_index);
        // In case `parent_size` is 0, all subsequent dimensions must also
        // be 0. Otherwise, we need to figure out the actual group size.
        int64_t group_size = 0;
        if (parent_size != 0) {
          if (child_size % parent_size != 0) {
            ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
                "parent_size=%d does not divide child_size=%d, so the "
                "placeholder dimension at index %d cannot be resolved",
                child_size, parent_size, placeholder_dim_index)));
            return;
          }
          group_size = child_size / parent_size;
        }
        ASSIGN_OR_RETURN(edges[placeholder_dim_index],
                         Edge::FromUniformGroups(parent_size, group_size),
                         ctx->set_status(std::move(_)));
      }
      ASSIGN_OR_RETURN(auto jagged_shape,
                       DataSlice::JaggedShape::FromEdges(std::move(edges)),
                       ctx->set_status(std::move(_)));
      if (jagged_shape.size() != size) {
        ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
            "invalid dimension specification - the resulting shape size=%d "
            "!= the expected size=%d",
            jagged_shape.size(), size)));
        return;
      }
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

absl::StatusOr<arolla::OperatorPtr>
JaggedShapeCreateWithSizeOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.empty()) {
    return absl::InvalidArgumentError("at least one input is required");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        absl::StrCat("unsupported input type: ", input_types[0]->name()));
  }
  for (const auto& input_type : input_types.subspan(1)) {
    if (input_type != arolla::GetQType<DataSlice>() &&
        input_type != arolla::GetQType<DataSlice::JaggedShape::Edge>()) {
      return absl::InvalidArgumentError(
          absl::StrCat("unsupported input type: ", input_type->name()));
    }
  }
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<JaggedShapeCreateWithSizeOperator>(input_types),
      input_types, output_type);
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
  auto new_x = x.WithBag(temp_db);
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
  return new_x.WithBag(x.GetBag());
}

}  // namespace koladata::ops
