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
#include "koladata/operators/group_by.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace {

absl::Status VerifyGroupByIndicesInputs(
    absl::Span<const arolla::QTypePtr> input_types) {
  if (input_types.empty()) {
    return absl::InvalidArgumentError("requires at least 1 argument");
  }

  for (const auto& args_type : input_types) {
    if (args_type != arolla::GetQType<DataSlice>()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "arguments must be DataSlices, but got ", args_type->name()));
    }
  }
  return absl::OkStatus();
}

static constexpr size_t kUndefinedGroup = ~size_t{};

// Helper class to process key data slices and find group indices.
class GroupByIndicesProcessor {
 public:
  GroupByIndicesProcessor(const arolla::DenseArrayEdge& edge_to_parent,
                          bool sort)
      : split_points_(edge_to_parent.edge_values().values.span()),
        group_id_(edge_to_parent.child_size(), 0),
        sort_(sort) {
    // TODO: implement sorting.
    CHECK(!sort_) << "sort is not implemented yet";
  }

  // Update groups with a new key data slice. The shape must correspond to
  // the `edge_to_parent` passed to the constructor.
  void ProcessGroupKey(const internal::DataSliceImpl& ds) {
    if (ds.is_empty_and_unknown()) {
      std::fill(group_id_.begin(), group_id_.end(), kUndefinedGroup);
      return;
    }
    if (ds.is_mixed_dtype()) {
      ProcessMixedType(ds);
      return;
    }
    ds.VisitValues([this](const auto& value) { ProcessSingleType(value); });
  }

  // Returns the data to construct the final DataSlice.
  // 1) Indices array.
  // 2) Split points for groups within the parent.
  // 3) Split points for items within the groups.
  std::tuple<arolla::DenseArray<int64_t>, arolla::DenseArrayEdge,
             arolla::DenseArrayEdge>
  CreateFinalDataSlice() {
    arolla::DenseArrayBuilder<int64_t> group_split_points_builder(
        split_points_.size());
    group_split_points_builder.Set(0, 0);

    size_t total_size =
        group_id_.size() - absl::c_count(group_id_, kUndefinedGroup);
    arolla::DenseArrayBuilder<int64_t> idx_builder(total_size);

    std::vector<int64_t> item_split_points;
    // we assume at least one group exists.
    item_split_points.reserve(split_points_.size());
    item_split_points.push_back(0);

    std::vector<size_t> group_id_count(group_id_.size(), 0);
    size_t output_index = 0;
    size_t local_group_prefix_sum = 0;
    for (size_t split_id = 1; split_id < split_points_.size(); ++split_id) {
      size_t begin = split_points_[split_id - 1];
      size_t end = split_points_[split_id];

      // Count the number of groups and elements in groups in the [beging, end).
      // Note that groups are numerated consequently within the [begin, end) by
      // construction. This loop also finds the range of existent groups.
      // Note that different [begin, end) couldn't have common groups.
      size_t local_group_count = 0;
      size_t start_local_group = group_id_.size() + 1;
      size_t end_local_group = 0;
      for (size_t i = begin; i < end; ++i) {
        size_t group = group_id_[i];
        if (group != kUndefinedGroup) {
          size_t& cnt = group_id_count[group];
          local_group_count += (cnt == 0);
          ++cnt;
          end_local_group = std::max(end_local_group, group + 1);
          start_local_group = std::min(start_local_group, group);
        }
      }
      local_group_prefix_sum += local_group_count;
      group_split_points_builder.Set(split_id, local_group_prefix_sum);
      if (local_group_count == 0) {
        continue;
      }

      // Converts `group_id_count` to be the starting position of the group
      // within the output indices array.
      // This loop also updates item_split_points.
      for (size_t i = start_local_group, prefix_sum = output_index;
           i < end_local_group; ++i) {
        size_t& cnt = group_id_count[i];
        if (cnt != 0) {
          prefix_sum += cnt;
          item_split_points.push_back(prefix_sum);
        }
        cnt = prefix_sum - cnt;
      }

      for (size_t i = begin; i < end; ++i) {
        size_t group = group_id_[i];
        if (group != kUndefinedGroup) {
          size_t idx = group_id_count[group]++;
          idx_builder.Set(idx, i - begin);
          ++output_index;
        }
      }
    }

    return {std::move(idx_builder).Build(),
            arolla::DenseArrayEdge::UnsafeFromSplitPoints(
                std::move(group_split_points_builder).Build()),
            // Transfer the ownership of the `std::vector` to the edge.
            arolla::DenseArrayEdge::UnsafeFromSplitPoints(
                arolla::DenseArray<int64_t>{arolla::Buffer<int64_t>::Create(
                    std::move(item_split_points))})};
  }

 private:
  struct DataItemPairHash {
    size_t operator()(const std::pair<size_t, internal::DataItem>& p) const {
      return absl::HashOf(p.first, internal::DataItem::Hash()(p.second));
    }
  };

  struct DataItemPairEq {
    size_t operator()(const std::pair<size_t, internal::DataItem>& a,
                      const std::pair<size_t, internal::DataItem>& b) const {
      return a.first == b.first && internal::DataItem::Eq()(a.second, b.second);
    }
  };

  void ProcessMixedType(const internal::DataSliceImpl& ds) {
    using Key = std::pair<size_t, internal::DataItem>;
    absl::flat_hash_map<Key, size_t, DataItemPairHash, DataItemPairEq>
        key_to_group_id;
    ProcessArray(ds.AsDataItemDenseArray(), key_to_group_id);
  }

  template <typename T>
  void ProcessSingleType(const arolla::DenseArray<T>& value) {
    using Key = std::pair<size_t, arolla::view_type_t<T>>;
    absl::flat_hash_map<Key, size_t> key_to_group_id;
    ProcessArray(value, key_to_group_id);
  }

  template <typename T, typename Map>
  void ProcessArray(const arolla::DenseArray<T>& value, Map& key_to_group_id) {
    using Key = typename Map::key_type;

    size_t new_group_id = 0;
    for (size_t split_id = 1; split_id < split_points_.size(); ++split_id) {
      size_t begin = split_points_[split_id - 1];
      size_t end = split_points_[split_id];
      // avoid clear to keep the memory.
      key_to_group_id.erase(key_to_group_id.begin(), key_to_group_id.end());
      for (size_t i = begin; i < end; ++i) {
        size_t& group = group_id_[i];
        if (!value.present(i) || group == kUndefinedGroup) {
          group = kUndefinedGroup;
          continue;
        }
        auto [it, inserted] =
            key_to_group_id.emplace(Key{group, value.values[i]}, new_group_id);
        if (inserted) {
          ++new_group_id;
        }
        group_id_[i] = it->second;
      }
    }
  }

  absl::Span<const int64_t> split_points_;
  std::vector<size_t> group_id_;
  bool sort_;
};

class GroupByIndicesQExprOperator : public arolla::QExprOperator {
 public:
  GroupByIndicesQExprOperator(absl::Span<const arolla::QTypePtr> types)
      : arolla::QExprOperator("kde.group_by_indices",
                              arolla::QExprOperatorSignature::Get(
                                  types, arolla::GetQType<DataSlice>())) {
    DCHECK(!types.empty());
  }

 private:
  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    std::vector<arolla::FrameLayout::Slot<DataSlice>> ds_slots;
    ds_slots.reserve(input_slots.size());
    for (const auto& input_slot : input_slots) {
      ds_slots.push_back(input_slot.UnsafeToSlot<DataSlice>());
    }
    return arolla::MakeBoundOperator(
        [output_slot = output_slot.UnsafeToSlot<DataSlice>(),
         ds_slots(std::move(ds_slots))](arolla::EvaluationContext* ctx,
                                        arolla::FramePtr frame) {
          const auto& shape = frame.Get(ds_slots[0]).GetShape();
          if (shape.rank() == 0) {
            ctx->set_status(absl::FailedPreconditionError(
                "group_by is not supported for scalar data"));
            return;
          }
          GroupByIndicesProcessor processor(shape.edges().back(),
                                            /*sort=*/false);
          for (const auto& ds_slot : ds_slots) {
            const auto& ds = frame.Get(ds_slot);
            if (!ds.GetShape().IsEquivalentTo(shape)) {
              ctx->set_status(absl::FailedPreconditionError(
                  "all arguments must have the same shape"));
              return;
            }
            processor.ProcessGroupKey(ds.slice());
          }
          auto [indices_array, group_split_points, item_split_points] =
              processor.CreateFinalDataSlice();
          ASSIGN_OR_RETURN(
              auto new_shape,
              shape.RemoveDims(/*from=*/shape.rank() - 1)
                  ->AddDims({group_split_points, item_split_points}),
              ctx->set_status(std::move(_)));
          ASSIGN_OR_RETURN(
              auto result,
              DataSlice::Create(
                  internal::DataSliceImpl::Create(std::move(indices_array)),
                  std::move(new_shape), internal::DataItem(schema::kInt64)),
              ctx->set_status(std::move(_)));
          frame.Set(output_slot, result);
        });
  }
};

}  // namespace

// kde.group_by_indices.
//
absl::StatusOr<arolla::OperatorPtr> GroupByIndicesFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  RETURN_IF_ERROR(VerifyGroupByIndicesInputs(input_types));
  if (output_type != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "the output must be a DataSlice, but got ", output_type->name()));
  }
  return std::make_unique<GroupByIndicesQExprOperator>(input_types);
}

}  // namespace koladata::ops
