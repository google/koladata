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
#include "koladata/operators/slices.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/base/attributes.h"
#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/quote.h"
#include "arolla/jagged_shape/dense_array/util/concat.h"
#include "arolla/jagged_shape/util/concat.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operator_errors.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/slice_qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/util/repr.h"
#include "arolla/util/view_types.h"
#include "koladata/adoption_utils.h"
#include "koladata/arolla_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/casting.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/ellipsis.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/collapse.h"
#include "koladata/internal/op_utils/error.h"
#include "koladata/internal/op_utils/inverse_select.h"
#include "koladata/internal/op_utils/qexpr.h"
#include "koladata/internal/op_utils/reverse.h"
#include "koladata/internal/op_utils/select.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/object_factories.h"
#include "koladata/operators/arolla_bridge.h"
#include "koladata/operators/utils.h"
#include "koladata/schema_utils.h"
#include "koladata/subslice_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {

template <typename T>
bool IsNan(const T& item) {
  if constexpr (std::numeric_limits<T>::has_quiet_NaN) {
    if constexpr (std::is_same_v<T, internal::DataItem>) {
      return item.is_nan();
    } else if constexpr (std::is_same_v<T, float>) {
      return std::isnan(item);
    } else if constexpr (std::is_same_v<T, double>) {
      return std::isnan(item);
    }
  }
  return false;
}

class AlignOperator : public arolla::QExprOperator {
 public:
  explicit AlignOperator(absl::Span<const arolla::QTypePtr> input_types)
      : arolla::QExprOperator(input_types,
                              arolla::MakeTupleQType(input_types)) {}

 private:
  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    DCHECK_EQ(input_slots.size(), output_slot.SubSlotCount());
    std::vector<arolla::FrameLayout::Slot<DataSlice>> ds_input_slots;
    ds_input_slots.reserve(input_slots.size());
    for (const auto& input_slot : input_slots) {
      ds_input_slots.push_back(input_slot.UnsafeToSlot<DataSlice>());
    }
    return MakeBoundOperator(
        "kd.slices.align",
        [ds_input_slots(std::move(ds_input_slots)), output_slot = output_slot](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          std::optional<DataSlice::JaggedShape> largest_shape;
          for (const auto& input_slot : ds_input_slots) {
            const DataSlice& input = frame.Get(input_slot);
            if (!largest_shape.has_value() ||
                input.GetShape().rank() > largest_shape->rank()) {
              largest_shape = input.GetShape();
            }
          }

          for (size_t i = 0; i < ds_input_slots.size(); ++i) {
            const auto& input_slot = ds_input_slots[i];
            const DataSlice& input = frame.Get(input_slot);
            ASSIGN_OR_RETURN(DataSlice output,
                             BroadcastToShape(input, largest_shape.value()));
            const auto& output_subslot =
                output_slot.SubSlot(i).UnsafeToSlot<DataSlice>();
            frame.Set(output_subslot, std::move(output));
          }
          return absl::OkStatus();
        });
  }
};

absl::StatusOr<DataSlice> ConcatOrStackImpl(bool stack, int64_t ndim,
                                            std::vector<DataSlice> args) {
  if (args.empty()) {
    // Special case: no arguments returns kd.slice([]).
    return DataSlice::Create(
        internal::DataSliceImpl::CreateEmptyAndUnknownType(0),
        DataSlice::JaggedShape::FlatFromSize(0),
        internal::DataItem(schema::kNone), nullptr);
  }

  const int64_t rank = args[0].GetShape().rank();
  for (const auto& ds : args) {
    if (ds.GetShape().rank() != rank) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "all concat/stack args must have the same rank, got %d and %d", rank,
          ds.GetShape().rank()));
    }
  }

  if (!stack) {  // concat
    if (rank == 0) {
      return absl::InvalidArgumentError(
          "concatentation of DataItems (rank=0) is not supported - use stack "
          "instead");
    } else if (ndim < 1 || ndim > rank) {
      return absl::InvalidArgumentError(
          absl::StrFormat("invalid ndim=%d for rank=%d concat", ndim, rank));
    }
  } else {  // stack
    if (ndim < 0 || ndim > rank) {
      return absl::InvalidArgumentError(
          absl::StrFormat("invalid ndim=%d for rank=%d stack", ndim, rank));
    }
  }

  if (args.size() == 1 && !stack) {
    return std::move(args[0]);
  }

  // Compute result schema.
  ASSIGN_OR_RETURN(auto aligned_schemas, AlignSchemas(std::move(args)));
  args = std::move(aligned_schemas.slices);
  internal::DataItem result_schema = std::move(aligned_schemas.common_schema);

  // Compute result data bag.
  ASSIGN_OR_RETURN(auto result_db, [&]() -> absl::StatusOr<DataBagPtr> {
    AdoptionQueue adoption_queue;
    for (const DataSlice& ds : args) {
      adoption_queue.Add(ds);
    }
    return adoption_queue.GetCommonOrMergedDb();
  }());

  if (rank == 0) {
    // Special case: rank == 0 iff all inputs are DataItems.
    DCHECK(stack);  // Implied by error checking above.
    internal::SliceBuilder impl_builder(args.size());
    for (int i = 0; i < args.size(); ++i) {
      impl_builder.InsertIfNotSetAndUpdateAllocIds(i, args[i].item());
    }
    return DataSlice::Create(std::move(impl_builder).Build(),
                             DataSlice::JaggedShape::FlatFromSize(args.size()),
                             std::move(result_schema), std::move(result_db));
  }

  std::vector<DataSlice::JaggedShape> shapes;
  shapes.reserve(args.size());
  for (const auto& ds : args) {
    shapes.push_back(ds.GetShape());
  }

  // Check whether all input slices have the same single dtype. If the result
  // dtype will be unknown/mixed, we convert all args to DenseArray<DataItem>
  // for uniform handling (at some performance cost).
  const bool has_mixed_result_dtype = [&]() -> bool {
    std::optional<arolla::QTypePtr> result_dtype;
    for (const auto& ds : args) {
      const auto& impl = ds.impl<internal::DataSliceImpl>();
      if (!impl.is_single_dtype()) {
        return true;
      }
      if (result_dtype.has_value() && result_dtype.value() != impl.dtype()) {
        return true;
      }
      if (!result_dtype.has_value()) {
        result_dtype = impl.dtype();
      }
    }
    return false;
  }();

  const auto process_arrays =
      [&]<typename T>(absl::Span<const arolla::DenseArray<T>> arrays)
      -> absl::StatusOr<DataSlice> {
    arolla::DenseArray<T> result_array;
    DataSlice::JaggedShape result_shape;
    if (stack) {
      ASSIGN_OR_RETURN(std::tie(result_array, result_shape),
                       arolla::StackJaggedArraysAlongDimension(
                           arrays, absl::MakeConstSpan(shapes), rank - ndim));
    } else {
      ASSIGN_OR_RETURN(std::tie(result_array, result_shape),
                       arolla::ConcatJaggedArraysAlongDimension(
                           arrays, absl::MakeConstSpan(shapes), rank - ndim));
    }
    return DataSlice::Create(
        internal::DataSliceImpl::Create(std::move(result_array)),
        std::move(result_shape), std::move(result_schema),
        std::move(result_db));
  };

  if (has_mixed_result_dtype) {
    std::vector<arolla::DenseArray<internal::DataItem>> arrays;
    arrays.reserve(args.size());
    for (const auto& ds : args) {
      arrays.push_back(
          ds.impl<internal::DataSliceImpl>().AsDataItemDenseArray());
    }
    return process_arrays(absl::MakeConstSpan(arrays));
  } else {
    // Note: VisitValues calls its callback exactly once, because args[0] has
    // a single dtype.
    DCHECK(args[0].impl<internal::DataSliceImpl>().is_single_dtype());
    std::optional<DataSlice> result;  // To avoid constructing empty DataSlice.
    RETURN_IF_ERROR(args[0].impl<internal::DataSliceImpl>().VisitValues(
        [&]<typename T>(const arolla::DenseArray<T>&) -> absl::Status {
          std::vector<arolla::DenseArray<T>> arrays;
          arrays.reserve(args.size());
          for (const auto& ds : args) {
            arrays.push_back(ds.impl<internal::DataSliceImpl>().values<T>());
          }
          ASSIGN_OR_RETURN(result, process_arrays(absl::MakeConstSpan(arrays)));
          return absl::OkStatus();
        }));
    DCHECK(result.has_value());  // Always populated by callback.
    return std::move(result).value();
  }
}

static constexpr size_t kUndefinedGroup = ~size_t{};

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

// Helper class to process key data slices and find group indices.
class GroupByIndicesProcessor {
 public:
  GroupByIndicesProcessor(const arolla::DenseArrayEdge& edge_to_parent,
                          bool sort)
      : split_points_(edge_to_parent.edge_values().values.span()),
        group_id_(edge_to_parent.child_size(), 0),
        sort_(sort) {}

  // Update groups with a new key data slice. The shape must correspond to
  // the `edge_to_parent` passed to the constructor.
  void ProcessGroupKey(const internal::DataSliceImpl& ds) {
    if (ds.is_empty_and_unknown()) {
      std::fill(group_id_.begin(), group_id_.end(), kUndefinedGroup);
      return;
    }
    if (ds.is_mixed_dtype()) {
      DCHECK(!sort_) << "sort is not supported for mixed dtype";
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
  template <typename T>
  class SortingData {
   public:
    explicit SortingData(bool sort) : sort_(sort) {}

    void Clear() {
      if (!sort_) {
        return;
      }
      keys_to_sort_.clear();
    }

    void AddUnique(size_t group, const T& value) {
      if (!sort_) {
        return;
      }
      keys_to_sort_.emplace_back(group, value);
    }

    void Sort(size_t start_group_id, absl::Span<size_t> group_ids) {
      if (!sort_) {
        return;
      }
      group_index_.resize(keys_to_sort_.size());
      group_to_sorted_index_.resize(keys_to_sort_.size());
      for (size_t i = 0; i < keys_to_sort_.size(); ++i) {
        group_index_[i] = i;
      }
      absl::c_sort(group_index_, [this](size_t a, size_t b) {
        if constexpr (internal::IsKodaScalarSortable<T>()) {
          return this->keys_to_sort_[a] < this->keys_to_sort_[b];
        } else {
          (void)this;  // suppress unused lambda capture warning.
          LOG(FATAL) << "sort for mixed type and ExprQuote is not allowed";
          return false;
        }
      });
      for (size_t i = 0; i < group_index_.size(); ++i) {
        group_to_sorted_index_[group_index_[i]] = i;
      }
      for (size_t& group_id : group_ids) {
        if (group_id != kUndefinedGroup) {
          group_id = group_to_sorted_index_[group_id - start_group_id] +
                     start_group_id;
        }
      }
    }

   private:
    bool sort_;
    std::vector<std::pair<size_t, T>> keys_to_sort_;
    std::vector<size_t> group_index_;
    std::vector<size_t> group_to_sorted_index_;
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

    SortingData<arolla::view_type_t<T>> sorting_data(sort_);

    size_t new_group_id = 0;
    for (size_t split_id = 1; split_id < split_points_.size(); ++split_id) {
      size_t begin = split_points_[split_id - 1];
      size_t end = split_points_[split_id];
      // avoid clear to keep the memory.
      key_to_group_id.erase(key_to_group_id.begin(), key_to_group_id.end());
      sorting_data.Clear();
      size_t start_group_id = new_group_id;
      for (size_t i = begin; i < end; ++i) {
        size_t& group = group_id_[i];
        if (!value.present(i) || group == kUndefinedGroup) {
          group = kUndefinedGroup;
          continue;
        }
        auto [it, inserted] =
            key_to_group_id.emplace(Key{group, value.values[i]}, new_group_id);
        if (inserted) {
          sorting_data.AddUnique(group, value.values[i]);
          ++new_group_id;
        }
        group_id_[i] = it->second;
      }
      sorting_data.Sort(start_group_id,
                        absl::MakeSpan(group_id_).subspan(begin, end - begin));
    }
  }

  absl::Span<const int64_t> split_points_;
  std::vector<size_t> group_id_;
  bool sort_;
};

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
    if (start_qtype != arolla::GetQType<DataSlice>() &&
        start_qtype != arolla::GetUnspecifiedQType()) {
      return absl::InvalidArgumentError(
          absl::StrCat("'start' argument of a Slice must be an integer "
                       "DataSlice or unspecified, got: ",
                       start_qtype->name()));
    }
    auto stop_qtype = subfields[1].GetType();
    if (stop_qtype != arolla::GetQType<DataSlice>() &&
        stop_qtype != arolla::GetUnspecifiedQType()) {
      return absl::InvalidArgumentError(
          absl::StrCat("'stop' argument of a Slice must be an integer "
                       "DataSlice or unspecified, got: ",
                       stop_qtype->name()));
    }
    auto step_qtype = subfields[2].GetType();
    if (step_qtype != arolla::GetUnspecifiedQType()) {
      return absl::InvalidArgumentError(
          absl::StrCat("'step' argument of a Slice is not supported, got: ",
                       step_qtype->name()));
    }
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("unsupported input type: ", qtype->name()));
  ;
}

absl::StatusOr<std::optional<DataSlice>> GetSliceArg(
    const arolla::TypedSlot& field, arolla::FramePtr frame) {
  if (field.GetType() == arolla::GetUnspecifiedQType()) {
    return std::nullopt;
  } else if (field.GetType() == arolla::GetQType<DataSlice>()) {
    return frame.Get(field.UnsafeToSlot<DataSlice>());
  } else {
    return absl::InvalidArgumentError("invalid slice argument.");
  }
}

absl::StatusOr<std::vector<subslice::SlicingArgType>> ExtractSlicingArgs(
    const std::vector<arolla::TypedSlot>& slots, arolla::FramePtr frame,
    const int64_t x_rank) {
  std::vector<subslice::SlicingArgType> slices;
  std::optional<int64_t> ellipsis_pos;
  for (auto i = 0; i < slots.size(); ++i) {
    const auto qtype = slots[i].GetType();
    if (qtype == arolla::GetQType<DataSlice>()) {
      slices.push_back(frame.Get(slots[i].UnsafeToSlot<DataSlice>()));
    } else if (arolla::IsSliceQType(qtype)) {
      ASSIGN_OR_RETURN(auto start, GetSliceArg(slots[i].SubSlot(0), frame));
      ASSIGN_OR_RETURN(auto stop, GetSliceArg(slots[i].SubSlot(1), frame));
      slices.emplace_back(subslice::Slice{std::move(start), std::move(stop)});
    } else if (qtype == arolla::GetQType<koladata::internal::Ellipsis>()) {
      ellipsis_pos = i;
    }
  }

  if (slices.size() > x_rank) {
    return absl::InvalidArgumentError(
        absl::StrFormat("cannot subslice DataSlice 'x' as the number of "
                        "provided non-ellipsis slicing arguments is larger "
                        "than x.ndim: %d > %d",
                        slices.size(), x_rank));
  }

  // Insert full slices (e.g. slice(None, None)) so that slices have the same
  // size as x_rank.
  // There is an optimization: when ellipsis is the first slicing argument,
  // only work with the last N dimensions where N is the number of
  // non-ellipsis slicing arguments.
  if (ellipsis_pos.has_value() && *ellipsis_pos != 0) {
    slices.insert(slices.begin() + *ellipsis_pos, x_rank - slices.size(),
                  subslice::Slice{std::nullopt, std::nullopt});
  }
  return slices;
}

class SubsliceOperator : public arolla::QExprOperator {
 public:
  explicit SubsliceOperator(absl::Span<const arolla::QTypePtr> types)
      : QExprOperator(types, arolla::GetQType<DataSlice>()) {}

 private:
  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const override {
    return MakeBoundOperator(
        "kd.slices.subslice",
        [x_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         slice_slots = std::vector(input_slots.begin() + 1, input_slots.end()),
         result_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          const auto& x = frame.Get(x_slot);

          ASSIGN_OR_RETURN(
              auto slice_args,
              ExtractSlicingArgs(slice_slots, frame, x.GetShape().rank()));
          ASSIGN_OR_RETURN(auto result,
                           subslice::Subslice(x, std::move(slice_args)));
          frame.Set(result_slot, std::move(result));
          return absl::OkStatus();
        });
  }
};

}  // namespace

absl::StatusOr<DataSlice> InverseMapping(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectInteger("x", x));
  return SimpleAggOverEval("array.inverse_mapping", {x});
}

absl::StatusOr<DataSlice> OrdinalRank(const DataSlice& x,
                                      const DataSlice& tie_breaker,
                                      const DataSlice& descending) {
  RETURN_IF_ERROR(ExpectCanBeOrdered("x", x));
  ASSIGN_OR_RETURN(
      auto tie_breaker_int64,
      CastToNarrow(tie_breaker, internal::DataItem(schema::kInt64)),
      internal::KodaErrorFromCause("tie_breaker must be integers",
                                   std::move(_)));
  RETURN_IF_ERROR(ExpectPresentScalar("descending", descending, schema::kBool));
  return SimpleAggOverEval(
      "array.ordinal_rank", {x, std::move(tie_breaker_int64), descending},
      /*output_schema=*/internal::DataItem(schema::kInt64), /*edge_index=*/2);
}

absl::StatusOr<DataSlice> DenseRank(const DataSlice& x,
                                    const DataSlice& descending) {
  RETURN_IF_ERROR(ExpectCanBeOrdered("x", x));
  RETURN_IF_ERROR(ExpectPresentScalar("descending", descending, schema::kBool));
  return SimpleAggOverEval(
      "array.dense_rank", {x, descending},
      /*output_schema=*/internal::DataItem(schema::kInt64));
}

absl::StatusOr<arolla::OperatorPtr> AlignOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  for (const auto& args_type : input_types) {
    if (args_type != arolla::GetQType<DataSlice>()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "arguments must be DataSlices, but got ", args_type->name()));
    }
  }

  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<AlignOperator>(input_types), input_types, output_type);
}

absl::StatusOr<DataSlice> Collapse(const DataSlice& ds) {
  const auto& shape = ds.GetShape();
  size_t rank = shape.rank();
  if (rank == 0) {
    return absl::InvalidArgumentError("DataItem is not supported");
  }
  return DataSlice::Create(
      internal::CollapseOp()(ds.slice(), shape.edges().back()),
      shape.RemoveDims(rank - 1), ds.GetSchemaImpl(), ds.GetBag());
}

absl::StatusOr<DataSlice> ConcatOrStack(
    absl::Span<const DataSlice* const> slices) {
  if (slices.size() < 2) {
    return absl::InvalidArgumentError(
        absl::StrCat("_concat_or_stack expected at least 2 arguments, but got ",
                     slices.size()));
  }
  ASSIGN_OR_RETURN(auto stack, ToArollaScalar<bool>(*slices[0]),
                   _ << "`stack` argument must be a scalar BOOLEAN, but got "
                     << arolla::Repr(*slices[0]));
  ASSIGN_OR_RETURN(auto ndim, ToArollaScalar<int64_t>(*slices[1]),
                   _ << "`ndim` argument must be a scalar INT64, but got "
                     << arolla::Repr(*slices[1]));
  std::vector<DataSlice> args;
  args.reserve(slices.size() - 2);
  for (const auto* const ds : slices.subspan(2)) {
    args.push_back(*ds);
  }
  return ConcatOrStackImpl(stack, ndim, std::move(args));
}

absl::StatusOr<DataSlice> EmptyShaped(const DataSlice::JaggedShape& shape,
                                      const DataSlice& schema) {
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  ASSIGN_OR_RETURN(auto ds,
                   DataSlice::Create(internal::DataSliceImpl::Create(
                                         shape.size(), internal::DataItem()),
                                     shape, internal::DataItem(schema::kMask)));
  return ds.WithSchema(schema);  // Attaches a DataBag if needed.
}

absl::StatusOr<DataSlice> GroupByIndices(
    absl::Span<const DataSlice* const> slices) {
  if (slices.size() < 2) {
    return absl::InvalidArgumentError(
        absl::StrCat("_group_by_indices expected at least 2 arguments, but "
                     "got ",
                     slices.size()));
  }

  ASSIGN_OR_RETURN(auto sort_bool, GetBoolArgument(*slices[0], "sort"));

  const auto& shape = slices[1]->GetShape();
  if (shape.rank() == 0) {
    return absl::InvalidArgumentError(
        "group_by arguments must be DataSlices with ndim > 0, got DataItems");
  }

  GroupByIndicesProcessor processor(shape.edges().back(), /*sort=*/sort_bool);
  for (const auto* const ds_ptr : slices.subspan(1)) {
    const auto& ds = *ds_ptr;
    if (!ds.GetShape().IsEquivalentTo(shape)) {
      return absl::InvalidArgumentError(
          "all arguments must have the same shape");
    }
    if (sort_bool) {
      if (ds.slice().is_mixed_dtype()) {
        return absl::InvalidArgumentError(
            "sort is not supported for mixed dtype");
      }
      if (!internal::IsKodaScalarQTypeSortable(ds.slice().dtype())) {
        return absl::InvalidArgumentError(absl::StrCat(
            "sort is not supported for ",
            schema::schema_internal::GetQTypeName(ds.slice().dtype())));
      }
    }
    processor.ProcessGroupKey(ds.slice());
  }
  auto [indices_array, group_split_points, item_split_points] =
      processor.CreateFinalDataSlice();
  ASSIGN_OR_RETURN(auto new_shape,
                   shape.RemoveDims(/*from=*/shape.rank() - 1)
                       .AddDims({std::move(group_split_points),
                                 std::move(item_split_points)}));
  return DataSlice::Create(
      internal::DataSliceImpl::Create(std::move(indices_array)),
      std::move(new_shape), internal::DataItem(schema::kInt64));
}

absl::StatusOr<DataSlice> Unique(const DataSlice& x, const DataSlice& sort) {
  if (x.is_item()) {
    return x;
  }
  ASSIGN_OR_RETURN(bool sort_bool, GetBoolArgument(sort, "sort"));
  if (sort_bool) {
    if (x.slice().is_mixed_dtype()) {
      return absl::FailedPreconditionError(
          "sort is not supported for mixed dtype");
    }
    if (!internal::IsKodaScalarQTypeSortable(x.slice().dtype())) {
      return absl::FailedPreconditionError(absl::StrCat(
          "sort is not supported for ",
          schema::schema_internal::GetQTypeName(x.slice().dtype())));
    }
  }

  const auto& split_points =
      x.GetShape().edges().back().edge_values().values.span();
  arolla::DenseArrayBuilder<int64_t> split_points_builder(split_points.size());
  split_points_builder.Set(0, 0);

  auto process_values =
      [&]<class T, class Map>(
          const arolla::DenseArray<T>& values,
          Map& map) -> absl::StatusOr<internal::DataSliceImpl> {
    std::vector<arolla::view_type_t<T>> unique_values;
    unique_values.reserve(values.size());
    map.reserve(values.size());

    for (size_t split_id = 1; split_id < split_points.size(); ++split_id) {
      std::optional<arolla::view_type_t<T>> nan_value;
      size_t begin = split_points[split_id - 1];
      size_t end = split_points[split_id];
      size_t unique_values_group_begin = unique_values.size();
      for (size_t i = begin; i < end; ++i) {
        if (!values.present(i)) {
          continue;
        }
        const auto& value = values.values[i];
        if (IsNan(value)) {
          if (!nan_value.has_value()) {
            nan_value = value;
            if (!sort_bool) {
              unique_values.push_back(value);
            }
          }
        } else {
          // We reuse the map to minimize amount of successful inserts.
          auto [it, inserted] = map.emplace(value, split_id);

          if (inserted || it->second != split_id) {
            unique_values.push_back(value);
            it->second = split_id;
          }
        }
      }
      if (sort_bool) {
        if constexpr (internal::IsKodaScalarSortable<T>()) {
          std::sort(unique_values.begin() + unique_values_group_begin,
                    unique_values.end());
          if (nan_value.has_value()) {
            unique_values.push_back(*std::move(nan_value));
          }
        } else {
          return absl::FailedPreconditionError(absl::StrCat(
              "sort is not supported for ", arolla::GetQType<T>()->name()));
        }
      }
      split_points_builder.Set(split_id, unique_values.size());
    }
    internal::SliceBuilder builder(unique_values.size());
    for (size_t i = 0; i < unique_values.size(); ++i) {
      if constexpr (std::is_same_v<T, internal::DataItem>) {
        builder.InsertIfNotSetAndUpdateAllocIds(i, unique_values[i]);
      } else {
        builder.InsertIfNotSet(
            i, internal::DataItem::View<T>(std::move(unique_values[i])));
        if constexpr (std::is_same_v<T, internal::ObjectId>) {
          builder.GetMutableAllocationIds().Insert(
              internal::AllocationId(unique_values[i]));
        }
      }
    }
    return std::move(builder).Build();
  };

  absl::StatusOr<internal::DataSliceImpl> res_impl;
  if (x.slice().is_empty_and_unknown()) {
    res_impl = internal::DataSliceImpl::CreateEmptyAndUnknownType(0);
    for (size_t split_id = 1; split_id < split_points.size(); ++split_id) {
      split_points_builder.Set(split_id, 0);
    }
  } else if (x.slice().is_mixed_dtype()) {
    absl::flat_hash_map<internal::DataItem, size_t, internal::DataItem::Hash,
                        internal::DataItem::Eq>
        map;
    res_impl = process_values(x.slice().AsDataItemDenseArray(), map);
  } else {
    // TODO: Remove this unused builder. It prevents from a linker
    // error that is not yet explained.
    ABSL_ATTRIBUTE_UNUSED arolla::DenseArrayBuilder<arolla::expr::ExprQuote>
        unused(0);
    x.slice().VisitValues([&]<class T>(const arolla::DenseArray<T>& values) {
      absl::flat_hash_map<arolla::view_type_t<T>, size_t> map;
      res_impl = process_values(values, map);
    });
  }

  RETURN_IF_ERROR(res_impl.status());
  ASSIGN_OR_RETURN(auto new_shape,
                   x.GetShape()
                       .RemoveDims(/*from=*/x.GetShape().rank() - 1)
                       .AddDims({arolla::DenseArrayEdge::UnsafeFromSplitPoints(
                           std::move(split_points_builder).Build())}));
  return DataSlice::Create(*std::move(res_impl), std::move(new_shape),
                           x.GetSchemaImpl(), x.GetBag());
}

absl::StatusOr<DataSlice> Reverse(const DataSlice& obj) {
  if (obj.impl_empty_and_unknown() || obj.is_item()) {
    return obj;
  }
  return DataSlice::Create(
      koladata::internal::ReverseOp{}(obj.slice(), obj.GetShape()),
      obj.GetShape(), obj.GetSchemaImpl(), obj.GetBag());
}

absl::StatusOr<DataSlice> Select(const DataSlice& ds, const DataSlice& filter,
                                 const DataSlice& expand_filter) {
  if (ds.is_item()) {
    return absl::InvalidArgumentError(
        "cannot select from DataItem because its size is always 1. "
        "Consider calling .flatten() beforehand to convert it "
        "to a 1-dimensional DataSlice");
  }
  ASSIGN_OR_RETURN(bool expand_filter_bool,
                   GetBoolArgument(expand_filter, "expand_filter"));
  const internal::DataItem& schema = filter.GetSchemaImpl();

  if (schema != schema::kObject && schema != schema::kMask) {
    return absl::InvalidArgumentError(
        "the schema of the `fltr` DataSlice should only be OBJECT or MASK or "
        "can be evaluated to such DataSlice (i.e. Python function or "
        "Koda Functor)");
  }
  const DataSlice::JaggedShape& fltr_shape =
      expand_filter_bool ? ds.GetShape() : filter.GetShape();
  ASSIGN_OR_RETURN(auto fltr, BroadcastToShape(filter, fltr_shape),
                   internal::KodaErrorFromCause(
                       "failed to broadcast `fltr` to `ds`", std::move(_)));

  return fltr.VisitImpl(
      [&](const auto& filter_impl) -> absl::StatusOr<DataSlice> {
        ASSIGN_OR_RETURN((auto [result_ds, result_shape]),
                         internal::SelectOp()(ds.slice(), ds.GetShape(),
                                              filter_impl, fltr.GetShape()));
        return DataSlice::Create(std::move(result_ds), std::move(result_shape),
                                 ds.GetSchemaImpl(), ds.GetBag());
      });
}

absl::StatusOr<DataSlice> InverseSelect(const DataSlice& ds,
                                        const DataSlice& filter) {
  const internal::DataItem& schema = filter.GetSchemaImpl();

  if (schema != schema::kObject && schema != schema::kMask) {
    return absl::InvalidArgumentError(
        "the schema of the fltr DataSlice should only be Object or Mask");
  }
  auto ds_shape = ds.GetShape();
  auto filter_shape = filter.GetShape();
  if (ds_shape.rank() != filter_shape.rank()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "the rank of the ds and fltr DataSlice must be the same. Got "
        "rank(ds): ",
        ds_shape.rank(), ", rank(fltr): ", filter_shape.rank()));
  }
  return ds.VisitImpl([&](const auto& ds_impl) {
    return filter.VisitImpl(
        [&](const auto& filter_impl) -> absl::StatusOr<DataSlice> {
          ASSIGN_OR_RETURN(
              auto res, internal::InverseSelectOp()(ds_impl, ds_shape,
                                                    filter_impl, filter_shape));
          return DataSlice::Create(std::move(res), filter_shape,
                                   ds.GetSchemaImpl(), ds.GetBag());
        });
  });
}

absl::StatusOr<arolla::OperatorPtr> SubsliceOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  // The following checks are already done at Expr qtype constraints level and
  // should never happen.
  if (input_types.empty()) {
    return OperatorNotDefinedError("kd.slices.subslice", input_types,
                                   "expected at least 1 argument");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return OperatorNotDefinedError("kd.slices.subslice", input_types,
                                   "'x' must be a DataSlice");
  }

  std::optional<int64_t> ellipsis_pos_for_error;
  // TODO: remove this to Expr operator constraints.
  for (size_t i = 1; i < input_types.size(); ++i) {
    auto input_type = input_types[i];
    if (auto status =
            IsSliceQTypeValid(input_type, i - 1, ellipsis_pos_for_error);
        !status.ok()) {
      return internal::OperatorEvalError(
          "kd.slices.subslice",
          absl::StrFormat("slicing argument at position %d is invalid: %s",
                          i - 1, status.message()));
    }
  }
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<SubsliceOperator>(input_types), input_types,
      output_type);
}

absl::StatusOr<DataSlice> Take(const DataSlice& x, const DataSlice& indices) {
  const auto& x_shape = x.GetShape();
  if (x_shape.rank() == 0) {
    return absl::InvalidArgumentError("DataItem is not supported.");
  }
  const auto shape_for_expansion = x_shape.RemoveDims(x_shape.rank() - 1);
  const auto& indices_shape = indices.GetShape();
  if (!shape_for_expansion.IsBroadcastableTo(indices_shape) &&
      !indices_shape.IsBroadcastableTo(shape_for_expansion)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "DataSlice with shape=%s is not compatible with shape=%s; kd.take "
        "requires shape(x)[:-1] to be compatible with shape(indices)",
        arolla::Repr(shape_for_expansion), arolla::Repr(indices_shape)));
  }
  return subslice::Subslice(x, {indices});
}

absl::StatusOr<DataSlice> Translate(const DataSlice& keys_to,
                                    const DataSlice& keys_from,
                                    const DataSlice& values_from) {
  const auto& from_shape = keys_from.GetShape();
  ASSIGN_OR_RETURN(
      auto expanded_values_from, BroadcastToShape(values_from, from_shape),
      internal::KodaErrorFromCause(
          "values_from must be broadcastable to keys_from", std::move(_)));

  if (from_shape.rank() == 0) {
    return absl::InvalidArgumentError(
        "keys_from and values_from must have at least one dimension");
  }

  auto shape_without_last_dim = from_shape.RemoveDims(from_shape.rank() - 1);
  if (!shape_without_last_dim.IsBroadcastableTo(keys_to.GetShape())) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "keys_from.get_shape()[:-1] must be broadcastable to keys_to, but "
        "got %s vs %s",
        arolla::Repr(shape_without_last_dim),
        arolla::Repr(keys_to.GetShape())));
  }

  ASSIGN_OR_RETURN(
      auto casted_keys_to, CastToNarrow(keys_to, keys_from.GetSchemaImpl()),
      internal::KodaErrorFromCause(
          "keys_to schema must be castable to keys_from schema", std::move(_)));

  ASSIGN_OR_RETURN(auto unique_keys,
                   Unique(keys_from, DataSlice::CreateFromScalar(false)));
  if (keys_from.present_count() != unique_keys.present_count()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "keys_from must be unique within each group of the last dimension: "
        "original DataSlice %s vs DataSlice after dedup %s. Consider using "
        "translate_group instead",
        arolla::Repr(keys_from), arolla::Repr(unique_keys)));
  }

  auto temp_db = DataBag::Empty();
  ASSIGN_OR_RETURN(auto lookup,
                   CreateDictShaped(temp_db, std::move(shape_without_last_dim),
                                    keys_from.WithBag(nullptr),
                                    expanded_values_from.WithBag(nullptr)));
  ASSIGN_OR_RETURN(auto res, lookup.GetFromDict(casted_keys_to));
  return res.WithBag(expanded_values_from.GetBag());
}

absl::StatusOr<DataSlice> GetRepr(const DataSlice& x) {
  ASSIGN_OR_RETURN(auto repr,
                   DataSliceToStr(x, ReprOption{.show_attributes = true}));
  return DataSlice::Create(internal::DataItem(arolla::Text(std::move(repr))),
                           internal::DataItem(schema::kString));
}

}  // namespace koladata::ops
