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
#ifndef KOLADATA_INTERNAL_OP_UTILS_DEEP_COMPARATOR_H_
#define KOLADATA_INTERNAL_OP_UTILS_DEEP_COMPARATOR_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stack>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/slice_builder.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

// An interface for a comparator that is used in DeepComparator.
class AbstractComparator {
 public:
  virtual ~AbstractComparator() = default;

  // Returns -1, 0, 1 if `lhs` should be placed before, in the same
  // position as, or after `rhs`.
  virtual int CompareOrder(TraverseHelper::TransitionKey lhs,
                           TraverseHelper::TransitionKey rhs) = 0;

  // Returns true if transitions on left and right sides are equal.
  virtual bool Equal(TraverseHelper::Transition lhs,
                     TraverseHelper::Transition rhs) = 0;

  // Returns a token that represents the (lhs, rhs) pair.
  virtual absl::StatusOr<DataItem> CreateToken(
      TraverseHelper::Transition lhs, TraverseHelper::Transition rhs) = 0;

  // Is called when the transition is the same on left and right sides.
  virtual absl::Status LhsRhsMatch(DataItem parent_token,
                                   TraverseHelper::TransitionKey key,
                                   DataItem child_token) = 0;

  // Is called when the transition is only present on left side.
  virtual absl::Status LhsOnlyAttribute(DataItem token,
                                        TraverseHelper::TransitionKey key,
                                        TraverseHelper::Transition lhs) = 0;

  // Is called when the transition is only present on right side.
  virtual absl::Status RhsOnlyAttribute(DataItem token,
                                        TraverseHelper::TransitionKey key,
                                        TraverseHelper::Transition rhs) = 0;

  // Is called when the transition is present on both sides, but the values are
  // different.
  virtual absl::Status LhsRhsMismatch(DataItem token,
                                      TraverseHelper::TransitionKey key,
                                      TraverseHelper::Transition lhs,
                                      TraverseHelper::Transition rhs) = 0;
};

template <typename ComparatorT>
class DeepComparator {
  // Recursively compares two DataSlices, based on the given comparator to
  // match the transitions (attributes, list items, dict keys, etc.).
  //
  // The comparator is used to:
  // - order the transitions from an item
  // - match transitions for lhs and rhs items
  // - assemble the results of the deep comparison
 public:
  DeepComparator(TraverseHelper lhs, TraverseHelper rhs,
                 std::unique_ptr<ComparatorT> comparator)
      : lhs_traverse_helper_(std::move(lhs)),
        rhs_traverse_helper_(std::move(rhs)),
        comparator_(std::move(comparator)) {
    static_assert(std::is_base_of<AbstractComparator, ComparatorT>());
  }

  absl::StatusOr<DataSliceImpl> CompareSlices(const DataSliceImpl& lhs_ds,
                                              const DataItem& lhs_schema,
                                              const DataSliceImpl& rhs_ds,
                                              const DataItem& rhs_schema) {
    if (lhs_ds.size() != rhs_ds.size()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("lhs_ds and rhs_ds have different sizes: %d vs %d",
                          lhs_ds.size(), rhs_ds.size()));
    }
    SliceBuilder result_slice(lhs_ds.size());
    for (int64_t i = 0; i < lhs_ds.size(); ++i) {
      ASSIGN_OR_RETURN(DataItem items_token,
                       AddToCompare({.item = lhs_ds[i], .schema = lhs_schema},
                                    {.item = rhs_ds[i], .schema = rhs_schema}));
      result_slice.InsertIfNotSetAndUpdateAllocIds(i, items_token);
    }
    RETURN_IF_ERROR(DeepCompare());
    return std::move(result_slice).Build();
  }

  ComparatorT& Comparator() { return *comparator_; }

 private:
  struct LhsRhsItems {
    DataItem lhs;
    DataItem lhs_schema;
    DataItem rhs;
    DataItem rhs_schema;

    bool operator==(const LhsRhsItems& other) const {
      return lhs == other.lhs && lhs_schema == other.lhs_schema &&
             rhs == other.rhs && rhs_schema == other.rhs_schema;
    }

    struct Hash {
      using is_transparent = void;

      size_t operator()(const LhsRhsItems& items) const {
        return absl::HashOf(
            DataItem::Hash()(items.lhs), DataItem::Hash()(items.lhs_schema),
            DataItem::Hash()(items.rhs), DataItem::Hash()(items.rhs_schema));
      }
    };

    using absl_container_hash = Hash;
  };

  absl::StatusOr<DataItem> AddToCompare(const TraverseHelper::Transition& lhs,
                                        const TraverseHelper::Transition& rhs) {
    LhsRhsItems items({lhs.item, rhs.item, lhs.schema, rhs.schema});
    auto [it, inserted] = token_map_.try_emplace(items, DataItem());
    if (!inserted) {
      return it->second;
    }
    ASSIGN_OR_RETURN(it->second,
                     comparator_->ComparatorT::CreateToken(lhs, rhs));
    to_compare_.push({it->second, lhs, rhs});
    return it->second;
  }

  void SortTransitions(std::vector<TraverseHelper::TransitionKey>& keys) {
    std::stable_sort(keys.begin(), keys.end(),
                     [&](const TraverseHelper::TransitionKey& lhs,
                         const TraverseHelper::TransitionKey& rhs) {
                       return comparator_->ComparatorT::CompareOrder(lhs, rhs) <
                              0;
                     });
  }

  absl::Status DeepCompare() {
    while (!to_compare_.empty()) {
      auto [token, lhs, rhs] = std::move(to_compare_.top());
      to_compare_.pop();
      if (lhs.schema == schema::kObject && lhs.item.holds_value<ObjectId>()) {
        ASSIGN_OR_RETURN(lhs.schema,
                         lhs_traverse_helper_.GetObjectSchema(lhs.item));
      }
      if (rhs.schema == schema::kObject && rhs.item.holds_value<ObjectId>()) {
        ASSIGN_OR_RETURN(rhs.schema,
                         rhs_traverse_helper_.GetObjectSchema(rhs.item));
      }
      ASSIGN_OR_RETURN(
          auto lhs_transitions_set,
          lhs_traverse_helper_.GetTransitions(lhs.item, lhs.schema));
      auto lhs_transition_keys = lhs_transitions_set.GetTransitionKeys();
      ASSIGN_OR_RETURN(
          auto rhs_transitions_set,
          rhs_traverse_helper_.GetTransitions(rhs.item, rhs.schema));
      auto rhs_transition_keys = rhs_transitions_set.GetTransitionKeys();

      auto lhs_only_attribute = [&](int64_t idx) -> absl::Status {
        ASSIGN_OR_RETURN(auto lhs_transition,
                         lhs_traverse_helper_.TransitionByKey(
                             lhs.item, lhs.schema, lhs_transitions_set,
                             lhs_transition_keys[idx]));
        return comparator_->ComparatorT::LhsOnlyAttribute(
            token, lhs_transition_keys[idx], lhs_transition);
      };
      auto rhs_only_attribute = [&](int64_t idx) -> absl::Status {
        ASSIGN_OR_RETURN(auto rhs_transition,
                         rhs_traverse_helper_.TransitionByKey(
                             rhs.item, rhs.schema, rhs_transitions_set,
                             rhs_transition_keys[idx]));
        return comparator_->ComparatorT::RhsOnlyAttribute(
            token, rhs_transition_keys[idx], rhs_transition);
      };

      SortTransitions(lhs_transition_keys);
      SortTransitions(rhs_transition_keys);
      int64_t lhs_idx = 0;
      int64_t rhs_idx = 0;
      while (lhs_idx < lhs_transition_keys.size() &&
             rhs_idx < rhs_transition_keys.size()) {
        int cmp_order = comparator_->ComparatorT::CompareOrder(
            lhs_transition_keys[lhs_idx], rhs_transition_keys[rhs_idx]);
        if (cmp_order < 0) {
          RETURN_IF_ERROR(lhs_only_attribute(lhs_idx++));
        } else if (cmp_order > 0) {
          RETURN_IF_ERROR(rhs_only_attribute(rhs_idx++));
        } else {
          ASSIGN_OR_RETURN(auto lhs_transition,
                           lhs_traverse_helper_.TransitionByKey(
                               lhs.item, lhs.schema, lhs_transitions_set,
                               lhs_transition_keys[lhs_idx]));
          ASSIGN_OR_RETURN(auto rhs_transition,
                           rhs_traverse_helper_.TransitionByKey(
                               rhs.item, rhs.schema, rhs_transitions_set,
                               rhs_transition_keys[rhs_idx]));
          if (comparator_->ComparatorT::Equal(lhs_transition, rhs_transition)) {
            ASSIGN_OR_RETURN(auto to_token,
                             AddToCompare({.item = lhs_transition.item,
                                           .schema = lhs_transition.schema},
                                          {.item = rhs_transition.item,
                                           .schema = rhs_transition.schema}));
            RETURN_IF_ERROR(comparator_->ComparatorT::LhsRhsMatch(
                token, lhs_transition_keys[lhs_idx], to_token));
          } else {
            RETURN_IF_ERROR(comparator_->ComparatorT::LhsRhsMismatch(
                token, lhs_transition_keys[lhs_idx], lhs_transition,
                rhs_transition));
          }
          ++lhs_idx;
          ++rhs_idx;
        }
      }

      while (lhs_idx < lhs_transition_keys.size()) {
        RETURN_IF_ERROR(lhs_only_attribute(lhs_idx++));
      }
      while (rhs_idx < rhs_transition_keys.size()) {
        RETURN_IF_ERROR(rhs_only_attribute(rhs_idx++));
      }
    }
    return absl::OkStatus();
  }

 private:
  TraverseHelper lhs_traverse_helper_;
  TraverseHelper rhs_traverse_helper_;
  std::unique_ptr<ComparatorT> comparator_;
  absl::flat_hash_map<LhsRhsItems, DataItem> token_map_;
  std::stack<std::tuple<DataItem, TraverseHelper::Transition,
                        TraverseHelper::Transition>>
      to_compare_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_DEEP_COMPARATOR_H_
