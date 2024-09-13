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
#include "koladata/internal/dict.h"

#include <cstddef>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/attributes.h"
#include "absl/container/flat_hash_set.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"

namespace koladata::internal {

template <bool kReturnValues>
ABSL_ATTRIBUTE_NOINLINE void Dict::CollectKeysOrValuesFromParentAndFallbacks(
    absl::Span<const Dict* const> fallbacks,
    std::vector<DataItem>& keys_or_values) const {
  const auto* orig_data = &data_;
  InternalMap empty_data;
  absl::flat_hash_set<DataItem, DataItem::Hash, DataItem::Eq> used_keys;
  absl::flat_hash_set<DataItem, DataItem::Hash, DataItem::Eq> removed_keys;
  for (const Dict* dict = parent_; true; dict = dict->parent_) {
    if (dict == nullptr) {
      if (fallbacks.empty()) {
        break;
      }
      // transfer non removed keys from original data to used_keys,
      // so that fallback not adding it.
      for (const auto& [key, value] : *orig_data) {
        if (value.has_value()) {
          used_keys.insert(key);
        }
      }
      orig_data = &empty_data;
      // removed keys are relevant only withing one dict chain
      removed_keys.clear();
      // moving to the new dict chain
      dict = fallbacks.front();
      fallbacks.remove_prefix(1);
    }
    for (const auto& [key, value] : dict->data_) {
      if (value.has_value()) {
        size_t hash = DataItem::Hash()(key);
        if (orig_data->find(key, hash) == orig_data->end() &&
            used_keys.find(key, hash) == used_keys.end() &&
            removed_keys.find(key, hash) == removed_keys.end()) {
          keys_or_values.push_back(kReturnValues ? value : key);
          if (dict->parent_ != nullptr || !fallbacks.empty()) {
            used_keys.insert(key);
          }
        }
      } else if (dict->parent_ != nullptr) {
        removed_keys.insert(key);
      }
    }
  }
}

std::vector<DataItem> Dict::GetKeys(
    absl::Span<const Dict* const> fallbacks) const {
  auto* dict = FindFirstNonEmpty();
  if (dict == nullptr) {
    if (fallbacks.empty()) {
      return {};
    }
    return fallbacks[0]->GetKeys(fallbacks.subspan(1));
  }
  std::vector<DataItem> keys;
  keys.reserve(dict->data_.size());
  for (const auto& [key, value] : dict->data_) {
    if (value.has_value()) {
      keys.push_back(key);
    }
  }
  if (dict->parent_ != nullptr || !fallbacks.empty()) {
    dict->CollectKeysOrValuesFromParentAndFallbacks</*kReturnValues=*/false>(
        fallbacks, keys);
  }
  return keys;
};

std::vector<DataItem> Dict::GetValues(
    absl::Span<const Dict* const> fallbacks) const {
  auto* dict = FindFirstNonEmpty();
  if (dict == nullptr) {
    if (fallbacks.empty()) {
      return {};
    }
    return fallbacks[0]->GetValues(fallbacks.subspan(1));
  }
  std::vector<DataItem> values;
  values.reserve(dict->data_.size());
  for (const auto& [key, value] : dict->data_) {
    if (value.has_value()) {
      values.push_back(value);
    }
  }
  if (dict->parent_ != nullptr || !fallbacks.empty()) {
    dict->CollectKeysOrValuesFromParentAndFallbacks</*kReturnValues=*/true>(
        fallbacks, values);
  }
  return values;
};

const absl::NoDestructor<DataItem> Dict::empty_item_;

}  // namespace koladata::internal
