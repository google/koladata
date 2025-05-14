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
#ifndef KOLADATA_INTERNAL_DICT_H_
#define KOLADATA_INTERNAL_DICT_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/types/span.h"
#include "arolla/expr/quote.h"
#include "arolla/util/meta.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/missing_value.h"

namespace koladata::internal {

// Dictionary DataItem -> DataItem with a link to parent.
class Dict {
 public:
  void Clear() {
    std::vector<DataItem> keys = GetKeys({});
    data_.clear();
    data_.reserve(keys.size());
    for (const auto& key : keys) {
      data_.emplace(key, DataItem());
    }
    parent_ = nullptr;
  }

  template <typename KeyT>
  static constexpr bool IsUnsupportedKeyType() {
    return std::is_same_v<KeyT, MissingValue> || std::is_same_v<KeyT, float> ||
           std::is_same_v<KeyT, double> ||
           std::is_same_v<KeyT, arolla::expr::ExprQuote>;
  }

  static bool IsUnsupportedDataItemKeyType(const DataItem& key) {
    return !key.has_value() || key.template holds_value<float>() ||
           key.template holds_value<double>() ||
           key.template holds_value<arolla::expr::ExprQuote>();
  }

  // `T` is either DataItem, or one of the types that can be stored in DataItem.
  // If key is missing (MissingValue or DataItem containing MissingValue),
  // the operation is no-op. Float and ExprQuote keys are not supported and are
  // treated as missing.
  template <typename T>
  void Set(const T& key, DataItem value) {
    if constexpr (std::is_same_v<T, DataItem>) {
      if (IsUnsupportedDataItemKeyType(key)) {
        DCHECK(false);
        return;
      }
    }
    using KeyT = arolla::meta::strip_template_t<DataItem::View, T>;
    DCHECK(!IsUnsupportedKeyType<KeyT>());
    if constexpr (!IsUnsupportedKeyType<KeyT>()) {
      data_[key] = std::move(value);
    }
  }

  // `T` is either DataItem, or one of the types that can be stored in DataItem.
  template <typename T>
  std::optional<std::reference_wrapper<const DataItem>> Get(
      const T& key) const {
    for (const Dict* dict = this; dict != nullptr; dict = dict->parent_) {
      if (auto it = dict->data_.find(key); it != dict->data_.end()) {
        return std::ref(it->second);
      }
    }
    return std::nullopt;
  }

  // Returns the reference to existing non empty element.
  // Otherwise assign new value and return it.
  // `T` is either DataItem, or one of the types that can be stored in DataItem.
  // Returns reference to empty DataItem in case key is missing or unsupported.
  template <typename T, typename ValueT>
  const DataItem& GetOrAssign(const T& key, ValueT&& value) {
    if constexpr (std::is_same_v<T, DataItem>) {
      if (IsUnsupportedDataItemKeyType(key)) {
        DCHECK(false);
        return *empty_item_;
      }
    }
    using KeyT = arolla::meta::strip_template_t<DataItem::View, T>;
    if constexpr (IsUnsupportedKeyType<KeyT>()) {
      DCHECK(false);
      return *empty_item_;
    }
    if (parent_ == nullptr) {
      auto [it, _] = data_.try_emplace(key, std::forward<ValueT>(value));
      return it->second;
    }
    if (auto it = data_.find(key); it != data_.end()) {
      if (!it->second.has_value()) {
        it->second = std::forward<ValueT>(value);
      }
      return it->second;
    }

    auto parent_value = parent_->Get(key);
    if (parent_value.has_value()) {
      return *parent_value;
    }

    auto [it, _] = data_.emplace(key, std::forward<ValueT>(value));
    return it->second;
  }

  // While the order of keys is arbitrary, it is the same as GetValues().
  // All keys from fallback dictionaries are merged into the result.
  std::vector<DataItem> GetKeys(
      absl::Span<const Dict* const> fallbacks = {}) const;

  // While the order of values is arbitrary, it is the same as GetKeys().
  // All values from fallback dictionaries are merged into the result.
  std::vector<DataItem> GetValues(
      absl::Span<const Dict* const> fallbacks = {}) const;

  size_t GetSizeNoFallbacks() const {
    auto* dict = FindFirstNonEmpty();
    if (dict == nullptr) {
      return 0;
    }
    if (dict->parent_ == nullptr) {
      return dict->data_.size();
    }
    return dict->GetKeys().size();
  }

 private:
  friend class DictVector;
  using InternalMap =
      absl::flat_hash_map<DataItem, DataItem, DataItem::Hash, DataItem::Eq>;

  const Dict* FindFirstNonEmpty() const {
    auto* dict = this;
    while (dict != nullptr && dict->data_.empty()) {
      dict = dict->parent_;
    }
    return dict;
  }

  // Add extra keys from parents and fallbacks.
  // Keys from the main dict are not added.
  // void CollectKeysFromParentAndFallbacks(
  //     absl::Span<const Dict* const> fallbacks,
  //     std::vector<DataItem>& keys) const;
  // Add keys or values from this dict, its parents and fallbacks.
  template <bool kReturnValues>
  void CollectKeysOrValuesFromParentAndFallbacks(
      absl::Span<const Dict* const> fallbacks,
      std::vector<DataItem>& keys_or_values) const;

  // If parent is nullptr we do not store missing values.
  InternalMap data_;

  // It can be set only by DictVector, and DictVector holds shared_ptr
  // to parent. So it is safe to use raw pointer if the DictVector still exists.
  const Dict* parent_ = nullptr;

  static const absl::NoDestructor<DataItem> empty_item_;
};

// Vector of dictionaries. Can be created from shared_ptr to another DictVector
// and in this case will store only the diffs between dicts.
class DictVector {
 public:
  explicit DictVector(size_t size) : data_(size) {}

  // Non copyable to avoid confusion with constructor from shared_ptr.
  DictVector(const DictVector&) = delete;
  DictVector& operator=(const DictVector&) = delete;

  explicit DictVector(std::shared_ptr<const DictVector> parent)
      : data_(parent->size()) {
    for (size_t i = 0; i < data_.size(); ++i) {
      const Dict& parent_dict = parent->data_[i];
      data_[i].parent_ =
          parent_dict.data_.empty() ? parent_dict.parent_ : &parent_dict;
    }
    parent_ = std::move(parent);
  }

  explicit DictVector(size_t size, std::shared_ptr<const Dict> parent_dict)
      : data_(size) {
    const Dict* parent_dict_ptr =
        parent_dict->data_.empty() ? parent_dict->parent_ : parent_dict.get();
    for (size_t i = 0; i < data_.size(); ++i) {
      data_[i].parent_ = parent_dict_ptr;
    }
    parent_ = std::move(parent_dict);
  }

  size_t size() const {
    return data_.size();
  }

  Dict& operator[](int64_t index) { return data_[index]; }
  const Dict& operator[](int64_t index) const { return data_[index]; }

 private:
  std::vector<Dict> data_;
  // All parent links are stored inside of the Dict. We only hold ownership.
  std::shared_ptr<const void> parent_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_DICT_H_
