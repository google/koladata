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
#include <memory>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/missing_value.h"
#include "arolla/expr/quote.h"
#include "arolla/util/meta.h"

namespace koladata::internal {

// Dictionary DataItem -> DataItem with a link to parent.
class Dict {
 public:
  void Clear() {
    data_.clear();
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
  const DataItem& Get(const T& key) const {
    return Get(key, DataItem::Hash()(key));
  }
  // Same as above with user provided `key_hash` that must be computed via
  // DataItem::Hash()(key).
  template <typename T>
  const DataItem& Get(const T& key, size_t key_hash) const {
    DCHECK_EQ(key_hash, DataItem::Hash()(key));
    for (const Dict* dict = this; dict != nullptr; dict = dict->parent_) {
      if (auto it = dict->data_.find(key, key_hash); it != dict->data_.end()) {
        return it->second;
      }
    }
    return *empty_item_;
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
      DataItem& this_value = data_[key];
      if (!this_value.has_value()) {
        this_value = std::forward<ValueT>(value);
      }
      return this_value;
    }
    auto key_hash = DataItem::Hash()(key);
    if (auto it = data_.find(key, key_hash); it != data_.end()) {
      if (!it->second.has_value()) {
        it->second = std::forward<ValueT>(value);
      }
      return it->second;
    }

    const DataItem& parent_value = parent_->Get(key, key_hash);
    if (parent_value.has_value()) {
      return parent_value;
    }

    auto [it, _] = data_.emplace(key, std::forward<ValueT>(value));
    return it->second;
  }

  // Keys are in arbitrary order.
  // All keys from fallback dictionaries are merged into the result.
  std::vector<DataItem> GetKeys(
      absl::Span<const Dict* const> fallbacks = {}) const;

  size_t GetSizeNoFallbacks() const {
    return parent_ ? GetKeys().size() : data_.size();
  }

 private:
  friend class DictVector;
  using InternalMap =
      absl::flat_hash_map<DataItem, DataItem, DataItem::Hash, DataItem::Eq>;

  // Add extra keys from parents and fallbacks.
  // Keys from the main dict are not added.
  void CollectKeysFromParentAndFallbacks(
      absl::Span<const Dict* const> fallbacks,
      std::vector<DataItem>& keys) const;

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

  explicit DictVector(std::shared_ptr<const DictVector> parent)
      : data_(parent->size()), parent_(std::move(parent)) {
    for (size_t i = 0; i < data_.size(); ++i) {
      const Dict& parent_dict = parent_->data_[i];
      data_[i].parent_ =
          parent_dict.data_.empty() ? parent_dict.parent_ : &parent_dict;
    }
  }

  size_t size() const {
    return data_.size();
  }

  Dict& operator[](int64_t index) { return data_[index]; }
  const Dict& operator[](int64_t index) const { return data_[index]; }

 private:
  std::vector<Dict> data_;
  std::shared_ptr<const DictVector> parent_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_DICT_H_
