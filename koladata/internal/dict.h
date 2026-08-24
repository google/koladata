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
#ifndef KOLADATA_INTERNAL_DICT_H_
#define KOLADATA_INTERNAL_DICT_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <variant>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/container/fixed_array.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/types/span.h"
#include "arolla/expr/quote.h"
#include "arolla/util/meta.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/memory_stats.h"
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
        return;
      }
    }
    using KeyT = arolla::meta::strip_template_t<DataItem::View, T>;
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

  // Either returns the reference to an existing (maybe empty) element, or
  // assigns the new value and returns it.
  // `T` is either DataItem, or one of the types that can be stored in DataItem.
  // Returns reference to empty DataItem in case key is missing or unsupported.
  template <typename T, typename ValueT>
  const DataItem& GetOrAssign(const T& key, ValueT&& value) {
    if constexpr (std::is_same_v<T, DataItem>) {
      if (IsUnsupportedDataItemKeyType(key)) {
        return *empty_item_;
      }
    }
    using KeyT = arolla::meta::strip_template_t<DataItem::View, T>;
    if constexpr (IsUnsupportedKeyType<KeyT>()) {
      return *empty_item_;
    }
    if (parent_ == nullptr) {
      auto [it, _] = data_.try_emplace(key, std::forward<ValueT>(value));
      return it->second;
    }
    if (auto it = data_.find(key); it != data_.end()) {
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

  // Similar to `GetKeys`, but sorted using DataItem::Less. Use together with
  // `GetSortedByKeyValues`.
  std::vector<DataItem> GetSortedKeys(
      absl::Span<const Dict* const> fallbacks = {}) const;

  // Returns the keys in this dict that are different from the parent dict.
  // Note: keys may be considered different even if values are identical.
  // If parent is nullptr or not present in the list of parents, behavior is
  // identical to GetKeys().
  std::vector<DataItem> GetModifiedKeys(const Dict* parent) const;

  // While the order of values is arbitrary, it is the same as GetKeys().
  // All values from fallback dictionaries are merged into the result.
  std::vector<DataItem> GetValues(
      absl::Span<const Dict* const> fallbacks = {}) const;

  // The values, sorted by the order of keys. Use together with `GetSortedKeys`.
  std::vector<DataItem> GetSortedByKeyValues(
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

  // Note: it doesn't include parent's size
  void AppendMemoryStats(MemoryStatsEntry& stats) const;

 private:
  friend class DictVector;
  class KeyValueCollector;
  using InternalMap =
      absl::flat_hash_map<DataItem, DataItem, DataItem::Hash, DataItem::Eq>;

  const Dict* FindFirstNonEmpty() const {
    auto* dict = this;
    while (dict != nullptr && dict->data_.empty()) {
      dict = dict->parent_;
    }
    return dict;
  }

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
 private:
  using Array = absl::FixedArray<Dict, 0>;

  static constexpr size_t kArraySizeThreshold = 4;

  struct Map {
    using MapType = absl::flat_hash_map<size_t, Dict>;
    MapType map;

    void ConvertToArray(Array& array, const DictVector* parent,
                        const Dict* single_parent_dict) &&;
  };

  bool is_map_mode() const {
    return std::holds_alternative<Map>(data_);
  }

  bool ShouldUseArray(size_t update_size) const {
    return size_ <= kArraySizeThreshold || update_size * 10 > size_ * 3;
  }

  friend struct DictVectorTestFriend;

 public:
  explicit DictVector(size_t size, size_t update_size) : size_(size) {
    if (ShouldUseArray(update_size)) {
      data_.emplace<Array>(size);
    } else {
      Map& m = data_.emplace<Map>();
      if (update_size > 0) {
        m.map.reserve(std::min(size, update_size));
      }
    }
  }

  // Non copyable to avoid confusion with constructor from shared_ptr.
  DictVector(const DictVector&) = delete;
  DictVector(DictVector&&) = delete;
  DictVector& operator=(const DictVector&) = delete;
  DictVector& operator=(DictVector&&) = delete;

  // Note: DictVector relies that `parent` is not modified during its lifetime.
  explicit DictVector(std::shared_ptr<const DictVector> parent,
                      size_t update_size)
      : size_(parent->size()), parent_(std::move(parent)) {
    if (ShouldUseArray(update_size)) {
      Array& array = data_.emplace<Array>(size_);
      for (size_t i = 0; i < size_; ++i) {
        const Dict* parent_dict = parent_->Get(i);
        if (parent_dict != nullptr) {
          array[i].parent_ =
              parent_dict->data_.empty() ? parent_dict->parent_ : parent_dict;
        }
      }
    } else {
      Map& m = data_.emplace<Map>();
      if (update_size > 0) {
        m.map.reserve(update_size);
      }
    }
  }

  explicit DictVector(size_t size, std::shared_ptr<const Dict> parent_dict,
                      size_t update_size)
      : size_(size),
        single_parent_dict_(std::move(parent_dict)),
        single_parent_dict_ptr_(
            single_parent_dict_ != nullptr
                ? (single_parent_dict_->data_.empty()
                       ? single_parent_dict_->parent_
                       : single_parent_dict_.get())
                : nullptr) {
    if (ShouldUseArray(update_size)) {
      Array& array = data_.emplace<Array>(size_);
      for (size_t i = 0; i < size_; ++i) {
        array[i].parent_ = single_parent_dict_ptr_;
      }
    } else {
      Map& m = data_.emplace<Map>();
      if (update_size > 0) {
        m.map.reserve(std::min(size, update_size));
      }
    }
  }

  size_t size() const { return size_; }

  const Dict* absl_nullable Get(size_t index) const {
    DCHECK_LT(index, size());
    if (std::holds_alternative<Array>(data_)) {
      const Dict& dict = std::get<Array>(data_)[index];
      if (dict.data_.empty() && dict.parent_ == nullptr) {
        return nullptr;
      }
      return &dict;
    }
    return GetFromMap(index);
  }

  Dict& GetMutable(size_t index) {
    DCHECK_LT(index, size());
    if (std::holds_alternative<Array>(data_)) {
      return GetMutableFromArray(index);
    }
    return GetMutableFromMap(index);
  }

  Dict& operator[](int64_t index) {
    DCHECK_GE(index, 0);
    return GetMutable(static_cast<size_t>(index));
  }

  // Note: it doesn't include parent's size.
  void AppendMemoryStats(MemoryStatsEntry& stats) const;

 private:
  const Dict* GetParentDict(size_t index) const;
  const Dict* absl_nullable GetFromMap(size_t index) const;
  Dict& GetMutableFromArray(size_t index);
  Dict& GetMutableFromMap(size_t index);

  size_t size_ = 0;
  std::variant<Map, Array> data_;

  std::shared_ptr<const DictVector> parent_;
  std::shared_ptr<const Dict> single_parent_dict_;
  const Dict* single_parent_dict_ptr_ = nullptr;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_DICT_H_
