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
#ifndef KOLADATA_INTERNAL_OP_UTILS_SLICE_ATTRIBUTE_TABLE_H_
#define KOLADATA_INTERNAL_OP_UTILS_SLICE_ATTRIBUTE_TABLE_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"

namespace koladata::internal {

// Stores attributes of a `DataSliceImpl` in a transposed (and compressed, when
// items have different sets of attributes) representation, providing per-item
// access to attribute names and values.
class SliceAttributeTable {
 public:
  class Builder;

  SliceAttributeTable() = default;
  SliceAttributeTable(SliceAttributeTable&&) = default;
  SliceAttributeTable& operator=(SliceAttributeTable&&) = default;
  SliceAttributeTable(const SliceAttributeTable&) = delete;
  SliceAttributeTable& operator=(const SliceAttributeTable&) = delete;

  // Returns the attribute names of the `i`-th item of the slice.
  absl::Span<const absl::string_view> NamesOf(
      int64_t i) const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, slice_size_);
    if (same_attr_names_) {
      return names_;
    }
    return absl::MakeConstSpan(names_).subspan(splits_[i],
                                               splits_[i + 1] - splits_[i]);
  }

  // Returns the attribute values of the `i`-th item of the slice (aligned with
  // `NamesOf(i)`).
  absl::Span<const DataItem> ValuesOf(
      int64_t i) const ABSL_ATTRIBUTE_LIFETIME_BOUND {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, slice_size_);
    return absl::MakeConstSpan(values_).subspan(splits_[i],
                                                splits_[i + 1] - splits_[i]);
  }

 private:
  int64_t slice_size_ = 0;
  bool same_attr_names_ = true;
  // Owns the attribute name strings referenced by `names_`; not modified after
  // `Builder::Build()`.
  std::vector<std::string> owned_names_;
  std::vector<absl::string_view> names_;
  std::vector<DataItem> values_;
  // Has size `slice_size_ + 1`. `[splits_[i], splits_[i + 1])` is the range in
  // `values_` (and in `names_` when `!same_attr_names_`) for the `i`-th item.
  std::vector<int64_t> splits_;
};

class SliceAttributeTable::Builder {
 public:
  Builder(int64_t slice_size, size_t num_attrs);

  // Adds an attribute that all items of the slice have.
  void Add(absl::string_view name, DataSliceImpl values);

  // Adds an attribute for the items of the slice where `has_attr` is present
  // (only presence is checked; values and dtype of `has_attr` are ignored).
  void Add(absl::string_view name, DataSliceImpl values,
           DataSliceImpl has_attr);

  SliceAttributeTable Build() &&;

 private:
  struct Attribute {
    std::string name;
    DataSliceImpl values;
    // Nullopt if all items have this attribute.
    std::optional<DataSliceImpl> has_attr;
  };

  int64_t slice_size_;
  std::vector<Attribute> attrs_;
  bool same_attr_names_ = true;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_SLICE_ATTRIBUTE_TABLE_H_
