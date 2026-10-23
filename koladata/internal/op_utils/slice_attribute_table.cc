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
#include "koladata/internal/op_utils/slice_attribute_table.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/data_slice.h"

namespace koladata::internal {
namespace {

template <typename Fn>
void ForEachItemWithAttr(int64_t slice_size,
                         const std::optional<DataSliceImpl>& has_attr, Fn fn) {
  if (!has_attr.has_value()) {
    for (int64_t i = 0; i < slice_size; ++i) {
      fn(i);
    }
  } else {
    has_attr->VisitValues([&](const auto& array) {
      array.ForEachPresent([&](int64_t i, auto) { fn(i); });
    });
  }
}

}  // namespace

SliceAttributeTable::Builder::Builder(int64_t slice_size, size_t num_attrs)
    : slice_size_(slice_size) {
  DCHECK_GE(slice_size_, 0);
  attrs_.reserve(num_attrs);
}

void SliceAttributeTable::Builder::Add(absl::string_view name,
                                       DataSliceImpl values) {
  DCHECK_EQ(values.size(), slice_size_);
  attrs_.push_back({.name = std::string(name), .values = std::move(values)});
}

void SliceAttributeTable::Builder::Add(absl::string_view name,
                                       DataSliceImpl values,
                                       DataSliceImpl has_attr) {
  DCHECK_EQ(values.size(), slice_size_);
  DCHECK_EQ(has_attr.size(), slice_size_);
  if (has_attr.present_count() == 0) {
    return;
  }
  if (has_attr.present_count() == slice_size_) {
    Add(name, std::move(values));
    return;
  }
  same_attr_names_ = false;
  attrs_.push_back({.name = std::string(name),
                    .values = std::move(values),
                    .has_attr = std::move(has_attr)});
}

SliceAttributeTable SliceAttributeTable::Builder::Build() && {
  SliceAttributeTable table;
  table.slice_size_ = slice_size_;
  table.same_attr_names_ = same_attr_names_;
  table.owned_names_.reserve(attrs_.size());
  for (Attribute& attr : attrs_) {
    table.owned_names_.push_back(std::move(attr.name));
  }

  table.splits_.assign(slice_size_ + 1, 0);
  for (const Attribute& attr : attrs_) {
    ForEachItemWithAttr(slice_size_, attr.has_attr,
                        [&](int64_t i) { ++table.splits_[i + 1]; });
  }
  for (int64_t i = 0; i < slice_size_; ++i) {
    table.splits_[i + 1] += table.splits_[i];
  }

  const int64_t total_entries = table.splits_[slice_size_];
  if (same_attr_names_) {
    table.names_.reserve(attrs_.size());
    for (const std::string& name : table.owned_names_) {
      table.names_.push_back(name);
    }
  } else {
    table.names_.resize(total_entries);
  }
  table.values_.resize(total_entries);

  std::vector<int64_t> offsets = table.splits_;
  for (size_t k = 0; k < attrs_.size(); ++k) {
    absl::string_view attr_name = table.owned_names_[k];
    const DataSliceImpl& values = attrs_[k].values;
    ForEachItemWithAttr(slice_size_, attrs_[k].has_attr, [&](int64_t i) {
      int64_t pos = offsets[i]++;
      if (!same_attr_names_) {
        table.names_[pos] = attr_name;
      }
      table.values_[pos] = values[i];
    });
  }
  return table;
}

}  // namespace koladata::internal
