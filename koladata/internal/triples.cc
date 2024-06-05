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
#include "koladata/internal/triples.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/log/check.h"
#include "absl/types/span.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "arolla/dense_array/edge.h"

namespace koladata::internal::debug {

void Triples::CollectDict(const DataBagContent::DictContent& dc,
                          std::vector<DictItemTriple>& dicts) {
  DCHECK_EQ(dc.keys.size(), dc.values.size());
  for (int64_t i = 0; i < dc.keys.size(); ++i) {
    dicts.push_back({dc.dict_id, dc.keys[i], dc.values[i]});
  }
}

void Triples::CollectLists(
    const DataBagContent::ListsContent& lc,
    absl::btree_map<ObjectId, std::vector<DataItem>>& lists) {
  size_t list_count = lc.lists_to_values_edge.parent_size();
  DCHECK_EQ(lc.values.size(), lc.lists_to_values_edge.child_size());
  DCHECK_EQ(lc.lists_to_values_edge.edge_type(),
            arolla::DenseArrayEdge::SPLIT_POINTS);
  absl::Span<const int64_t> split_points =
      lc.lists_to_values_edge.edge_values().values.span();
  for (size_t i = 0; i < list_count; ++i) {
    ObjectId list_id = lc.alloc_id.ObjectByOffset(i);
    int64_t from = split_points[i];
    int64_t to = split_points[i + 1];
    std::vector<DataItem>& values = lists[list_id];
    values.reserve(to - from);
    for (int64_t j = from; j < to; ++j) {
      values.push_back(lc.values[j]);
    }
  }
}

void Triples::CollectAttr(const std::string& attr_name,
                          const DataBagContent::AttrContent& attr_content,
                          std::vector<AttrTriple>& attributes) {
  for (const DataBagContent::AttrAllocContent& ac : attr_content.allocs) {
    for (size_t i = 0; i < ac.values.size(); ++i) {
      DataItem v = ac.values[i];
      if (v.has_value()) {
        attributes.push_back(
            {ac.alloc_id.ObjectByOffset(i), attr_name, std::move(v)});
      }
    }
  }
  for (const DataBagContent::AttrItemContent& ic : attr_content.items) {
    attributes.push_back({ic.object_id, attr_name, ic.value});
  }
}

Triples::Triples(const DataBagContent& content) {
  for (const auto& [attr_name, attr_content] : content.attrs) {
    CollectAttr(attr_name, attr_content, attributes_);
  }
  std::sort(attributes_.begin(), attributes_.end(),
            [](const AttrTriple& lhs, const AttrTriple& rhs) -> bool {
              if (lhs.object == rhs.object) {
                return lhs.attribute < rhs.attribute;
              } else {
                return lhs.object < rhs.object;
              }
            });

  for (const DataBagContent::ListsContent& lc : content.lists) {
    CollectLists(lc, lists_);
  }
  for (const DataBagContent::DictContent& dc : content.dicts) {
    CollectDict(dc, dicts_);
  }
}

}  // namespace koladata::internal::debug
