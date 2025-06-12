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
#ifndef KOLADATA_INTERNAL_TRIPLES_H_
#define KOLADATA_INTERNAL_TRIPLES_H_

#include <algorithm>
#include <string>
#include <tuple>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal::debug {

struct AttrTriple {
  ObjectId object;
  std::string attribute;
  DataItem value;

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const AttrTriple& t) {
    absl::Format(&sink, "ObjectId=%v attr=%s value=%v", t.object, t.attribute,
                 t.value);
  }
  std::string DebugString() const { return absl::StrCat(*this); }
};

struct DictItemTriple {
  ObjectId object;
  DataItem key;
  DataItem value;

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const DictItemTriple& t) {
    if (t.object.IsSchema()) {
      absl::Format(&sink, "SchemaId=%v key=%v value=%v", t.object, t.key,
                   t.value);
    } else {
      absl::Format(&sink, "DictId=%v key=%v value=%v", t.object, t.key,
                   t.value);
    }
  }
  std::string DebugString() const { return absl::StrCat(*this); }
};

inline bool operator==(const AttrTriple& lhs, const AttrTriple& rhs) {
  return std::tie(lhs.object, lhs.attribute, lhs.value) ==
         std::tie(rhs.object, rhs.attribute, rhs.value);
}

inline bool operator==(const DictItemTriple& lhs, const DictItemTriple& rhs) {
  return std::tie(lhs.object, lhs.key, lhs.value) ==
         std::tie(rhs.object, rhs.key, rhs.value);
}

// Converting DataBag to Triples is slow. Intended for tests and debug.
class Triples {
 public:
  explicit Triples(const DataBagContent& content);

  // Returns attribute triples sorted by (object_id, attr).
  std::vector<AttrTriple>& attributes() { return attributes_; }

  // Returns dict item triples sorted by (dict_id, key).
  std::vector<DictItemTriple>& dicts() { return dicts_; }

  // Returns lists sorted by list_id.
  absl::btree_map<ObjectId, std::vector<DataItem>>& lists() { return lists_; }

  const std::vector<AttrTriple>& attributes() const { return attributes_; }
  const std::vector<DictItemTriple>& dicts() const { return dicts_; }
  const absl::btree_map<ObjectId, std::vector<DataItem>>& lists() const {
    return lists_;
  }

  std::string DebugString() const { return absl::StrCat(*this); }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const Triples& t) {
    sink.Append("DataBag {\n");
    for (const AttrTriple& t : t.attributes_) {
      absl::Format(&sink, "  %v\n", t);
    }
    for (const DictItemTriple& t : t.dicts_) {
      absl::Format(&sink, "  %v\n", t);
    }
    for (const auto& [list_id, values] : t.lists_) {
      absl::Format(&sink, "  ListId=%v [%s]\n", list_id,
                   absl::StrJoin(values, ", "));
    }
    sink.Append("}");
  }

  Triples Subtract(const Triples& rhs) const {
    Triples result;
    for (const AttrTriple& attr_triple : attributes_) {
      if (std::find(rhs.attributes().begin(), rhs.attributes().end(),
                    attr_triple) == rhs.attributes().end()) {
        result.attributes_.push_back(attr_triple);
      }
    }
    for (const DictItemTriple& dict_triple : dicts_) {
      if (std::find(rhs.dicts().begin(), rhs.dicts().end(), dict_triple) ==
          rhs.dicts().end()) {
        result.dicts_.push_back(dict_triple);
      }
    }
    for (const auto& [list_id, values] : lists_) {
      const auto it = rhs.lists().find(list_id);
      if (it == rhs.lists().end() || it->second != values) {
        result.lists_[list_id] = values;
      }
    }
    return result;
  }

 private:
  using AttrTripleKey = std::tuple<ObjectId, std::string>;

  // btree_map is used because we need sorted keys.
  using AttrTripleMap = absl::btree_map<AttrTripleKey, DataItem>;

  static void CollectDict(const DataBagContent::DictContent&,
                          std::vector<DictItemTriple>& dicts);
  static void CollectLists(
      const DataBagContent::ListsContent&,
      absl::btree_map<ObjectId, std::vector<DataItem>>& lists);
  static void CollectAttr(const std::string& attr_name,
                          const DataBagContent::AttrContent& attr_content,
                          std::vector<AttrTriple>& attributes);

  Triples() = default;

  std::vector<AttrTriple> attributes_;
  std::vector<DictItemTriple> dicts_;
  absl::btree_map<ObjectId, std::vector<DataItem>> lists_;
};

inline bool operator==(const Triples& lhs, const Triples& rhs) {
  return lhs.attributes() == rhs.attributes() && lhs.lists() == rhs.lists() &&
         lhs.dicts() == rhs.dicts();
}

}  // namespace koladata::internal::debug

#endif  // KOLADATA_INTERNAL_TRIPLES_H_
