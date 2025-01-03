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
#include "koladata/operators/json.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "nlohmann/json.hpp"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/casting.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/operators/utils.h"
#include "koladata/schema_utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {

// TODO: Move to pointwise utils.
template <typename Fn>
absl::Status ForEachDataItem(const DataSlice& slice, Fn fn) {
  DCHECK_EQ(slice.GetShape().rank(), 1);
  int64_t i = 0;
  for (const auto& item_impl : slice.slice().AsDataItemDenseArray()) {
    ASSIGN_OR_RETURN(auto item,
                     DataSlice::Create(item_impl.value, slice.GetSchemaImpl(),
                                       slice.GetBag()));
    RETURN_IF_ERROR(fn(i, item));
    ++i;
  }
  return absl::OkStatus();
}

constexpr absl::string_view kKeyListAttr = "__koladata_json_object_keys__";
constexpr absl::string_view kValueListAttr = "__koladata_json_object_values__";

// Map-like class that preserves insertion order *and* duplicate keys. This is
// used to give us more complete control over the JSON serializer.
//
// NOTE: Implements only parts of the map concept needed to get nlohmann JSON
// serialization to compile.
template <typename K, typename V, typename Compare = std::less<K>,
          typename Allocator = std::allocator<std::pair<const K, V>>>
class FullInsertionOrderMap {
 public:
  using key_type = K;
  using mapped_type = V;
  using value_type = std::pair<const K, V>;
  using size_type = std::vector<value_type>::size_type;
  using difference_type = std::vector<value_type>::difference_type;
  using key_compare = Compare;
  using iterator = std::vector<value_type>::iterator;
  using const_iterator = std::vector<value_type>::const_iterator;

  FullInsertionOrderMap() = default;
  explicit FullInsertionOrderMap(std::vector<value_type> items)
      : items_(std::move(items)) {}

  iterator begin() { return items_.begin(); }
  const_iterator cbegin() const { return items_.cbegin(); }
  iterator end() { return items_.end(); }
  const_iterator cend() const { return items_.cend(); }

  bool empty() { return items_.empty(); }
  size_type size() { return items_.size(); }

  void clear() { items_.clear(); }

  template <typename... Args>
  std::pair<iterator, bool> emplace(Args&&... args) {
    items_.emplace_back(std::forward<Args>(args)...);
    return std::make_tuple(std::prev(items_.end()), true);
  }

 private:
  std::vector<std::pair<const K, V>, Allocator> items_;
};

using SerializableJson = nlohmann::basic_json<FullInsertionOrderMap>;
using SerializableJsonObject = SerializableJson::object_t;

template <typename T>
absl::StatusOr<SerializableJson> PrimitiveValueToSerializableJson(
    const T& value, const internal::DataItem& schema) {
  if constexpr (std::is_same_v<T, internal::ObjectId>) {
    DCHECK((!std::is_same_v<T, internal::ObjectId>));
    return SerializableJson(nullptr);  // Should be unreachable.
  } else if constexpr (std::is_same_v<T, internal::MissingValue>) {
    if (schema == schema::kMask) {
      return SerializableJson(false);  // false
    } else {
      return SerializableJson(nullptr);  // null
    }
  } else if constexpr (std::is_same_v<T, int> ||
                       std::is_same_v<T, int64_t>) {
    return SerializableJson(static_cast<int64_t>(value));
  } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
    if (std::isfinite(value)) {
      return SerializableJson(static_cast<double>(value));
    } else if (std::isnan(value)) {
      return SerializableJson("nan");
    } else if (value > 0) {
      return SerializableJson("inf");
    } else {
      return SerializableJson("-inf");
    }
  } else if constexpr (std::is_same_v<T, bool>) {
    return SerializableJson(value);
  } else if constexpr (std::is_same_v<T, arolla::Unit>) {
    return SerializableJson(true);
  } else if constexpr (std::is_same_v<T, arolla::Text>) {
    return SerializableJson(std::string(value.view()));
  } else {
    return absl::InvalidArgumentError(absl::StrFormat(
        "unsupported schema %s for json serialization",
        schema::schema_internal::GetQTypeName(arolla::GetQType<T>())));
  }
}

absl::StatusOr<SerializableJson> PrimitiveDataItemToSerializableJson(
    const DataSlice& item) {
  return item.item().VisitValue([&]<typename T>(const T& value) {
    return PrimitiveValueToSerializableJson(value, item.GetSchemaImpl());
  });
}

// Forward declaration for recursion.
//
// NOTE: nlohmann json serialization uses recursion internally, so there isn't
// much of a robustness benefit to making this function non-recursive, but it
// could be done in a similar way to ToProto/FromProto.
absl::StatusOr<SerializableJson> DataItemToSerializableJson(
    const DataSlice& item,
    absl::flat_hash_set<internal::ObjectId>& path_object_ids);

absl::StatusOr<SerializableJson> ListDataItemToSerializableJson(
    const DataSlice& item,
    absl::flat_hash_set<internal::ObjectId>& path_object_ids) {
  DCHECK(item.is_item());

  ASSIGN_OR_RETURN(auto list_items, item.ExplodeList(0, std::nullopt));
  std::vector<SerializableJson> json_array;
  json_array.reserve(list_items.size());
  RETURN_IF_ERROR(ForEachDataItem(
      list_items, [&](auto, const DataSlice& list_item) -> absl::Status {
        ASSIGN_OR_RETURN(
            json_array.emplace_back(),
            DataItemToSerializableJson(list_item, path_object_ids));
        return absl::OkStatus();
      }));
  return SerializableJson(std::move(json_array));
}

absl::StatusOr<SerializableJson> DictDataItemToSerializableJson(
    const DataSlice& item,
    absl::flat_hash_set<internal::ObjectId>& path_object_ids) {
  DCHECK(item.is_item());

  ASSIGN_OR_RETURN(auto keys, item.GetDictKeys());
  DCHECK(!keys.is_item());
  DCHECK(keys.slice().present_count() == keys.slice().size());
  if (keys.GetSchemaImpl() != schema::kString &&
      keys.GetSchemaImpl() != schema::kObject) {
    return absl::InvalidArgumentError(
        absl::StrFormat("unsupported dict key schema %v for json serialization",
                        keys.GetSchemaImpl()));
  }
  if (!keys.IsEmpty() &&
      keys.slice().dtype() != arolla::GetQType<arolla::Text>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "unsupported dict key dtype %s for json serialization",
        schema::schema_internal::GetQTypeName(keys.slice().dtype())));
  }
  if (keys.IsEmpty()) {
    return SerializableJson(SerializableJsonObject());
  }

  const auto& keys_array = keys.slice().values<arolla::Text>().values;

  std::vector<int64_t> keys_order;
  keys_order.reserve(keys_array.size());
  for (int64_t i = 0; i < keys_array.size(); ++i) {
    keys_order.push_back(i);
  }
  std::sort(keys_order.begin(), keys_order.end(), [&](int64_t i, int64_t j) {
    return keys_array[i] < keys_array[j];
  });

  ASSIGN_OR_RETURN(auto values, item.GetDictValues());
  std::vector<DataSlice> values_array;
  values_array.reserve(values.size());
  RETURN_IF_ERROR(ForEachDataItem(
      values, [&](auto, const DataSlice& value) -> absl::Status {
        values_array.push_back(value);
        return absl::OkStatus();
      }));

  std::vector<std::pair<const std::string, SerializableJson>> object_items;
  for (int64_t i : keys_order) {
    ASSIGN_OR_RETURN(auto value, DataItemToSerializableJson(
                                     values_array[i], path_object_ids));
    object_items.emplace_back(keys_array[i], std::move(value));
  }
  return SerializableJson(SerializableJsonObject(std::move(object_items)));
}

absl::StatusOr<SerializableJson> EntityDataItemToSerializableJson(
    const DataSlice& item,
    absl::flat_hash_set<internal::ObjectId>& path_object_ids) {
  DCHECK(item.is_item());

  ASSIGN_OR_RETURN(auto attr_names, item.GetAttrNames());

  std::vector<std::pair<const std::string, SerializableJson>> object_items;
  if (attr_names.contains(kKeyListAttr)) {
    ASSIGN_OR_RETURN(auto key_list, item.GetAttr(kKeyListAttr));
    RETURN_IF_ERROR(key_list.GetSchema().VerifyIsListSchema());
    ASSIGN_OR_RETURN(auto key_list_items,
                     key_list.ExplodeList(0, std::nullopt));
    if (key_list_items.GetSchemaImpl() != schema::kString) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected json key list to contain STRING, got %v",
                          key_list_items.GetSchemaImpl()));
    }
    if (!key_list_items.IsEmpty()) {
      if (key_list_items.size() != key_list_items.present_count()) {
        return absl::InvalidArgumentError(
            "expected json key list not to have missing items");
      }

      // Primitive STRING DataSlice must contain only strings.
      DCHECK_EQ(key_list_items.dtype(), arolla::GetQType<arolla::Text>());
      const auto& key_list_values =
          key_list_items.slice().values<arolla::Text>().values;

      // Object keys and order are specified by kKeyListAttr.
      if (attr_names.contains(kValueListAttr)) {
        // Object values are specified by kValueListAttr (supports duplicate
        // keys with differing values).
        ASSIGN_OR_RETURN(auto value_list, item.GetAttr(kValueListAttr));
        RETURN_IF_ERROR(value_list.GetSchema().VerifyIsListSchema());
        ASSIGN_OR_RETURN(auto value_list_items,
                         value_list.ExplodeList(0, std::nullopt));
        if (value_list_items.size() != key_list_items.size()) {
          return absl::InvalidArgumentError(
              absl::StrFormat("expected json key list and json value list to "
                              "have the same length, got %d and %d",
                              key_list_items.size(), value_list_items.size()));
        }

        RETURN_IF_ERROR(ForEachDataItem(
            value_list_items,
            [&](int64_t i, const DataSlice& value) -> absl::Status {
              const auto& key = key_list_values[i];
              ASSIGN_OR_RETURN(auto json_value, DataItemToSerializableJson(
                                                    value, path_object_ids));
              object_items.emplace_back(key, std::move(json_value));
              return absl::OkStatus();
            }));
      } else {
        // Object values are specified by attr values.
        for (const auto& key : key_list_values) {
          ASSIGN_OR_RETURN(auto attr_value, item.GetAttr(key));
          ASSIGN_OR_RETURN(
              auto json_attr_value,
              DataItemToSerializableJson(attr_value, path_object_ids));
          object_items.emplace_back(key, std::move(json_attr_value));
        }
      }
    }
  } else {
    // Object keys are attr names in lexicographic order, and their values are
    // the attr values.
    for (const auto& attr_name : attr_names) {
      ASSIGN_OR_RETURN(auto attr_value, item.GetAttr(attr_name));
      ASSIGN_OR_RETURN(
          auto json_attr_value,
          DataItemToSerializableJson(attr_value, path_object_ids));
      object_items.emplace_back(attr_name, std::move(json_attr_value));
    }
  }
  return SerializableJson(SerializableJsonObject(std::move(object_items)));
}

absl::StatusOr<SerializableJson> DataItemToSerializableJson(
    const DataSlice& item,
    absl::flat_hash_set<internal::ObjectId>& path_object_ids) {
  DCHECK(item.is_item());

  if (item.GetSchemaImpl() == schema::kSchema ||
      item.item().holds_value<schema::DType>()) {
    return absl::InvalidArgumentError(
        "unsupported schema SCHEMA for json serialization");
  }

  std::optional<internal::ObjectId> item_object_id;
  if (item.item().holds_value<internal::ObjectId>()) {
    item_object_id = item.item().value<internal::ObjectId>();
  }

  if (item_object_id.has_value()) {
    if (!path_object_ids.insert(*item_object_id).second) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "cycle detected in json serialization at %s",
          DataSliceRepr(item)));
    }
  }

  SerializableJson json;
  if (item_object_id.has_value()) {
    if (item.GetSchemaImpl() == schema::kItemId) {
      return absl::InvalidArgumentError(
          "unsupported schema ITEMID for json serialization");
    } else if (item_object_id->IsList()) {
      ASSIGN_OR_RETURN(json,
                       ListDataItemToSerializableJson(item, path_object_ids));
    } else if (item_object_id->IsDict()) {
      ASSIGN_OR_RETURN(json,
                       DictDataItemToSerializableJson(item, path_object_ids));
    } else {
      ASSIGN_OR_RETURN(json,
                       EntityDataItemToSerializableJson(item, path_object_ids));
    }
  } else {
    ASSIGN_OR_RETURN(json, PrimitiveDataItemToSerializableJson(item));
  }

  if (item_object_id.has_value()) {
    path_object_ids.erase(*item_object_id);
  }

  return json;
}

}  // namespace

absl::StatusOr<DataSlice> ToJson(DataSlice x, DataSlice indent,
                                 DataSlice ensure_ascii) {
  int indent_value = 0;
  bool indent_is_none = true;
  if (!indent.IsEmpty()) {
    RETURN_IF_ERROR(ExpectPresentScalar("indent", indent, schema::kInt32));
    indent_value = indent.item().value<int32_t>();
    indent_is_none = false;
  }
  ASSIGN_OR_RETURN(bool ensure_ascii_value,
                   GetBoolArgument(ensure_ascii, "ensure_ascii"));

  ASSIGN_OR_RETURN(auto flat_x, x.Reshape(x.GetShape().FlatFromSize(x.size())));

  arolla::DenseArrayBuilder<arolla::Text> result_builder(x.size());
  RETURN_IF_ERROR(ForEachDataItem(
      flat_x, [&](int64_t i, const DataSlice& item) -> absl::Status {
        if (!item.item().has_value()) {
          return absl::OkStatus();
        }

        absl::flat_hash_set<internal::ObjectId> path_object_ids;
        ASSIGN_OR_RETURN(auto json,
                         DataItemToSerializableJson(item, path_object_ids));
        std::string result =
            json.dump(indent_value, ' ', ensure_ascii_value,
                      nlohmann::detail::error_handler_t::ignore);

        if (indent_is_none) {
          // For compatibility with python json indent=None, we want a mode
          // where we get padding after "," and ":", and nlohmann json doesn't
          // support this natively. Instead, we do some hacky post-processing
          // on its indent=0 output to replicate the padding. indent=0 already
          // has a space after ":" and adds a newline after every comma, so we
          // replace the comma+newline with comma+space and strip the other
          // newlines.
          //
          // This substitution doesn't affect the value of the JSON data because
          // newlines are always escaped inside of JSON strings, so the newline
          // we are substituting out must be whitespace.
          result = absl::StrReplaceAll(result, {{",\n", ", "}, {"\n", ""}});
        }

        result_builder.Add(i, std::move(result));
        return absl::OkStatus();
      }));

  return DataSlice::Create(
      internal::DataSliceImpl::Create(std::move(result_builder).Build()),
      x.GetShape(), internal::DataItem(schema::kString));
}

}  // namespace koladata::ops
