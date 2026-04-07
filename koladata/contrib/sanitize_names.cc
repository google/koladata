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
#include "koladata/contrib/sanitize_names.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/traverser.h"
#include "koladata/internal/schema_attrs.h"
#include "arolla/util/status_macros_backport.h"  // NOLINT

namespace koladata::contrib {
namespace {

using ::koladata::internal::AbstractVisitor;
using ::koladata::internal::DataBagImpl;
using ::koladata::internal::DataBagImplPtr;
using ::koladata::internal::DataItem;
using ::koladata::internal::DataSliceImpl;
using ::koladata::internal::ObjectId;
using ::koladata::internal::Traverser;

constexpr std::string_view kSanitizePrefix = "san_";

inline bool IsValidChar(const char c) {
  return c == '_' || absl::ascii_isalnum(c);
}

bool IsValid(absl::string_view name) {
  if (name.empty()) return false;
  if (absl::ascii_isdigit(name.front())) return false;
  for (char c : name) {
    if (!IsValidChar(c)) {
      return false;
    }
  }
  return true;
}

std::string SanitizeName(absl::string_view name) {
  if (name.empty()) {
    return std::string{kSanitizePrefix};
  }
  std::string new_name;
  new_name.reserve(name.size());
  bool add_sanitized_prefix = absl::ascii_isdigit(name.front());
  for (char c : name) {
    if (IsValidChar(c)) {
      new_name += c;
    } else {
      new_name += '_';
      add_sanitized_prefix = true;
    }
  }
  if (add_sanitized_prefix) {
    return absl::StrCat(kSanitizePrefix, new_name);
  }
  return new_name;
}

std::vector<int64_t> SortedIndices(const arolla::DenseArray<arolla::Text>& s) {
  // Return indices of s, so that if i < j, s[i] <= s[j].
  std::vector<int64_t> indices(s.size());
  std::iota(indices.begin(), indices.end(), int64_t{0});

  std::sort(indices.begin(), indices.end(), [&](int64_t i, int64_t j) {
    return s[i].value < s[j].value;
  });
  return indices;
}

class SanitizeNamesVisitor final : public AbstractVisitor {
 public:
  explicit SanitizeNamesVisitor(DataBagImplPtr new_databag)
      : new_databag_(std::move(new_databag)) {}

  absl::StatusOr<bool> Previsit(
      const DataItem& from_item, const DataItem& from_schema,
      const std::optional<absl::string_view>& from_item_attr_name,
      const DataItem& item, const DataItem& schema) override {
    // Visit all reachable items, including attributes and schemas.
    return true;
  }

  absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                    const DataItem& schema) override {
    // We do not clone objects or recreate allocation IDs. We simply return the
    // original item, allowing traversal to write to the new databag under the
    // exact same IDs.
    return item;
  }

  absl::Status VisitList(const DataItem& list, const DataItem& schema,
                         bool is_object_schema,
                         const DataSliceImpl& items) override {
    DCHECK(list.holds_value<ObjectId>() && list.value<ObjectId>().IsList());
    if (is_object_schema) {
      RETURN_IF_ERROR(SetSchemaAttr(list, schema));
    }
    // We just write to new databag directly since GetValue returns the same id.
    RETURN_IF_ERROR(new_databag_->ExtendList(list, items));
    return absl::OkStatus();
  }

  absl::Status VisitDict(const DataItem& dict, const DataItem& schema,
                         bool is_object_schema, const DataSliceImpl& keys,
                         const DataSliceImpl& values) override {
    DCHECK(dict.holds_value<ObjectId>() && dict.value<ObjectId>().IsDict());
    DCHECK(keys.size() == values.size());
    if (is_object_schema) {
      RETURN_IF_ERROR(SetSchemaAttr(dict, schema));
    }
    RETURN_IF_ERROR(new_databag_->SetInDict(
        DataSliceImpl::Create(keys.size(), dict), keys, values));
    return absl::OkStatus();
  }

  absl::Status VisitObject(
      const DataItem& object, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_values) override {
    DCHECK(object.holds_value<ObjectId>());
    if (is_object_schema) {
      RETURN_IF_ERROR(SetSchemaAttr(object, schema));
    }
    DCHECK(attr_names.size() == attr_values.size());
    DCHECK(attr_names.IsAllPresent());
    SetAllValidNamesAsUsed(schema, attr_names, attr_values);
    // When we have collisions, we add suffixes. We sort the attr_names so we
    // can assign suffixes deterministically.
    const auto indices = SortedIndices(attr_names);
    for (int64_t i : indices) {
      if (attr_values.present(i)) {
        auto attr_name = attr_names[i].value;
        std::string sanitized_name = Sanitize(schema, attr_name);
        const DataItem& value = attr_values[i].value;
        RETURN_IF_ERROR(new_databag_->SetAttr(object, sanitized_name, value));
      }
    }
    return absl::OkStatus();
  }

  absl::Status VisitSchema(
      const DataItem& item, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_schema) override {
    DCHECK(item.holds_value<ObjectId>());
    if (is_object_schema) {
      RETURN_IF_ERROR(SetSchemaAttr(item, schema));
    }
    DCHECK(attr_names.size() == attr_schema.size());
    DCHECK(attr_names.IsAllPresent());
    // Use the schema object itself (item) as the sanitization context key,
    // NOT the schema parameter (which is always kSchema for schema
    // objects). This ensures:
    // 1. Each schema object gets independent sanitization.
    // 2. Consistency with VisitObject, where data attrs are sanitized
    //    under the per-object schema ID — which is the same as `item`
    //    here.
    SetAllValidNamesAsUsed(item, attr_names, attr_schema);
    // When we have collisions, we add suffixes. We sort the attr_names so we
    // can assign suffixes deterministically.
    const auto indices = SortedIndices(attr_names);
    for (int64_t i : indices) {
      if (attr_schema.present(i)) {
        auto attr_name = attr_names[i].value;
        std::string sanitized_name = Sanitize(item, attr_name);
        const DataItem& value = attr_schema[i].value;
        RETURN_IF_ERROR(
            new_databag_->SetSchemaAttr(item, sanitized_name, value));
      }
    }
    return absl::OkStatus();
  }

 private:
  absl::Status SetSchemaAttr(const DataItem& item, const DataItem& schema) {
    return new_databag_->SetAttr(item, schema::kSchemaAttr, schema);
    }

  void SetAllValidNamesAsUsed(
      const DataItem& schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_schema) {
    auto& used = per_schema_used_names_[schema];
    auto& mapped = per_schema_mapped_names_[schema];
    attr_names.ForEachPresent(
        [&](int64_t offset, std::string_view attr_name) {
          if (IsValid(attr_name)) {
            used.emplace(attr_name);
            mapped[attr_name] = attr_name;
          }
        });
  }

  // Sanitize a name within the context of a particular schema.
  // Objects sharing the same schema get the same name mapping.
  std::string Sanitize(const DataItem& schema, absl::string_view name) {
    auto& mapped = per_schema_mapped_names_[schema];
    auto& used = per_schema_used_names_[schema];
    if (auto it = mapped.find(name); it != mapped.end()) {
      return it->second;
    }
    DCHECK(!IsValid(name));  // because we called SetAllValidNamesAsUsed.
    std::string candidate = SanitizeName(name);
    std::string result = candidate;
    int counter = 0;
    while (!used.emplace(result).second) {
      result = absl::StrCat(candidate, "_", counter++);
    }
    mapped[name] = result;
    return result;
  }

  DataBagImplPtr new_databag_;
  // Per-schema name mapping to ensure independent collision
  // resolution for each schema.
  absl::flat_hash_map<DataItem, absl::flat_hash_map<std::string, std::string>,
                      DataItem::Hash>
      per_schema_mapped_names_;
  absl::flat_hash_map<DataItem, absl::flat_hash_set<std::string>,
                      DataItem::Hash>
      per_schema_used_names_;
};

}  // namespace

namespace internal {

class SanitizeNamesOp {
 public:
  explicit SanitizeNamesOp(koladata::internal::DataBagImpl* new_databag)
      : new_databag_(new_databag) {}

  absl::StatusOr<koladata::internal::DataSliceImpl> operator()(
      const koladata::internal::DataSliceImpl& ds,
      const koladata::internal::DataItem& schema,
      const koladata::internal::DataBagImpl& databag,
      koladata::internal::DataBagImpl::FallbackSpan fallbacks = {}) const;

  absl::StatusOr<koladata::internal::DataItem> operator()(
      const koladata::internal::DataItem& item,
      const koladata::internal::DataItem& schema,
      const koladata::internal::DataBagImpl& databag,
      koladata::internal::DataBagImpl::FallbackSpan fallbacks = {}) const;

 private:
  koladata::internal::DataBagImpl* new_databag_;
};

absl::StatusOr<DataSliceImpl> SanitizeNamesOp::operator()(
    const DataSliceImpl& ds, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks) const {
  auto visitor = std::make_shared<SanitizeNamesVisitor>(
      DataBagImplPtr::NewRef(new_databag_));
  auto traverse_op =
      Traverser<SanitizeNamesVisitor>(databag, fallbacks, std::move(visitor));
  RETURN_IF_ERROR(traverse_op.TraverseSlice(ds, schema));
  return ds;
}

absl::StatusOr<DataItem> SanitizeNamesOp::operator()(
    const DataItem& item, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks) const {
  ASSIGN_OR_RETURN(auto result_slice,
                   (*this)(DataSliceImpl::Create(/*size=*/1, item), schema,
                           databag, fallbacks));
  DCHECK_EQ(result_slice.size(), 1);
  return result_slice[0];
}

}  // namespace internal

absl::StatusOr<DataSlice> SanitizeNames(const DataSlice& ds) {
  const auto& schema_impl = ds.GetSchemaImpl();
  const auto& db_ptr = ds.GetBag();
  if (db_ptr == nullptr) {
    return absl::InvalidArgumentError(
        "Cannot sanitize names without a DataBag");
  }
  const auto& db = *db_ptr;
  FlattenFallbackFinder fb_finder(db);
  auto fallbacks_span = fb_finder.GetFlattenFallbacks();
  return ds.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    auto result_db = DataBag::EmptyMutable();
    ASSIGN_OR_RETURN(auto& result_db_impl, result_db->GetMutableImpl());
    internal::SanitizeNamesOp sanitize_op(&result_db_impl);
    ASSIGN_OR_RETURN(
        auto result_slice_impl,
        sanitize_op(impl, schema_impl, db.GetImpl(), fallbacks_span));
    result_db->UnsafeMakeImmutable();
    return DataSlice::Create(std::move(result_slice_impl), ds.GetShape(),
                             schema_impl, std::move(result_db));
  });
}

}  // namespace koladata::contrib
