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
#include "koladata/internal/data_bag.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/refcount_ptr.h"
#include "arolla/util/status.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_list.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dense_source.h"
#include "koladata/internal/dict.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/errors.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/presence_or.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/internal/sparse_source.h"
#include "koladata/internal/types_buffer.h"
#include "koladata/internal/uuid_object.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

namespace {

constexpr size_t kSparseSourceSparsityCoef = 64;
ABSL_CONST_INIT const DataList kEmptyList;

using DenseSourceSpan = absl::Span<const DenseSource* const>;
using SparseSourceSpan = absl::Span<const SparseSource* const>;

bool HasTooManyAllocationIds(size_t allocation_id_count, size_t obj_count) {
  static constexpr int64_t kAllowedAllocsForBatchLookup = 5;
  return allocation_id_count > kAllowedAllocsForBatchLookup &&
         // Too many allocations if allocation_id_count > 2 * log2(obj_count).
         (allocation_id_count / 2 >= sizeof(size_t) * 8 ||
          (size_t{1} << (allocation_id_count / 2)) > obj_count);
}

absl::StatusOr<DataSliceImpl> GetAttributeFromSources(
    const DataSliceImpl& slice, DenseSourceSpan dense_sources,
    SparseSourceSpan sparse_sources) {
  if (slice.is_empty_and_unknown()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(slice.size());
  }
  if (slice.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError(
        "Getting attribute from primitive (or mixed) values is not supported");
  }
  const ObjectIdArray& objs = slice.values<ObjectId>();

  SliceBuilder bldr(objs.size());
  bldr.ApplyMask(objs.ToMask());
  absl::Span<const ObjectId> objs_span = objs.values.span();
  // Sparse sources have priority over dense sources.
  for (const SparseSource* source : sparse_sources) {
    if (bldr.is_finalized()) {
      break;
    }
    source->Get(objs_span, bldr);
  }
  for (const DenseSource* source : dense_sources) {
    if (bldr.is_finalized()) {
      break;
    }
    source->Get(objs_span, bldr);
  }
  return std::move(bldr).Build();
}

std::optional<DataItem> GetAttributeFromSources(
    ObjectId id, DenseSourceSpan dense_sources,
    SparseSourceSpan sparse_sources) {
  // There shouldn't be more than one dense source because they override each
  // other.
  DCHECK_LE(dense_sources.size(), 1);

  for (const auto& source : sparse_sources) {
    if (std::optional<DataItem> res = source->Get(id); res.has_value()) {
      return res;
    }
  }
  for (const auto& source : dense_sources) {
    if (std::optional<DataItem> res = source->Get(id); res.has_value()) {
      return res;
    }
  }
  return std::nullopt;
}

absl::StatusOr<ObjectId> ItemToListObjectId(const DataItem& list) {
  if (list.holds_value<ObjectId>()) {
    if (ObjectId list_id = list.value<ObjectId>(); list_id.IsList()) {
      return list_id;
    }
  }
  return absl::FailedPreconditionError(
      absl::StrCat("list expected, got ", list));
}

absl::StatusOr<ObjectId> ItemToDictObjectId(const DataItem& dict) {
  if (dict.holds_value<ObjectId>()) {
    if (ObjectId dict_id = dict.value<ObjectId>(); dict_id.IsDict()) {
      return dict_id;
    }
  }
  return absl::FailedPreconditionError(
      absl::StrCat("dict expected, got ", dict));
}

absl::Status InvalidSchemaObjectId(const DataItem& schema_item) {
  if (schema_item.holds_value<schema::DType>()) {
    return absl::FailedPreconditionError(
        absl::StrCat("cannot get or set attributes on schema: ", schema_item));
  }
  return absl::FailedPreconditionError(
      absl::StrCat("schema expected, got ", schema_item));
}

absl::StatusOr<ObjectId> ItemToSchemaObjectId(const DataItem& schema_item) {
  if (schema_item.holds_value<ObjectId>()) {
    if (ObjectId schema_id = schema_item.value<ObjectId>();
        schema_id.IsSchema()) {
      return schema_id;
    }
  }
  return InvalidSchemaObjectId(schema_item);
}

absl::Status VerifyIsSchema(const DataItem& item) {
  if (item.holds_value<schema::DType>() ||
      (item.holds_value<ObjectId>() && item.value<ObjectId>().IsSchema())) {
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(absl::StrCat(
      "only schemas can be assigned as attributes of schemas, got: ",
      item.DebugString()));
}

// Compares if two values are different for the purposes of merge conflict
// resolution. Here we treat NaN as just another value (different from other
// values, not different from itself). Different flavors of NaN are not
// considered different.
bool ValuesAreDifferent(const DataItem& a, const DataItem& b) {
  return !(a == b || (a.is_nan() && b.is_nan()));
}

// Merges the given sources ordered by priority into a mutable sparse source.
std::shared_ptr<SparseSource> MergeToMutableSparseSource(
    absl::Span<const SparseSource* const> sources) {
  if (sources.empty()) {
    return nullptr;
  }
  // copy top most sparse collection
  auto result = std::make_shared<SparseSource>(*sources.back());
  sources.remove_suffix(1);
  for (auto source_it = sources.rbegin(); source_it != sources.rend();
       ++source_it) {
    for (const auto& [key, item] : (*source_it)->GetAll()) {
      result->Set(key, item);
    }
  }
  return result;
}

// Processes the given sparse sources ordered by priority by calling
// process_source(sources[i], skip_already_used_object_fn).
template <class ProcessFn>
absl::Status ProcessSparseSources(
    ProcessFn& process_source, absl::Span<const SparseSource* const> sources) {
  if (sources.empty()) {
    return absl::OkStatus();
  }
  RETURN_IF_ERROR(process_source(sources[0], [](ObjectId) { return false; }));
  if (sources.size() > 1) {
    RETURN_IF_ERROR(process_source(sources[1], [&sources](ObjectId id) {
      return sources[0]->Get(id).has_value();
    }));
  }
  if (sources.size() > 2) {
    absl::flat_hash_set<ObjectId> seen_ids;
    for (const auto& [key, _] : sources[0]->GetAll()) {
      seen_ids.insert(key);
    }
    for (int64_t i = 2; i < sources.size(); ++i) {
      for (const auto& [key, _] : sources[i - 1]->GetAll()) {
        seen_ids.insert(key);
      }
      RETURN_IF_ERROR(process_source(sources[i], [&seen_ids](ObjectId id) {
        return seen_ids.contains(id);
      }));
    }
  }
  return absl::OkStatus();
}

internal::DataBagMergeConflictError MakeEntityOrObjectMergeError(
    const internal::ObjectId& object, absl::string_view attr) {
  return {.conflict = internal::DataBagMergeConflictError::EntityObjectConflict{
              .object_id = internal::DataItem(object),
              .attr_name = std::string(attr),
          }};
}

// Merges the given sparse sources ordered by priority into a mutable dense
// source.
// Conflicts are resolved according to MergeOptions.
absl::Status MergeToMutableDenseSourceOnlySparse(
    DenseSource& result, absl::Span<const SparseSource* const> sources,
    MergeOptions options, absl::string_view attr) {
  DCHECK(result.IsMutable());
  auto process_source = [&](const SparseSource* source,
                            auto skip_obj_fn) -> absl::Status {
    for (const auto& [key, item] : source->GetAll()) {
      if (skip_obj_fn(key)) {
        continue;
      }
      if (options.data_conflict_policy == MergeOptions::kOverwrite) {
        RETURN_IF_ERROR(result.Set(key, item));
        continue;
      }
      if (auto this_result = result.Get(key); !this_result.has_value()) {
        RETURN_IF_ERROR(result.Set(key, item));
      } else if (options.data_conflict_policy ==
                     MergeOptions::kRaiseOnConflict &&
                 ValuesAreDifferent(*this_result, item)) {
        return arolla::WithPayload(
            absl::FailedPreconditionError(absl::StrCat(
                "conflict ", key, ": ", *this_result, " vs ", item)),
            MakeEntityOrObjectMergeError(key, attr));
      }
    }
    return absl::OkStatus();
  };
  return ProcessSparseSources(process_source, sources);
}

// Merges the given sparse sources ordered by priority into a mutable sparse
// source.
// Conflicts are resolved according to MergeOptions.
absl::Status MergeToMutableSparseSourceOnlySparse(
    SparseSource& result, absl::Span<const SparseSource* const> sources,
    MergeOptions options, absl::string_view attr) {
  auto process_source = [&](const SparseSource* source,
                            auto skip_obj_fn) -> absl::Status {
    for (const auto& [key, item] : source->GetAll()) {
      if (!item.has_value() || skip_obj_fn(key)) {
        continue;
      }
      if (options.data_conflict_policy == MergeOptions::kOverwrite) {
        result.Set(key, item);
        continue;
      }
      if (std::optional<DataItem> this_result = result.Get(key);
          !this_result.has_value() || !this_result->has_value()) {
        result.Set(key, item);
      } else if (options.data_conflict_policy ==
                     MergeOptions::kRaiseOnConflict &&
                 ValuesAreDifferent(*this_result, item)) {
        return arolla::WithPayload(
            absl::FailedPreconditionError(absl::StrCat(
                "conflict ", key, ": ", *this_result, " vs ", item)),
            MakeEntityOrObjectMergeError(key, attr));
      }
    }
    return absl::OkStatus();
  };
  return ProcessSparseSources(process_source, sources);
}

// Merges the given sources ordered by priority into a mutable dense source.
// SparseSources always overwrite DenseSource.
// Conflicts are resolved according to MergeOptions.
absl::Status MergeToMutableDenseSource(
    DenseSource& result, AllocationId alloc, const DenseSource& dense_source,
    absl::Span<const SparseSource* const> sparse_sources, MergeOptions options,
    absl::string_view attr) {
  DCHECK(result.IsMutable());
  int64_t size = std::min<int64_t>(result.size(), dense_source.size());
  if (sparse_sources.empty()) {
    DCHECK_EQ(dense_source.allocation_id(), alloc);
    internal::DataBagMergeConflictError error;
    absl::Status status = result.Merge(dense_source,
                     {.option = options.data_conflict_policy,
                      .on_conflict_callback =
                          [attr, &error](const ObjectId& obj_id) mutable {
                            error = MakeEntityOrObjectMergeError(obj_id, attr);
                          }});
    return arolla::WithPayload(std::move(status), std::move(error));
  }

  auto objects = DataSliceImpl::ObjectsFromAllocation(alloc, size);
  ASSIGN_OR_RETURN(
      auto other_items,
      GetAttributeFromSources(objects, {&dense_source}, sparse_sources));
  // GetAttributeFromSources returns information about unset values in
  // TypesBuffer withing DataSliceImpl. Ensure that TypesBuffer is there.
  DCHECK_GT(size, 0);
  DCHECK(!other_items.types_buffer().id_to_typeidx.empty());

  if (options.data_conflict_policy == MergeOptions::kOverwrite) {
    const auto& objects_array = objects.values<ObjectId>();
    return result.Set(
        arolla::DenseArray<ObjectId>{
            objects_array.values,
            other_items.types_buffer().ToInvertedBitmap(TypesBuffer::kUnset)},
        other_items);
  }

  for (int64_t offset = 0; offset < size; ++offset) {
    // TODO: try to use batch `GetAttributeFromSources`.
    if (other_items.types_buffer().id_to_typeidx[offset] ==
        TypesBuffer::kUnset) {
      continue;
    }
    auto other_item = other_items[offset];
    auto obj_id = alloc.ObjectByOffset(offset);
    if (auto this_result = result.Get(obj_id); !this_result.has_value()) {
      RETURN_IF_ERROR(result.Set(obj_id, other_item));
    } else {
      if (options.data_conflict_policy == MergeOptions::kRaiseOnConflict &&
          ValuesAreDifferent(*this_result, other_item)) {
        return arolla::WithPayload(
            absl::FailedPreconditionError(absl::StrCat(
                "conflict ", obj_id, ": ", *this_result, " vs ", other_item)),
            MakeEntityOrObjectMergeError(obj_id, attr));
      }
    }
  }
  return absl::OkStatus();
}

MergeOptions::ConflictHandlingOption ReverseConflictHandlingOption(
    const MergeOptions::ConflictHandlingOption& option) {
  switch (option) {
    case MergeOptions::kRaiseOnConflict:
      return MergeOptions::kRaiseOnConflict;
    case MergeOptions::kOverwrite:
      return MergeOptions::kKeepOriginal;
    case MergeOptions::kKeepOriginal:
      return MergeOptions::kOverwrite;
  }
  LOG(FATAL) << "Invalid enum value encountered: " << static_cast<int>(option);
  return MergeOptions::kRaiseOnConflict;
}

internal::DataBagMergeConflictError MakeSchemaOrDictMergeError(
    const internal::ObjectId& object, const DataItem& key,
    const DataItem& expected_value, const DataItem& assigned_value) {
  return {.conflict = internal::DataBagMergeConflictError::DictConflict{
              .object_id = internal::DataItem(object),
              .key = key,
              .expected_value = expected_value,
              .assigned_value = assigned_value,
          }};
}

internal::DataBagMergeConflictError MakeListSizeMergeError(
    const internal::ObjectId& list_object_id, const int64_t first_list_size,
    const int64_t second_list_size) {
  return {.conflict = internal::DataBagMergeConflictError::ListSizeConflict{
              .list_object_id = internal::DataItem(list_object_id),
              .first_list_size = first_list_size,
              .second_list_size = second_list_size,
          }};
}

internal::DataBagMergeConflictError MakeListItemMergeError(
    const internal::ObjectId& list_object_id,
    const int64_t list_item_conflict_index, const DataItem& first_list_item,
    const DataItem& second_list_item) {
  return {.conflict = internal::DataBagMergeConflictError::ListContentConflict{
              .list_object_id = internal::DataItem(list_object_id),
              .list_item_conflict_index = list_item_conflict_index,
              .first_conflicting_item = first_list_item,
              .second_conflicting_item = second_list_item,
          }};
}
}  // namespace

MergeOptions ReverseMergeOptions(const MergeOptions& options) {
  return MergeOptions{
      .data_conflict_policy =
          ReverseConflictHandlingOption(options.data_conflict_policy),
      .schema_conflict_policy =
          ReverseConflictHandlingOption(options.schema_conflict_policy),
  };
}

void DataBagStatistics::Add(const DataBagStatistics& other) {
  entity_and_object_count +=
        other.entity_and_object_count;
  total_non_empty_lists += other.total_non_empty_lists;
  total_items_in_lists += other.total_items_in_lists;
  total_non_empty_dicts += other.total_non_empty_dicts;
  total_items_in_dicts += other.total_items_in_dicts;
  total_explicit_schemas += other.total_explicit_schemas;
  total_explicit_schema_attrs += other.total_explicit_schema_attrs;
  for (const auto& [attr, size] : other.attr_values_sizes) {
    attr_values_sizes[attr] += size;
  }
}

// *******  Factory interface

/*static*/ DataBagImplPtr DataBagImpl::CreateEmptyDatabag() {
  return DataBagImplPtr::Make(PrivateConstructorTag{});
}

DataBagImplPtr DataBagImpl::PartiallyPersistentFork() const {
  auto res_db = DataBagImpl::CreateEmptyDatabag();
  res_db->parent_data_bag_ =
      IsPristine() ? parent_data_bag_ : DataBagImplConstPtr::NewRef(this);
  return res_db;
}

// *******  Const interface

bool DataBagImpl::IsPristine() const {
  return sources_.empty() && small_alloc_sources_.empty() && lists_.empty() &&
         dicts_.empty();
}

std::optional<DataItem> DataBagImpl::LookupAttrInDataSourcesMap(
    ObjectId object_id, absl::string_view attr) const {
  const DataBagImpl* cur_data_bag = this;
  AllocationId alloc_id(object_id);
  DCHECK(!alloc_id.IsSmall());
  SourceKeyView search_key{alloc_id, attr};
  size_t search_hash = absl::HashOf(search_key);
  while (cur_data_bag != nullptr) {
    if (auto it = cur_data_bag->sources_.find(search_key, search_hash);
        it != cur_data_bag->sources_.end()) {
      const SourceCollection& collection = it->second;
      // mutable source overrides const source if both present.
      if (auto* s = collection.mutable_sparse_source.get(); s != nullptr) {
        if (std::optional<DataItem> res = s->Get(object_id); res.has_value()) {
          return res;
        }
      }
      if (auto* s = collection.mutable_dense_source.get(); s != nullptr) {
        DCHECK_EQ(collection.const_dense_source, nullptr);
        if (std::optional<DataItem> res = s->Get(object_id); res.has_value()) {
          return res;
        }
      }
      if (auto* s = collection.const_dense_source.get(); s != nullptr) {
        if (std::optional<DataItem> res = s->Get(object_id); res.has_value()) {
          return res;
        }
      }
      cur_data_bag = collection.lookup_parent
                         ? cur_data_bag->parent_data_bag_.get()
                         : nullptr;
    } else {
      cur_data_bag = cur_data_bag->parent_data_bag_.get();
    }
  }
  return std::nullopt;
}

SparseSource& DataBagImpl::GetMutableSmallAllocSource(absl::string_view attr) {
  return small_alloc_sources_[attr];
}

absl::StatusOr<DataSliceImpl> DataBagImpl::GetAttrImpl(
    const DataSliceImpl& objects, absl::string_view attr,
    FallbackSpan fallbacks, bool with_removed) const {
  if (objects.is_empty_and_unknown()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(objects.size());
  }
  if (objects.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError(
        "getting attributes of primitives is not allowed");
  }
  const arolla::DenseArray<ObjectId>& objs = objects.values<ObjectId>();
  absl::Span<const ObjectId> objs_span = objs.values.span();

  auto next_fallback = [&fallbacks]() -> const DataBagImpl* {
    if (fallbacks.empty()) {
      return nullptr;
    }
    if (arolla::Cancelled()) [[unlikely]] {
      return nullptr;
    }
    auto res = fallbacks.front();
    fallbacks = fallbacks.subspan(1);
    return res;
  };

  bool has_too_many_allocs = HasTooManyAllocationIds(
      objects.allocation_ids().size(), objs_span.size());

  std::optional<SliceBuilder> bldr;
  if (with_removed || has_too_many_allocs) {
    bldr.emplace(objs.size());
    bldr->ApplyMask(objs.ToMask());
  }

  for (const DataBagImpl* db = this; db != nullptr; db = next_fallback()) {
    ConstSparseSourceArray sparse_sources;
    if (objects.allocation_ids().contains_small_allocation_id()) {
      db->GetSmallAllocDataSources(attr, sparse_sources);
    }
    if (has_too_many_allocs) {
      DCHECK(bldr.has_value());
      // Process small allocations.
      for (const SparseSource* source : sparse_sources) {
        source->Get(objs_span, *bldr);
        if (bldr->is_finalized()) {
          break;
        }
      }
      // Process big allocations.
      for (size_t i = 0; i < objs_span.size(); ++i) {
        if (bldr->IsSet(i)) {
          continue;
        }
        const auto& object_id = objs_span[i];
        if (object_id.IsSmallAlloc()) {
          continue;
        }
        auto item = db->LookupAttrInDataSourcesMap(object_id, attr);
        if (item.has_value()) {
          bldr->InsertIfNotSetAndUpdateAllocIds(i, std::move(*item));
        }
      }
      if (bldr->is_finalized()) {
        break;
      }
      continue;
    }

    ConstDenseSourceArray dense_sources;
    for (AllocationId alloc_id : objects.allocation_ids()) {
      db->GetAttributeDataSources(alloc_id, attr, dense_sources,
                                  sparse_sources);
    }
    if (dense_sources.empty() && sparse_sources.empty()) {
      continue;
    }

    if (!bldr.has_value()) {
      // Performance optimization: get data from a single dense source without
      // creating a builder.
      if (fallbacks.empty() && sparse_sources.empty() &&
          dense_sources.size() == 1) {
        bool check_alloc_id =
          objects.allocation_ids().contains_small_allocation_id() ||
          objects.allocation_ids().ids().size() > 1;
        return dense_sources[0]->Get(objs, check_alloc_id);
      }

      bldr.emplace(objs.size());
      bldr->ApplyMask(objs.ToMask());
    }

    // Sparse sources have priority over dense sources.
    for (const SparseSource* source : sparse_sources) {
      if (bldr->is_finalized()) {
        break;
      }
      source->Get(objs_span, *bldr);
    }
    for (const DenseSource* source : dense_sources) {
      if (bldr->is_finalized()) {
        break;
      }
      source->Get(objs_span, *bldr);
    }
    if (bldr->is_finalized()) {
      break;
    }
  }
  RETURN_IF_ERROR(arolla::CheckCancellation());

  if (bldr.has_value()) {
    return std::move(*bldr).Build();
  } else {
    return DataSliceImpl::CreateEmptyAndUnknownType(objects.size());
  }
}

absl::StatusOr<DataSliceImpl> DataBagImpl::GetAttr(
    const DataSliceImpl& objects, absl::string_view attr,
    FallbackSpan fallbacks) const {
  return GetAttrImpl(objects, attr, fallbacks, /*with_removed=*/false);
}


absl::StatusOr<DataSliceImpl> DataBagImpl::GetAttrWithRemoved(
    const DataSliceImpl& objects, absl::string_view attr,
    FallbackSpan fallbacks) const {
  return GetAttrImpl(objects, attr, fallbacks, /*with_removed=*/true);
}

std::optional<DataItem> DataBagImpl::GetAttrWithRemoved(
    ObjectId object_id, absl::string_view attr, FallbackSpan fallbacks) const {
  auto lookup = [&](auto lookup_one_bag) -> std::optional<DataItem> {
    std::optional<DataItem> result = lookup_one_bag(*this);
    if (result.has_value()) {
      return std::move(*result);
    }
    for (const DataBagImpl* fallback : fallbacks) {
      if (auto item = lookup_one_bag(*fallback);
          item.has_value()) {
        return std::move(*item);
      }
    }
    return std::nullopt;
  };
  AllocationId alloc_id(object_id);
  if (alloc_id.IsSmall()) {
    return lookup([&](const DataBagImpl& db) {
      return db.LookupAttrInDataItemMap(object_id, attr);
    });
  }
  return lookup([&](const DataBagImpl& db) {
    return db.LookupAttrInDataSourcesMap(object_id, attr);
  });
}

absl::StatusOr<DataItem> DataBagImpl::GetAttr(const DataItem& object,
                                              absl::string_view attr,
                                              FallbackSpan fallbacks) const {
  if (!object.holds_value<ObjectId>()) {
    if (object.has_value()) {
      return absl::FailedPreconditionError(
          "getting attribute of a primitive is not allowed");
    } else {
      return DataItem();
    }
  }
  ObjectId object_id = object.value<ObjectId>();
  return GetAttrWithRemoved(object_id, attr, fallbacks).value_or(DataItem());
}

absl::StatusOr<DataItem> DataBagImpl::GetObjSchemaAttr(
    const DataItem& item, FallbackSpan fallbacks) const {
  ASSIGN_OR_RETURN(auto schema, GetAttr(item, schema::kSchemaAttr, fallbacks));
  if (schema.has_value() || !item.has_value()) {
    return schema;
  }

  return arolla::WithPayload(
      absl::InvalidArgumentError(
          absl::StrFormat("object %v is missing __schema__ attribute", item)),
      internal::MissingObjectSchemaError{.missing_schema_item = item});
}

absl::StatusOr<DataSliceImpl> DataBagImpl::GetObjSchemaAttr(
    const DataSliceImpl& slice, FallbackSpan fallbacks) const {
  ASSIGN_OR_RETURN(auto schema, GetAttr(slice, schema::kSchemaAttr, fallbacks));
  if (schema.present_count() >= slice.present_count()) {
    return schema;
  }

  internal::DataItem item_missing_schema;
  RETURN_IF_ERROR(arolla::DenseArraysForEach(
      [&](int64_t id, bool valid, internal::DataItem item,
          arolla::OptionalValue<internal::DataItem> attr) {
        if (!item_missing_schema.has_value() && valid && item.has_value() &&
            !attr.present) {
          item_missing_schema = item;
        }
      },
      slice.AsDataItemDenseArray(), schema.AsDataItemDenseArray()));

  return arolla::WithPayload(
      absl::InvalidArgumentError(
          absl::StrFormat("object %v is missing __schema__ attribute", slice)),
      internal::MissingObjectSchemaError{.missing_schema_item =
                                             std::move(item_missing_schema)});
}

void DataBagImpl::GetSmallAllocDataSources(
    absl::string_view attr, ConstSparseSourceArray& res_sources) const {
  for (const DataBagImpl* db = this; db != nullptr;
       db = db->parent_data_bag_.get()) {
    if (auto it = db->small_alloc_sources_.find(attr);
        it != db->small_alloc_sources_.end()) {
      res_sources.push_back(&it->second);
    }
  }
}

int64_t DataBagImpl::GetAttributeDataSources(
    AllocationId alloc, absl::string_view attr,
    ConstDenseSourceArray& dense_sources,
    ConstSparseSourceArray& sparse_sources) const {
  int64_t size = alloc.Capacity();
  if (alloc.IsSmall()) {
    GetSmallAllocDataSources(attr, sparse_sources);
    return size;
  }
  const DataBagImpl* cur_data_bag = this;
  SourceKeyView search_key{alloc, attr};
  size_t search_hash = absl::HashOf(search_key);
  while (cur_data_bag != nullptr) {
    if (auto it = cur_data_bag->sources_.find(search_key, search_hash);
        it != cur_data_bag->sources_.end()) {
      const SourceCollection& collection = it->second;
      cur_data_bag = collection.lookup_parent
                         ? cur_data_bag->parent_data_bag_.get()
                         : nullptr;
      // mutable source overriding const source if both present.
      if (auto* s = collection.mutable_sparse_source.get(); s != nullptr) {
        sparse_sources.push_back(s);
      }
      if (auto* s = collection.mutable_dense_source.get(); s != nullptr) {
        DCHECK_EQ(collection.const_dense_source, nullptr);
        size = s->size();
        dense_sources.push_back(s);
        // Any dense source overrides all values for this alloc, so we shouldn't
        // look to the parent.
        break;
      }
      if (auto* s = collection.const_dense_source.get(); s != nullptr) {
        size = s->size();
        dense_sources.push_back(s);
        break;
      }
    } else {
      cur_data_bag = cur_data_bag->parent_data_bag_.get();
    }
  }
  return size;
}

// *******  Mutable interface

absl::Status DataBagImpl::GetOrCreateMutableSourceInCollection(
    SourceCollection& collection, AllocationId alloc_id, absl::string_view attr,
    const arolla::QType* qtype, size_t update_size) {
  if (!collection.mutable_dense_source) {
    if (update_size <= alloc_id.Capacity() / kSparseSourceSparsityCoef) {
      if (!collection.mutable_sparse_source) {
        collection.mutable_sparse_source =
            std::make_shared<SparseSource>(alloc_id);
      }
      return absl::OkStatus();
    }
    RETURN_IF_ERROR(CreateMutableDenseSource(collection, alloc_id, attr, qtype,
                                             alloc_id.Capacity()));
  }
  return absl::OkStatus();
}

absl::StatusOr<DataSliceImpl>
DataBagImpl::InternalSetUnitAttrAndReturnMissingObjects(
    const DataSliceImpl& objects, absl::string_view attr) {
  if (objects.is_empty_and_unknown()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(0);
  }
  if (objects.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError(
        "getting unique objects out of primitives is not allowed");
  }
  if (parent_data_bag_ != nullptr) {
    return absl::FailedPreconditionError(
        "getting unique objects out of a DataBag with parent is not allowed");
  }
  std::vector<ObjectId> missing_objects;
  missing_objects.reserve(objects.size());
  AllocationIdSet missing_objects_alloc_ids;

  if (objects.allocation_ids().contains_small_allocation_id()) {
    const auto missing_objects_sz = missing_objects.size();
    auto& source = GetMutableSmallAllocSource(attr);
    RETURN_IF_ERROR(source.SetUnitAndUpdateMissingObjects(
        objects.values<ObjectId>(), missing_objects));
    if (missing_objects.size() != missing_objects_sz) {
      missing_objects_alloc_ids.InsertSmallAllocationId();
    }
  }

  auto process_allocation = [&](const ObjectIdArray& objs,
                                AllocationId alloc_id) -> absl::Status {
    absl::Cleanup complete_processing_allocation =
        [&, missing_objects_sz = missing_objects.size()] {
          if (missing_objects.size() != missing_objects_sz) {
            missing_objects_alloc_ids.Insert(alloc_id);
          }
        };
    SourceCollection& collection = GetOrCreateSourceCollection(alloc_id, attr);
    if (collection.const_dense_source) {
      return absl::InvalidArgumentError("const source is not supported");
    }
    RETURN_IF_ERROR(GetOrCreateMutableSourceInCollection(
        collection, alloc_id, attr, arolla::GetQType<arolla::Unit>(),
        /*update_size=*/objs.size()));
    if (collection.mutable_dense_source) {
      return collection.mutable_dense_source->SetUnitAndUpdateMissingObjects(
          objs, missing_objects);
    }
    DCHECK(collection.mutable_sparse_source);
    return collection.mutable_sparse_source->SetUnitAndUpdateMissingObjects(
        objs, missing_objects);
  };

  const auto& objects_array = objects.values<ObjectId>();
  if (HasTooManyAllocationIds(objects.allocation_ids().size(),
                              objects.size())) {
    auto status = absl::OkStatus();
    std::vector<ObjectId> to_process_vec(1);
    ObjectIdArray to_process_array{
        arolla::Buffer<ObjectId>{nullptr, absl::MakeConstSpan(to_process_vec)}};
    objects_array.ForEachPresent([&](int64_t i, ObjectId obj) {
      if (!status.ok() || obj.IsSmallAlloc()) {
        return;
      }
      to_process_vec[0] = obj;
      status = process_allocation(to_process_array, AllocationId(obj));
    });
    RETURN_IF_ERROR(std::move(status));
  } else {
    for (AllocationId alloc_id : objects.allocation_ids()) {
      RETURN_IF_ERROR(process_allocation(objects_array, alloc_id));
    }
  }
  if (missing_objects.size() == objects.size()) {
    return objects;
  }
  return DataSliceImpl::CreateObjectsDataSlice(
      arolla::DenseArray<ObjectId>{
          arolla::Buffer<ObjectId>::Create(std::move(missing_objects))},
      missing_objects_alloc_ids);
}

absl::StatusOr<DataSliceImpl> DataBagImpl::CreateObjectsFromFields(
    absl::Span<const absl::string_view> attr_names,
    const std::vector<std::reference_wrapper<const DataSliceImpl>>& slices) {
  DCHECK_EQ(attr_names.size(), slices.size());
  int64_t ds_size = -1;
  for (const auto& value : slices) {
    if (ds_size == -1) {
      ds_size = value.get().size();
    } else {
      if (value.get().size() != ds_size) {
        return absl::FailedPreconditionError(
            "Size mismatch in CreateObjectInplace");
      }
    }
  }
  if (ds_size == 0 || ds_size == -1) {
    return DataSliceImpl::CreateEmptyAndUnknownType(0);
  }
  AllocationId alloc_id = Allocate(ds_size);
  auto objects = DataSliceImpl::ObjectsFromAllocation(alloc_id, ds_size);
  if (alloc_id.IsSmall()) {
    for (int i = 0; i < attr_names.size(); ++i) {
      RETURN_IF_ERROR(SetAttr(objects, attr_names[i], slices[i].get()));
    }
    return objects;
  }
  for (int i = 0; i < attr_names.size(); ++i) {
    std::shared_ptr<DenseSource> source = nullptr;
    if (!slices[i].get().is_empty_and_unknown()) {
      ASSIGN_OR_RETURN(
          source, DenseSource::CreateReadonly(alloc_id, slices[i]));
    } else {
      ASSIGN_OR_RETURN(source, DenseSource::CreateAllRemoved(alloc_id));
    }
    sources_.emplace(SourceKey{alloc_id, std::string(attr_names[i])},
                     SourceCollection{.const_dense_source = std::move(source),
                                      .lookup_parent = false});
  }
  return objects;
}

absl::StatusOr<DataItem> DataBagImpl::CreateObjectsFromFields(
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> items) {
  DCHECK_EQ(attr_names.size(), items.size());
  ObjectId object_id = AllocateSingleObject();
  for (int i = 0; i < attr_names.size(); ++i) {
    auto& source = GetMutableSmallAllocSource(attr_names[i]);
    source.Set(object_id, items[i].get());
  }
  return DataItem(object_id);
}

absl::Status DataBagImpl::CreateMutableDenseSource(
    SourceCollection& collection, AllocationId alloc_id, absl::string_view attr,
    const arolla::QType* qtype, int64_t size) const {
  DCHECK_EQ(collection.mutable_dense_source, nullptr);
  if (collection.const_dense_source) {
    collection.mutable_dense_source =
        collection.const_dense_source->CreateMutableCopy();
    DCHECK_EQ(collection.lookup_parent, false);
    collection.const_dense_source = nullptr;
    if (collection.mutable_sparse_source == nullptr) {
      return absl::OkStatus();
    }
  }
  ConstDenseSourceArray dense_sources;
  ConstSparseSourceArray sparse_sources;
  if (collection.mutable_sparse_source) {
    sparse_sources.push_back(collection.mutable_sparse_source.get());
  }
  if (collection.lookup_parent) {
    parent_data_bag_->GetAttributeDataSources(alloc_id, attr, dense_sources,
                                              sparse_sources);
    collection.lookup_parent = false;
  }
  if (!dense_sources.empty()) {
    // there can not be more than one DenseSource for a single alloc_id
    DCHECK_EQ(dense_sources.size(), 1);
    collection.mutable_dense_source =
        dense_sources.front()->CreateMutableCopy();
  }
  if (!collection.mutable_dense_source) {
    ASSIGN_OR_RETURN(collection.mutable_dense_source,
                     DenseSource::CreateMutable(alloc_id, size, qtype));
  }

  if (!sparse_sources.empty()) {
    std::unique_ptr<absl::flat_hash_map<ObjectId, DataItem>> mutable_data;
    const absl::flat_hash_map<ObjectId, DataItem>* data = nullptr;
    if (sparse_sources.size() == 1) {
      data = &sparse_sources.front()->GetAll();
    } else {
      mutable_data = std::make_unique<absl::flat_hash_map<ObjectId, DataItem>>(
          sparse_sources.front()->GetAll());
      for (int i = 1; i < sparse_sources.size(); ++i) {
        for (const auto& [obj, data_item] : sparse_sources[i]->GetAll()) {
          mutable_data->try_emplace(obj, data_item);
        }
      }
      data = mutable_data.get();
    }
    arolla::Buffer<ObjectId>::Builder objs_bldr(data->size());
    int64_t offset = 0;
    SliceBuilder slice_bldr(data->size());
    for (const auto& [obj, data_item] : *data) {
      objs_bldr.Set(offset, obj);
      slice_bldr.InsertIfNotSetAndUpdateAllocIds(offset, data_item);
      offset++;
    }
    RETURN_IF_ERROR(collection.mutable_dense_source->Set(
        ObjectIdArray{std::move(objs_bldr).Build()},
        std::move(slice_bldr).Build()));
  }

  collection.mutable_sparse_source = nullptr;
  return absl::OkStatus();
}

DataBagImpl::SourceCollection& DataBagImpl::GetOrCreateSourceCollection(
    AllocationId alloc_id, absl::string_view attr) {
  auto [it, _] = sources_.emplace(
      SourceKeyView{alloc_id, attr},
      SourceCollection{.lookup_parent = parent_data_bag_ != nullptr});
  return it->second;
}

absl::Status DataBagImpl::SetAttr(const DataSliceImpl& objects,
                                  absl::string_view attr,
                                  const DataSliceImpl& values) {
  if (objects.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  if (objects.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError(
        "setting attribute of primitives is not allowed");
  }
  if (objects.allocation_ids().empty() &&
      !objects.allocation_ids().contains_small_allocation_id()) {
    return absl::OkStatus();
  }
  const auto& objects_array = objects.values<ObjectId>();
  if (objects.allocation_ids().contains_small_allocation_id()) {
    auto& source = GetMutableSmallAllocSource(attr);
    RETURN_IF_ERROR(source.Set(objects_array, values));
  }

  // Process big allocations.

  if (HasTooManyAllocationIds(objects.allocation_ids().size(), values.size())) {
    auto status = absl::OkStatus();
    objects_array.ForEachPresent([&, this](int64_t i, ObjectId obj) {
      if (!status.ok() || obj.IsSmallAlloc()) {
        return;
      }
      status = this->SetAttr(DataItem(obj), attr, values[i]);
    });
    return status;
  }

  for (AllocationId alloc_id : objects.allocation_ids()) {
    SourceCollection& collection = GetOrCreateSourceCollection(alloc_id, attr);
    const arolla::QType* qtype =
        values.dtype() == arolla::GetNothingQType() ? nullptr : values.dtype();
    RETURN_IF_ERROR(GetOrCreateMutableSourceInCollection(
        collection, alloc_id, attr, qtype, /*update_size=*/objects.size()));
    if (collection.mutable_dense_source) {
      RETURN_IF_ERROR(
          collection.mutable_dense_source->Set(objects_array, values));
      continue;
    }
    RETURN_IF_ERROR(
        collection.mutable_sparse_source->Set(objects_array, values));
  }
  return absl::OkStatus();
}

absl::Status DataBagImpl::SetAttr(const DataItem& object,
                                  absl::string_view attr, DataItem value) {
  if (!object.holds_value<ObjectId>()) {
    if (object.has_value()) {
      return absl::FailedPreconditionError(
          "setting attribute of a primitive is not allowed");
    } else {
      return absl::OkStatus();
    }
  }
  ObjectId object_id = object.value<ObjectId>();
  if (object_id.IsSmallAlloc()) {
    auto& source = GetMutableSmallAllocSource(attr);
    source.Set(object_id, value);
    return absl::OkStatus();
  }
  AllocationId alloc_id(object_id);
  SourceCollection& collection = GetOrCreateSourceCollection(alloc_id, attr);
  const arolla::QType* qtype = value.has_value() ? value.dtype() : nullptr;
  RETURN_IF_ERROR(GetOrCreateMutableSourceInCollection(
      collection, alloc_id, attr, qtype, /*update_size=*/1));
  if (collection.mutable_dense_source) {
    return collection.mutable_dense_source->Set(object_id, value);
  }
  collection.mutable_sparse_source->Set(object_id, value);
  return absl::OkStatus();
}

// ************************* List functions

class DataBagImpl::ReadOnlyListGetter {
 public:
  explicit ReadOnlyListGetter(const DataBagImpl* bag) : bag_(bag) {}

  // Returns nullptr if the list is UNSET.
  const DataList* absl_nullable operator()(ObjectId list_id) {
    AllocationId alloc_id(list_id);
    if (alloc_id != current_alloc_) {
      if (ABSL_PREDICT_FALSE(!alloc_id.IsListsAlloc())) {
        status_ = absl::FailedPreconditionError("lists expected");
        return nullptr;
      }
      lists_vec_ = bag_->GetConstListsOrNull(alloc_id);
      current_alloc_ = alloc_id;
    }
    return lists_vec_ ? (*lists_vec_)->Get(list_id.Offset()) : nullptr;
  }

  const absl::Status& status() { return status_; }

 private:
  const DataBagImpl* bag_ = nullptr;
  absl::Status status_ = absl::OkStatus();
  std::optional<AllocationId> current_alloc_;
  const std::shared_ptr<DataListVector>* lists_vec_ = nullptr;
};

class DataBagImpl::MutableListGetter {
 public:
  explicit MutableListGetter(DataBagImpl* bag) : bag_(bag) {}

  DataList* operator()(ObjectId list_id) {
    AllocationId alloc_id(list_id);
    if (alloc_id != current_alloc_) {
      if (!alloc_id.IsListsAlloc()) {
        status_ = absl::FailedPreconditionError("lists expected");
        return nullptr;
      }
      lists_vec_ = &bag_->GetOrCreateMutableLists(alloc_id);
      current_alloc_ = alloc_id;
    }
    return &lists_vec_->GetMutable(list_id.Offset());
  }

  const absl::Status& status() { return status_; }

 private:
  DataBagImpl* bag_ = nullptr;
  absl::Status status_ = absl::OkStatus();
  std::optional<AllocationId> current_alloc_;
  DataListVector* lists_vec_ = nullptr;
};

const std::shared_ptr<DataListVector>* DataBagImpl::GetConstListsOrNull(
    AllocationId alloc_id) const {
  return GetConstListsOrNull(alloc_id, absl::HashOf(alloc_id));
}

const std::shared_ptr<DataListVector>* DataBagImpl::GetConstListsOrNull(
    AllocationId alloc_id, size_t alloc_hash) const {
  DCHECK(alloc_id.IsListsAlloc());
  DCHECK_EQ(alloc_hash, absl::HashOf(alloc_id));
  const DataBagImpl* bag = this;
  while (bag) {
    auto it = bag->lists_.find(alloc_id);
    if (it != bag->lists_.end()) {
      return &it->second;
    }
    bag = bag->parent_data_bag_.get();
  }
  return nullptr;
}

const DataList& DataBagImpl::GetFirstPresentList(ObjectId list_id,
                                                 FallbackSpan fallbacks) const {
  const DataList* result = nullptr;

  AllocationId alloc_id(list_id);
  size_t alloc_hash = absl::HashOf(alloc_id);

  if (const auto* lists = GetConstListsOrNull(alloc_id, alloc_hash);
      lists != nullptr) {
    result = (*lists)->Get(list_id.Offset());
  }

  if (result) {
    return *result;
  }
  if (fallbacks.empty()) {
    return kEmptyList;
  }

  for (const DataBagImpl* fallback : fallbacks) {
    if (const auto* lists = fallback->GetConstListsOrNull(alloc_id, alloc_hash);
        lists != nullptr) {
      result = (*lists)->Get(list_id.Offset());
      if (result) {
        break;
      }
    }
  }

  return result ? *result : kEmptyList;
}

std::vector<DataBagImpl::ReadOnlyListGetter>
DataBagImpl::CreateFallbackListGetters(FallbackSpan fallbacks) {
  std::vector<ReadOnlyListGetter> fallback_list_getters;
  fallback_list_getters.reserve(fallbacks.size());
  for (const DataBagImpl* fallback : fallbacks) {
    fallback_list_getters.emplace_back(fallback);
  }
  return fallback_list_getters;
}

const DataList& DataBagImpl::GetFirstPresentList(
    ObjectId list_id, ReadOnlyListGetter& list_getter,
    absl::Span<ReadOnlyListGetter> fallback_list_getters) const {
  if (const DataList* list = list_getter(list_id); list != nullptr) {
    return *list;
  }

  for (ReadOnlyListGetter& fallback_list_getter : fallback_list_getters) {
    if (const DataList* list = fallback_list_getter(list_id); list != nullptr) {
      return *list;
    }
  }

  return kEmptyList;
}

DataListVector& DataBagImpl::GetOrCreateMutableLists(AllocationId alloc_id) {
  DCHECK(alloc_id.IsListsAlloc());
  auto [it, inserted] = lists_.try_emplace(alloc_id);
  if (inserted) {
    const std::shared_ptr<DataListVector>* parent_lists = nullptr;
    if (parent_data_bag_ != nullptr) {
      parent_lists = parent_data_bag_->GetConstListsOrNull(alloc_id);
    }
    if (parent_lists) {
      // Create new DataListVector using *parent_lists (which is shared_ptr)
      // as a parent.
      it->second = std::make_shared<DataListVector>(*parent_lists);
    } else {
      it->second = std::make_shared<DataListVector>(alloc_id.Capacity());
    }
  }
  return *it->second;
}

absl::StatusOr<arolla::DenseArray<int64_t>> DataBagImpl::GetListSize(
    const DataSliceImpl& lists, FallbackSpan fallbacks,
    bool return_zero_for_unset) const {
  if (lists.is_empty_and_unknown()) {
    return arolla::CreateEmptyDenseArray<int64_t>(lists.size());
  }
  if (lists.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("lists expected");
  }

  ReadOnlyListGetter list_getter(this);
  arolla::OptionalValue<int64_t> value_for_missing =
      return_zero_for_unset ? 0 : arolla::OptionalValue<int64_t>();

  arolla::DenseArray<int64_t> res;

  if (fallbacks.empty()) {
    auto op = arolla::CreateDenseOp(
        [&](ObjectId list_id) -> arolla::OptionalValue<int64_t> {
          const auto* l = list_getter(list_id);
          return l ? l->size() : value_for_missing;
        });
    res = op(lists.values<ObjectId>());
  } else {
    std::vector<ReadOnlyListGetter> fallback_list_getters =
        DataBagImpl::CreateFallbackListGetters(fallbacks);
    auto op = arolla::CreateDenseOp(
        [&](ObjectId list_id) -> arolla::OptionalValue<int64_t> {
          const DataList& list = GetFirstPresentList(
              list_id, list_getter, absl::MakeSpan(fallback_list_getters));
          return (&list == &kEmptyList) ? value_for_missing : list.size();
        });
    res = op(lists.values<ObjectId>());
  }

  RETURN_IF_ERROR(list_getter.status());
  return res;
}

absl::StatusOr<DataSliceImpl> DataBagImpl::GetFromLists(
    const DataSliceImpl& lists, const arolla::DenseArray<int64_t>& indices,
    FallbackSpan fallbacks) const {
  if (lists.is_empty_and_unknown()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(lists.size());
  }
  if (lists.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("lists expected");
  }

  ReadOnlyListGetter list_getter(this);
  SliceBuilder bldr(lists.size());

  auto set_from_list = [&](int64_t offset, const DataList& list, int64_t pos) {
    if (pos < 0) {
      pos += list.size();
    }
    if (pos >= 0 && pos < list.size()) {
      bldr.InsertIfNotSetAndUpdateAllocIds(offset, list.Get(pos));
    }
    // Note: we don't return an error if `pos` is out of range.
  };

  if (fallbacks.empty()) {
    RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
        [&](int64_t offset, ObjectId list_id, int64_t pos) {
          if (const DataList* list = list_getter(list_id); list) {
            set_from_list(offset, *list, pos);
          }
        },
        lists.values<ObjectId>(), indices));
  } else {
    std::vector<ReadOnlyListGetter> fallback_list_getters =
        DataBagImpl::CreateFallbackListGetters(fallbacks);
    RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
        [&](int64_t offset, ObjectId list_id, int64_t pos) {
          const DataList& list = GetFirstPresentList(
              list_id, list_getter, absl::MakeSpan(fallback_list_getters));
          set_from_list(offset, list, pos);
        },
        lists.values<ObjectId>(), indices));
  }

  RETURN_IF_ERROR(list_getter.status());
  return std::move(bldr).Build();
}

absl::StatusOr<DataSliceImpl> DataBagImpl::PopFromLists(
    const DataSliceImpl& lists, const arolla::DenseArray<int64_t>& indices) {
  if (lists.is_empty_and_unknown()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(lists.size());
  }
  if (lists.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("lists expected");
  }

  MutableListGetter list_getter(this);
  SliceBuilder bldr(lists.size());

  RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
      [&](int64_t offset, ObjectId list_id, int64_t pos) {
        DataList* list = list_getter(list_id);
        if (list != nullptr) {
          if (pos < 0) {
            pos += list->size();
          }
          if (pos >= 0 && pos < list->size()) {
            bldr.InsertIfNotSetAndUpdateAllocIds(offset, list->Get(pos));
            list->Remove(pos, 1);
          }
          // Note: we don't return an error if `pos` is out of range.
        }
      },
      lists.values<ObjectId>(), indices));

  RETURN_IF_ERROR(list_getter.status());
  return std::move(bldr).Build();
}

absl::StatusOr<std::pair<DataSliceImpl, arolla::DenseArrayEdge>>
DataBagImpl::ExplodeLists(const DataSliceImpl& lists, ListRange range,
                          FallbackSpan fallbacks) const {
  if (lists.is_empty_and_unknown()) {
    ASSIGN_OR_RETURN(auto empty_edge, arolla::DenseArrayEdge::FromUniformGroups(
                                          lists.size(), 0));
    return std::make_pair(DataSliceImpl::CreateEmptyAndUnknownType(0),
                          empty_edge);
  }
  if (lists.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("lists expected");
  }

  std::vector<const DataList*> list_ptrs(lists.size(), nullptr);
  arolla::Buffer<int64_t>::Builder split_points_bldr(lists.size() + 1);
  split_points_bldr.Set(0, 0);
  int64_t cum_size = 0;

  ReadOnlyListGetter list_getter(this);
  auto process_list = [&](int64_t offset, const DataList& list) {
    cum_size += std::max<int64_t>(
        0, range.CalculateTo(list.size()) - range.CalculateFrom(list.size()));
    list_ptrs[offset] = &list;
  };

  if (fallbacks.empty()) {
    lists.values<ObjectId>().ForEach(
        [&](int64_t offset, bool present, ObjectId list_id) {
          if (present) {
            const DataList* list = list_getter(list_id);
            if (list) {
              process_list(offset, *list);
            }
          }
          split_points_bldr.Set(offset + 1, cum_size);
        });
  } else {
    std::vector<ReadOnlyListGetter> fallback_list_getters =
        DataBagImpl::CreateFallbackListGetters(fallbacks);

    lists.values<ObjectId>().ForEach(
        [&](int64_t offset, bool present, ObjectId list_id) {
          if (arolla::Cancelled()) [[unlikely]] {
            return;
          }
          if (present) {
            const DataList& list = GetFirstPresentList(
                list_id, list_getter, absl::MakeSpan(fallback_list_getters));
            process_list(offset, list);
          }
          split_points_bldr.Set(offset + 1, cum_size);
        });
  }
  RETURN_IF_ERROR(arolla::CheckCancellation());
  RETURN_IF_ERROR(list_getter.status());

  arolla::Buffer<int64_t> split_points = std::move(split_points_bldr).Build();
  SliceBuilder slice_bldr(cum_size);
  for (int64_t i = 0; i < lists.size(); ++i) {
    if (const DataList* list = list_ptrs[i]; list != nullptr) {
      auto [from, to] = range.Calculate(list->size());
      if (from < to) {
        list->AddToDataSlice(slice_bldr, split_points[i], from, to);
      }
    }
  }

  ASSIGN_OR_RETURN(auto edge, arolla::DenseArrayEdge::FromSplitPoints(
                                  {std::move(split_points)}));
  return std::make_pair(std::move(slice_bldr).Build(), std::move(edge));
}

absl::Status DataBagImpl::ExtendLists(
    const DataSliceImpl& lists, const DataSliceImpl& values,
    const arolla::DenseArrayEdge& values_to_lists) {
  if (lists.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  if (lists.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("lists expected");
  }
  ASSIGN_OR_RETURN(auto splits_edge, values_to_lists.ToSplitPointsEdge());
  absl::Span<const int64_t> split_points =
      splits_edge.edge_values().values.span();

  MutableListGetter list_getter(this);

  lists.values<ObjectId>().ForEachPresent([&](int64_t i, ObjectId list_id) {
    DataList* list = list_getter(list_id);
    if (ABSL_PREDICT_FALSE(list == nullptr)) {
      return;
    }
    int64_t src_pos = split_points[i];
    int64_t dst_pos = list->size();
    int64_t new_values_count = split_points[i + 1] - src_pos;

    list->Resize(list->size() + new_values_count);
    values.VisitValues([&](const auto& values_arr) {
      using T = std::decay_t<decltype(values_arr)>::base_type;
      for (int64_t offset = 0; offset < new_values_count; ++offset) {
        auto opt_v = values_arr[src_pos + offset];
        if (opt_v.present) {
          list->Set(dst_pos + offset, T(opt_v.value));
        }
      }
    });
  });

  return list_getter.status();
}

absl::Status DataBagImpl::ReplaceInLists(
    const DataSliceImpl& lists, ListRange range, const DataSliceImpl& values,
    const arolla::DenseArrayEdge& values_to_lists) {
  DCHECK_EQ(values_to_lists.parent_size(), lists.size());
  DCHECK_EQ(values_to_lists.child_size(), values.size());
  if (lists.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  if (lists.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("lists expected");
  }
  ASSIGN_OR_RETURN(auto splits_edge, values_to_lists.ToSplitPointsEdge());
  absl::Span<const int64_t> split_points =
      splits_edge.edge_values().values.span();

  MutableListGetter list_getter(this);

  auto find_and_process_list = [&](int64_t i, ObjectId list_id,
                                   auto process_list_fn) {
    DataList* list = list_getter(list_id);
    if (ABSL_PREDICT_FALSE(list == nullptr)) {
      return;
    }
    int64_t src_pos = split_points[i];
    int64_t new_values_count = split_points[i + 1] - src_pos;
    int64_t dst_pos = RemoveAndReserveInList(*list, range, new_values_count);
    process_list_fn(*list, dst_pos, src_pos, new_values_count);
  };

  if (ABSL_PREDICT_TRUE(values.is_single_dtype())) {
    values.VisitValues([&]<typename T>(const arolla::DenseArray<T>& arr) {
      auto arr_unowned = arr.MakeUnowned();
      lists.values<ObjectId>().ForEachPresent([&](int64_t i, ObjectId list_id) {
        find_and_process_list(
            i, list_id,
            [&](DataList& list, int64_t dst_pos, int64_t src_pos,
                int64_t count) {
              list.SetN(dst_pos, arr_unowned.Slice(src_pos, count));
            });
      });
    });
    return list_getter.status();
  }

  if (values.present_count() == 0) {
    lists.values<ObjectId>().ForEachPresent([&](int64_t i, ObjectId list_id) {
      find_and_process_list(
          i, list_id,
          [&](DataList& list, int64_t dst_pos, int64_t src_pos, int64_t count) {
            list.SetMissingRange(dst_pos, dst_pos + count);
          });
    });
    return list_getter.status();
  }

  lists.values<ObjectId>().ForEachPresent([&](int64_t i, ObjectId list_id) {
    find_and_process_list(
        i, list_id,
        [&](DataList& list, int64_t dst_pos, int64_t src_pos, int64_t count) {
          for (int64_t offset = 0; offset < count; ++offset) {
            list.Set(dst_pos + offset, values[src_pos + offset]);
          }
        });
  });
  return list_getter.status();
}

absl::Status DataBagImpl::SetInLists(const DataSliceImpl& lists,
                                     const arolla::DenseArray<int64_t>& indices,
                                     const DataSliceImpl& values) {
  if (lists.size() != values.size()) {
    return absl::FailedPreconditionError("lists.size() != values.size()");
  }
  if (lists.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  if (lists.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("lists expected");
  }

  MutableListGetter list_getter(this);

  auto iterate_fn = [&](auto&& set_fn) {
    return arolla::DenseArraysForEachPresent(
        [&](int64_t offset, ObjectId list_id, int64_t pos) {
          DataList* list = list_getter(list_id);
          if (ABSL_PREDICT_FALSE(list == nullptr)) {
            return;
          }
          if (pos < 0) {
            pos += list->size();
          }
          if (pos >= 0 && pos < list->size()) {
            set_fn(*list, pos, offset);
          }
        },
        lists.values<ObjectId>(), indices);
  };

  if (!values.is_single_dtype()) {
    RETURN_IF_ERROR(
        iterate_fn([&](DataList& list, int64_t pos, int64_t offset) {
          list.Set(pos, values[offset]);
        }));
  } else {
    absl::Status status = absl::OkStatus();
    values.VisitValues([&](const auto& values_arr) {
      using T = std::decay_t<decltype(values_arr)>::base_type;
      absl::Status iterate_status =
          iterate_fn([&](DataList& list, int64_t pos, int64_t offset) {
            auto opt_v = values_arr[offset];
            if (opt_v.present) {
              list.Set(pos, T(opt_v.value));
            } else {
              list.SetToMissing(pos);
            }
          });
      if (!iterate_status.ok()) {
        status = iterate_status;
      }
    });
    RETURN_IF_ERROR(status);
  }

  // Note: in case of error the operation is still applied to the lists where it
  // was possible.
  return list_getter.status();
}

absl::Status DataBagImpl::RemoveInList(
    const DataSliceImpl& lists, const arolla::DenseArray<int64_t>& indices) {
  if (lists.size() != indices.size()) {
    return absl::FailedPreconditionError("lists.size() != indices.size()");
  }
  if (lists.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  if (lists.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("lists expected");
  }

  MutableListGetter list_getter(this);
  RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
      [&](int64_t offset, ObjectId list_id, int64_t pos) {
        DataList* list = list_getter(list_id);
        if (ABSL_PREDICT_FALSE(list == nullptr)) {
          return;
        }
        if (pos < 0) {
          pos += list->size();
        }
        if (pos >= 0 && pos < list->size()) {
          list->Remove(pos, 1);
        }
      },
      lists.values<ObjectId>(), indices));

  // Note: in case of error the operation is still applied to the lists where it
  // was possible.
  return list_getter.status();
}

absl::Status DataBagImpl::AppendToList(const DataSliceImpl& lists,
                                       const DataSliceImpl& values) {
  if (lists.size() != values.size()) {
    return absl::FailedPreconditionError("lists.size() != values.size()");
  }
  if (lists.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  if (lists.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("lists expected");
  }

  MutableListGetter list_getter(this);

  auto iterate_fn = [&](auto&& insert_fn) {
    lists.values<ObjectId>().ForEachPresent(
        [&](int64_t offset, ObjectId list_id) {
          if (DataList* list = list_getter(list_id); list != nullptr) {
            insert_fn(*list, offset);
          }
        });
  };

  if (!values.is_single_dtype()) {
    iterate_fn([&](DataList& list, int64_t offset) {
      list.Insert(list.size(), values[offset]);
    });
  } else {
    values.VisitValues([&](const auto& values_arr) {
      using T = std::decay_t<decltype(values_arr)>::base_type;
      iterate_fn([&](DataList& list, int64_t offset) {
        auto opt_v = values_arr[offset];
        if (opt_v.present) {
          list.Insert(list.size(), T(opt_v.value));
        } else {
          list.Insert(list.size(), std::optional<T>());
        }
      });
    });
  }

  // Note: in case of non-list object id present the function returns an error,
  // but the operation is still applied to all object ids that were actually
  // lists.
  return list_getter.status();
}

absl::Status DataBagImpl::RemoveInList(const DataSliceImpl& lists,
                                       ListRange range) {
  if (lists.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  if (lists.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("lists expected");
  }

  MutableListGetter list_getter(this);

  lists.values<ObjectId>().ForEachPresent(
      [&](int64_t offset, ObjectId list_id) {
        DataList* list = list_getter(list_id);
        if (ABSL_PREDICT_FALSE(list == nullptr)) {
          return;
        }
        auto [from, to] = range.Calculate(list->size());
        if (to > from) {
          list->Remove(from, to - from);
        }
      });

  // Note: in case of error (i.e. if some object ids are not lists)
  // the operation is still applied to the lists where it was possible.
  return list_getter.status();
}

absl::StatusOr<DataItem> DataBagImpl::GetListSize(
    const DataItem& list, FallbackSpan fallbacks) const {
  if (!list.has_value()) {
    return DataItem();
  }
  ASSIGN_OR_RETURN(ObjectId list_id, ItemToListObjectId(list));
  return DataItem(
      static_cast<int64_t>(GetFirstPresentList(list_id, fallbacks).size()));
}

absl::StatusOr<DataItem> DataBagImpl::GetFromList(
    const DataItem& list, int64_t index, FallbackSpan fallbacks) const {
  if (!list.has_value()) {
    return DataItem();
  }
  ASSIGN_OR_RETURN(ObjectId list_id, ItemToListObjectId(list));
  const DataList& dlist = GetFirstPresentList(list_id, fallbacks);
  if (index < 0) {
    index += dlist.size();
  }
  if (index >= 0 && index < dlist.size()) {
    return dlist[index];
  }
  return DataItem();
}

absl::StatusOr<DataItem> DataBagImpl::PopFromList(const DataItem& list,
                                                  int64_t index) {
  if (!list.has_value()) {
    return DataItem();
  }
  ASSIGN_OR_RETURN(ObjectId list_id, ItemToListObjectId(list));
  DataList& dlist = GetOrCreateMutableLists(AllocationId(list_id))
                        .GetMutable(list_id.Offset());
  if (index < 0) {
    index += dlist.size();
  }
  if (index >= 0 && index < dlist.size()) {
    DataItem res = dlist[index];
    dlist.Remove(index, 1);
    return res;
  }
  return DataItem();
}

absl::StatusOr<DataSliceImpl> DataBagImpl::ExplodeList(
    const DataItem& list, ListRange range, FallbackSpan fallbacks) const {
  if (!list.has_value()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(0);
  }
  ASSIGN_OR_RETURN(ObjectId list_id, ItemToListObjectId(list));
  const DataList& dlist = GetFirstPresentList(list_id, fallbacks);
  auto [from, to] = range.Calculate(dlist.size());
  if (to <= from) {
    return DataSliceImpl();
  }
  SliceBuilder bldr(to - from);
  dlist.AddToDataSlice(bldr, 0, from, to);
  return std::move(bldr).Build();
}

absl::Status DataBagImpl::SetInList(const DataItem& list, int64_t index,
                                    DataItem value) {
  if (!list.has_value()) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(ObjectId list_id, ItemToListObjectId(list));
  DataList& dlist = GetOrCreateMutableLists(AllocationId(list_id))
                        .GetMutable(list_id.Offset());
  if (index < 0) {
    index += dlist.size();
  }
  if (index >= 0 && index < dlist.size()) {
    dlist.Set(index, std::move(value));
  }
  return absl::OkStatus();
}

absl::Status DataBagImpl::AppendToList(const DataItem& list, DataItem value) {
  if (!list.has_value()) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(ObjectId list_id, ItemToListObjectId(list));
  DataList& dlist = GetOrCreateMutableLists(AllocationId(list_id))
                        .GetMutable(list_id.Offset());
  dlist.Insert(dlist.size(), std::move(value));
  return absl::OkStatus();
}

absl::Status DataBagImpl::ExtendList(const DataItem& list,
                                     const DataSliceImpl& values) {
  if (!list.has_value()) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(ObjectId list_id, ItemToListObjectId(list));

  // `GetMutable` triggers creation of the list if it was UNSET.
  // We do it even if `values.size() == 0`.
  DataList& dlist = GetOrCreateMutableLists(AllocationId(list_id))
                        .GetMutable(list_id.Offset());

  if (values.size() == 0) {
    return absl::OkStatus();
  }
  size_t offset = dlist.size();
  dlist.Resize(offset + values.size());
  values.VisitValues([&](const auto& vec) {
    vec.ForEachPresent([&](int64_t id, auto v) {
      dlist.Set(offset + id,
                (typename std::decay_t<decltype(vec)>::base_type)(v));
    });
  });
  return absl::OkStatus();
}

absl::Status DataBagImpl::ReplaceInList(const DataItem& list, ListRange range,
                                        const DataSliceImpl& values) {
  if (!list.has_value()) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(ObjectId list_id, ItemToListObjectId(list));
  DataList& dlist = GetOrCreateMutableLists(AllocationId(list_id))
                        .GetMutable(list_id.Offset());
  int64_t from = RemoveAndReserveInList(dlist, range, values.size());
  if (!values.is_single_dtype()) {
    for (int64_t i = 0; i < values.size(); ++i) {
      dlist.Set(from + i, values[i]);
    }
  } else {
    values.VisitValues([&](const auto& vec) {
      vec.ForEach([&](int64_t id, bool present, auto v) {
        using T = typename std::decay_t<decltype(vec)>::base_type;
        dlist.Set(from + id, present ? std::make_optional(T(v)) : std::nullopt);
      });
    });
  }
  return absl::OkStatus();
}

int64_t DataBagImpl::RemoveAndReserveInList(DataList& list, ListRange range,
                                            int64_t new_values_count) {
  auto [from, to] = range.Calculate(list.size());
  if (from <= to) {
    int64_t size_delta = new_values_count - (to - from);
    if (size_delta < 0) {
      list.Remove(to + size_delta, -size_delta);
    } else if (size_delta > 0) {
      list.InsertMissing(to, size_delta);
    }
  } else {
    list.InsertMissing(from, new_values_count);
  }
  return from;
}

absl::Status DataBagImpl::RemoveInList(const DataItem& list, ListRange range) {
  if (!list.has_value()) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(ObjectId list_id, ItemToListObjectId(list));
  DataList& dlist = GetOrCreateMutableLists(AllocationId(list_id))
                        .GetMutable(list_id.Offset());
  auto [from, to] = range.Calculate(dlist.size());
  if (to > from) {
    dlist.Remove(from, to - from);
  }
  return absl::OkStatus();
}

// ************************* Dict functions

const std::shared_ptr<DictVector>* DataBagImpl::GetConstDictsOrNull(
    AllocationId alloc_id) const {
  DCHECK(alloc_id.IsDictsAlloc() || alloc_id.IsSchemasAlloc());
  return GetConstDictsOrNull(alloc_id, absl::HashOf(alloc_id));
}

const std::shared_ptr<DictVector>* DataBagImpl::GetConstDictsOrNull(
    AllocationId alloc_id, size_t alloc_hash) const {
  DCHECK_EQ(alloc_hash, absl::HashOf(alloc_id));
  const DataBagImpl* bag = this;
  while (bag) {
    auto it = bag->dicts_.find(alloc_id);
    if (it != bag->dicts_.end()) {
      return &it->second;
    }
    bag = bag->parent_data_bag_.get();
  }
  return nullptr;
}

DictVector& DataBagImpl::GetOrCreateMutableDicts(AllocationId alloc_id,
                                                 std::optional<size_t> size) {
  DCHECK(alloc_id.IsDictsAlloc() || alloc_id.IsSchemasAlloc());
  auto [it, inserted] = dicts_.try_emplace(alloc_id);
  if (inserted) {
    const std::shared_ptr<DictVector>* parent_dicts = nullptr;
    if (parent_data_bag_ != nullptr) {
      parent_dicts = parent_data_bag_->GetConstDictsOrNull(alloc_id);
    }
    if (parent_dicts != nullptr) {
      // Create new DictVector using *parent_dicts (which is shared_ptr)
      // as a parent.
      it->second = std::make_shared<DictVector>(*parent_dicts);
    } else {
      if (!size.has_value()) {
        size = alloc_id.Capacity();
      }
      it->second = std::make_shared<DictVector>(*size);
    }
  }
  return *it->second;
}

inline Dict& DataBagImpl::GetOrCreateMutableDict(ObjectId object_id) {
  return GetOrCreateMutableDicts(AllocationId(object_id))[object_id.Offset()];
}

namespace {

struct DictsAllocCheckFn {
  inline bool operator()(AllocationId alloc_id) {
    return alloc_id.IsDictsAlloc();
  }
};

struct SchemasAllocCheckFn {
  inline bool operator()(AllocationId alloc_id) {
    return alloc_id.IsSchemasAlloc();
  }
};

}  // namespace

template <class AllocCheckFn>
class DataBagImpl::ReadOnlyDictGetter {
 public:
  // Returns reference to empty dict that is returned in case dict is not
  // found.
  static const Dict& GetEmptyDict() { return empty_dict_; }

  explicit ReadOnlyDictGetter(const DataBagImpl* bag) : bag_(bag) {}

  // Returns reference to dict if found, otherwise returns GetEmptyDict.
  const Dict& operator()(ObjectId dict_id) {
    AllocationId alloc_id(dict_id);
    if (alloc_id != current_alloc_) {
      if (ABSL_PREDICT_FALSE(!alloc_check_(alloc_id))) {
        status_ = absl::FailedPreconditionError("dicts expected");
        return empty_dict_;
      }
      dicts_vec_ = bag_->GetConstDictsOrNull(alloc_id);
      current_alloc_ = alloc_id;
    }
    return dicts_vec_ ? (**dicts_vec_)[dict_id.Offset()] : empty_dict_;
  }

  const absl::Status& status() { return status_; }

 private:
  const DataBagImpl* bag_ = nullptr;
  AllocCheckFn alloc_check_;
  absl::Status status_ = absl::OkStatus();
  std::optional<AllocationId> current_alloc_;
  const std::shared_ptr<DictVector>* dicts_vec_ = nullptr;
  static const Dict empty_dict_;
};

template <class AllocCheckFn>
const Dict DataBagImpl::ReadOnlyDictGetter<AllocCheckFn>::empty_dict_;

template <class AllocCheckFn>
class DataBagImpl::MutableDictGetter {
 public:
  explicit MutableDictGetter(DataBagImpl* bag) : bag_(bag) {}

  Dict* operator()(ObjectId dict_id) {
    AllocationId alloc_id(dict_id);
    if (alloc_id != current_alloc_) {
      if (!alloc_check_(alloc_id)) {
        status_ = absl::FailedPreconditionError("dicts expected");
        return nullptr;
      }
      dicts_vec_ = &bag_->GetOrCreateMutableDicts(alloc_id);
      current_alloc_ = alloc_id;
    }
    return &(*dicts_vec_)[dict_id.Offset()];
  }

  const absl::Status& status() { return status_; }

 private:
  DataBagImpl* bag_ = nullptr;
  AllocCheckFn alloc_check_;
  absl::Status status_ = absl::OkStatus();
  std::optional<AllocationId> current_alloc_;
  DictVector* dicts_vec_ = nullptr;
};

template <bool kReturnValues>
absl::StatusOr<std::pair<DataSliceImpl, arolla::DenseArrayEdge>>
DataBagImpl::GetDictKeysOrValues(const DataSliceImpl& dicts,
                                 FallbackSpan fallbacks) const {
  if (dicts.is_empty_and_unknown()) {
    ASSIGN_OR_RETURN(auto empty_edge, arolla::DenseArrayEdge::FromUniformGroups(
                                          dicts.size(), 0));
    return std::make_pair(DataSliceImpl::CreateEmptyAndUnknownType(0),
                          empty_edge);
  }
  if (dicts.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("dicts expected");
  }

  ReadOnlyDictGetter<DictsAllocCheckFn> dict_getter(this);
  std::vector<ReadOnlyDictGetter<DictsAllocCheckFn>> dict_fallback_getters;
  dict_fallback_getters.reserve(fallbacks.size());
  for (const DataBagImpl* fallback : fallbacks) {
    dict_fallback_getters.emplace_back(fallback);
  }
  std::vector<std::vector<DataItem>> results(dicts.size());
  std::vector<int64_t> split_points{0};
  split_points.reserve(dicts.size() + 1);

  std::vector<const Dict*> fallback_dicts;
  fallback_dicts.reserve(fallbacks.size());

  dicts.values<ObjectId>().ForEach(
      [&](int64_t offset, bool present, ObjectId dict_id) {
        if (!present) {
          split_points.push_back(split_points.back());
          return;
        }
        std::vector<DataItem>& res_vec = results[offset];
        const Dict& dict = dict_getter(dict_id);
        if (!fallbacks.empty()) {
          fallback_dicts.clear();
          for (auto& fallback_getter : dict_fallback_getters) {
            if (auto* fb_dict = &fallback_getter(dict_id);
                fb_dict !=
                &ReadOnlyDictGetter<DictsAllocCheckFn>::GetEmptyDict()) {
              fallback_dicts.push_back(fb_dict);
            }
          }
        }
        if constexpr (kReturnValues) {
          res_vec = dict.GetValues(fallback_dicts);
        } else {
          res_vec = dict.GetKeys(fallback_dicts);
        }
        split_points.push_back(split_points.back() + res_vec.size());
      });
  RETURN_IF_ERROR(dict_getter.status());

  SliceBuilder bldr(split_points.back());
  for (int64_t i = 0; i < results.size(); ++i) {
    const auto& res_vec = results[i];
    for (int64_t j = 0; j < res_vec.size(); ++j) {
      bldr.InsertIfNotSetAndUpdateAllocIds(split_points[i] + j, res_vec[j]);
    }
  }

  auto split_points_buf =
      arolla::Buffer<int64_t>::Create(std::move(split_points));
  ASSIGN_OR_RETURN(auto edge, arolla::DenseArrayEdge::FromSplitPoints(
                                  {std::move(split_points_buf)}));
  return std::make_pair(std::move(bldr).Build(), std::move(edge));
}

absl::StatusOr<std::pair<DataSliceImpl, arolla::DenseArrayEdge>>
DataBagImpl::GetDictKeys(const DataSliceImpl& dicts,
                         FallbackSpan fallbacks) const {
  return GetDictKeysOrValues</*kReturnValues=*/false>(dicts, fallbacks);
}

absl::StatusOr<std::pair<DataSliceImpl, arolla::DenseArrayEdge>>
DataBagImpl::GetDictValues(const DataSliceImpl& dicts,
                           FallbackSpan fallbacks) const {
  return GetDictKeysOrValues</*kReturnValues=*/true>(dicts, fallbacks);
}

absl::StatusOr<DataSliceImpl> DataBagImpl::GetDictSize(
    const DataSliceImpl& dicts, FallbackSpan fallbacks) const {
  if (dicts.is_empty_and_unknown()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(dicts.size());
  }
  if (dicts.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("dicts expected");
  }

  ReadOnlyDictGetter<DictsAllocCheckFn> dict_getter(this);
  std::vector<ReadOnlyDictGetter<DictsAllocCheckFn>> dict_fallback_getters;
  dict_fallback_getters.reserve(fallbacks.size());
  for (const DataBagImpl* fallback : fallbacks) {
    dict_fallback_getters.emplace_back(fallback);
  }

  arolla::DenseArray<int64_t> sizes;

  if (fallbacks.empty()) {
    sizes = arolla::CreateDenseOp([&](ObjectId dict_id) -> int64_t {
      return dict_getter(dict_id).GetSizeNoFallbacks();
    })(dicts.values<ObjectId>());
  } else {
    std::vector<const Dict*> fallback_dicts;
    fallback_dicts.reserve(fallbacks.size());
    sizes = arolla::CreateDenseOp([&](ObjectId dict_id) -> int64_t {
      fallback_dicts.clear();
      for (auto& fallback_getter : dict_fallback_getters) {
        if (auto* fb_dict = &fallback_getter(dict_id);
            fb_dict != &ReadOnlyDictGetter<DictsAllocCheckFn>::GetEmptyDict()) {
          fallback_dicts.push_back(fb_dict);
        }
      }
      const Dict& dict = dict_getter(dict_id);
      if (fallback_dicts.empty()) {
        return dict.GetSizeNoFallbacks();
      } else if (dict.GetSizeNoFallbacks() == 0 && fallback_dicts.size() == 1) {
        return fallback_dicts[0]->GetSizeNoFallbacks();
      } else {
        return dict.GetKeys(fallback_dicts).size();
      }
    })(dicts.values<ObjectId>());
  }

  RETURN_IF_ERROR(dict_getter.status());
  return DataSliceImpl::Create(std::move(sizes));
}

template <class AllocCheckFn>
absl::Status DataBagImpl::GetFromDictNoFallback(
    const arolla::DenseArray<ObjectId>& dicts, const DataSliceImpl& keys,
    SliceBuilder& bldr) const {
  if (dicts.size() != keys.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("dicts and keys sizes don't match: %d vs %d",
                        dicts.size(), keys.size()));
  }

  ReadOnlyDictGetter<AllocCheckFn> dict_getter(this);

  if (keys.is_mixed_dtype()) {
    dicts.ForEachPresent([&](int64_t offset, ObjectId dict_id) {
      if (bldr.IsSet(offset)) {
        return;
      }
      if (auto val = dict_getter(dict_id).Get(keys[offset]); val.has_value()) {
        bldr.InsertGuaranteedNotSetAndUpdateAllocIds(offset, *val);
      }
    });
  } else {
    absl::Status status = absl::OkStatus();
    keys.VisitValues([&](const auto& vec) {
      using T = typename std::decay_t<decltype(vec)>::base_type;
      status = arolla::DenseArraysForEachPresent(
          [&](int64_t offset, ObjectId dict_id, arolla::view_type_t<T> key) {
            if (bldr.IsSet(offset)) {
              return;
            }
            if (auto val = dict_getter(dict_id).Get(DataItem::View<T>{key});
                val.has_value()) {
              bldr.InsertGuaranteedNotSetAndUpdateAllocIds(offset, *val);
            }
          },
          dicts, vec);
    });
    RETURN_IF_ERROR(status);
  }

  return dict_getter.status();
}

template <class AllocCheckFn>
absl::Status DataBagImpl::GetFromDictNoFallback(
    const arolla::DenseArray<ObjectId>& dicts, const DataItem& keys,
    SliceBuilder& bldr) const {
  ReadOnlyDictGetter<AllocCheckFn> dict_getter(this);

  keys.VisitValue([&](const auto& val) {
    using T = typename std::decay_t<decltype(val)>;
    dicts.ForEachPresent([&](int64_t offset, ObjectId dict_id) {
      if (bldr.IsSet(offset)) {
        return;
      }
      bldr.InsertGuaranteedNotSetAndUpdateAllocIds(
          offset, dict_getter(dict_id).Get(DataItem::View<T>{val}));
    });
  });

  return dict_getter.status();
}

template <class AllocCheckFn, class DataSliceImplT>
inline absl::StatusOr<DataSliceImpl> DataBagImpl::GetFromDictImpl(
    const DataSliceImpl& dicts, const DataSliceImplT& keys,
    FallbackSpan fallbacks) const {
  const auto& dict_objects = dicts.values<ObjectId>();
  SliceBuilder bldr(dicts.size());
  RETURN_IF_ERROR(
      GetFromDictNoFallback<AllocCheckFn>(dict_objects, keys, bldr));
  for (const DataBagImpl* fallback : fallbacks) {
    if (bldr.is_finalized()) {
      break;
    }
    RETURN_IF_ERROR(arolla::CheckCancellation());
    RETURN_IF_ERROR(fallback->GetFromDictNoFallback<AllocCheckFn>(dict_objects,
                                                                  keys, bldr));
  }
  return std::move(bldr).Build();
}

absl::StatusOr<DataSliceImpl> DataBagImpl::GetFromDict(
    const DataSliceImpl& dicts, const DataSliceImpl& keys,
    FallbackSpan fallbacks) const {
  if (dicts.is_empty_and_unknown()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(dicts.size());
  }
  if (dicts.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("dicts expected");
  }

  return GetFromDictImpl<DictsAllocCheckFn>(dicts, keys, fallbacks);
}

absl::Status DataBagImpl::SetInDict(const DataSliceImpl& dicts,
                                    const DataSliceImpl& keys,
                                    const DataSliceImpl& values) {
  if (dicts.size() != keys.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("dicts and keys sizes don't match: %d vs %d",
                        dicts.size(), keys.size()));
  }
  if (dicts.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  if (dicts.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("dicts expected");
  }
  const arolla::QType* unsupported_key_type = nullptr;
  keys.VisitValues([&]<typename T>(const arolla::DenseArray<T>&) {
    if (Dict::IsUnsupportedKeyType<T>()) {
      unsupported_key_type = arolla::GetQType<T>();
    }
  });
  if (unsupported_key_type) {
    return absl::InvalidArgumentError(
        absl::StrCat("invalid key type: ", unsupported_key_type->name()));
  }

  MutableDictGetter<DictsAllocCheckFn> dict_getter(this);

  if (keys.is_mixed_dtype()) {
    dicts.values<ObjectId>().ForEachPresent(
        [&](int64_t offset, ObjectId dict_id) {
          Dict* dict = dict_getter(dict_id);
          if (ABSL_PREDICT_FALSE(dict == nullptr)) {
            return;
          }
          dict->Set(keys[offset], values[offset]);
        });
  } else if (!values.is_single_dtype()) {
    keys.VisitValues([&](const auto& vec) {
      using T = typename std::decay_t<decltype(vec)>::base_type;
      dicts.values<ObjectId>().ForEachPresent(
          [&](int64_t offset, ObjectId dict_id) {
            if (auto opt_key = vec[offset]; opt_key.present) {
              Dict* dict = dict_getter(dict_id);
              if (ABSL_PREDICT_FALSE(dict == nullptr)) {
                return;
              }
              dict->Set(DataItem::View<T>{opt_key.value}, values[offset]);
            }
          });
    });
  } else {
    absl::Status status = absl::OkStatus();
    values.VisitValues([&](const auto& values_vec) {
      using ValueT = typename std::decay_t<decltype(values_vec)>::base_type;
      using ValueView = arolla::view_type_t<ValueT>;
      keys.VisitValues([&](const auto& keys_vec) {
        using KeyT = typename std::decay_t<decltype(keys_vec)>::base_type;
        using KeyView = arolla::view_type_t<KeyT>;
        status = arolla::DenseArraysForEachPresent(
            [&](int64_t /*id*/, ObjectId dict_id, KeyView key,
                arolla::OptionalValue<ValueView> value) {
              Dict* dict = dict_getter(dict_id);
              if (ABSL_PREDICT_FALSE(dict == nullptr)) {
                return;
              }
              dict->Set(
                  DataItem::View<KeyT>{key},
                  value.present ? DataItem(ValueT(value.value)) : DataItem());
            },
            dicts.values<ObjectId>(), keys_vec, values_vec);
      });
    });
    RETURN_IF_ERROR(status);
  }

  return dict_getter.status();
}

absl::Status DataBagImpl::ClearDict(const DataSliceImpl& dicts) {
  if (dicts.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  if (dicts.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("dicts expected");
  }

  MutableDictGetter<DictsAllocCheckFn> dict_getter(this);
  dicts.values<ObjectId>().ForEachPresent(
      [&](int64_t offset, ObjectId dict_id) {
        Dict* dict = dict_getter(dict_id);
        if (ABSL_PREDICT_FALSE(dict == nullptr)) {
          return;
        }
        dict->Clear();
      });

  return dict_getter.status();
}

template <bool kReturnValues>
std::vector<DataItem> DataBagImpl::GetDictKeysOrValuesAsVector(
    ObjectId dict_id, FallbackSpan fallbacks) const {
  AllocationId alloc_id(dict_id);

  const std::shared_ptr<DictVector>* main_dicts = GetConstDictsOrNull(alloc_id);
  while (main_dicts == nullptr && !fallbacks.empty()) {
    main_dicts = fallbacks[0]->GetConstDictsOrNull(alloc_id);
    fallbacks.remove_prefix(1);
  }

  if (main_dicts == nullptr) {
    return {};
  }

  const Dict& dict = (**main_dicts)[dict_id.Offset()];
  if (fallbacks.empty()) {
    if constexpr (kReturnValues) {
      return dict.GetValues();
    } else {
      return dict.GetKeys();
    }
  }

  absl::InlinedVector<const Dict*, 1> fallback_dicts;
  for (const DataBagImpl* fallback : fallbacks) {
    const std::shared_ptr<DictVector>* fb_dicts =
        fallback->GetConstDictsOrNull(alloc_id);
    if (fb_dicts != nullptr) {
      fallback_dicts.push_back(&(**fb_dicts)[dict_id.Offset()]);
    }
  }
  if constexpr (kReturnValues) {
    return dict.GetValues(fallback_dicts);
  } else {
    return dict.GetKeys(fallback_dicts);
  }
}

template <bool kReturnValues>
absl::StatusOr<std::pair<DataSliceImpl, arolla::DenseArrayEdge>>
DataBagImpl::GetDictKeysOrValues(const DataItem& dict,
                                 FallbackSpan fallbacks) const {
  if (!dict.has_value()) {
    ASSIGN_OR_RETURN(auto empty_edge,
                     arolla::DenseArrayEdge::FromUniformGroups(1, 0));
    return std::make_pair(DataSliceImpl::CreateEmptyAndUnknownType(0),
                          empty_edge);
  }
  ASSIGN_OR_RETURN(ObjectId dict_id, ItemToDictObjectId(dict));
  auto results = GetDictKeysOrValuesAsVector<kReturnValues>(dict_id, fallbacks);
  ASSIGN_OR_RETURN(
      auto edge, arolla::DenseArrayEdge::FromUniformGroups(1, results.size()));
  return std::pair<DataSliceImpl, arolla::DenseArrayEdge>{
      DataSliceImpl::Create(std::move(results)), std::move(edge)};
}

absl::StatusOr<std::pair<DataSliceImpl, arolla::DenseArrayEdge>>
DataBagImpl::GetDictKeys(const DataItem& dict, FallbackSpan fallbacks) const {
  return GetDictKeysOrValues</*kReturnValues=*/false>(dict, fallbacks);
}

absl::StatusOr<std::pair<DataSliceImpl, arolla::DenseArrayEdge>>
DataBagImpl::GetDictValues(const DataItem& dict, FallbackSpan fallbacks) const {
  return GetDictKeysOrValues</*kReturnValues=*/true>(dict, fallbacks);
}

absl::StatusOr<DataItem> DataBagImpl::GetDictSize(
    const DataItem& dict, FallbackSpan fallbacks) const {
  if (!dict.has_value()) {
    return DataItem();
  }
  ASSIGN_OR_RETURN(ObjectId dict_id, ItemToDictObjectId(dict));
  size_t size;
  if (fallbacks.empty()) {
    const std::shared_ptr<DictVector>* dicts =
        GetConstDictsOrNull(AllocationId(dict_id));
    size = dicts ? (**dicts)[dict_id.Offset()].GetSizeNoFallbacks() : 0;
  } else {
    size =
        GetDictKeysOrValuesAsVector</*kReturnValues=*/false>(dict_id, fallbacks)
            .size();
  }
  return DataItem(static_cast<int64_t>(size));
}

template <typename Key>
ABSL_ATTRIBUTE_ALWAYS_INLINE DataItem
DataBagImpl::GetFromDictObject(ObjectId dict_id, const Key& key) const {
  AllocationId alloc_id(dict_id);
  if (const auto* dicts = GetConstDictsOrNull(alloc_id); dicts != nullptr) {
    if (auto res_opt = (**dicts)[dict_id.Offset()].Get(key);
        res_opt.has_value()) {
      return *res_opt;
    }
  }
  return DataItem();
}

template <typename Key>
ABSL_ATTRIBUTE_ALWAYS_INLINE DataItem
DataBagImpl::GetFromDictObjectWithFallbacks(ObjectId dict_id, const Key& key,
                                            FallbackSpan fallbacks) const {
  if (ABSL_PREDICT_TRUE(fallbacks.empty())) {
    return GetFromDictObject(dict_id, key);
  }
  AllocationId alloc_id(dict_id);
  size_t alloc_hash = absl::HashOf(alloc_id);
  int64_t offset = dict_id.Offset();
  if (const auto* dicts = GetConstDictsOrNull(alloc_id, alloc_hash);
      dicts != nullptr) {
    auto res = (**dicts)[offset].Get(key);
    if (res.has_value() || fallbacks.empty()) {
      return *res;
    }
  }
  for (const DataBagImpl* fallback : fallbacks) {
    if (const auto* dicts = fallback->GetConstDictsOrNull(alloc_id, alloc_hash);
        dicts != nullptr) {
      auto res = (**dicts)[offset].Get(key);
      if (res.has_value()) {
        return *res;
      }
    }
  }
  return DataItem();
}

absl::StatusOr<DataItem> DataBagImpl::GetFromDict(
    const DataItem& dict, const DataItem& key, FallbackSpan fallbacks) const {
  if (!dict.has_value()) {
    return DataItem();
  }
  ASSIGN_OR_RETURN(ObjectId dict_id, ItemToDictObjectId(dict));
  return GetFromDictObjectWithFallbacks(dict_id, key, fallbacks);
}

absl::Status DataBagImpl::SetInDict(const DataItem& dict, const DataItem& key,
                                    const DataItem& value) {
  if (!dict.has_value()) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(ObjectId dict_id, ItemToDictObjectId(dict));
  if (Dict::IsUnsupportedDataItemKeyType(key)) {
    return absl::InvalidArgumentError(
        absl::StrCat("invalid key type: ", key.dtype()->name()));
  }
  GetOrCreateMutableDict(dict_id).Set(key, value);
  return absl::OkStatus();
}

absl::Status DataBagImpl::ClearDict(const DataItem& dict) {
  if (!dict.has_value()) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(ObjectId dict_id, ItemToDictObjectId(dict));
  GetOrCreateMutableDict(dict_id).Clear();
  return absl::OkStatus();
}

// ************************* Schema functions

absl::StatusOr<DataSliceImpl> DataBagImpl::GetSchemaAttrs(
    const DataItem& schema_item, FallbackSpan fallbacks) const {
  ASSIGN_OR_RETURN(auto keys,
                   GetSchemaAttrsAsVector(schema_item, fallbacks));
  if (keys.empty()) {
    return DataSliceImpl::Create(
        arolla::CreateEmptyDenseArray<arolla::Text>(/*size=*/0));
  }
  auto keys_array = arolla::DenseArrayBuilder<arolla::Text>(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    DCHECK(keys[i].holds_value<arolla::Text>());
    keys_array.Add(i, keys[i].value<arolla::Text>());
  }
  return DataSliceImpl::Create(std::move(keys_array).Build());
}

absl::StatusOr<std::vector<DataItem>> DataBagImpl::GetSchemaAttrsAsVector(
    const DataItem& schema_item, FallbackSpan fallbacks) const {
  if (!schema_item.holds_value<ObjectId>()) {
    if (!schema_item.has_value()) {
      return std::vector<DataItem>();
    }
    return InvalidSchemaObjectId(schema_item);
  }
  ASSIGN_OR_RETURN(ObjectId schema_id, ItemToSchemaObjectId(schema_item));
  if (schema_id.IsSmallAlloc()) {
    return GetDictKeysOrValuesAsVector</*kReturnValues=*/false>(
        schema_id, fallbacks);
  }
  auto attr_names = GetDictKeysOrValuesAsVector</*kReturnValues=*/false>(
      AllocationId(schema_id).ObjectByOffset(0), fallbacks);
  std::vector<DataItem> result;
  result.reserve(attr_names.size());
  for (const auto& attr_name : attr_names) {
    DCHECK(attr_name.holds_value<arolla::Text>());
    auto attr_schema = GetAttrWithRemoved(
        schema_id, attr_name.value<arolla::Text>(), fallbacks);
    if (attr_schema.has_value()) {
      result.push_back(DataItem(attr_name));
    }
  }
  return result;
}

std::vector<DataItem> DataBagImpl::GetSchemaAttrsForBigAllocationAsVector(
    const AllocationId& alloc_id, FallbackSpan fallbacks) const {
  DCHECK(!alloc_id.IsSmall());
  return GetDictKeysOrValuesAsVector</*kReturnValues=*/false>(
      alloc_id.ObjectByOffset(0), fallbacks);
}

absl::StatusOr<DataItem> DataBagImpl::GetSchemaAttr(
    const DataItem& schema_item, absl::string_view attr,
    FallbackSpan fallbacks) const {
  ASSIGN_OR_RETURN(auto res,
                   GetSchemaAttrAllowMissing(schema_item, attr, fallbacks));
  if (!res.has_value() && schema_item.has_value()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "the attribute '", attr,
        "' is missing on the schema.\n\nIf it is not a typo, perhaps ignore "
        "the schema when getting the attribute. For example, ds.maybe('",
        attr, "')"));
  }
  return std::move(res);
}

absl::StatusOr<DataItem> DataBagImpl::GetSchemaAttrAllowMissing(
    const DataItem& schema_item, absl::string_view attr,
    FallbackSpan fallbacks) const {
  if (!schema_item.holds_value<ObjectId>()) {
    if (!schema_item.has_value()) {
      return DataItem();
    }
    return InvalidSchemaObjectId(schema_item);
  }
  if (schema_item.value<ObjectId>().IsSmallAlloc()) {
    ASSIGN_OR_RETURN(ObjectId schema_id, ItemToSchemaObjectId(schema_item));
    return GetFromDictObjectWithFallbacks(
        schema_id, DataItem::View<arolla::Text>(attr), fallbacks);
  }
  return GetAttr(schema_item, attr, fallbacks);
}

absl::StatusOr<DataSliceImpl> DataBagImpl::GetSchemaAttr(
    const DataSliceImpl& schema_slice, absl::string_view attr,
    FallbackSpan fallbacks) const {
  ASSIGN_OR_RETURN(auto result,
                   GetSchemaAttrAllowMissing(schema_slice, attr, fallbacks));
  if (result.present_count() != schema_slice.present_count()) {
    std::optional<int64_t> first_missing_schema_index;
    RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
        [&first_missing_schema_index](
            int64_t index, DataItem schema_item,
            arolla::OptionalValue<DataItem> schema_attr) {
          if (!schema_attr.present) {
            first_missing_schema_index = index;
          }
        },
        schema_slice.AsDataItemDenseArray(), result.AsDataItemDenseArray()));
    DCHECK(first_missing_schema_index.has_value());
    return absl::InvalidArgumentError(absl::StrCat(
        "the attribute '", attr,
        "' is missing for at least one object at ds.flatten().S[",
        *first_missing_schema_index, "]\n\n",
        "If it is not a typo, perhaps ignore "
        "the schema when getting the attribute. For example, ds.maybe('",
        attr, "')"));
  }
  return result;
}

absl::StatusOr<DataSliceImpl> DataBagImpl::GetSchemaAttrAllowMissing(
    const DataSliceImpl& schema_slice, absl::string_view attr,
    FallbackSpan fallbacks) const {
  if (schema_slice.is_empty_and_unknown()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(schema_slice.size());
  }
  if (schema_slice.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "cannot get schema attribute of a non-schema slice %v", schema_slice));
  }
  const AllocationIdSet& alloc_ids = schema_slice.allocation_ids();
  std::optional<DataSliceImpl> big_alloc_result;
  if (!alloc_ids.empty()) {
    if (!alloc_ids.contains_small_allocation_id()) {
      return GetAttr(schema_slice, attr, fallbacks);
    } else {
      ASSIGN_OR_RETURN(
          big_alloc_result,
          GetAttr(WithoutSmallAllocs(schema_slice), attr, fallbacks));
    }
  }
  // For small allocations, we store the schema attribute values directly in the
  // dict.
  // TODO: we may consider to change that for a few reasons
  // (including code simplicity).
  SliceBuilder small_alloc_result_builder(schema_slice.size());
  absl::Status status = absl::OkStatus();
  std::optional<ObjectId> last_schema_id;
  DataItem attr_value;
  schema_slice.values<ObjectId>().ForEachPresent(
      [&](int64_t offset, ObjectId schema_id) {
        if (!status.ok()) {
          return;
        }
        if (!schema_id.IsSmallAlloc()) {
          return;
        }
        if (schema_id != last_schema_id) {
          last_schema_id = schema_id;
          absl::StatusOr<DataItem> attr_value_or =
              GetSchemaAttrAllowMissing(DataItem(schema_id), attr, fallbacks);
          if (!attr_value_or.ok()) {
            status = attr_value_or.status();
            return;
          }
          attr_value = std::move(*attr_value_or);
        }
        small_alloc_result_builder.InsertIfNotSetAndUpdateAllocIds(offset,
                                                                   attr_value);
      });
  RETURN_IF_ERROR(std::move(status));
  if (!big_alloc_result.has_value()) {
    return std::move(small_alloc_result_builder).Build();
  }
  return PresenceOrOp</*disjoint=*/false>{}(
      *big_alloc_result, std::move(small_alloc_result_builder).Build());
}

namespace {

absl::Status InvalidLhsSetSchemaAttrError(const DataSliceImpl& not_a_schema) {
  return absl::InvalidArgumentError(absl::StrFormat(
      "cannot set schema attribute of a non-schema slice %v", not_a_schema));
}

absl::Status InvalidRhsSetSchemaAttrError(const DataSliceImpl& not_a_schema) {
  return absl::InvalidArgumentError(absl::StrFormat(
      "cannot set a non-schema slice %v as a schema attribute", not_a_schema));
}

}  // namespace

absl::Status DataBagImpl::SetSchemaAttr(const DataItem& schema_item,
                                        absl::string_view attr,
                                        const DataItem& value) {
  if (!schema_item.holds_value<ObjectId>()) {
    if (!schema_item.has_value()) {
      return absl::OkStatus();
    }
    return InvalidSchemaObjectId(schema_item);
  }
  if (attr == schema::kSchemaNameAttr) {
    if (!value.holds_value<arolla::Text>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "only Text can be used as a schema name, got: %v", value));
    }
  } else if (attr == schema::kSchemaMetadataAttr) {
    if (!value.holds_value<ObjectId>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
        "only ItemId can be used as a schema metadata, got: %v", value
      ));
    }
  } else {
    RETURN_IF_ERROR(VerifyIsSchema(value));
  }
  ASSIGN_OR_RETURN(ObjectId schema_id, ItemToSchemaObjectId(schema_item));
  if (!schema_id.IsSmallAlloc()) {
    auto& dict =
        GetOrCreateMutableDicts(AllocationId(schema_id), /*size=*/1)[0];
    RETURN_IF_ERROR(SetAttr(schema_item, attr, value));
    dict.Set(DataItem::View<arolla::Text>(attr), DataItem(arolla::kPresent));
  } else {
    auto& dict = GetOrCreateMutableDict(schema_id);
    dict.Set(DataItem::View<arolla::Text>(attr), value);
  }
  return absl::OkStatus();
}

DataSliceImpl DataBagImpl::WithoutSmallAllocs(
    const DataSliceImpl& slice) const {
  // TODO: consider to only mark off flag of small allocations.
  arolla::DenseArrayBuilder<ObjectId> big_alloc_objs(slice.size());
  slice.values<ObjectId>().ForEachPresent(
      [&](int64_t offset, ObjectId schema_id) {
        if (!schema_id.IsSmallAlloc()) {
          big_alloc_objs.Add(offset, schema_id);
        }
      });
  AllocationIdSet big_alloc_ids(slice.allocation_ids().ids());
  return DataSliceImpl::CreateObjectsDataSlice(
      std::move(big_alloc_objs).Build(), std::move(big_alloc_ids));
}

absl::Status DataBagImpl::SetSchemaAttr(const DataSliceImpl& schema_slice,
                                        absl::string_view attr,
                                        const DataItem& value) {
  if (attr != schema::kSchemaNameAttr && attr != schema::kSchemaMetadataAttr) {
    RETURN_IF_ERROR(VerifyIsSchema(value));
  }
  if (schema_slice.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  if (schema_slice.dtype() != arolla::GetQType<ObjectId>()) {
    return InvalidLhsSetSchemaAttrError(schema_slice);
  }
  const AllocationIdSet& alloc_ids = schema_slice.allocation_ids();
  SchemasAllocCheckFn schemas_alloc_check_fn;
  absl::Status status = absl::OkStatus();
  for (const auto& alloc_id : alloc_ids) {
    if (ABSL_PREDICT_FALSE(!schemas_alloc_check_fn(alloc_id))) {
      status = InvalidLhsSetSchemaAttrError(schema_slice);
    } else {
      Dict& schema_dict = GetOrCreateMutableDicts(alloc_id, /*size=*/1)[0];
      schema_dict.Set(DataItem::View<arolla::Text>(attr),
                      DataItem(arolla::kPresent));
    }
  }
  if (!alloc_ids.empty()) {
    if (!alloc_ids.contains_small_allocation_id()) {
      return SetAttr(schema_slice, attr,
                     DataSliceImpl::Create(schema_slice.size(), value));
    } else {
      RETURN_IF_ERROR(
          SetAttr(WithoutSmallAllocs(schema_slice), attr,
                  DataSliceImpl::Create(schema_slice.size(), value)));
    }
  }
  if (alloc_ids.contains_small_allocation_id()) {
    schema_slice.values<ObjectId>().ForEachPresent(
        [&](int64_t /*id*/, ObjectId schema_id) {
          if (schema_id.IsSmallAlloc()) {
            const auto& alloc_id = AllocationId(schema_id);
            if (ABSL_PREDICT_FALSE(!schemas_alloc_check_fn(alloc_id))) {
              status = InvalidLhsSetSchemaAttrError(schema_slice);
            } else {
              Dict& schema_dict = GetOrCreateMutableDict(schema_id);
              schema_dict.Set(DataItem::View<arolla::Text>(attr), value);
            }
          }
        });
  }
  return status;
}

absl::Status DataBagImpl::SetSchemaAttr(const DataSliceImpl& schema_slice,
                                        absl::string_view attr,
                                        const DataSliceImpl& values) {
  if (schema_slice.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  if (schema_slice.dtype() != arolla::GetQType<ObjectId>()) {
    return InvalidLhsSetSchemaAttrError(schema_slice);
  }
  const AllocationIdSet& alloc_ids = schema_slice.allocation_ids();
  if (!alloc_ids.empty()) {
    if (!alloc_ids.contains_small_allocation_id()) {
      RETURN_IF_ERROR(SetAttr(schema_slice, attr, values));
    } else {
      RETURN_IF_ERROR(SetAttr(WithoutSmallAllocs(schema_slice), attr, values));
    }
  }
  SchemasAllocCheckFn schemas_alloc_check_fn;
  absl::Status status = absl::OkStatus();
  for (const auto& alloc_id : alloc_ids) {
    if (ABSL_PREDICT_FALSE(!schemas_alloc_check_fn(alloc_id))) {
      status = InvalidLhsSetSchemaAttrError(schema_slice);
    } else {
      Dict& schema_dict = GetOrCreateMutableDicts(alloc_id, /*size=*/1)[0];
      schema_dict.Set(DataItem::View<arolla::Text>(attr),
                      DataItem(arolla::kPresent));
    }
  }
  if (values.is_single_dtype()) {
    return values.VisitValues([&]<class T>(const T& vec) -> absl::Status {
      using ValueT = typename T::base_type;
      if constexpr (std::is_same_v<ValueT, arolla::Text>) {
        if (attr != schema::kSchemaNameAttr) {
          return InvalidRhsSetSchemaAttrError(values);
        }
      }
      if constexpr (std::is_same_v<ValueT, schema::DType> ||
                    std::is_same_v<ValueT, ObjectId> ||
                    std::is_same_v<ValueT, arolla::Text>) {
        absl::Status status = absl::OkStatus();
        RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
            [&](int64_t /*id*/, ObjectId schema_id,
            arolla::view_type_t<ValueT> value) {
              if constexpr (std::is_same_v<ValueT, ObjectId>) {
                if (!value.IsSchema() && attr != schema::kSchemaMetadataAttr) {
                  status = InvalidRhsSetSchemaAttrError(values);
                  return;
                }
              }
              if (!schema_id.IsSmallAlloc()) {
                return;
              }
              const auto& alloc_id = AllocationId(schema_id);
              if (ABSL_PREDICT_FALSE(!schemas_alloc_check_fn(alloc_id))) {
                status = InvalidLhsSetSchemaAttrError(schema_slice);
              } else {
                Dict& schema_dict = GetOrCreateMutableDict(schema_id);
                schema_dict.Set(
                    DataItem::View<arolla::Text>(attr),
                    DataItem(ValueT{value}));
              }
            },
            schema_slice.values<ObjectId>(), vec));
        return status;
      } else {
        return InvalidRhsSetSchemaAttrError(values);
      }
    });
  } else {
    absl::Status status = absl::OkStatus();
    schema_slice.values<ObjectId>().ForEachPresent(
        [&](int64_t offset, ObjectId schema_id) {
          DataItem value = values[offset];
          if (value.has_value() && !VerifyIsSchema(value).ok() &&
              attr != schema::kSchemaMetadataAttr &&
              attr != schema::kSchemaNameAttr) {
            status = InvalidRhsSetSchemaAttrError(values);
            return;
          }
          if (!schema_id.IsSmallAlloc()) {
            return;
          }
          const auto& alloc_id = AllocationId(schema_id);
          if (ABSL_PREDICT_FALSE(!schemas_alloc_check_fn(alloc_id))) {
            status = InvalidLhsSetSchemaAttrError(schema_slice);
          } else {
            Dict& schema_dict = GetOrCreateMutableDict(schema_id);
            schema_dict.Set(DataItem::View<arolla::Text>(attr),
                            std::move(value));
          }
        });
    return status;
  }
}

absl::Status DataBagImpl::DelSchemaAttr(const DataItem& schema_item,
                                        absl::string_view attr) {
  RETURN_IF_ERROR(GetSchemaAttr(schema_item, attr).status());
  if (!schema_item.has_value()) {
    return absl::OkStatus();
  }
  return DelSchemaAttr(DataSliceImpl::Create(1, schema_item), attr);
}

absl::Status DataBagImpl::DelSchemaAttr(const DataSliceImpl& schema_slice,
                                        absl::string_view attr) {
  RETURN_IF_ERROR(GetSchemaAttr(schema_slice, attr).status());
  return SetSchemaAttr(schema_slice, attr,
                       DataSliceImpl::Create(schema_slice.size(), DataItem()));
}

template <typename ImplT>
absl::Status SetSchemaFields(
    const ImplT&,  absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> items) {
  static_assert(sizeof(ImplT) == 0,
                "SetSchemaFields is not supported for ImplT not in "
                "{DataSliceImpl, DataItem}");
  return absl::UnimplementedError("Never called!");
}

template<>
absl::Status DataBagImpl::SetSchemaFields(
    const DataItem& schema_item,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> items) {
  if (!schema_item.holds_value<ObjectId>()) {
    if (!schema_item.has_value()) {
      return absl::OkStatus();
    }
    return InvalidSchemaObjectId(schema_item);
  }
  DCHECK_EQ(attr_names.size(), items.size());
  for (const auto& item : items) {
    RETURN_IF_ERROR(VerifyIsSchema(item.get()));
  }
  for (int i = 0; i < attr_names.size(); ++i) {
    RETURN_IF_ERROR(SetSchemaAttr(schema_item, attr_names[i], items[i]));
  }
  return absl::OkStatus();
}

template <>
absl::Status DataBagImpl::SetSchemaFields(
    const DataSliceImpl& schema_slice,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> items) {
  DCHECK_EQ(attr_names.size(), items.size());
  if (schema_slice.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  for (size_t i = 0; i < attr_names.size(); ++i) {
    RETURN_IF_ERROR(SetSchemaAttr(schema_slice, attr_names[i], items[i]));
  }
  return absl::OkStatus();
}

absl::Status DataBagImpl::SetSchemaFieldsForEntireAllocation(
    AllocationId schema_alloc_id, size_t size,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> items) {
  DCHECK_EQ(attr_names.size(), items.size());
  DCHECK(schema_alloc_id.IsSchemasAlloc());
  if (size == 0) {
    return absl::OkStatus();
  }
  for (const auto& item : items) {
    RETURN_IF_ERROR(VerifyIsSchema(item.get()));
  }
  if (schema_alloc_id.IsSmall()) {
    return SetSchemaFields(
        DataSliceImpl::ObjectsFromAllocation(schema_alloc_id, size), attr_names,
        items);
  }
  Dict& schema_dict = GetOrCreateMutableDicts(schema_alloc_id, /*size=*/1)[0];
  for (int i = 0; i < attr_names.size(); ++i) {
    schema_dict.Set(DataItem::View<arolla::Text>(attr_names[i]),
                    DataItem(arolla::kPresent));
    ASSIGN_OR_RETURN(
        auto source,
        // TODO: consider creating ConstDenseSource to save RAM.
        DenseSource::CreateReadonly(
            schema_alloc_id, DataSliceImpl::Create(size, items[i].get())));
    sources_.insert_or_assign(
        SourceKey{schema_alloc_id, std::string(attr_names[i])},
        SourceCollection{.const_dense_source = std::move(source),
                         .lookup_parent = false});
  }
  return absl::OkStatus();
}

absl::StatusOr<DataItem> DataBagImpl::CreateExplicitSchemaFromFields(
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> items) {
  DCHECK_EQ(attr_names.size(), items.size());
  auto schema_id = internal::AllocateExplicitSchema();
  RETURN_IF_ERROR(SetSchemaFields(DataItem(schema_id), attr_names, items));
  return DataItem(schema_id);
}

absl::StatusOr<DataItem> DataBagImpl::CreateUuSchemaFromFields(
    absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> items) {
  DCHECK_EQ(attr_names.size(), items.size());
  auto schema_id = internal::CreateSchemaUuidFromFields(
      seed, attr_names, items);
  RETURN_IF_ERROR(
      SetSchemaFields(DataItem(schema_id), attr_names, items));
  return DataItem(schema_id);
}

// ********* Merging

absl::Status DataBagImpl::MergeSmallAllocInplace(const DataBagImpl& other,
                                                 MergeOptions options) {
  for (const DataBagImpl* other_db = &other; other_db != nullptr;
       other_db = other_db->parent_data_bag_.get()) {
    for (const auto& [attr_name, other_source_top] :
         other_db->small_alloc_sources_) {
      ConstSparseSourceArray other_sources;
      // NOTE we are taking sources from the top level DataBagImpl.
      other.GetSmallAllocDataSources(attr_name, other_sources);
      if (other_sources[0] != &other_source_top) {
        // this attr_name was processed in previous db.
        continue;
      }

      ConstSparseSourceArray this_sources;
      GetSmallAllocDataSources(attr_name, this_sources);

      if (this_sources.empty() && other_sources.size() == 1) {
        // Copy entire source
        small_alloc_sources_.emplace(attr_name, other_source_top);
        continue;
      }
      SparseSource* this_mutable_source = nullptr;
      auto process_other_source = [&](const SparseSource* other_source,
                                      auto skip_object_id) -> absl::Status {
        for (const auto& [obj_id, other_item] : other_source->GetAll()) {
          if (skip_object_id(obj_id)) {
            continue;
          }
          std::optional<DataItem> this_value;
          if (options.data_conflict_policy != MergeOptions::kOverwrite) {
            this_value =
                  GetAttributeFromSources(obj_id, {}, this_sources);
          }
          if (this_value.has_value()) {
            if (options.data_conflict_policy ==
                    MergeOptions::kRaiseOnConflict &&
                ValuesAreDifferent(*this_value, other_item)) {
              return arolla::WithPayload(
                  absl::FailedPreconditionError(absl::StrCat(
                      "conflicting values for ", attr_name, " for ", obj_id,
                      ": ", *this_value, " vs ", other_item)),
                  MakeEntityOrObjectMergeError(obj_id, attr_name));
            }
          } else {
            if (this_mutable_source == nullptr) {
              this_mutable_source = &GetMutableSmallAllocSource(attr_name);
              // NOTE it is fine that this_sources doesn't contain newly created
              // source, because we never write the same key twice.
            }
            this_mutable_source->Set(obj_id, other_item);
          }
        }
        return absl::OkStatus();
      };
      RETURN_IF_ERROR(
          ProcessSparseSources(process_other_source, other_sources));
    }
  }
  return absl::OkStatus();
}

absl::Status DataBagImpl::MergeBigAllocInplace(const DataBagImpl& other,
                                               MergeOptions options) {
  absl::flat_hash_set<SourceKey> used_keys;
  for (const DataBagImpl* other_db = &other; other_db != nullptr;
       other_db = other_db->parent_data_bag_.get()) {
    for (const auto& [source_key, other_source_collection] :
         other_db->sources_) {
      if (!used_keys.insert(source_key).second) {
        continue;
      }
      const auto& [alloc, attr_name] = source_key;
      ConstDenseSourceArray other_dense_sources;
      ConstSparseSourceArray other_sparse_sources;
      // NOTE we are taking sources from the top level DataBagImpl.
      other.GetAttributeDataSources(alloc, attr_name, other_dense_sources,
                                    other_sparse_sources);

      ConstDenseSourceArray this_dense_sources;
      ConstSparseSourceArray this_sparse_sources;
      GetAttributeDataSources(alloc, attr_name, this_dense_sources,
                              this_sparse_sources);

      SourceCollection& this_collection =
          GetOrCreateSourceCollection(alloc, attr_name);

      if (this_dense_sources.empty() && this_sparse_sources.empty()) {
        this_collection.lookup_parent = false;
        if (!other_dense_sources.empty()) {
          DCHECK_EQ(other_dense_sources.size(), 1);
          SourceCollection other_source_collection_copy =
              other_source_collection;
          if (other_source_collection_copy.mutable_dense_source != nullptr) {
            other_source_collection_copy.const_dense_source =
                other_source_collection_copy.mutable_dense_source;
            other_source_collection_copy.mutable_dense_source = nullptr;
          }
          DCHECK(other_source_collection_copy.const_dense_source != nullptr);
          // Merge sparse and dense sources into single mutable dense source.
          // We want to avoid references into data from a different DataBag.
          // In case single dense source is immutable, it is fine to share it
          // across DataBags.
          if (!other_sparse_sources.empty() ||
              other_source_collection_copy.const_dense_source->IsMutable()) {
            RETURN_IF_ERROR(other_db->CreateMutableDenseSource(
                other_source_collection_copy, alloc, attr_name,
                // QType is not needed if dense source is present
                nullptr, alloc.Capacity()));
          }
          this_collection = other_source_collection_copy;
          continue;
        }
        this_collection.mutable_sparse_source =
            MergeToMutableSparseSource(other_sparse_sources);
        DCHECK(!this_collection.lookup_parent);
        continue;
      }

      if (!this_dense_sources.empty()) {
        if (this_collection.mutable_dense_source == nullptr) {
          RETURN_IF_ERROR(CreateMutableDenseSource(
              this_collection, alloc, attr_name,
              // QType is not needed if dense source is present
              nullptr, this_dense_sources.front()->size()));
        }
        DCHECK(this_collection.mutable_dense_source != nullptr);
        DCHECK(this_collection.mutable_sparse_source == nullptr);
        DCHECK(!this_collection.lookup_parent);
        if (other_dense_sources.empty()) {
          RETURN_IF_ERROR(MergeToMutableDenseSourceOnlySparse(
              *this_collection.mutable_dense_source, other_sparse_sources,
              options, attr_name));
          continue;
        }
        DCHECK_EQ(other_dense_sources.size(), 1);
        RETURN_IF_ERROR(MergeToMutableDenseSource(
            *this_collection.mutable_dense_source, alloc,
            *other_dense_sources.front(), other_sparse_sources, options,
            attr_name));
        continue;
      }

      if (!other_dense_sources.empty()) {
        // This sparse, other dense
        SourceCollection other_source_collection_copy = other_source_collection;
        if (other_source_collection_copy.mutable_dense_source == nullptr) {
          RETURN_IF_ERROR(other_db->CreateMutableDenseSource(
              other_source_collection_copy, alloc, attr_name,
              // QType is not needed if dense source is present
              nullptr, other_dense_sources.front()->size()));
        }
        RETURN_IF_ERROR(MergeToMutableDenseSourceOnlySparse(
            *other_source_collection_copy.mutable_dense_source,
            this_sparse_sources, ReverseMergeOptions(options), attr_name));
        this_collection = other_source_collection_copy;
        continue;
      }

      // Both this and other sparse
      DCHECK(this_collection.mutable_dense_source == nullptr);
      DCHECK(this_collection.const_dense_source == nullptr);
      DCHECK(other_dense_sources.empty());
      this_collection.mutable_sparse_source =
          MergeToMutableSparseSource(this_sparse_sources);
      this_collection.lookup_parent = false;
      RETURN_IF_ERROR(MergeToMutableSparseSourceOnlySparse(
          *this_collection.mutable_sparse_source, other_sparse_sources, options,
          attr_name));
    }
  }
  return absl::OkStatus();
}

absl::Status DataBagImpl::MergeListsInplace(const DataBagImpl& other,
                                            MergeOptions options) {
  absl::flat_hash_set<AllocationId> used_keys;
  for (const DataBagImpl* other_db = &other; other_db != nullptr;
       other_db = other_db->parent_data_bag_.get()) {
    for (const auto& [alloc_id, other_lists] : other_db->lists_) {
      if (!used_keys.insert(alloc_id).second) {
        continue;
      }
      auto& this_lists = GetOrCreateMutableLists(alloc_id);
      for (size_t i = 0; i < other_lists->size(); ++i) {
        const auto* other_list = other_lists->Get(i);
        if (other_list == nullptr) {
          continue;
        }
        bool this_list_unset = this_lists.Get(i) == nullptr;
        auto& this_list = this_lists.GetMutable(i);
        if (options.data_conflict_policy == MergeOptions::kOverwrite ||
            this_list_unset) {
          this_list = *other_list;
          continue;
        }
        if (options.data_conflict_policy == MergeOptions::kRaiseOnConflict) {
          if (this_list.size() != other_list->size()) {
            return arolla::WithPayload(
                absl::FailedPreconditionError(
                    absl::StrCat("conflicting list sizes for ", alloc_id, ": ",
                                 this_list.size(), " vs ", other_list->size())),
                MakeListSizeMergeError(alloc_id.ObjectByOffset(i),
                                       this_list.size(), other_list->size()));
          }
          for (size_t j = 0; j < other_list->size(); ++j) {
            if (ValuesAreDifferent(this_list[j], (*other_list)[j])) {
              return arolla::WithPayload(
                  absl::FailedPreconditionError(absl::StrCat(
                      "conflicting list values for ", alloc_id, "at index ", j,
                      ": ", this_list[j], " vs ", (*other_list)[j])),
                  MakeListItemMergeError(alloc_id.ObjectByOffset(i), j,
                                         this_list[j], (*other_list)[j]));
            }
          }
        }
      }
    }
  }
  return absl::OkStatus();
}

absl::Status DataBagImpl::MergeDictsInplace(const DataBagImpl& other,
                                            MergeOptions options) {
  absl::flat_hash_set<AllocationId> used_keys;
  for (const DataBagImpl* other_db = &other; other_db != nullptr;
       other_db = other_db->parent_data_bag_.get()) {
    for (const auto& [alloc_id, other_dicts] : other_db->dicts_) {
      if (!used_keys.insert(alloc_id).second) {
        continue;
      }
      const auto& conflict_policy = alloc_id.IsExplicitSchemasAlloc()
                                        ? options.schema_conflict_policy
                                        : options.data_conflict_policy;
      auto& this_dicts = GetOrCreateMutableDicts(alloc_id);
      for (size_t i = 0; i < other_dicts->size(); ++i) {
        const auto& other_dict = (*other_dicts)[i];
        auto& this_dict = this_dicts[i];
        for (const DataItem& key : other_dict.GetKeys()) {
          if (conflict_policy == MergeOptions::kOverwrite) {
            this_dict.Set(key, *other_dict.Get(key));
            continue;
          }
          auto other_value = other_dict.Get(key);
          if (!other_value.has_value()) {
            continue;
          }
          const DataItem& this_value =
              this_dict.GetOrAssign(key, other_value->get());
          if (conflict_policy == MergeOptions::kRaiseOnConflict &&
              ValuesAreDifferent(this_value, other_value->get())) {
            internal::ObjectId object_id = alloc_id.ObjectByOffset(i);
            return arolla::WithPayload(
                absl::FailedPreconditionError(absl::StrCat(
                    "conflicting dict values for ", object_id, " key ", key,
                    ": ", this_value, " vs ", *other_value)),
                MakeSchemaOrDictMergeError(object_id, key, this_value,
                                           *other_value));
          }
        }
      }
    }
  }
  return absl::OkStatus();
}

// Merge additional attributes and objects from `other`.
// Returns non-ok Status on conflict.
absl::Status DataBagImpl::MergeInplace(const DataBagImpl& other,
                                       MergeOptions options) {
  if (this == &other) {
    return absl::OkStatus();
  }
  // sources_
  RETURN_IF_ERROR(MergeBigAllocInplace(other, options));
  // small_alloc_sources_
  RETURN_IF_ERROR(MergeSmallAllocInplace(other, options));
  // lists_
  RETURN_IF_ERROR(MergeListsInplace(other, options));
  // dicts_
  RETURN_IF_ERROR(MergeDictsInplace(other, options));
  return absl::OkStatus();
}

// TODO: Consider removing this destructor once databag
// automatically squashes long chains.
DataBagImpl::~DataBagImpl() noexcept {
  // The first destructed DataBag will perform clean up.
  thread_local bool is_cleanup_ongoing = false;
  // Dependant DataBagImpl's to remove.
  //
  // NOTE(b/343432263): NoDestructor is used to avoid issues with the
  // destruction order of globals vs thread_locals.
  thread_local absl::NoDestructor<DataBagImplConstPtr> to_destruct;

  if (parent_data_bag_ == nullptr) {
    return;
  }

  // Postpone removing to avoid recursion.
  *to_destruct = std::move(parent_data_bag_);
  if (is_cleanup_ongoing) {
    return;
  }

  is_cleanup_ongoing = true;
  absl::Cleanup reset_flag = [&] { is_cleanup_ongoing = false; };
  while (*to_destruct != nullptr) {
    // NOTE: `to_destruct` can be reassigned during this operation.
    to_destruct->reset();
  }
}

DataBagIndex DataBagImpl::CreateIndex() const {
  DataBagIndex index;
  absl::flat_hash_set<AllocationId> lists_set;
  absl::flat_hash_set<AllocationId> dicts_set;
  absl::flat_hash_map<std::string, absl::flat_hash_set<AllocationId>> attrs_map;
  for (const DataBagImpl* cur_db = this; cur_db != nullptr;
       cur_db = cur_db->parent_data_bag_.get()) {
    for (const auto& [alloc_id, _] : cur_db->lists_) {
      DCHECK(alloc_id.IsListsAlloc());
      lists_set.insert(alloc_id);
    }
    for (const auto& [alloc_id, dicts] : cur_db->dicts_) {
      DCHECK(alloc_id.IsDictsAlloc() || alloc_id.IsSchemasAlloc());
      dicts_set.insert(alloc_id);
    }
    for (const auto& [skey, _] : cur_db->sources_) {
      attrs_map[skey.attr].insert(skey.alloc);
    }
    for (const auto& [attr_name, source] : cur_db->small_alloc_sources_) {
      index.attrs[attr_name].with_small_allocs = true;
    }
  }
  for (const auto& [attr_name, allocs] : attrs_map) {
    std::vector<AllocationId>& vec = index.attrs[attr_name].allocations;
    vec.assign(allocs.begin(), allocs.end());
    std::sort(vec.begin(), vec.end());
  }
  index.lists.assign(lists_set.begin(), lists_set.end());
  std::sort(index.lists.begin(), index.lists.end());
  index.dicts.assign(dicts_set.begin(), dicts_set.end());
  std::sort(index.dicts.begin(), index.dicts.end());
  return index;
}

absl::StatusOr<DataBagContent::ListsContent> DataBagImpl::ListVectorToContent(
    AllocationId alloc, const DataListVector& list_vector) const {
  size_t total_size = 0;
  std::vector<const DataList*> list_ptrs(list_vector.size());
  std::vector<bool> unset_lists(list_vector.size(), false);
  for (size_t i = 0; i < list_vector.size(); ++i) {
    const DataList* l = list_vector.Get(i);
    list_ptrs[i] = l;
    if (l == nullptr) {
      unset_lists[i] = true;
    } else {
      total_size += l->size();
    }
  }
  SliceBuilder bldr(total_size);
  arolla::Buffer<int64_t>::Builder splits_bldr(list_ptrs.size() + 1);
  auto splits_inserter = splits_bldr.GetInserter();
  splits_inserter.Add(0);
  size_t pos = 0;
  for (const DataList* l : list_ptrs) {
    if (l) {
      l->AddToDataSlice(bldr, pos);
      pos += l->size();
    }
    splits_inserter.Add(pos);
  }
  ASSIGN_OR_RETURN(auto edge, arolla::DenseArrayEdge::FromSplitPoints(
                                  {std::move(splits_bldr).Build()}));
  return DataBagContent::ListsContent{alloc, std::move(bldr).Build(),
                                      std::move(edge), std::move(unset_lists)};
}

void DataBagImpl::AddDictToContent(
    ObjectId dict_id, const Dict& dict,
    std::vector<DataBagContent::DictContent>& res) const {
  std::vector<DataItem> keys = dict.GetKeys();
  if (keys.empty()) {
    return;
  }
  std::sort(keys.begin(), keys.end(), DataItem::Less());
  std::vector<DataItem> values;
  values.reserve(keys.size());
  for (size_t ki = 0; ki < keys.size(); ++ki) {
    values.push_back(*dict.Get(keys[ki]));
  }
  res.push_back({dict_id, std::move(keys), std::move(values)});
}

std::vector<DataBagContent::AttrItemContent>
DataBagImpl::ExtractSmallAllocAttrContent(absl::string_view attr_name) const {
  std::vector<DataBagContent::AttrItemContent> res;
  ConstSparseSourceArray sources;
  GetSmallAllocDataSources(attr_name, sources);
  absl::flat_hash_set<ObjectId> visited_ids;
  for (const SparseSource* source : sources) {
    for (const auto& [obj, value] : source->GetAll()) {
      if (bool inserted = visited_ids.insert(obj).second; inserted) {
        res.push_back({obj, value});
      }
    }
  }
  std::sort(res.begin(), res.end(),
            [](const DataBagContent::AttrItemContent& lhs,
               const DataBagContent::AttrItemContent& rhs) {
              return lhs.object_id < rhs.object_id;
            });
  return res;
}

absl::StatusOr<DataBagContent::AttrContent> DataBagImpl::ExtractAttrContent(
    absl::string_view attr_name, const DataBagIndex::AttrIndex& index) const {
  DataBagContent::AttrContent content;
  if (index.with_small_allocs) {
    content.items = ExtractSmallAllocAttrContent(attr_name);
  }
  for (AllocationId alloc : index.allocations) {
    ConstDenseSourceArray dense_sources;
    ConstSparseSourceArray sparse_sources;
    int64_t size = GetAttributeDataSources(alloc, attr_name, dense_sources,
                                           sparse_sources);
    if (size == 0) {
      continue;
    }
    auto objects = DataSliceImpl::ObjectsFromAllocation(alloc, size);
    ASSIGN_OR_RETURN(auto values, GetAttributeFromSources(
                                      objects, dense_sources, sparse_sources));
    content.allocs.push_back({alloc, values});
  }
  return content;
}

absl::StatusOr<DataBagStatistics> DataBagImpl::GetStatistics() const {
  const DataBagIndex index = CreateIndex();

  DataBagStatistics stats;

  // Lists
  for (AllocationId alloc : index.lists) {
    DCHECK(alloc.IsListsAlloc());
    if (const std::shared_ptr<DataListVector>* list_vector =
            DataBagImpl::GetConstListsOrNull(alloc);
        list_vector != nullptr) {
      for (size_t list_id = 0; list_id < (*list_vector)->size(); ++list_id) {
        // Only count non empty list to users for avoiding confusion because:
        // 1. Empty lists created by `kd.list_like` are not stored in the
        // data bag.
        // 2. Allocation size is usually larger than user requested and the
        // trailing parts are empty and invisible to users.
        const DataList* list = (*list_vector)->Get(list_id);
        if (list != nullptr && !list->empty()) {
          stats.total_items_in_lists += list->size();
          ++stats.total_non_empty_lists;
        }
      }
    }
  }

  stats.attr_values_sizes.reserve(index.attrs.size());
  // Dicts and explicit schemas in small allocs
  for (AllocationId alloc : index.dicts) {
    bool is_dict = alloc.IsDictsAlloc();
    bool is_schema = alloc.IsSchemasAlloc();
    DCHECK(is_dict || is_schema);
    if (is_schema && !alloc.IsSmall()) {
      continue;
    }
    if (const std::shared_ptr<DictVector>* dict_vector =
            DataBagImpl::GetConstDictsOrNull(alloc);
        dict_vector != nullptr) {
      for (size_t i = 0; i < (**dict_vector).size(); ++i) {
        if (size_t dict_size = (**dict_vector)[i].GetSizeNoFallbacks()) {
          if (is_dict) {
            stats.total_items_in_dicts += dict_size;
            ++stats.total_non_empty_dicts;
          } else {
            stats.total_explicit_schema_attrs += dict_size;
            ++stats.total_explicit_schemas;
          }
        }
      }
    }
  }

  // Attrs and explicit schemas in big allocs
  absl::flat_hash_set<internal::ObjectId> object_set;
  absl::flat_hash_map<internal::AllocationId, size_t> allocation_sizes;
  for (const auto& [attr_name, attr_index] : index.attrs) {
    size_t value_count = 0;
    size_t schemas_value_count = 0;

    if (attr_name.starts_with("__")) {
      continue;
    }
    if (attr_index.with_small_allocs) {
      ConstSparseSourceArray sources;
      GetSmallAllocDataSources(attr_name, sources);
      for (const SparseSource* source : sources) {
        for (const auto& [obj, value] : source->GetAll()) {
          object_set.insert(obj);
          if (value.has_value()) {
            ++value_count;
          }
        }
      }
    }
    for (const auto& alloc : attr_index.allocations) {
      ConstDenseSourceArray dense_sources;
      ConstSparseSourceArray sparse_sources;
      int64_t size = GetAttributeDataSources(alloc, attr_name, dense_sources,
                                             sparse_sources);

      allocation_sizes[alloc] = size;
      auto objects = DataSliceImpl::ObjectsFromAllocation(alloc, size);
      ASSIGN_OR_RETURN(
          auto values,
          GetAttributeFromSources(objects, dense_sources, sparse_sources));
      if (alloc.IsSchemasAlloc()) {
        schemas_value_count += values.present_count();
      } else {
        value_count += values.present_count();
      }
    }
    stats.total_explicit_schema_attrs += schemas_value_count;
    stats.attr_values_sizes[attr_name] += value_count;
  }

  for (ObjectId obj : object_set) {
    stats.entity_and_object_count +=
        allocation_sizes.contains(AllocationId(obj)) ? 0 : 1;
  }
  for (const auto& [alloc, size] : allocation_sizes) {
    if (alloc.IsSchemasAlloc()) {
      stats.total_explicit_schemas += size;
    } else {
      stats.entity_and_object_count += size;
    }
  }

  return stats;
}

absl::StatusOr<DataBagContent> DataBagImpl::ExtractContent(
    const DataBagIndex& index) const {
  DataBagContent content;

  // Lists
  for (AllocationId alloc : index.lists) {
    if (!alloc.IsListsAlloc()) {
      return absl::FailedPreconditionError("lists expected");
    }
    if (const std::shared_ptr<DataListVector>* list_vector =
            DataBagImpl::GetConstListsOrNull(alloc);
        list_vector != nullptr) {
      ASSIGN_OR_RETURN(content.lists.emplace_back(),
                       ListVectorToContent(alloc, **list_vector));
    }
  }

  // Dicts
  for (AllocationId alloc : index.dicts) {
    if (!alloc.IsDictsAlloc() && !alloc.IsSchemasAlloc()) {
      return absl::FailedPreconditionError("dicts or schemas expected");
    }
    if (const std::shared_ptr<DictVector>* dict_vector =
            DataBagImpl::GetConstDictsOrNull(alloc);
        dict_vector != nullptr) {
      for (size_t i = 0; i < (**dict_vector).size(); ++i) {
        AddDictToContent(alloc.ObjectByOffset(i), (**dict_vector)[i],
                         content.dicts);
      }
    }
  }

  // Attrs
  for (const auto& [attr_name, attr_index] : index.attrs) {
    ASSIGN_OR_RETURN(auto attr_content,
                     ExtractAttrContent(attr_name, attr_index));
    content.attrs.emplace(attr_name, std::move(attr_content));
  }

  return content;
}

int64_t DataBagImpl::GetApproxTotalSize() const {
  int64_t size = 0;

  auto add_sparse_source = [&size](const SparseSource& s) {
    size += static_cast<int64_t>(s.GetAll().size());
  };
  auto add_collection =
      [&size, &add_sparse_source](const SourceCollection& collection) {
        if (collection.mutable_dense_source != nullptr) {
          size += collection.mutable_dense_source->size();
        }
        if (collection.const_dense_source != nullptr) {
          size += collection.const_dense_source->size();
        }
        if (collection.mutable_sparse_source != nullptr) {
          add_sparse_source(*collection.mutable_sparse_source);
        }
      };

  auto add_lists = [&size](const DataListVector& lists) {
    for (size_t i = 0; i < lists.size(); ++i) {
      const DataList* list = lists.Get(i);
      if (list) {
        size += static_cast<int64_t>(list->size());
      }
    }
  };

  auto add_dicts = [&size](const DictVector& dicts) {
    for (size_t i = 0; i < dicts.size(); ++i) {
      size += static_cast<int64_t>(dicts[i].GetSizeNoFallbacks());
    }
  };

  for (const auto* db = this; db != nullptr; db = db->parent_data_bag_.get()) {
    for (const auto& [key, source_collection] : db->sources_) {
      add_collection(source_collection);
    }
    for (const auto& [key, source] : db->small_alloc_sources_) {
      add_sparse_source(source);
    }
    for (const auto& [key, lists] : db->lists_) {
      add_lists(*lists);
    }
    for (const auto& [key, dicts] : db->dicts_) {
      add_dicts(*dicts);
    }
  }
  return size;
}

}  // namespace koladata::internal
