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
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_list.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/data_slice_accessors.h"
#include "koladata/internal/dense_source.h"
#include "koladata/internal/dict.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/has.h"
#include "koladata/internal/op_utils/presence_or.h"
#include "koladata/internal/sparse_source.h"
#include "koladata/internal/uuid_object.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/refcount_ptr.h"
#include "arolla/util/text.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

namespace {

constexpr size_t kSparseSourceSparsityCoef = 64;
ABSL_CONST_INIT const DataList kEmptyList;

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
        absl::StrCat("cannot get or set attributes on schema constants: ",
                     schema_item));
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

// Merges the given sparse sources ordered by priority into a mutable dense
// source.
// Conflicts are resolved according to MergeOptions.
absl::Status MergeToMutableDenseSourceOnlySparse(
    DenseSource& result, absl::Span<const SparseSource* const> sources,
    MergeOptions options) {
  DCHECK(result.IsMutable());
  auto process_source = [&](const SparseSource* source,
                            auto skip_obj_fn) -> absl::Status {
    for (const auto& [key, item] : source->GetAll()) {
      if (!item.has_value() || skip_obj_fn(key)) {
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
                 this_result != item) {
        return absl::FailedPreconditionError(
            absl::StrCat("conflict ", key, ": ", this_result, " vs ", item));
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
    MergeOptions options) {
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
                 *this_result != item) {
        return absl::FailedPreconditionError(
            absl::StrCat("conflict ", key, ": ", *this_result, " vs ", item));
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
    DenseSource& result, AllocationId alloc,
    const DenseSource& dense_source,
    absl::Span<const SparseSource* const> sparse_sources,
    MergeOptions options) {
  DCHECK(result.IsMutable());
  int64_t size = std::min<int64_t>(result.size(), dense_source.size());
  if (options.data_conflict_policy == MergeOptions::kOverwrite &&
      sparse_sources.empty()) {
    DCHECK_EQ(dense_source.allocation_id(), alloc);
    // TODO: Change to `result.Merge(dense_source, kOverwrite)`
    //   and try to optimize other cases as well.
    return result.MergeOverwrite(dense_source);
  }

  auto objects = DataSliceImpl::ObjectsFromAllocation(alloc, size);
  ASSIGN_OR_RETURN(
      auto other_items,
      GetAttributeFromSources(objects, {&dense_source}, sparse_sources));

  if (options.data_conflict_policy == MergeOptions::kOverwrite) {
    const auto& objects_array = objects.values<ObjectId>();
    return other_items.VisitValues([&](const auto& items_array) {
      return result.Set(
          arolla::DenseArray<ObjectId>{objects_array.values, items_array.bitmap,
                                       items_array.bitmap_bit_offset},
          DataSliceImpl::CreateWithAllocIds(
              AllocationIdSet(),  // AllocationIdSet is not used.
              items_array));
    });
  }

  for (int64_t offset = 0; offset < size; ++offset) {
    auto obj_id = alloc.ObjectByOffset(offset);
    // TODO: try to use batch `GetAttributeFromSources`.
    auto other_item = other_items[offset];
    if (!other_item.has_value()) {
      continue;
    }
    if (auto this_result = result.Get(obj_id); !this_result.has_value()) {
      RETURN_IF_ERROR(result.Set(obj_id, other_item));
    } else {
      if (options.data_conflict_policy == MergeOptions::kRaiseOnConflict &&
          this_result != other_item) {
        return absl::FailedPreconditionError(absl::StrCat(
            "conflict ", obj_id, ": ", this_result, " vs ", other_item));
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
  LOG(FATAL) << "Invalid enum value encountered: " << option;
  return MergeOptions::kRaiseOnConflict;
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

// *******  Factory interface

/*static*/ DataBagImplPtr DataBagImpl::CreateEmptyDatabag() {
  return DataBagImplPtr::Make(PrivateConstructorTag{});
}

DataBagImplPtr DataBagImpl::PartiallyPersistentFork() const {
  auto res_db = DataBagImpl::CreateEmptyDatabag();
  res_db->parent_data_bag_ = sources_.empty() && small_alloc_sources_.empty() &&
                                     lists_.empty() && dicts_.empty()
                                 ? parent_data_bag_
                                 : DataBagImplConstPtr::NewRef(this);
  return res_db;
}

// *******  Const interface

DataItem DataBagImpl::LookupAttrInDataSourcesMap(ObjectId object_id,
                                                 absl::string_view attr) const {
  const DataBagImpl* cur_data_bag = this;
  AllocationId alloc_id(object_id);
  SourceKeyView search_key{alloc_id, attr};
  size_t search_hash = absl::HashOf(search_key);
  while (cur_data_bag != nullptr) {
    if (auto it = cur_data_bag->sources_.find(search_key, search_hash);
        it != cur_data_bag->sources_.end()) {
      const SourceCollection& collection = it->second;
      // mutable source overrides const source if both present.
      if (auto* s = collection.mutable_sparse_source.get(); s != nullptr) {
        std::optional<DataItem> res = s->Get(object_id);
        if (res.has_value()) {
          return *res;
        }
      }
      if (auto* s = collection.mutable_dense_source.get(); s != nullptr) {
        DCHECK_EQ(collection.const_dense_source, nullptr);
        return s->Get(object_id);
      }
      if (auto* s = collection.const_dense_source.get(); s != nullptr) {
        return s->Get(object_id);
      }
      cur_data_bag = collection.lookup_parent
                         ? cur_data_bag->parent_data_bag_.get()
                         : nullptr;
    } else {
      cur_data_bag = cur_data_bag->parent_data_bag_.get();
    }
  }
  return DataItem();
}

SparseSource& DataBagImpl::GetMutableSmallAllocSource(absl::string_view attr) {
  return small_alloc_sources_[attr];
}

absl::StatusOr<DataSliceImpl> DataBagImpl::GetAttrFromSources(
    const DataSliceImpl& objects, absl::string_view attr) const {
  ConstDenseSourceArray dense_sources;
  ConstSparseSourceArray sparse_sources;
  if (objects.allocation_ids().contains_small_allocation_id()) {
    GetSmallAllocDataSources(attr, sparse_sources);
  }
  if (objects.allocation_ids().empty() && sparse_sources.empty()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(objects.size());
  }
  for (AllocationId alloc_id : objects.allocation_ids()) {
    GetAttributeDataSources(alloc_id, attr, dense_sources, sparse_sources);
  }
  return GetAttributeFromSources(objects, dense_sources, sparse_sources);
}

absl::StatusOr<DataSliceImpl> DataBagImpl::GetAttr(
    const DataSliceImpl& objects, absl::string_view attr,
    FallbackSpan fallbacks) const {
  if (objects.is_empty_and_unknown()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(objects.size());
  }
  ASSIGN_OR_RETURN(auto result, GetAttrFromSources(objects, attr));
  if (fallbacks.empty()) {
    return result;
  }

  if (result.is_empty_and_unknown()) {
    return fallbacks[0]->GetAttr(objects, attr, fallbacks.subspan(1));
  }

  auto presence_result = PresenceDenseArray(result);
  if (presence_result.PresentCount() == objects.present_count()) {
    return result;
  }

  for (const DataBagImpl* fallback : fallbacks) {
    ASSIGN_OR_RETURN(auto fb_result,
                     fallback->GetAttrFromSources(objects, attr));
    ASSIGN_OR_RETURN(result, PresenceOrOp{}(result, fb_result));
  }
  return result;
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
  AllocationId alloc_id(object_id);
  if (alloc_id.IsSmall()) {
    auto result = LookupAttrInDataItemMap(object_id, attr);
    if (result.has_value() || fallbacks.empty()) {
      return result;
    }
    for (const DataBagImpl* fallback : fallbacks) {
      if (auto item = fallback->LookupAttrInDataItemMap(object_id, attr);
          item.has_value()) {
        return item;
      }
    }
    return DataItem();
  }

  auto result = LookupAttrInDataSourcesMap(object_id, attr);
  if (result.has_value() || fallbacks.empty()) {
    return result;
  }
  for (const DataBagImpl* fallback : fallbacks) {
    if (auto item = fallback->LookupAttrInDataSourcesMap(object_id, attr);
        item.has_value()) {
      return item;
    }
  }
  return DataItem();
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

absl::StatusOr<DataSliceImpl> DataBagImpl::CreateObjectsFromFields(
    const std::vector<absl::string_view>& attr_names,
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
    return DataSliceImpl::CreateAllMissingObjectDataSlice(0);
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
    }
    sources_.emplace(SourceKey{alloc_id, std::string(attr_names[i])},
                     SourceCollection{.const_dense_source = std::move(source),
                                      .lookup_parent = false});
  }
  return objects;
}

absl::StatusOr<DataItem> DataBagImpl::CreateObjectsFromFields(
    const std::vector<absl::string_view>& attr_names,
    const std::vector<std::reference_wrapper<const DataItem>>& items) {
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
    DataSliceImpl::Builder slice_bldr(data->size());
    for (const auto& [obj, data_item] : *data) {
      objs_bldr.Set(offset, obj);
      slice_bldr.Insert(offset, data_item);
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
  for (AllocationId alloc_id : objects.allocation_ids()) {
    SourceCollection& collection = GetOrCreateSourceCollection(alloc_id, attr);
    if (!collection.mutable_dense_source) {
      if (objects.size() <= alloc_id.Capacity() / kSparseSourceSparsityCoef) {
        if (!collection.mutable_sparse_source) {
          collection.mutable_sparse_source =
              std::make_shared<SparseSource>(alloc_id);
        }
        RETURN_IF_ERROR(collection.mutable_sparse_source->Set(
            objects.values<ObjectId>(), values));
        continue;
      }
      const arolla::QType* qtype = values.dtype() == arolla::GetNothingQType() ?
          nullptr : values.dtype();
      RETURN_IF_ERROR(CreateMutableDenseSource(collection, alloc_id, attr,
                                               qtype, alloc_id.Capacity()));
    }
    RETURN_IF_ERROR(collection.mutable_dense_source->Set(
        objects.values<ObjectId>(), values));
  }
  if (objects.allocation_ids().contains_small_allocation_id()) {
    auto& source = GetMutableSmallAllocSource(attr);
    return source.Set(objects.values<ObjectId>(), values);
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
  if (collection.mutable_dense_source) {
    return collection.mutable_dense_source->Set(object_id, value);
  } else if (alloc_id.Capacity() < kSparseSourceSparsityCoef) {
    const arolla::QType* qtype = value.has_value() ? value.dtype() : nullptr;
    RETURN_IF_ERROR(CreateMutableDenseSource(collection, alloc_id, attr, qtype,
                                             alloc_id.Capacity()));
    return collection.mutable_dense_source->Set(object_id, value);
  }
  if (!collection.mutable_sparse_source) {
    collection.mutable_sparse_source = std::make_shared<SparseSource>(alloc_id);
  }
  collection.mutable_sparse_source->Set(object_id, value);
  return absl::OkStatus();
}

// ************************* List functions

class DataBagImpl::ReadOnlyListGetter {
 public:
  explicit ReadOnlyListGetter(const DataBagImpl* bag) : bag_(bag) {}

  const DataList& operator()(ObjectId list_id) {
    AllocationId alloc_id(list_id);
    if (alloc_id != current_alloc_) {
      if (ABSL_PREDICT_FALSE(!alloc_id.IsListsAlloc())) {
        status_ = absl::FailedPreconditionError("lists expected");
        return kEmptyList;
      }
      lists_vec_ = bag_->GetConstListsOrNull(alloc_id);
      current_alloc_ = alloc_id;
    }
    return lists_vec_ ? (*lists_vec_)->Get(list_id.Offset()) : kEmptyList;
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
    auto it = bag->lists_.find(alloc_id, alloc_hash);
    if (it != bag->lists_.end()) {
      return &it->second;
    }
    bag = bag->parent_data_bag_.get();
  }
  return nullptr;
}

const DataList& DataBagImpl::GetFirstPresentList(ObjectId list_id,
                                                 FallbackSpan fallbacks) const {
  const DataList* result = &kEmptyList;

  AllocationId alloc_id(list_id);
  size_t alloc_hash = absl::HashOf(alloc_id);

  if (const auto* lists = GetConstListsOrNull(alloc_id, alloc_hash);
      lists != nullptr) {
    result = &(*lists)->Get(list_id.Offset());
  }

  if (!result->empty() || fallbacks.empty()) {
    return *result;
  }

  for (const DataBagImpl* fallback : fallbacks) {
    if (const auto* lists = fallback->GetConstListsOrNull(alloc_id, alloc_hash);
        lists != nullptr) {
      result = &(*lists)->Get(list_id.Offset());
    }
    if (!result->empty()) {
      return *result;
    }
  }

  return *result;
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
  if (const DataList& list = list_getter(list_id); !list.empty()) {
    return list;
  }

  for (ReadOnlyListGetter& fallback_list_getter : fallback_list_getters) {
    if (const DataList& list = fallback_list_getter(list_id); !list.empty()) {
      return list;
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
    const DataSliceImpl& lists, FallbackSpan fallbacks) const {
  if (lists.is_empty_and_unknown()) {
    return arolla::CreateEmptyDenseArray<int64_t>(lists.size());
  }
  if (lists.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("lists expected");
  }

  ReadOnlyListGetter list_getter(this);

  arolla::DenseArray<int64_t> res;

  if (fallbacks.empty()) {
    auto op = arolla::CreateDenseOp([&](ObjectId list_id) -> int64_t {
      return list_getter(list_id).size();
    });
    res = op(lists.values<ObjectId>());
  } else {
    std::vector<ReadOnlyListGetter> fallback_list_getters =
        DataBagImpl::CreateFallbackListGetters(fallbacks);
    auto op = arolla::CreateDenseOp([&](ObjectId list_id) -> int64_t {
      return GetFirstPresentList(list_id, list_getter,
                                 absl::MakeSpan(fallback_list_getters))
          .size();
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
  DataSliceImpl::Builder bldr(lists.size());

  auto set_from_list = [&](int64_t offset, const DataList& list, int64_t pos) {
    if (pos < 0) {
      pos += list.size();
    }
    if (pos >= 0 && pos < list.size()) {
      bldr.Insert(offset, list.Get(pos));
    }
    // Note: we don't return an error if `pos` is out of range.
  };

  if (fallbacks.empty()) {
    RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
        [&](int64_t offset, ObjectId list_id, int64_t pos) {
          const DataList& list = list_getter(list_id);
          set_from_list(offset, list, pos);
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
  DataSliceImpl::Builder bldr(lists.size());

  RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
      [&](int64_t offset, ObjectId list_id, int64_t pos) {
        DataList* list = list_getter(list_id);
        if (list != nullptr) {
          if (pos < 0) {
            pos += list->size();
          }
          if (pos >= 0 && pos < list->size()) {
            bldr.Insert(offset, list->Get(pos));
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
            const DataList& list = list_getter(list_id);
            process_list(offset, list);
          }
          split_points_bldr.Set(offset + 1, cum_size);
        });
  } else {
    std::vector<ReadOnlyListGetter> fallback_list_getters =
        DataBagImpl::CreateFallbackListGetters(fallbacks);

    lists.values<ObjectId>().ForEach(
        [&](int64_t offset, bool present, ObjectId list_id) {
          if (present) {
            const DataList& list = GetFirstPresentList(
                list_id, list_getter, absl::MakeSpan(fallback_list_getters));
            process_list(offset, list);
          }
          split_points_bldr.Set(offset + 1, cum_size);
        });
  }

  RETURN_IF_ERROR(list_getter.status());

  arolla::Buffer<int64_t> split_points = std::move(split_points_bldr).Build();
  DataSliceImpl::Builder slice_bldr(cum_size);
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
  DataSliceImpl::Builder bldr(to - from);
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
  if (values.size() == 0) {
    return absl::OkStatus();
  }
  DataList& dlist = GetOrCreateMutableLists(AllocationId(list_id))
                        .GetMutable(list_id.Offset());
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
    auto it = bag->dicts_.find(alloc_id, alloc_hash);
    if (it != bag->dicts_.end()) {
      return &it->second;
    }
    bag = bag->parent_data_bag_.get();
  }
  return nullptr;
}

DictVector& DataBagImpl::GetOrCreateMutableDicts(AllocationId alloc_id) {
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
      it->second = std::make_shared<DictVector>(alloc_id.Capacity());
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

absl::StatusOr<std::pair<DataSliceImpl, arolla::DenseArrayEdge>>
DataBagImpl::GetDictKeys(const DataSliceImpl& dicts,
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
  std::vector<std::vector<DataItem>> keys(dicts.size());
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
        std::vector<DataItem>& keys_vec = keys[offset];
        if (fallbacks.empty()) {
          keys_vec = dict_getter(dict_id).GetKeys();
        } else {
          fallback_dicts.clear();
          for (auto& fallback_getter : dict_fallback_getters) {
            if (auto* fb_dict = &fallback_getter(dict_id);
                fb_dict !=
                &ReadOnlyDictGetter<DictsAllocCheckFn>::GetEmptyDict()) {
              fallback_dicts.push_back(fb_dict);
            }
          }
          keys_vec = dict_getter(dict_id).GetKeys(fallback_dicts);
        }
        split_points.push_back(split_points.back() + keys_vec.size());
      });
  RETURN_IF_ERROR(dict_getter.status());

  DataSliceImpl::Builder bldr(split_points.back());
  for (int64_t i = 0; i < keys.size(); ++i) {
    const auto& keys_vec = keys[i];
    for (int64_t j = 0; j < keys_vec.size(); ++j) {
      bldr.Insert(split_points[i] + j, keys_vec[j]);
    }
  }

  auto split_points_buf =
      arolla::Buffer<int64_t>::Create(std::move(split_points));
  ASSIGN_OR_RETURN(auto edge, arolla::DenseArrayEdge::FromSplitPoints(
                                  {std::move(split_points_buf)}));
  return std::make_pair(std::move(bldr).Build(), std::move(edge));
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
absl::StatusOr<DataSliceImpl> DataBagImpl::GetFromDictNoFallback(
    const arolla::DenseArray<ObjectId>& dicts,
    const DataSliceImpl& keys) const {
  if (dicts.size() != keys.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("dicts and keys sizes don't match: %d vs %d",
                        dicts.size(), keys.size()));
  }

  ReadOnlyDictGetter<AllocCheckFn> dict_getter(this);
  DataSliceImpl::Builder bldr(dicts.size());

  if (keys.is_mixed_dtype()) {
    dicts.ForEachPresent([&](int64_t offset, ObjectId dict_id) {
      bldr.Insert(offset, dict_getter(dict_id).Get(keys[offset]));
    });
  } else {
    absl::Status status = absl::OkStatus();
    keys.VisitValues([&](const auto& vec) {
      using T = typename std::decay_t<decltype(vec)>::base_type;
      status = arolla::DenseArraysForEachPresent(
          [&](int64_t offset, ObjectId dict_id, arolla::view_type_t<T> key) {
            bldr.Insert(offset,
                        dict_getter(dict_id).Get(DataItem::View<T>{key}));
          },
          dicts, vec);
    });
    RETURN_IF_ERROR(status);
  }

  RETURN_IF_ERROR(dict_getter.status());
  return std::move(bldr).Build();
}

template <class AllocCheckFn>
absl::StatusOr<DataSliceImpl> DataBagImpl::GetFromDictNoFallback(
    const arolla::DenseArray<ObjectId>& dicts, const DataItem& keys) const {
  ReadOnlyDictGetter<AllocCheckFn> dict_getter(this);
  DataSliceImpl::Builder bldr(dicts.size());

  keys.VisitValue([&](const auto& val) {
    using T = typename std::decay_t<decltype(val)>;
    dicts.ForEachPresent([&](int64_t offset, ObjectId dict_id) {
      bldr.Insert(offset, dict_getter(dict_id).Get(DataItem::View<T>{val}));
    });
  });

  RETURN_IF_ERROR(dict_getter.status());
  return std::move(bldr).Build();
}

template <class AllocCheckFn, class DataSliceImplT>
inline absl::StatusOr<DataSliceImpl> DataBagImpl::GetFromDictImpl(
    const DataSliceImpl& dicts, const DataSliceImplT& keys,
    FallbackSpan fallbacks) const {
  const auto& dict_objects = dicts.values<ObjectId>();
  ASSIGN_OR_RETURN(auto result,
                   GetFromDictNoFallback<AllocCheckFn>(dict_objects, keys));
  if (fallbacks.empty()) {
    return result;
  }

  for (const DataBagImpl* fallback : fallbacks) {
    // TODO: avoid requesting already known objects.
    ASSIGN_OR_RETURN(
        auto fb_result,
        fallback->GetFromDictNoFallback<AllocCheckFn>(dict_objects, keys));
    ASSIGN_OR_RETURN(result, PresenceOrOp{}(result, fb_result));
  }
  return result;
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

std::vector<DataItem> DataBagImpl::GetDictKeysAsVector(
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

  if (fallbacks.empty()) {
    return (**main_dicts)[dict_id.Offset()].GetKeys();
  }

  absl::InlinedVector<const Dict*, 1> fallback_dicts;
  for (const DataBagImpl* fallback : fallbacks) {
    const std::shared_ptr<DictVector>* fb_dicts =
        fallback->GetConstDictsOrNull(alloc_id);
    if (fb_dicts != nullptr) {
      fallback_dicts.push_back(&(**fb_dicts)[dict_id.Offset()]);
    }
  }
  return (**main_dicts)[dict_id.Offset()].GetKeys(fallback_dicts);
}

absl::StatusOr<std::pair<DataSliceImpl, arolla::DenseArrayEdge>>
DataBagImpl::GetDictKeys(const DataItem& dict, FallbackSpan fallbacks) const {
  if (!dict.has_value()) {
    ASSIGN_OR_RETURN(auto empty_edge,
                     arolla::DenseArrayEdge::FromUniformGroups(1, 0));
    return std::make_pair(DataSliceImpl::CreateEmptyAndUnknownType(0),
                          empty_edge);
  }
  ASSIGN_OR_RETURN(ObjectId dict_id, ItemToDictObjectId(dict));
  auto keys = GetDictKeysAsVector(dict_id, fallbacks);
  ASSIGN_OR_RETURN(auto edge,
                   arolla::DenseArrayEdge::FromUniformGroups(1, keys.size()));
  return std::pair<DataSliceImpl, arolla::DenseArrayEdge>{
      DataSliceImpl::Create(std::move(keys)), std::move(edge)};
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
    size = GetDictKeysAsVector(dict_id, fallbacks).size();
  }
  return DataItem(static_cast<int64_t>(size));
}

template <typename Key>
ABSL_ATTRIBUTE_ALWAYS_INLINE DataItem
DataBagImpl::GetFromDictObject(ObjectId dict_id, const Key& key) const {
  AllocationId alloc_id(dict_id);
  if (const auto* dicts = GetConstDictsOrNull(alloc_id); dicts != nullptr) {
    return (**dicts)[dict_id.Offset()].Get(key);
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
  size_t key_hash = DataItem::Hash()(key);
  int64_t offset = dict_id.Offset();
  if (const auto* dicts = GetConstDictsOrNull(alloc_id, alloc_hash);
      dicts != nullptr) {
    DataItem res = (**dicts)[offset].Get(key, key_hash);
    if (res.has_value() || fallbacks.empty()) {
      return res;
    }
  }
  for (const DataBagImpl* fallback : fallbacks) {
    if (const auto* dicts = fallback->GetConstDictsOrNull(alloc_id, alloc_hash);
        dicts != nullptr) {
      DataItem res = (**dicts)[offset].Get(key, key_hash);
      if (res.has_value()) {
        return res;
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
  if (!schema_item.holds_value<ObjectId>()) {
    if (!schema_item.has_value()) {
      return DataSliceImpl::CreateEmptyAndUnknownType(0);
    }
    return InvalidSchemaObjectId(schema_item);
  }
  ASSIGN_OR_RETURN(ObjectId schema_id, ItemToSchemaObjectId(schema_item));
  std::vector<DataItem> keys = GetDictKeysAsVector(schema_id, fallbacks);
  if (keys.empty()) {
    return DataSliceImpl::Create(
        arolla::CreateEmptyDenseArray<arolla::Text>(/*size=*/0));
  }
  return DataSliceImpl::Create(keys);
}

absl::StatusOr<DataItem> DataBagImpl::GetSchemaAttr(
    const DataItem& schema_item, absl::string_view attr,
    FallbackSpan fallbacks) const {
  ASSIGN_OR_RETURN(auto res,
                   GetSchemaAttrAllowMissing(schema_item, attr, fallbacks));
  if (!res.has_value() && schema_item.has_value()) {
    return absl::InvalidArgumentError(
        absl::StrCat("the attribute '", attr, "' is missing"));
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
  ASSIGN_OR_RETURN(ObjectId schema_id, ItemToSchemaObjectId(schema_item));
  return GetFromDictObjectWithFallbacks(schema_id,
                                        DataItem::View<arolla::Text>(attr),
                                        fallbacks);
}

absl::StatusOr<DataSliceImpl> DataBagImpl::GetSchemaAttr(
    const DataSliceImpl& schema_slice, absl::string_view attr,
    FallbackSpan fallbacks) const {
  ASSIGN_OR_RETURN(auto result,
                   GetSchemaAttrAllowMissing(schema_slice, attr, fallbacks));
  if (result.present_count() != schema_slice.present_count()) {
    return absl::InvalidArgumentError(
        absl::StrCat("the attribute '", attr, "' is missing"));
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
  return GetFromDictImpl<SchemasAllocCheckFn>(
      schema_slice, DataItem(arolla::Text(attr)), fallbacks);
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
  RETURN_IF_ERROR(VerifyIsSchema(value));
  ASSIGN_OR_RETURN(ObjectId schema_id, ItemToSchemaObjectId(schema_item));
  GetOrCreateMutableDict(schema_id).Set(DataItem::View<arolla::Text>(attr),
                                        value);
  return absl::OkStatus();
}

absl::Status DataBagImpl::SetSchemaAttr(const DataSliceImpl& schema_slice,
                                        absl::string_view attr,
                                        const DataItem& value) {
  RETURN_IF_ERROR(VerifyIsSchema(value));
  if (schema_slice.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  if (schema_slice.dtype() != arolla::GetQType<ObjectId>()) {
    return InvalidLhsSetSchemaAttrError(schema_slice);
  }

  MutableDictGetter<SchemasAllocCheckFn> schema_dict_getter(this);
  absl::Status status = absl::OkStatus();
  schema_slice.values<ObjectId>().ForEachPresent(
      [&](int64_t /*id*/, ObjectId schema_id) {
        Dict* schema_dict = schema_dict_getter(schema_id);
        if (ABSL_PREDICT_FALSE(schema_dict == nullptr)) {
          status = InvalidLhsSetSchemaAttrError(schema_slice);
          return;
        }
        schema_dict->Set(DataItem::View<arolla::Text>(attr), value);
      });
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

  MutableDictGetter<SchemasAllocCheckFn> schema_dict_getter(this);

  if (values.is_single_dtype()) {
    return values.VisitValues([&]<class T>(const T& vec) -> absl::Status {
      using ValueT = typename T::base_type;
      if constexpr (std::is_same_v<ValueT, schema::DType> ||
                    std::is_same_v<ValueT, ObjectId>) {
        absl::Status status = absl::OkStatus();
        RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
            [&](int64_t /*id*/, ObjectId schema_id,
                arolla::OptionalValue<ValueT> value) {
              if constexpr (std::is_same_v<ValueT, ObjectId>) {
                if (value.present && !value.value.IsSchema()) {
                  status = InvalidRhsSetSchemaAttrError(values);
                  return;
                }
              }
              Dict* schema_dict = schema_dict_getter(schema_id);
              if (ABSL_PREDICT_FALSE(schema_dict == nullptr)) {
                status = InvalidLhsSetSchemaAttrError(schema_slice);
                return;
              }
              schema_dict->Set(
                  DataItem::View<arolla::Text>(attr),
                  value.present ? DataItem(value.value) : DataItem());
            }, schema_slice.values<ObjectId>(), vec));
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
          if (value.has_value() && !VerifyIsSchema(value).ok()) {
            status = InvalidRhsSetSchemaAttrError(values);
            return;
          }
          Dict* schema_dict = schema_dict_getter(schema_id);
          if (ABSL_PREDICT_FALSE(schema_dict == nullptr)) {
            status = InvalidLhsSetSchemaAttrError(schema_slice);
            return;
          }
          schema_dict->Set(DataItem::View<arolla::Text>(attr), value);
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
  ASSIGN_OR_RETURN(ObjectId schema_id, ItemToSchemaObjectId(schema_item));
  // Setting None as a schema attribute is not allowed through SetSchemaAttr
  // API, only through `del schema.attr`.
  GetOrCreateMutableDict(schema_id).Set(DataItem::View<arolla::Text>(attr),
                                        DataItem());
  return absl::OkStatus();
}

absl::Status DataBagImpl::DelSchemaAttr(const DataSliceImpl& schema_slice,
                                        absl::string_view attr) {
  RETURN_IF_ERROR(GetSchemaAttr(schema_slice, attr).status());
  return SetSchemaAttr(schema_slice, attr,
                       DataSliceImpl::Create(schema_slice.size(), DataItem()));
}

template <typename ImplT>
absl::Status SetSchemaFields(
    const ImplT&,  const std::vector<absl::string_view>& attr_names,
    const std::vector<std::reference_wrapper<const DataItem>>& items) {
  static_assert(false,
                "SetSchemaFields is not supported for ImplT not in "
                "{DataSliceImpl, DataItem}");
}

template<>
absl::Status DataBagImpl::SetSchemaFields(
    const DataItem& schema_item,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<std::reference_wrapper<const DataItem>>& items) {
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
  auto& schema_dict = GetOrCreateMutableDict(schema_item.value<ObjectId>());
  for (int i = 0; i < attr_names.size(); ++i) {
    schema_dict.Set(DataItem::View<arolla::Text>(attr_names[i]), items[i]);
  }
  return absl::OkStatus();
}

template <>
absl::Status DataBagImpl::SetSchemaFields(
    const DataSliceImpl& schema_slice,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<std::reference_wrapper<const DataItem>>& items) {
  DCHECK_EQ(attr_names.size(), items.size());
  if (schema_slice.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  for (const auto& item : items) {
    RETURN_IF_ERROR(VerifyIsSchema(item.get()));
  }

  MutableDictGetter<SchemasAllocCheckFn> schema_dict_getter(this);
  absl::Status status = absl::OkStatus();
  schema_slice.values<ObjectId>().ForEachPresent(
      [&](int64_t, ObjectId schema_obj) {
        if (!status.ok()) {
          return;
        }
        // TODO: Potentially optimize with something like
        // schema_dict_ptr->Update(dict), instead of separately setting all
        // attributes of all dicts.
        auto* schema_dict_ptr = schema_dict_getter(schema_obj);
        if (schema_dict_ptr == nullptr) {
          status = schema_dict_getter.status();
          return;
        }
        for (int i = 0; i < attr_names.size(); ++i) {
            schema_dict_ptr->Set(DataItem::View<arolla::Text>(attr_names[i]),
                                 items[i]);
        }
      });
  return status;
}

template <typename ImplT>
absl::Status OverwriteSchemaFields(
    const ImplT&,  const std::vector<absl::string_view>& attr_names,
    const std::vector<std::reference_wrapper<const DataItem>>& items) {
  static_assert(false,
                "OverwriteSchemaFields is not supported for ImplT not in "
                "{DataSliceImpl, DataItem}");
}

template <>
absl::Status DataBagImpl::OverwriteSchemaFields(
    const DataItem& schema_item,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<std::reference_wrapper<const DataItem>>& items) {
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
  auto& schema_dict = GetOrCreateMutableDict(schema_item.value<ObjectId>());
  schema_dict.Clear();
  for (int i = 0; i < attr_names.size(); ++i) {
    schema_dict.Set(DataItem::View<arolla::Text>(attr_names[i]), items[i]);
  }
  return absl::OkStatus();
}

template <>
absl::Status DataBagImpl::OverwriteSchemaFields(
    const DataSliceImpl& schema_slice,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<std::reference_wrapper<const DataItem>>& items) {
  DCHECK_EQ(attr_names.size(), items.size());
  if (schema_slice.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  for (const auto& item : items) {
    RETURN_IF_ERROR(VerifyIsSchema(item.get()));
  }

  MutableDictGetter<SchemasAllocCheckFn> schema_dict_getter(this);
  Dict* schema_dict_first = nullptr;
  absl::Status status = absl::OkStatus();
  schema_slice.values<ObjectId>().ForEachPresent(
      [&](int64_t, ObjectId schema_obj) {
        if (!status.ok()) {
          return;
        }
        auto* schema_dict_ptr = schema_dict_getter(schema_obj);
        if (schema_dict_ptr == nullptr) {
          status = schema_dict_getter.status();
          return;
        }
        if (schema_dict_first == nullptr) {
          schema_dict_first = schema_dict_ptr;
          schema_dict_first->Clear();
          for (int i = 0; i < attr_names.size(); ++i) {
            schema_dict_first->Set(DataItem::View<arolla::Text>(attr_names[i]),
                                   items[i]);
          }
        } else {
          // Copy the schema_dict_first to all other schemas in `schema_slice`.
          *schema_dict_ptr = *schema_dict_first;
        }
      });
  return status;
}

absl::StatusOr<DataItem> DataBagImpl::CreateExplicitSchemaFromFields(
    const std::vector<absl::string_view>& attr_names,
    const std::vector<std::reference_wrapper<const DataItem>>& items) {
  DCHECK_EQ(attr_names.size(), items.size());
  auto schema_id = internal::AllocateExplicitSchema();
  RETURN_IF_ERROR(
      OverwriteSchemaFields(DataItem(schema_id), attr_names, items));
  return DataItem(schema_id);
}

absl::StatusOr<DataItem> DataBagImpl::CreateUuSchemaFromFields(
    absl::string_view seed,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<std::reference_wrapper<const DataItem>>& items) {
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
          if (!other_item.has_value() || skip_object_id(obj_id)) {
            continue;
          }
          DataItem this_value;
          if (options.data_conflict_policy != MergeOptions::kOverwrite) {
            this_value =
                  GetAttributeFromSources(obj_id, {}, this_sources);
          }
          if (this_value.has_value()) {
            if (options.data_conflict_policy ==
                    MergeOptions::kRaiseOnConflict &&
                this_value != other_item) {
              return absl::FailedPreconditionError(
                  absl::StrCat("conflicting values for ", attr_name, " for ",
                               obj_id, ": ", this_value, " vs ", other_item));
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
          // This is necessary to avoid references into data from a different
          // data bag.
          RETURN_IF_ERROR(other_db->CreateMutableDenseSource(
              other_source_collection_copy, alloc, attr_name,
              // QType is not needed if dense source is present
              nullptr, alloc.Capacity()));
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
              options));
          continue;
        }
        DCHECK_EQ(other_dense_sources.size(), 1);
        RETURN_IF_ERROR(MergeToMutableDenseSource(
            *this_collection.mutable_dense_source, alloc,
            *other_dense_sources.front(), other_sparse_sources, options));
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
            this_sparse_sources, ReverseMergeOptions(options)));
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
          *this_collection.mutable_sparse_source, other_sparse_sources,
          options));
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
        const auto& other_list = other_lists->Get(i);
        if (other_list.empty()) {
          continue;
        }
        auto& this_list = this_lists.GetMutable(i);
        if (options.data_conflict_policy == MergeOptions::kOverwrite ||
            this_list.empty()) {
          this_list = other_list;
          continue;
        }
        if (options.data_conflict_policy == MergeOptions::kRaiseOnConflict) {
          if (this_list.size() != other_list.size()) {
            return absl::FailedPreconditionError(
                absl::StrCat("conflicting list sizes for ", alloc_id, ": ",
                             this_list.size(), " vs ", other_list.size()));
          }
          for (size_t j = 0; j < other_list.size(); ++j) {
            if (this_list[j] != other_list[j]) {
              return absl::FailedPreconditionError(absl::StrCat(
                  "conflicting list values for ", alloc_id, "at index ", j,
                  ": ", this_list[j], " vs ", other_list[j]));
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
            this_dict.Set(key, other_dict.Get(key));
            continue;
          }
          const DataItem& other_value = other_dict.Get(key);
          if (!other_value.has_value()) {
            continue;
          }
          const DataItem& this_value = this_dict.GetOrAssign(key, other_value);
          if (conflict_policy == MergeOptions::kRaiseOnConflict &&
              this_value != other_value) {
            return absl::FailedPreconditionError(absl::StrCat(
                "conflicting dict values for ", alloc_id.ObjectByOffset(i),
                " key", key, ": ", this_value, " vs ", other_value));
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
  for (size_t i = 0; i < list_vector.size(); ++i) {
    const DataList& l = list_vector.Get(i);
    list_ptrs[i] = &l;
    total_size += l.size();
  }
  DataSliceImpl::Builder bldr(total_size);
  arolla::Buffer<int64_t>::Builder splits_bldr(list_ptrs.size() + 1);
  auto splits_inserter = splits_bldr.GetInserter();
  splits_inserter.Add(0);
  size_t pos = 0;
  for (const DataList* l : list_ptrs) {
    l->AddToDataSlice(bldr, pos);
    pos += l->size();
    splits_inserter.Add(pos);
  }
  ASSIGN_OR_RETURN(auto edge, arolla::DenseArrayEdge::FromSplitPoints(
                                  {std::move(splits_bldr).Build()}));
  return DataBagContent::ListsContent{alloc, std::move(bldr).Build(), edge};
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
    values.push_back(dict.Get(keys[ki]));
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
      if (bool inserted = visited_ids.insert(obj).second;
          inserted && value.has_value()) {
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

}  // namespace koladata::internal
