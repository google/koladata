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
#include "koladata/internal/uuid_object.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/stable_fingerprint.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/memory/buffer.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

namespace {

arolla::Fingerprint ComputeFingerPrintFromKwargs(
    absl::string_view seed,
    absl::Span<const std::pair<absl::string_view, arolla::Fingerprint>>
        fingerprints) {
  StableFingerprintHasher hasher(absl::StrCat("uuid", seed));
  for (const auto& [attr, value] : fingerprints) {
    hasher.Combine(attr, value);
  }
  return std::move(hasher).Finish();
}

absl::StatusOr<int64_t> CommonKwargsSize(
    absl::Span<const std::reference_wrapper<const DataSliceImpl>> values) {
  int64_t size = -1;
  for (const auto& value : values) {
    if (size == -1) {
      size = value.get().size();
    } else {
      if (value.get().size() != size) {
        return absl::FailedPreconditionError("Size mismatch in kwargs");
      }
    }
  }
  return size;
}

arolla::Fingerprint UuidWithMainObjectFingerprint(AllocationId alloc_id,
                                                  absl::string_view salt) {
  return StableFingerprintHasher("uuid_with_main_object")
      .Combine(salt, alloc_id)
      .Finish();
}

arolla::Fingerprint UuidWithMainObjectFingerprint(ObjectId object_id,
                                                  absl::string_view salt) {
  return UuidWithMainObjectFingerprint(AllocationId(object_id), salt);
}

}  // namespace

ObjectId CreateUuidObject(arolla::Fingerprint fingerprint, UuidType uuid_type) {
  int64_t flags = 0;
  if (uuid_type == UuidType::kList) {
    flags = ObjectId::kUuidFlag | ObjectId::kListFlag;
  } else if (uuid_type == UuidType::kDict) {
    flags = ObjectId::kUuidFlag | ObjectId::kDictFlag;
  } else {
    // Default uuid. Only uuid metadata is set.
    CHECK(uuid_type == UuidType::kDefault);
    flags = ObjectId::kUuidFlag;
  }
  return CreateUuidObjectWithMetadata(std::move(fingerprint), flags);
}

arolla::Fingerprint ComputeFingerprintFromFields(
    absl::string_view seed, absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> values) {
  DCHECK_EQ(attr_names.size(), values.size());
  std::vector<std::pair<absl::string_view, arolla::Fingerprint>> fingerprints;
  fingerprints.reserve(attr_names.size() + 1);
  for (int64_t i = 0; i < attr_names.size(); ++i) {
    fingerprints.emplace_back(attr_names[i],
                              values[i].get().StableFingerprint());
  }
  std::sort(fingerprints.begin(), fingerprints.end(),
            [](const auto& x, const auto& y) { return x.first < y.first; });

  return ComputeFingerPrintFromKwargs(seed, absl::MakeSpan(fingerprints));
}

DataItem CreateUuidFromFields(
    absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> values,
    UuidType uuid_type) {
  return DataItem(CreateUuidObject(
      ComputeFingerprintFromFields(seed, attr_names, values), uuid_type));
}

absl::StatusOr<DataSliceImpl> CreateUuidFromFields(
    absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataSliceImpl>> values,
    UuidType uuid_type) {
  DCHECK_EQ(attr_names.size(), values.size());
  ASSIGN_OR_RETURN(int64_t size, CommonKwargsSize(values));
  if (size == 0 || size == -1) {
    return DataSliceImpl::CreateEmptyAndUnknownType(0);
  }

  std::vector<
      std::pair<absl::string_view, std::reference_wrapper<const DataSliceImpl>>>
      sorted_kwargs;
  sorted_kwargs.reserve(attr_names.size());
  for (int64_t i = 0; i < attr_names.size(); ++i) {
    sorted_kwargs.emplace_back(attr_names[i], values[i]);
  }
  std::sort(sorted_kwargs.begin(), sorted_kwargs.end(),
            [](const auto& x, const auto& y) { return x.first < y.first; });

  std::vector<std::pair<absl::string_view, arolla::Fingerprint>> fingerprints;
  fingerprints.reserve(sorted_kwargs.size());
  for (const auto& [attr, value] : sorted_kwargs) {
    fingerprints.emplace_back(attr, arolla::Fingerprint{.value = 0});
  }

  arolla::Buffer<ObjectId>::Builder values_builder(size);

  for (int64_t offset = 0; offset < size; ++offset) {
    for (int64_t i = 0; i != fingerprints.size(); ++i) {
      fingerprints[i].second =
          sorted_kwargs[i].second.get()[offset].StableFingerprint();
    }
    values_builder.Set(offset,
                       CreateUuidObject(ComputeFingerPrintFromKwargs(
                                            seed, absl::MakeSpan(fingerprints)),
                                        uuid_type));
  }

  return DataSliceImpl::CreateObjectsDataSlice(
      ObjectIdArray{std::move(values_builder).Build()},
      AllocationIdSet(/*contains_small_allocation_id=*/true));
}

DataItem CreateListUuidFromItemsAndFields(
    absl::string_view seed, const DataSliceImpl& list_items,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> values) {
  StableFingerprintHasher hasher(absl::StrCat("list_uuid", seed));
  hasher.Combine(ComputeFingerprintFromFields(seed, attr_names, values));
  hasher.Combine(list_items.size());
  for (const auto& item : list_items) {
    hasher.Combine(item.StableFingerprint());
  }
  return DataItem(
      CreateUuidObject(std::move(hasher).Finish(), UuidType::kList));
}

DataItem CreateDictUuidFromKeysValuesAndFields(
    absl::string_view seed, const DataSliceImpl& dict_keys,
    const DataSliceImpl& dict_values,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> values) {
  DCHECK_EQ(dict_keys.size(), dict_values.size());
  StableFingerprintHasher hasher(absl::StrCat("dict_uuid", seed));
  hasher.Combine(ComputeFingerprintFromFields(seed, attr_names, values));
  hasher.Combine(dict_keys.size());
  std::vector<std::pair<arolla::Fingerprint, arolla::Fingerprint>>
      items_fingerprints;
  items_fingerprints.reserve(dict_keys.size());
  for (int64_t i = 0; i < dict_keys.size(); ++i) {
    items_fingerprints.emplace_back(dict_keys[i].StableFingerprint(),
                                    dict_values[i].StableFingerprint());
  }
  // Sort (key, value) pairs by key fingerprint, to get a deterministic uuid.
  std::sort(items_fingerprints.begin(), items_fingerprints.end(),
            [](const auto& x, const auto& y) {
              return x.first.value < y.first.value;
            });
  for (const auto& [attr, value] : items_fingerprints) {
    hasher.Combine(attr, value);
  }
  return DataItem(
      CreateUuidObject(std::move(hasher).Finish(), UuidType::kDict));
}

DataItem CreateSchemaUuidFromFields(absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> items) {
  DCHECK_EQ(attr_names.size(), items.size());
  std::vector<std::pair<absl::string_view, arolla::Fingerprint>> fingerprints;
  fingerprints.reserve(attr_names.size());
  for (int i = 0; i < attr_names.size(); ++i) {
    fingerprints.emplace_back(attr_names[i],
                              items[i].get().StableFingerprint());
  }
  std::sort(fingerprints.begin(), fingerprints.end(),
            [](const auto& x, const auto& y) { return x.first < y.first; });
  return DataItem(CreateUuidExplicitSchema(
      ComputeFingerPrintFromKwargs(seed, absl::MakeSpan(fingerprints))));
}

template <int64_t uuid_flag>
absl::StatusOr<DataItem> CreateUuidWithMainObject(const DataItem& main_object,
                                                  absl::string_view salt) {
  if (!main_object.has_value()) {
    return DataItem();
  }
  if (main_object.holds_value<ObjectId>()) {
    ObjectId obj = main_object.value<ObjectId>();
    return DataItem(CreateUuidWithMainObject<uuid_flag>(
        obj, UuidWithMainObjectFingerprint(obj, salt)));
  }
  return absl::FailedPreconditionError("Main object must be ObjectId");
}

template absl::StatusOr<DataItem>
CreateUuidWithMainObject<ObjectId::kUuidFlag>(
    const DataItem& main_objects, absl::string_view salt);

template absl::StatusOr<DataItem>
CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
    const DataItem& main_objects, absl::string_view salt);

template <int64_t uuid_flag>
absl::StatusOr<DataSliceImpl> CreateUuidWithMainObject(
    const DataSliceImpl& main_objects, absl::string_view salt) {
  if (main_objects.is_empty_and_unknown()) {
    return main_objects;
  }
  if (main_objects.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::FailedPreconditionError("Main objects must be ObjectId");
  }
  const ObjectIdArray& objects = main_objects.values<ObjectId>();

  const AllocationIdSet& source_id_set = main_objects.allocation_ids();

  auto compute_uu_obj_id = [&](ObjectId obj_id) {
    return CreateUuidWithMainObject<uuid_flag>(
        obj_id, UuidWithMainObjectFingerprint(obj_id, salt));
  };

  if (!source_id_set.contains_small_allocation_id() &&
      source_id_set.size() == 1) {
    ObjectId zero_obj_id = source_id_set.begin()->ObjectByOffset(0);
    auto uu_alloc_id = AllocationId(compute_uu_obj_id(zero_obj_id));
    auto op = arolla::CreateDenseOp([&](ObjectId obj_id) {
      return uu_alloc_id.ObjectByOffset(obj_id.Offset());
    });
    return DataSliceImpl::CreateObjectsDataSlice(op(objects),
                                                 AllocationIdSet(uu_alloc_id));
  }

  AllocationIdSet final_id_set(source_id_set.contains_small_allocation_id());

  absl::flat_hash_map<AllocationId, AllocationId> big_alloc_id_map(
      source_id_set.size());

  for (AllocationId alloc_id : source_id_set) {
    ObjectId obj_id = alloc_id.ObjectByOffset(0);
    auto uu_alloc_id = AllocationId(compute_uu_obj_id(obj_id));
    final_id_set.Insert(uu_alloc_id);
    big_alloc_id_map.emplace(alloc_id, uu_alloc_id);
  }

  auto op = arolla::CreateDenseOp([&](ObjectId obj_id) {
    if (obj_id.IsSmallAlloc()) {
      return compute_uu_obj_id(obj_id);
    }
    AllocationId alloc_id(obj_id);
    return big_alloc_id_map.find(alloc_id)->second.ObjectByOffset(
        obj_id.Offset());
  });

  return DataSliceImpl::CreateObjectsDataSlice(op(objects), final_id_set);
}

template absl::StatusOr<DataSliceImpl>
CreateUuidWithMainObject<ObjectId::kUuidFlag>(
    const DataSliceImpl& main_objects, absl::string_view salt);

template absl::StatusOr<DataSliceImpl>
CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
    const DataSliceImpl& main_objects, absl::string_view salt);

}  // namespace koladata::internal
