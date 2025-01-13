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
#ifndef KOLADATA_INTERNAL_SPARSE_SOURCE_H_
#define KOLADATA_INTERNAL_SPARSE_SOURCE_H_

#include <optional>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::internal {

// SparseSource represents a single attribute of some set of objects.
// Internally it is absl::flat_hash_map<ObjectId, DataItem>.
class SparseSource {
 public:
  // If alloc_id is specified, this SparseSource contains only values for
  // given allocation. Otherwise it can contain only values for small allocs
  // (with object_id.IsSmallAlloc() == true) which are not tracked by
  // allocation.
  explicit SparseSource(std::optional<AllocationId> alloc_id = std::nullopt)
      : alloc_id_(alloc_id) {}

  // Returns the attribute for the specified object.
  // The result is std::nullopt if the value is missing in this data source,
  // but can potentially present in another one. The result is an empty DataItem
  // if the value is explicitly removed by this SparseSource.
  std::optional<DataItem> Get(ObjectId object) const;

  // Returns DataSliceImpl with values corresponding to the specified objects.
  // It is not possible to distinguish missing and removed values using this
  // function, so it is useful only for getting values from a single
  // SparseSource (that is a very important use case). In case of several data
  // sources use `Get` with SliceBuilder.
  DataSliceImpl Get(const ObjectIdArray& objects) const;

  // Gets values for those of `objs` that are not set yet in the builder.
  void Get(absl::Span<const ObjectId> objs, SliceBuilder& bldr) const;

  // Returns all content of the SparseSource. ObjectId with empty (MissingValue)
  // DataItem means "removed" and there will be no lookups in parents and
  // fallbacks for this ObjectId.
  const absl::flat_hash_map<ObjectId, DataItem>& GetAll() const {
    return data_item_map_;
  }

  // Sets the value for the specified object.
  void Set(ObjectId object, const DataItem& value);

  // Sets the values for the specified objects.
  // Returns an error if IsMutable is false.
  // Items with missing ObjectId in `objects` will be ignored
  // Items with present ObjectId, but missing value in `values` will be removed.
  absl::Status Set(const ObjectIdArray& objects, const DataSliceImpl& values);

  // Sets the value to kPresent for the specified objects.
  // Returns an error if IsMutable is false.
  // Items with missing ObjectId in `objects` will be ignored.
  // Updates missing_objects with the list of objects that were missing.
  absl::Status SetUnitAndUpdateMissingObjects(
      const ObjectIdArray& objects, std::vector<ObjectId>& missing_objects);

 private:
  bool ObjectBelongs(ObjectId object) const {
    return alloc_id_.has_value() ? alloc_id_->Contains(object)
                                 : object.IsSmallAlloc();
  }

  // Hash map object_id->value.
  absl::flat_hash_map<ObjectId, DataItem> data_item_map_;
  // If nullopt, only small allocs will be used.
  std::optional<AllocationId> alloc_id_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_SPARSE_SOURCE_H_
