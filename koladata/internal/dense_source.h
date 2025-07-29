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
#ifndef KOLADATA_INTERNAL_DENSE_SOURCE_H_
#define KOLADATA_INTERNAL_DENSE_SOURCE_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::internal {

// DenseSource represents a single attribute of some set of objects.
class DenseSource {
 public:
  virtual ~DenseSource() = default;

  virtual AllocationId allocation_id() const = 0;

  // `size()` can be <= `allocation_id().Capacity()`.
  // ObjectIds that match `allocation_id()` are expected to have
  // `ObjectId::Offset() < DenseSource::size()`. Note that Get/Set functions
  // don't validate it for performance reasons.
  virtual int64_t size() const = 0;

  // Returns the attribute for the specified object.
  // ObjectId must be inside of the alloc this DenseSource was created for.
  // The result is std::nullopt if the value is missing in this data source,
  // but can potentially present in another one. The result is an empty DataItem
  // if the value is explicitly removed by this DenseSource.
  virtual std::optional<DataItem> Get(ObjectId object) const = 0;

  // Returns DataSliceImpl with values corresponding to the specified objects.
  // It is not possible to distinguish missing and removed values using this
  // function, so it is useful only for getting values from a single DenseSource
  // (that is a very important use case). In case of several data sources use
  // Get with SliceBuilder. If `check_alloc_id` is false, objects
  // allocation id must match allocation id of the DenseSource.
  virtual DataSliceImpl Get(const ObjectIdArray& objects,
                            bool check_alloc_id = true) const = 0;

  // Gets values for `objects` skipping indices that are already set in the
  // builder. Instead of creating a new DataSliceImpl adds the values to an
  // existing SliceBuilder.
  virtual void Get(absl::Span<const ObjectId> objects,
                   SliceBuilder& bldr) const = 0;

  // Returns true if DenseSource allow mutation.
  // Returns false in the following cases (not exhaustive):
  //   * Shares data with other immutable data structures.
  //    E.g., DenseArray.
  //   * Implementation is not efficient for modification, so new DenseSource
  //     needs to be created for efficient modifications.
  virtual bool IsMutable() const = 0;

  // Sets the value for the specified object.
  // Returns an error if IsMutable is false.
  virtual absl::Status Set(ObjectId object, const DataItem& value) = 0;

  // Sets the values for the specified objects.
  // Returns an error if IsMutable is false.
  // Items with missing ObjectId in `objects` will be ignored
  // Items with present ObjectId, but missing value in `values` will be removed.
  virtual absl::Status Set(const ObjectIdArray& objects,
                           const DataSliceImpl& values) = 0;

  // Sets the value to kPresent for the specified objects.
  // Returns an error if IsMutable is false.
  // Items with missing ObjectId in `objects` will be ignored.
  // Updates missing_objects with the list of objects that were missing.
  virtual absl::Status SetUnitAndUpdateMissingObjects(
      const ObjectIdArray& objects, std::vector<ObjectId>& missing_objects) = 0;

  struct ConflictHandlingOption {
    enum Option {
      kRaiseOnConflict = 0,
      kOverwrite = 1,
      kKeepOriginal = 2,
    };
    Option option = ConflictHandlingOption::kRaiseOnConflict;
    // Callback to be called for getting the conflicting object id.
    std::function<void(ObjectId)> on_conflict_callback;
  };

  absl::Status Merge(
      const DenseSource& source,
      const ConflictHandlingOption& option = ConflictHandlingOption{
          .option = ConflictHandlingOption::kRaiseOnConflict,
          .on_conflict_callback = {}}) {
    return MergeImpl(source.GetAll(), option);
  }

  virtual std::shared_ptr<DenseSource> CreateMutableCopy() const = 0;

  static absl::StatusOr<std::shared_ptr<DenseSource>> CreateReadonly(
      AllocationId alloc, const DataSliceImpl& data);

  // Returns a DenseSource that represents all removed values.
  static absl::StatusOr<std::shared_ptr<DenseSource>> CreateAllRemoved(
      AllocationId alloc);

  // `main_type` is optional. When specified the DataSource will work faster if
  // there are no values of other types (and slower if there are).
  static absl::StatusOr<std::shared_ptr<DenseSource>> CreateMutable(
      AllocationId alloc, int64_t size,
      const arolla::QType* absl_nullable main_type = nullptr);

 private:
  // It is private because it can return internal data of a mutable
  // DenseSource. The returned DataSliceImpl is not guaranteed to be immutable.
  // Used in `MergeOverwrite`.
  // Returned DataSlice may contain TypesBuffer even if the DataSlice is single
  // type. TypesBuffer is used to distinguish kRemoved and kUnset values.
  // If in a single type slice TypesBuffer is empty, all missing values are
  // kRemoved.
  virtual DataSliceImpl GetAll() const = 0;

  // Add all present items from `values` to this DenseSource. Depending on
  // `options` in case of conflicts (i.e. another value for the same index
  // already present in the source) it can either overwrite, keep original, or
  // return an error.
  virtual absl::Status MergeImpl(const DataSliceImpl& values,
                                 const ConflictHandlingOption& option) = 0;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_DENSE_SOURCE_H_
