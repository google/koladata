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
#ifndef KOLADATA_INTERNAL_DATA_BAG_H_
#define KOLADATA_INTERNAL_DATA_BAG_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/hash/hash.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_list.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dense_source.h"
#include "koladata/internal/dict.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/sparse_source.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/refcount_ptr.h"

namespace koladata::internal {

struct MergeOptions {
  using ConflictHandlingOption = DenseSource::ConflictHandlingOption;
  using ConflictHandlingOption::kRaiseOnConflict;
  using ConflictHandlingOption::kKeepOriginal;
  using ConflictHandlingOption::kOverwrite;
  // Policy for merging data, i. e. object attributes, lists, dicts and
  // implicit schemas.
  ConflictHandlingOption data_conflict_policy = kRaiseOnConflict;
  // Policy for merging explicit schemas.
  ConflictHandlingOption schema_conflict_policy = kRaiseOnConflict;

  friend bool operator==(const MergeOptions&, const MergeOptions&) = default;
};

struct DataBagIndex {
  struct AttrIndex {
    std::vector<AllocationId> allocations;
    bool with_small_allocs;
  };
  absl::btree_map<std::string, AttrIndex> attrs;
  std::vector<AllocationId> lists;
  std::vector<AllocationId> dicts;
};

struct DataBagContent {
  struct AttrAllocContent {
    AllocationId alloc_id;
    DataSliceImpl values;
  };
  struct AttrItemContent {
    ObjectId object_id;
    DataItem value;
  };
  struct AttrContent {
    std::vector<AttrAllocContent> allocs;
    std::vector<AttrItemContent> items;
  };
  struct ListsContent {
    AllocationId alloc_id;
    DataSliceImpl values;
    arolla::DenseArrayEdge lists_to_values_edge;
  };
  struct DictContent {
    ObjectId dict_id;
    std::vector<DataItem> keys;
    std::vector<DataItem> values;
  };
  absl::btree_map<std::string, AttrContent> attrs;
  std::vector<ListsContent> lists;
  std::vector<DictContent> dicts;
};

// Returns merge options that would achieve the same result, but when
// the DataBags are merged in the reverse order.
MergeOptions ReverseMergeOptions(const MergeOptions& options);

class DataBagImpl;

using DataBagImplPtr = arolla::RefcountPtr<DataBagImpl>;
using DataBagImplConstPtr = arolla::RefcountPtr<const DataBagImpl>;

class DataBagImpl : public arolla::RefcountedBase {
  struct PrivateConstructorTag {};

 public:
  explicit DataBagImpl(PrivateConstructorTag) {}
  ~DataBagImpl() noexcept;

  // *******  Factory interface
  static DataBagImplPtr CreateEmptyDatabag();

  // Returns partially persistent fork of the Databag.
  // Modifications of the fork would not affect original DataBagImpl.
  // https://en.wikipedia.org/wiki/Persistent_data_structure
  // Original/parent DataBagImpl is not expected to be modified during the
  // lifetime of the fork.
  DataBagImplPtr PartiallyPersistentFork() const;

  // *******  Const interface

  using ConstDenseSourceArray = absl::InlinedVector<const DenseSource*, 1>;
  using ConstSparseSourceArray = absl::InlinedVector<const SparseSource*, 1>;
  using FallbackSpan = absl::Span<const DataBagImpl* const>;

  // Returns DataSliceImpl with attribute for every object.
  // Missing values are looked up in the fallback databags.
  absl::StatusOr<DataSliceImpl> GetAttr(
      const DataSliceImpl& objects,
      absl::string_view attr,
      FallbackSpan fallbacks = {}) const;

  absl::StatusOr<DataItem> GetAttr(
      const DataItem& object,
      absl::string_view attr,
      FallbackSpan fallbacks = {}) const;

  // Gets __schema__ attribute for objects and returns an Error if DataSlice has
  // primitives or objects do not have __schema__ attribute.
  absl::StatusOr<DataItem> GetObjSchemaAttr(const DataItem& item,
                                            FallbackSpan fallbacks = {}) const;
  absl::StatusOr<DataSliceImpl> GetObjSchemaAttr(
      const DataSliceImpl& slice, FallbackSpan fallbacks = {}) const;

  // Gets DataSources for given allocation id and attribute.
  // Appends DataSources to preallocated vectors of dense and sparse sources.
  // It is lower level API that is useful for combining several DataBags.
  // DataSource with lower index will override values of sources with
  // higher indices.
  // Sparse sources always override dense sources.
  // Returns size of the alloc (can be less then alloc.Capacity() e.g. if
  // an initial dense source for this allocation was created from a pre-existing
  // DenseArray).
  int64_t GetAttributeDataSources(AllocationId alloc, absl::string_view attr,
                                  ConstDenseSourceArray& dense_sources,
                                  ConstSparseSourceArray& sparse_sources) const;

  // Returns information about all AllocationId stored in
  // the DataBag. The function is relatively slow (i.e. not O(1) ) as it
  // iterates over all internal data structures. Returned index is sorted by
  // allocation id and has no duplicates.
  DataBagIndex CreateIndex() const;

  // Returns content of the DataBag including data from parents. The content
  // is filtered by the allocation ids present in DataBagIndex.
  // Example:
  //     DataBagIndex index = db.CreateIndex();
  //     index.lists.clear();
  //     DataBagContent content = db.ExtractContent(index);
  // `content.lists` will be empty because `index.lists` was cleared,
  // but `content.attrs` can still have references to the lists.
  // Data in `content` is in the same order as allocation ids in `index`.
  // Note that if DataBagIndex has duplicate entries (db.CreateIndex() doesn't
  // return duplicate entries, but such index can be created manually), then
  // returned DataBagContent may also contain duplicate entries.
  absl::StatusOr<DataBagContent> ExtractContent(const DataBagIndex&) const;
  absl::StatusOr<DataBagContent> ExtractContent() const {
    return ExtractContent(CreateIndex());
  }

  // *******  Mutable interface

  // Allocates new objects with provided attributes and store them in
  // DataBagImpl. All DataSliceImpl's must have the same size.
  absl::StatusOr<DataSliceImpl> CreateObjectsFromFields(
      const std::vector<absl::string_view>& attr_names,
      const std::vector<std::reference_wrapper<const DataSliceImpl>>& slices);

  // Allocates new object with provided attributes and store them in
  // DataBagImpl.
  absl::StatusOr<DataItem> CreateObjectsFromFields(
      const std::vector<absl::string_view>& attr_names,
      const std::vector<std::reference_wrapper<const DataItem>>& items);

  // Updates DataBagImpl with new attribute values for the specified objects.
  // Items with missing `objects` are ignored
  // Items with missing `values` and present `objects` will be removed.
  //
  // In case of duplicated ids in `objects` the last present value is used.
  // The function can either modify existent DenseSource or create a new one.
  absl::Status SetAttr(const DataSliceImpl& objects, absl::string_view attr,
                       const DataSliceImpl& values);

  absl::Status SetAttr(const DataItem& object, absl::string_view attr,
                       DataItem value);

  // Updates DataBagImpl by setting attribute to present for specified objects.
  // Returns a slice of unique ObjectIds that had an attribute missing before.
  absl::StatusOr<DataSliceImpl>
  InternalSetUnitAttrAndReturnMissingObjects(const DataSliceImpl& objects,
                                             absl::string_view attr);

  // ListRange{a, b} represent python-style slicing [a:b]. Negative indices mean
  // offset from the end of a list.
  // Unspecified or nullopt `to` means the end of the list.
  // If index is out of range, it is clamped to the range. E.g.
  //   ListRange(-100) - up to 100 last elements (less if list_size < 100)
  //   ListRange(0, 100) - up to 100 first elements (less if list_size < 100).
  class ListRange {
    static constexpr int64_t kUnspecifiedTo =
        std::numeric_limits<int64_t>::max();

   public:
    // Creates range from the given index till the end of the list.
    explicit ListRange(int64_t from = 0) : from_(from) {}

    explicit ListRange(int64_t from, std::optional<int64_t> to)
        : from_(from), to_(to.value_or(kUnspecifiedTo)) {}
    explicit ListRange(int64_t from, int64_t to) : from_(from), to_(to) {}

    int64_t CalculateFrom(int64_t list_size) {
      if (ABSL_PREDICT_TRUE(from_ == 0)) {
        return 0;
      }
      return std::clamp<int64_t>(from_ < 0 ? from_ + list_size : from_, 0,
                                 list_size);
    }

    int64_t CalculateTo(int64_t list_size) {
      if (ABSL_PREDICT_TRUE(to_ == kUnspecifiedTo)) {
        return list_size;
      }
      return std::clamp<int64_t>(to_ < 0 ? to_ + list_size : to_, 0, list_size);
    }

    // Returns a range within [0, list_size).
    std::pair<int64_t, int64_t> Calculate(int64_t list_size) {
      return std::make_pair(CalculateFrom(list_size), CalculateTo(list_size));
    }

   private:
    int64_t from_;
    int64_t to_ = kUnspecifiedTo;
  };

  // ******* Batch list functions

  // Note: all modifications happen sequentially. I.e. if the same list is
  // present in `lists` twice, then the operation will be applied to the list
  // twice, in the order of iteration over `lists`.

  // Get size of each it list.
  // In case list is empty, size of the first non empty list in fallbacks
  // is returned.
  // 0 is returned in case all lists are empty.
  absl::StatusOr<arolla::DenseArray<int64_t>> GetListSize(
      const DataSliceImpl& lists, FallbackSpan fallbacks = {}) const;

  // Get one value from each list using the corresponding index. Negative index
  // means offset from the end of a list.
  // In case list is empty, the first non empty list in fallbacks is used.
  // If index is out of range in the first non empty list,
  // the value will be missing.
  absl::StatusOr<DataSliceImpl> GetFromLists(
      const DataSliceImpl& lists, const arolla::DenseArray<int64_t>& indices,
      FallbackSpan fallbacks = {}) const;

  // Similar to GetFromLists, but also removes the values from the lists.
  absl::StatusOr<DataSliceImpl> PopFromLists(
      const DataSliceImpl& lists, const arolla::DenseArray<int64_t>& indices);

  // Get `range` of each list (by default the entire content) and return as
  // a data slice in child index type. Also returns an edge from `lists` to
  // the created data slice.
  // In case list is empty, the first non empty list in fallbacks is used.
  // NOTE: Empty range will not consult fallback if list is not empty.
  absl::StatusOr<std::pair<DataSliceImpl, arolla::DenseArrayEdge>> ExplodeLists(
      const DataSliceImpl& lists, ListRange range = ListRange(),
      FallbackSpan fallbacks = {}) const;

  // Set one value in each list using the corresponding index. Negative index
  // means offset from the end of a list. If some index is out of range,
  // the corresponding value will be ignored.
  absl::Status SetInLists(const DataSliceImpl& lists,
                          const arolla::DenseArray<int64_t>& indices,
                          const DataSliceImpl& values);

  // Appends one value to the end of each list. Size of `lists` must be equal
  // to the size of `values`.
  absl::Status AppendToList(const DataSliceImpl& lists,
                            const DataSliceImpl& values);

  // Similar to `AppendToLists`, but can append multiple values to the end of
  // each list. The additional argument `values_to_lists` is an edge indicating
  // which values should be added to each list.
  // `values_to_lists.parent_size()` must be equal to `lists.size()`.
  // `values_to_lists.child_size()` must be equal to `values.size()`.
  absl::Status ExtendLists(const DataSliceImpl& lists,
                           const DataSliceImpl& values,
                           const arolla::DenseArrayEdge& values_to_lists);

  // Replaces `range` slice of each list with given values.
  // `values` and `values_to_lists` have the same meaning as in `ExtendLists`.
  // `values_to_lists.parent_size()` must be equal to `lists.size()`.
  // `values_to_lists.child_size()` must be equal to `values.size()`.
  absl::Status ReplaceInLists(const DataSliceImpl& lists, ListRange range,
                              const DataSliceImpl& values,
                              const arolla::DenseArrayEdge& values_to_lists);

  // Remove given range of elements in each list. Equivalent to `ReplaceInLists`
  // with empty `values`.
  absl::Status RemoveInList(const DataSliceImpl& lists, ListRange range);

  // Remove given indices. Size of `lists` must be equal to the size of
  // `indices`.
  absl::Status RemoveInList(const DataSliceImpl& lists,
                            const arolla::DenseArray<int64_t>& indices);

  // ******* Single list functions

  // Returns int64_t DataItem with a size of the list.
  // In case list is empty, size of the first non empty list in fallbacks
  // is returned.
  // 0 is returned in case all lists are empty.
  absl::StatusOr<DataItem> GetListSize(const DataItem& list,
                                       FallbackSpan fallbacks = {}) const;

  // Negative index means offset from the end of the list.
  // In case list is empty, the first non empty list in fallbacks is used.
  // If index is out of range in the first non empty list,
  // returns an empty DataItem.
  absl::StatusOr<DataItem> GetFromList(const DataItem& list, int64_t index,
                                       FallbackSpan fallbacks = {}) const;

  // Similar to GetFromList, but also removes the value from the list.
  absl::StatusOr<DataItem> PopFromList(const DataItem& list, int64_t index);

  // Returns a slice of a list. In case of empty list or empty range returns
  // default-constructed `DataSliceImpl()` with dtype=arolla::GetNothingQType().
  // In case list is empty, the first non empty list in fallbacks is used.
  absl::StatusOr<DataSliceImpl> ExplodeList(const DataItem& list,
                                            ListRange range = ListRange(),
                                            FallbackSpan fallbacks = {}) const;

  // Sets element with given index. Negative index means offset from the end of
  // the list. If index is out of range, the function does nothing and returns
  // OK status.
  absl::Status SetInList(const DataItem& list, int64_t index, DataItem value);

  // Append single value to the end of the list.
  absl::Status AppendToList(const DataItem& list, DataItem value);

  // Append multiple values to the end of the list.
  absl::Status ExtendList(const DataItem& list, const DataSliceImpl& values);

  // Replaces given range in `list` with `values`.
  absl::Status ReplaceInList(const DataItem& list, ListRange range,
                             const DataSliceImpl& values);

  // Remove `range` elements.
  absl::Status RemoveInList(const DataItem& list, ListRange range);

  // ******* Batch Dict functions

  // Get all keys of all given dict in one data slice. Returned edge is
  // the edge from dicts to keys.
  absl::StatusOr<std::pair<DataSliceImpl, arolla::DenseArrayEdge>> GetDictKeys(
      const DataSliceImpl& dicts, FallbackSpan fallbacks = {}) const;

  // Returns int64_t DataItem with a size of the dict.
  // In case dict is empty, size of the first non empty dict in fallbacks
  // is returned. 0 is returned in case all dicts are empty.
  absl::StatusOr<DataSliceImpl> GetDictSize(const DataSliceImpl& dicts,
                                            FallbackSpan fallbacks = {}) const;

  // Returns one value from each dict. Size of `dicts` must be equal to the size
  // of keys. Returned data slice will have the same size.
  absl::StatusOr<DataSliceImpl> GetFromDict(const DataSliceImpl& dicts,
                                            const DataSliceImpl& keys,
                                            FallbackSpan fallbacks = {}) const;

  // Sets a value in each dict for specified key. `dicts`, `keys`, and `values`
  // must have the same size.
  absl::Status SetInDict(const DataSliceImpl& dicts, const DataSliceImpl& keys,
                         const DataSliceImpl& values);

  // Clear given dicts.
  absl::Status ClearDict(const DataSliceImpl& dicts);

  // ******* Single dict functions

  // Equivalent to functions above, but for a single dict.
  absl::StatusOr<DataItem> GetDictSize(const DataItem& dict,
                                       FallbackSpan fallbacks = {}) const;
  absl::StatusOr<std::pair<DataSliceImpl, arolla::DenseArrayEdge>> GetDictKeys(
      const DataItem& dict, FallbackSpan fallbacks = {}) const;
  absl::StatusOr<DataItem> GetFromDict(const DataItem& dict,
                                       const DataItem& key,
                                       FallbackSpan fallbacks = {}) const;
  absl::Status SetInDict(const DataItem& dict, const DataItem& key,
                         const DataItem& value);
  absl::Status ClearDict(const DataItem& dict);

  // ******* Schema functions
  //
  // NOTE: Schema attributes are stored in a Dict, but are not modifiable
  // through a DataBag's Dict API to prevent users from mistakenly updating
  // schema without using schema-specific API.

  // Equivalent to functions above, but work for DataItems holding Schema
  // ObjectId(s).
  //
  // Returns attribute names of all attributes of the given `schema_item`.
  absl::StatusOr<DataSliceImpl> GetSchemaAttrs(
      const DataItem& schema_item, FallbackSpan fallbacks = {}) const;

  // Returns a DataItem that represents schema of an attribute `attr` in the
  // given `schema_item`. In case the attribute is missing appropriate error is
  // returned. In case `schema_item` does not contain a schema object, error is
  // returned.
  absl::StatusOr<DataItem> GetSchemaAttr(const DataItem& schema_item,
                                         absl::string_view attr,
                                         FallbackSpan fallbacks = {}) const;

  // Returns a DataItem that represents schema of an attribute `attr` in the
  // given `schema_item`. In case the attribute is missing, an empty DataItem is
  // returned.
  absl::StatusOr<DataItem> GetSchemaAttrAllowMissing(
      const DataItem& schema_item, absl::string_view attr,
      FallbackSpan fallbacks = {}) const;

  // Returns a DataSliceImpl that represents schema of an attribute `attr` in
  // the given `schema_slice` slice. In case the attribute is missing
  // appropriate error is returned. In case `schema_slice` does not contain a
  // schema object, error is returned.
  absl::StatusOr<DataSliceImpl> GetSchemaAttr(
      const DataSliceImpl& schema_slice, absl::string_view attr,
      FallbackSpan fallbacks = {}) const;

  // Returns a DataSliceImpl that represents schema of an attribute `attr` in
  // the given `schema_slice` slice. The attribute is returned as is, even if
  // some elements are missing where schema contains a valid schema ObjectId.
  // E.g.
  //
  // [schema_id_0, None, schema_id_2].GetSchemAttrAllowMissing("a")
  // Returns: [INT32, None, None]
  //
  // The GetSchemaAttr (that does not allow missing where schema_id is present)
  // returns an Error.
  absl::StatusOr<DataSliceImpl> GetSchemaAttrAllowMissing(
      const DataSliceImpl& schema_slice, absl::string_view attr,
      FallbackSpan fallbacks = {}) const;

  // Sets attribute to the provided `schema_item`. In case `value` is not a
  // schema or `schema_item` is not a schema object, appropriate error is
  // returned.
  absl::Status SetSchemaAttr(const DataItem& schema_item,
                             absl::string_view attr, const DataItem& value);

  // Sets attribute to the provided `schema_slice` slice. `value` is always an
  // item and set as an attribute to each schema object in `schema_slice`. In
  // case `value` is not a schema or `schema_slice` does not contain all schema
  // objects, appropriate error is returned.
  absl::Status SetSchemaAttr(const DataSliceImpl& schema_slice,
                             absl::string_view attr, const DataItem& value);

  // Sets attribute to the provided `schema_slice` slice. In case `value` is not
  // a schema or `schema_slice` does not contain all schema objects, appropriate
  // error is returned.
  absl::Status SetSchemaAttr(const DataSliceImpl& schema_slice,
                             absl::string_view attr,
                             const DataSliceImpl& value);

  // Removes a schema attribute `attr`, when `schema_item` is a DataItem.
  absl::Status DelSchemaAttr(const DataItem& schema_item,
                             absl::string_view attr);

  // Removes a schema attribute `attr`, when `schema_slice` is a DataSlice.
  absl::Status DelSchemaAttr(const DataSliceImpl& schema_slice,
                             absl::string_view attr);

  // Returns a new Schema DataItem with attributes from `attr_names` set to
  // schemas in `items` in the same order. In case some of the `items` are not
  // valid schemas, appropriate error is returned. The allocated schema is
  // explicit schema.
  absl::StatusOr<DataItem> CreateExplicitSchemaFromFields(
      const std::vector<absl::string_view>& attr_names,
      const std::vector<std::reference_wrapper<const DataItem>>& items);

  // Returns an explicit UuSchema DataItem with attributes from `attr_names` set
  // to schemas in `items` in the same order. In case some of the `items` are
  // not valid schemas, appropriate error is returned.
  absl::StatusOr<DataItem> CreateUuSchemaFromFields(
      absl::string_view seed,
      const std::vector<absl::string_view>& attr_names,
      const std::vector<std::reference_wrapper<const DataItem>>& items);

  // Sets attributes from `attr_names` to schemas in `items` in the same order.
  // Attributes are added to existing schemas, i.e. existing attributes remain
  // and are overwritten only if specified in the arguments.
  // In case some of the `items` are not valid schemas, appropriate error is
  // returned. The passed DataItem/DataSliceImpl must be an item / slice of
  // ObjectIds which are schemas. Otherwise, an error is returned.
  template <typename ImplT>
  absl::Status SetSchemaFields(
      const ImplT&,  const std::vector<absl::string_view>& attr_names,
      const std::vector<std::reference_wrapper<const DataItem>>& items);

  // Sets attributes from `attr_names` to schemas in `items` in the same order.
  // All schemas are overwritten. It is the fast version of SetSchemaFields for
  // cases in which existing schemas don't need to be preserved. This should
  // be preferred in case either can be used.
  // In case some of the `items` are not valid schemas, appropriate error is
  // returned. The passed DataItem/DataSliceImpl must be an item / slice of
  // ObjectIds which are schemas. Otherwise, an error is returned.
  template <typename ImplT>
  absl::Status OverwriteSchemaFields(
      const ImplT&,  const std::vector<absl::string_view>& attr_names,
      const std::vector<std::reference_wrapper<const DataItem>>& items);

  // ********* Merging

  // Merge additional attributes and objects from `other`.
  // If overwrite is false, returns non-ok Status on conflict, otherwise
  // overwrites the value.
  absl::Status MergeInplace(const DataBagImpl& other,
                            MergeOptions options = MergeOptions());

  // Assigns this DataBagImpl to a DataBag. This should called every time a
  // DataBag is created from this DataBagImpl to make sure DataBagImpl is never
  // reused.
  void AssignToDataBag() {
    if (is_assigned_) {
      LOG(FATAL) << "attempting to re-use DataBagImpl that is already used in "
                     "another DataBag; DataBagImpl can only belong to one "
                     "DataBag at a time";
    }
    is_assigned_ = true;
  }

 private:
  DataBagImpl() = default;

  // Search attribute value for the given object in small_alloc_sources_
  // including parents.
  ABSL_ATTRIBUTE_ALWAYS_INLINE DataItem
  LookupAttrInDataItemMap(ObjectId object_id, absl::string_view attr) const {
    size_t attr_hash = absl::HashOf(attr);
    for (const DataBagImpl* db = this; db != nullptr;
         db = db->parent_data_bag_.get()) {
      if (auto attr_it = db->small_alloc_sources_.find(attr, attr_hash);
          attr_it != db->small_alloc_sources_.end()) {
        const auto& obj2item = attr_it->second;
        if (auto item = obj2item.Get(object_id); item.has_value()) {
          return *item;
        }
      }
    }
    return DataItem();
  }

  // Search attribute value for the given object in sources_
  // including parents.
  DataItem LookupAttrInDataSourcesMap(ObjectId object_id,
                                      absl::string_view attr) const;

  // Lower level utility for batch GetAttr without fallbacks support.
  absl::StatusOr<DataSliceImpl> GetAttrFromSources(
    const DataSliceImpl& objects, absl::string_view attr) const;

  // Lower level utility for GetDictKeys that returns a vector.
  // This function bypass verification of object id.
  std::vector<DataItem> GetDictKeysAsVector(ObjectId dict_id,
                                            FallbackSpan fallbacks) const;

  // Lower level utility for batch GetFromDict without fallbacks support.
  // `AllocCheckFn` is a customization to provide which allocation type `dicts`
  // ObjectIds belong to (e.g. IsDictsAlloc or IsSchemasAlloc).
  template <class AllocCheckFn>
  absl::StatusOr<DataSliceImpl> GetFromDictNoFallback(
    const arolla::DenseArray<ObjectId>& dicts, const DataSliceImpl& keys) const;

  // Lower level utility for batch GetFromDict with a single key without
  // fallbacks support.
  // `AllocCheckFn` is a customization to provide which allocation type `dicts`
  // ObjectIds belong to (e.g. IsDictsAlloc or IsSchemasAlloc).
  template <class AllocCheckFn>
  absl::StatusOr<DataSliceImpl> GetFromDictNoFallback(
    const arolla::DenseArray<ObjectId>& dicts, const DataItem& keys) const;

  // Implementation of GetFromDict that can be customized with `AllocCheckFn`.
  // Returns one value from each dict. Dicts get be quieried by a single key or
  // slice of keys. In case of slice of keys, size of `dicts` must be equal to
  // the size of keys. Returned data slice will have the same size.
  template <class AllocCheckFn, class DataSliceImplT>
  absl::StatusOr<DataSliceImpl> GetFromDictImpl(
      const DataSliceImpl& dicts, const DataSliceImplT& keys,
      FallbackSpan fallbacks = {}) const;

  // Returns existing or newly created SparseSource for small alloc objects for
  // the given attribute.
  SparseSource& GetMutableSmallAllocSource(absl::string_view attr);

  // Add small alloc data sources for the given attribute into `res_sources`.
  void GetSmallAllocDataSources(absl::string_view attr,
                                ConstSparseSourceArray& res_sources) const;

  std::vector<DataBagContent::AttrItemContent> ExtractSmallAllocAttrContent(
      absl::string_view attr_name) const;
  absl::StatusOr<DataBagContent::AttrContent> ExtractAttrContent(
      absl::string_view attr_name, const DataBagIndex::AttrIndex& index) const;

  // *** Merging helpers
  absl::Status MergeSmallAllocInplace(const DataBagImpl& other,
                                      MergeOptions options);
  absl::Status MergeBigAllocInplace(const DataBagImpl& other,
                                    MergeOptions options);
  absl::Status MergeListsInplace(const DataBagImpl& other,
                                 MergeOptions options);
  absl::Status MergeDictsInplace(const DataBagImpl& other,
                                 MergeOptions options);

  DataBagImplConstPtr parent_data_bag_ = nullptr;
  bool is_assigned_ = false;

  struct SourceKey {
    AllocationId alloc;
    std::string attr;

    friend bool operator==(const SourceKey& lhs, const SourceKey& rhs) {
      return lhs.alloc == rhs.alloc && lhs.attr == rhs.attr;
    }

    template <typename H>
    friend H AbslHashValue(H h, const SourceKey& k) {
      return H::combine(std::move(h), k.alloc, k.attr);
    }
  };

  struct SourceKeyView {
    AllocationId alloc;
    absl::string_view attr;

    explicit operator SourceKey() const {
      return SourceKey{alloc, std::string(attr)};
    }

    friend bool operator==(const SourceKeyView& lhs, const SourceKeyView& rhs) {
      return lhs.alloc == rhs.alloc && lhs.attr == rhs.attr;
    }

    template <typename H>
    friend H AbslHashValue(H h, const SourceKeyView& k) {
      return H::combine(std::move(h), k.alloc, k.attr);
    }
  };

  struct SourceKeyHashAndEq {
    using is_transparent = void;  // go/totw/144

    // equality
    bool operator()(const SourceKey& a, const SourceKey& b) const {
      return a == b;
    }
    bool operator()(const SourceKeyView& a, const SourceKeyView& b) const {
      return a == b;
    }
    bool operator()(const SourceKey& a, const SourceKeyView& b) const {
      return a.alloc == b.alloc && a.attr == b.attr;
    }

    // hash
    size_t operator()(const SourceKey& a) const { return absl::HashOf(a); }
    size_t operator()(const SourceKeyView& a) const { return absl::HashOf(a); }
  };

  struct SourceCollection {
    // Mutable data source that can be modified in place.
    // "dense" and "sparse" can not present both at the same time.
    std::shared_ptr<DenseSource> mutable_dense_source;
    std::shared_ptr<SparseSource> mutable_sparse_source;
    // DenseSource that can not be modified in-place. E.g., because of
    // sharing memory with another version of DataBagImpl or a DataSliceImpl.
    std::shared_ptr<const DenseSource> const_dense_source;
    // If true, extra data sources from parent_data_bag_ should be requested.
    // False when data sources from parent were merged or cached in
    // const_source.
    bool lookup_parent = true;
  };

  SourceCollection& GetOrCreateSourceCollection(AllocationId alloc_id,
                                                absl::string_view attr);

  absl::Status CreateMutableDenseSource(SourceCollection& collection,
                                        AllocationId alloc_id,
                                        absl::string_view attr,
                                        const arolla::QType* qtype,
                                        int64_t size) const;

  class ReadOnlyListGetter;
  class MutableListGetter;

  const std::shared_ptr<DataListVector>* GetConstListsOrNull(
      AllocationId alloc_id) const;
  // Same as above with alloc_hash computed as absl::HashOf(alloc_id).
  const std::shared_ptr<DataListVector>* GetConstListsOrNull(
      AllocationId alloc_id, size_t alloc_hash) const;

  // Returns first present (non-empty) list or empty if nothing is present.
  const DataList& GetFirstPresentList(ObjectId list_id,
                                      FallbackSpan fallbacks) const;

  // Creates list getters for the provided fallbacks.
  static std::vector<ReadOnlyListGetter> CreateFallbackListGetters(
      FallbackSpan fallbacks);

  // Returns first present (non-empty) list or empty if nothing is present.
  const DataList& GetFirstPresentList(
      ObjectId list_id, ReadOnlyListGetter& list_getter,
      absl::Span<ReadOnlyListGetter> fallback_list_getters) const;

  // Create (if not yet created) and return DataListVector for given `alloc_id`
  // in `this->lists_`. Uses a corresponding DataListVector from
  // parent_data_bag_ as a parent if available.
  DataListVector& GetOrCreateMutableLists(AllocationId alloc_id);

  // Create (if not yet created) a mutable source in the given `collection`.
  // Modified collection will have either mutable_dense_source or
  // mutable_sparse_source.
  absl::Status GetOrCreateMutableSourceInCollection(
      SourceCollection& collection, AllocationId alloc_id,
      absl::string_view attr, const arolla::QType* qtype, size_t update_size);

  // Replaces `range` with `new_values_count` values (uninitialized).
  // Returns index of the first affected value.
  int64_t RemoveAndReserveInList(DataList& list, ListRange range,
                                 int64_t new_values_count);

  absl::StatusOr<DataBagContent::ListsContent> ListVectorToContent(
    AllocationId alloc, const DataListVector& list_vector) const;

  // `AllocCheckFn` is a customization to provide which allocation type `dicts`
  // ObjectIds belong to (e.g. IsDictsAlloc or IsSchemasAlloc).
  template <class AllocCheckFn>
  class ReadOnlyDictGetter;
  template <class AllocCheckFn>
  class MutableDictGetter;

  // Returns pointer to the dicts vector for the given allocation.
  // Nullptr if absent.
  const std::shared_ptr<DictVector>* GetConstDictsOrNull(
      AllocationId alloc_id) const;
  // Same as above with alloc_hash that must be computed as
  // absl::HashOf(alloc_id).
  const std::shared_ptr<DictVector>* GetConstDictsOrNull(
      AllocationId alloc_id, size_t alloc_hash) const;

  // Lower level utility to lookup in Dict by provided ObjectId.
  // This function bypass verification and support custom key.
  template <typename Key>
  DataItem GetFromDictObject(ObjectId dict_id, const Key& key) const;
  template <typename Key>
  DataItem GetFromDictObjectWithFallbacks(ObjectId dict_id, const Key& key,
                                          FallbackSpan fallbacks) const;

  // Create (if not yet created) and return DictVector for given `alloc_id`
  // in `this->dicts_`. Uses a corresponding DictVector from
  // parent_data_bag_ as a parent if available.
  DictVector& GetOrCreateMutableDicts(AllocationId alloc_id);

  // Like GetOrCreateMutableDicts, but also selects a Dict for `object_id`.
  Dict& GetOrCreateMutableDict(ObjectId object_id);

  void AddDictToContent(ObjectId dict_id, const Dict& dict,
                        std::vector<DataBagContent::DictContent>& res) const;

  absl::flat_hash_map<SourceKey, SourceCollection, SourceKeyHashAndEq,
                      SourceKeyHashAndEq>
      sources_;
  // Map `attribute -> SparseSource`. Each data source contains all small alloc
  // objects for given attribute.
  absl::flat_hash_map<std::string, SparseSource> small_alloc_sources_;

  absl::flat_hash_map<AllocationId, std::shared_ptr<DataListVector>> lists_;
  absl::flat_hash_map<AllocationId, std::shared_ptr<DictVector>> dicts_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_DATA_BAG_H_
