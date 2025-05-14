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
#ifndef KOLADATA_INTERNAL_TESTING_DEEP_OP_UTILS_H_
#define KOLADATA_INTERNAL_TESTING_DEEP_OP_UTILS_H_

#include <array>
#include <cstdint>
#include <initializer_list>
#include <string_view>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/memory/optional_value.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::internal::testing {

namespace deep_op_utils {

using ::arolla::CreateDenseArray;

enum DbParam { kMainDb, kFallbackDb };
enum AllocParam { kSingleAlloc, kMultipleAllocs };

struct DeepOpTestParam {
  DbParam db_param;
  AllocParam alloc_param;
};

class DeepOpTest : public ::testing::TestWithParam<DeepOpTestParam> {
 public:
  using AttrsT = std::vector<std::pair<std::string_view, DataItem>>;
  using TriplesT = std::vector<std::pair<DataItem, AttrsT>>;

  DataBagImplPtr GetMainDb(DataBagImplPtr db) {
    switch (GetParam().db_param) {
      case kMainDb:
        return db;
      case kFallbackDb:
        return DataBagImpl::CreateEmptyDatabag();
    }
    DCHECK(false);
  }
  DataBagImplPtr GetFallbackDb(DataBagImplPtr db) {
    switch (GetParam().db_param) {
      case kMainDb:
        return DataBagImpl::CreateEmptyDatabag();
      case kFallbackDb:
        return db;
    }
    DCHECK(false);
  }
  DataItem AllocateSchema() {
    return DataItem(internal::AllocateExplicitSchema());
  }
  DataSliceImpl AllocateEmptyObjects(int64_t size) {
    switch (GetParam().alloc_param) {
      case kSingleAlloc:
        return DataSliceImpl::AllocateEmptyObjects(size);
      case kMultipleAllocs:
        SliceBuilder bldr(size);
        for (int i = 0; i < size; ++i) {
          auto alloc_id = DataSliceImpl::AllocateEmptyObjects(size);
          bldr.InsertIfNotSetAndUpdateAllocIds(i, alloc_id[i]);
        }
        return std::move(bldr).Build();
    }
    DCHECK(false);
  }
  DataSliceImpl AllocateEmptyLists(int64_t size) {
    switch (GetParam().alloc_param) {
      case kSingleAlloc:
        return DataSliceImpl::ObjectsFromAllocation(AllocateLists(size), size);
      case kMultipleAllocs:
        SliceBuilder bldr(size);
        for (int i = 0; i < size; ++i) {
          auto alloc_id = AllocateLists(size);
          bldr.InsertIfNotSetAndUpdateAllocIds(
              i, DataItem(alloc_id.ObjectByOffset(i)));
        }
        return std::move(bldr).Build();
    }
    DCHECK(false);
  }
  DataSliceImpl AllocateEmptyDicts(int64_t size) {
    switch (GetParam().alloc_param) {
      case kSingleAlloc:
        return DataSliceImpl::ObjectsFromAllocation(AllocateDicts(size), size);
      case kMultipleAllocs:
        SliceBuilder bldr(size);
        for (int i = 0; i < size; ++i) {
          auto alloc_id = AllocateDicts(size);
          bldr.InsertIfNotSetAndUpdateAllocIds(
              i, DataItem(alloc_id.ObjectByOffset(i)));
        }
        return std::move(bldr).Build();
    }
    DCHECK(false);
  }
  TriplesT GenDataTriplesForTest() {
    auto obj_ids = AllocateEmptyObjects(5);
    auto a0 = obj_ids[0];
    auto a1 = obj_ids[1];
    auto a2 = obj_ids[2];
    auto a3 = obj_ids[3];
    auto a4 = obj_ids[4];
    TriplesT data = {
        {a0, {{"x", DataItem(1)}, {"next", a1}}},
        {a1, {{"y", DataItem(3)}, {"prev", a0}, {"next", a2}}},
        {a3, {{"x", DataItem(1)}, {"y", DataItem(2)}, {"next", a4}}},
        {a4, {{"prev", a3}}}};
    return data;
  }
  TriplesT GenSchemaTriplesFoTests() {
    auto schema0 = AllocateSchema();
    auto schema1 = AllocateSchema();
    auto int_dtype = DataItem(schema::kInt32);
    TriplesT schema_triples = {
        {schema0, {{"self", schema0}, {"next", schema1}, {"x", int_dtype}}},
        {schema1, {{"prev", schema0}, {"y", int_dtype}}}};
    return schema_triples;
  }
  template <typename T>
  static DataSliceImpl CreateSlice(
      absl::Span<const arolla::OptionalValue<T>> values) {
    return DataSliceImpl::Create(CreateDenseArray<T>(values));
  }

  static void SetSchemaTriples(DataBagImpl& db,
                               const TriplesT& schema_triples) {
    for (auto [schema, attrs] : schema_triples) {
      for (auto [attr_name, attr_schema] : attrs) {
        EXPECT_OK(db.SetSchemaAttr(schema, attr_name, attr_schema));
      }
    }
  }

  static void SetDataTriples(DataBagImpl& db, const TriplesT& data_triples) {
    for (const auto& [item, attrs] : data_triples) {
      for (const auto& [attr_name, attr_data] : attrs) {
        EXPECT_OK(db.SetAttr(item, attr_name, attr_data));
      }
    }
  }

  static void SetListValues(DataBagImplPtr db, const DataSliceImpl& lists,
                            const DataSliceImpl& values) {
    DCHECK(lists.size() <= values.size());
    std::vector<arolla::OptionalValue<int64_t>> split_points;
    split_points.push_back(0);
    int64_t remaining_size = values.size();
    for (int i = 0; i < lists.size() - 1; ++i) {
      split_points.push_back(1);
      --remaining_size;
    }
    split_points.push_back(remaining_size);

    ASSERT_OK_AND_ASSIGN(const arolla::DenseArrayEdge edge,
                         arolla::DenseArrayEdge::FromSplitPoints(
                             CreateDenseArray<int64_t>(split_points)));

    ASSERT_OK(db->ExtendLists(lists, values, edge));
  }

  static void SetSingleListValues(DataBagImplPtr db, const DataItem& list,
                                  const DataSliceImpl& values) {
    const DataSliceImpl list_expanded =
        DataSliceImpl::Create(CreateDenseArray<DataItem>({list}));
    SetListValues(std::move(db), list_expanded, values);
  }
};

inline std::array<DeepOpTestParam, 4> test_param_values(
    {{kMainDb, kSingleAlloc},
     {kMainDb, kMultipleAllocs},
     {kFallbackDb, kSingleAlloc},
     {kFallbackDb, kMultipleAllocs}});

}  // namespace deep_op_utils

}  // namespace koladata::internal::testing

#endif  // KOLADATA_INTERNAL_TESTING_DEEP_OP_UTILS_H_
