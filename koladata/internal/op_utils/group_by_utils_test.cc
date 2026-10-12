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
#include "koladata/internal/op_utils/group_by_utils.h"

#include <cstdint>
#include <initializer_list>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {
namespace {

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Pair;

using ::arolla::CreateDenseArray;

TEST(ObjectsGroupByTest, EdgeFromSchemasArray) {
  auto alloc = AllocateExplicitSchemas(10);
  auto schemas = CreateDenseArray<ObjectId>(
      {alloc.ObjectByOffset(0), alloc.ObjectByOffset(1),
       alloc.ObjectByOffset(0), alloc.ObjectByOffset(2),
       alloc.ObjectByOffset(6), std::nullopt, alloc.ObjectByOffset(1),
       std::nullopt});
  auto group_by = ObjectsGroupBy();

  ASSERT_OK_AND_ASSIGN(auto edge, group_by.EdgeFromSchemasArray(schemas));
  EXPECT_THAT(edge.edge_values(),
              ElementsAre(0, 1, 0, 2, 3, std::nullopt, 1, std::nullopt));
}

TEST(ObjectsGroupByTest, EdgeFromSchemaPairs) {
  auto alloc = AllocateExplicitSchemas(10);
  auto schemas_a = DataSliceImpl::Create(CreateDenseArray<ObjectId>(
      {alloc.ObjectByOffset(0), alloc.ObjectByOffset(1),
       alloc.ObjectByOffset(0), alloc.ObjectByOffset(1),
       alloc.ObjectByOffset(6), std::nullopt, alloc.ObjectByOffset(1),
       alloc.ObjectByOffset(1)}));
  auto schemas_b = DataSliceImpl::Create(CreateDenseArray<ObjectId>(
      {alloc.ObjectByOffset(4), alloc.ObjectByOffset(4),
       alloc.ObjectByOffset(4), alloc.ObjectByOffset(7),
       alloc.ObjectByOffset(6), std::nullopt, alloc.ObjectByOffset(6),
       std::nullopt}));
  auto group_by = ObjectsGroupBy();

  ASSERT_OK_AND_ASSIGN(auto edge,
                       group_by.EdgeFromSchemaPairs(schemas_a, schemas_b));
  EXPECT_THAT(edge.edge_values(),
              ElementsAre(0, 1, 0, 2, 3, std::nullopt, 4, std::nullopt));
}

TEST(ObjectsGroupByTest, CollapseByEdge) {
  auto group_by = ObjectsGroupBy();
  {
    auto alloc = AllocateExplicitSchemas(10);
    auto schemas = CreateDenseArray<ObjectId>(
        {alloc.ObjectByOffset(0), alloc.ObjectByOffset(1),
         alloc.ObjectByOffset(0), alloc.ObjectByOffset(2),
         alloc.ObjectByOffset(6), std::nullopt, alloc.ObjectByOffset(1),
         std::nullopt});
    ASSERT_OK_AND_ASSIGN(auto edge, group_by.EdgeFromSchemasArray(schemas));
    ASSERT_OK_AND_ASSIGN(auto result, group_by.CollapseByEdge(edge, schemas));

    EXPECT_THAT(result,
                ElementsAre(alloc.ObjectByOffset(0), alloc.ObjectByOffset(1),
                            alloc.ObjectByOffset(2), alloc.ObjectByOffset(6)));
  }
  {
    auto alloc = AllocateExplicitSchemas(10);
    auto schemas = DataSliceImpl::Create(
        CreateDenseArray<ObjectId>(
            {alloc.ObjectByOffset(0), alloc.ObjectByOffset(1),
             alloc.ObjectByOffset(0), alloc.ObjectByOffset(2),
             alloc.ObjectByOffset(6), std::nullopt, alloc.ObjectByOffset(1),
             std::nullopt}),
        CreateDenseArray<schema::DType>(
            {std::nullopt, std::nullopt, std::nullopt, std::nullopt,
             std::nullopt, schema::kObject, std::nullopt, schema::kObject}));
    ASSERT_OK_AND_ASSIGN(auto edge, group_by.EdgeFromSchemasSlice(schemas));
    ASSERT_OK_AND_ASSIGN(auto result, group_by.CollapseByEdge(edge, schemas));

    EXPECT_THAT(result.AsDataItemDenseArray(),
                ElementsAre(DataItem(alloc.ObjectByOffset(0)),
                            DataItem(alloc.ObjectByOffset(1)),
                            DataItem(alloc.ObjectByOffset(2)),
                            DataItem(alloc.ObjectByOffset(6)),
                            DataItem(schema::kObject)));
  }
}

TEST(ObjectsGroupByTest, BySchemas) {
  auto group_by = ObjectsGroupBy();

  auto ds = DataSliceImpl::AllocateEmptyObjects(8);
  auto schema_alloc = AllocateExplicitSchemas(10);
  auto schemas = DataSliceImpl::Create(CreateDenseArray<ObjectId>(
      {schema_alloc.ObjectByOffset(0), schema_alloc.ObjectByOffset(1),
        schema_alloc.ObjectByOffset(0), schema_alloc.ObjectByOffset(2),
        schema_alloc.ObjectByOffset(6), schema_alloc.ObjectByOffset(2),
        schema_alloc.ObjectByOffset(1), schema_alloc.ObjectByOffset(2)}));

  ASSERT_OK_AND_ASSIGN((auto [group_schemas, ds_grouped]),
                        group_by.BySchemas(schemas, ds));
  EXPECT_THAT(group_schemas, ElementsAre(schema_alloc.ObjectByOffset(0),
                                          schema_alloc.ObjectByOffset(1),
                                          schema_alloc.ObjectByOffset(2),
                                          schema_alloc.ObjectByOffset(6)));
  EXPECT_EQ(ds_grouped.size(), 4);
  EXPECT_THAT(ds_grouped[0],
              ElementsAre(ds[0].value<ObjectId>(), ds[2].value<ObjectId>()));
  EXPECT_THAT(ds_grouped[1],
              ElementsAre(ds[1].value<ObjectId>(), ds[6].value<ObjectId>()));
  EXPECT_THAT(ds_grouped[2],
              ElementsAre(ds[3].value<ObjectId>(), ds[5].value<ObjectId>(),
                          ds[7].value<ObjectId>()));
  EXPECT_THAT(ds_grouped[3], ElementsAre(ds[4].value<ObjectId>()));
}

TEST(ObjectsGroupByTest, ByEdge) {
  auto group_by = ObjectsGroupBy();

  auto ds = DataSliceImpl::AllocateEmptyObjects(8);
  ASSERT_OK_AND_ASSIGN(
      auto edge, arolla::DenseArrayEdge::FromMapping(
                     CreateDenseArray<int64_t>({0, 1, 0, 2, 3, 2, 1, 2}), 4));

  ASSERT_OK_AND_ASSIGN(auto ds_grouped,
                       group_by.ByEdge(edge, ds.values<ObjectId>()));
  EXPECT_EQ(ds_grouped.size(), 4);
  EXPECT_THAT(ds_grouped[0],
              ElementsAre(ds[0].value<ObjectId>(), ds[2].value<ObjectId>()));
  EXPECT_THAT(ds_grouped[1],
              ElementsAre(ds[1].value<ObjectId>(), ds[6].value<ObjectId>()));
  EXPECT_THAT(ds_grouped[2],
              ElementsAre(ds[3].value<ObjectId>(), ds[5].value<ObjectId>(),
                          ds[7].value<ObjectId>()));
  EXPECT_THAT(ds_grouped[3], ElementsAre(ds[4].value<ObjectId>()));
}

TEST(ObjectsGroupByTest, GetItemIfAllEqual) {
  auto group_by = ObjectsGroupBy();
  {
    auto ds = DataSliceImpl::Create(
        CreateDenseArray<int64_t>({1, 1, std::nullopt, 1}));
    EXPECT_EQ(group_by.GetItemIfAllEqual(ds), 1);
  }
  {
    auto ds = DataSliceImpl::Create(
        CreateDenseArray<int64_t>({1, 1, std::nullopt, 2}));
    EXPECT_EQ(group_by.GetItemIfAllEqual(ds), std::nullopt);
  }
  {
    auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>(
        {DataItem(1), DataItem(1.0), DataItem(), DataItem(1)}));
    EXPECT_EQ(group_by.GetItemIfAllEqual(ds), std::nullopt);
  }
  {
    auto ds = DataSliceImpl::Create(
        CreateDenseArray<int64_t>({std::nullopt, std::nullopt}));
    EXPECT_EQ(group_by.GetItemIfAllEqual(ds), std::nullopt);
  }
  {
    auto ds = DataSliceImpl::Create(CreateDenseArray<int64_t>({}));
    EXPECT_EQ(group_by.GetItemIfAllEqual(ds), std::nullopt);
  }
  {
    auto ds = DataSliceImpl::CreateEmptyAndUnknownType(3);
    EXPECT_EQ(group_by.GetItemIfAllEqual(ds), std::nullopt);
  }
}

// Returns the (key, objects) groups passed to ForEach().
absl::StatusOr<std::vector<std::pair<DataItem, DataSliceImpl>>> GroupByKeys(
    ObjectsGroupBy& group_by, DataSliceImpl keys, DataSliceImpl objects) {
  std::vector<std::pair<DataItem, DataSliceImpl>> groups;
  RETURN_IF_ERROR(group_by.ByKeys(std::move(keys))
                      .Split(std::move(objects))
                      .ForEach([&](const DataItem& key, DataSliceImpl objects) {
                        groups.emplace_back(key, std::move(objects));
                        return absl::OkStatus();
                      }));
  return groups;
}

TEST(ObjectsGroupByTest, ByKeysSingleGroup) {
  ObjectsGroupBy group_by;
  AllocationId alloc = Allocate(10);
  // An extra allocation id that is kept, since no split is needed.
  auto objects = DataSliceImpl::CreateWithAllocIds(
      AllocationIdSet({alloc, Allocate(10)}),
      CreateDenseArray<ObjectId>(
          {alloc.ObjectByOffset(0), alloc.ObjectByOffset(1)}));
  {
    auto keys = DataSliceImpl::Create(CreateDenseArray<int32_t>({7, 7}));
    ASSERT_OK_AND_ASSIGN(auto groups, GroupByKeys(group_by, keys, objects));
    ASSERT_THAT(groups, ElementsAre(Pair(DataItem(7),
                                         ElementsAre(objects[0], objects[1]))));
    EXPECT_EQ(groups[0].second.allocation_ids(), objects.allocation_ids());
  }
  {
    auto keys =
        DataSliceImpl::Create(CreateDenseArray<int32_t>({7, std::nullopt}));
    ASSERT_OK_AND_ASSIGN(auto groups, GroupByKeys(group_by, keys, objects));
    ASSERT_THAT(groups,
                ElementsAre(Pair(DataItem(7), ElementsAre(objects[0]))));
    EXPECT_EQ(groups[0].second.allocation_ids(), AllocationIdSet(alloc));
  }
}

TEST(ObjectsGroupByTest, ByKeysInOrderOfFirstOccurrence) {
  ObjectsGroupBy group_by;
  auto objs = DataSliceImpl::AllocateEmptyObjects(5);
  auto keys = DataSliceImpl::Create(CreateDenseArray<int32_t>({2, 1, 2, 3, 1}));
  ASSERT_OK_AND_ASSIGN(auto groups, GroupByKeys(group_by, keys, objs));
  EXPECT_THAT(groups,
              ElementsAre(Pair(DataItem(2), ElementsAre(objs[0], objs[2])),
                          Pair(DataItem(1), ElementsAre(objs[1], objs[4])),
                          Pair(DataItem(3), ElementsAre(objs[3]))));
}

TEST(ObjectsGroupByTest, ByMixedKeys) {
  ObjectsGroupBy group_by;
  auto objs = DataSliceImpl::AllocateEmptyObjects(3);
  auto keys = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {DataItem(1), DataItem(schema::kInt32), DataItem(1)}));
  ASSERT_OK_AND_ASSIGN(auto groups, GroupByKeys(group_by, keys, objs));
  EXPECT_THAT(
      groups,
      ElementsAre(Pair(DataItem(1), ElementsAre(objs[0], objs[2])),
                  Pair(DataItem(schema::kInt32), ElementsAre(objs[1]))));
}

TEST(ObjectsGroupByTest, ByMultipleKeys) {
  ObjectsGroupBy group_by;
  auto objs = DataSliceImpl::AllocateEmptyObjects(4);
  auto keys_a = DataSliceImpl::Create(CreateDenseArray<int32_t>({1, 2, 1, 1}));
  auto keys_b =
      DataSliceImpl::Create(CreateDenseArray<int64_t>({10, 10, 20, 10}));
  auto keys_const =
      DataSliceImpl::Create(CreateDenseArray<int32_t>({5, 5, 5, 5}));
  {
    std::vector<std::pair<DataItem, DataItem>> keys;
    std::vector<DataSliceImpl> groups;
    ASSERT_OK(group_by.ByKeys(keys_a, keys_b)
                  .Split(objs)
                  .ForEach([&](const DataItem& key_a, const DataItem& key_b,
                               DataSliceImpl objects) {
                    keys.emplace_back(key_a, key_b);
                    groups.push_back(std::move(objects));
                    return absl::OkStatus();
                  }));
    EXPECT_THAT(keys, ElementsAre(Pair(DataItem(1), DataItem(int64_t{10})),
                                  Pair(DataItem(2), DataItem(int64_t{10})),
                                  Pair(DataItem(1), DataItem(int64_t{20}))));
    EXPECT_THAT(groups,
                ElementsAre(ElementsAre(objs[0], objs[3]), ElementsAre(objs[1]),
                            ElementsAre(objs[2])));
  }
  {
    std::vector<std::pair<DataItem, DataItem>> keys;
    std::vector<DataSliceImpl> groups;
    ASSERT_OK(group_by.ByKeys(keys_const, keys_a)
                  .Split(objs)
                  .ForEach([&](const DataItem& key_const, const DataItem& key_a,
                               DataSliceImpl objects) {
                    keys.emplace_back(key_const, key_a);
                    groups.push_back(std::move(objects));
                    return absl::OkStatus();
                  }));
    EXPECT_THAT(keys, ElementsAre(Pair(DataItem(5), DataItem(1)),
                                  Pair(DataItem(5), DataItem(2))));
    EXPECT_THAT(groups, ElementsAre(ElementsAre(objs[0], objs[2], objs[3]),
                                    ElementsAre(objs[1])));
  }
}

TEST(ObjectsGroupByTest, ByKeysDropsMissingItems) {
  ObjectsGroupBy group_by;
  auto objs = DataSliceImpl::AllocateEmptyObjects(5);
  AllocationId schema_alloc = AllocateExplicitSchemas(10);
  std::vector<DataItem> schemas;
  for (int64_t i = 0; i < 5; ++i) {
    schemas.push_back(DataItem(schema_alloc.ObjectByOffset(i)));
  }
  auto objects = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {objs[0], objs[1], DataItem(), objs[3], objs[4]}));
  auto object_schemas = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {schemas[0], schemas[1], schemas[2], schemas[3], DataItem()}));
  auto keys = DataSliceImpl::Create(
      CreateDenseArray<int32_t>({1, std::nullopt, 1, 2, 2}));
  std::vector<DataItem> group_keys;
  std::vector<std::pair<DataSliceImpl, DataSliceImpl>> groups;
  ASSERT_OK(group_by.ByKeys(keys)
                .Split(objects, object_schemas)
                .ForEach([&](const DataItem& key, DataSliceImpl objects,
                             DataSliceImpl schemas) {
                  group_keys.push_back(key);
                  groups.emplace_back(std::move(objects), std::move(schemas));
                  return absl::OkStatus();
                }));
  EXPECT_THAT(group_keys, ElementsAre(DataItem(1), DataItem(2)));
  EXPECT_THAT(groups,
              ElementsAre(Pair(ElementsAre(objs[0]), ElementsAre(schemas[0])),
                          Pair(ElementsAre(objs[3]), ElementsAre(schemas[3]))));
}

TEST(ObjectsGroupByTest, ByKeysNothingPresent) {
  ObjectsGroupBy group_by;
  ASSERT_OK_AND_ASSIGN(
      auto groups,
      GroupByKeys(group_by, DataSliceImpl::CreateEmptyAndUnknownType(3),
                  DataSliceImpl::AllocateEmptyObjects(3)));
  EXPECT_THAT(groups, ElementsAre());
}

TEST(ObjectsGroupByTest, ByKeysRecomputesAllocationIds) {
  ObjectsGroupBy group_by;
  AllocationId alloc_a = Allocate(10);
  AllocationId alloc_b = Allocate(10);
  auto objects = DataSliceImpl::Create(CreateDenseArray<ObjectId>(
      {alloc_a.ObjectByOffset(0), alloc_b.ObjectByOffset(0),
       alloc_a.ObjectByOffset(1)}));
  auto keys = DataSliceImpl::Create(CreateDenseArray<int32_t>({1, 2, 1}));
  ASSERT_OK_AND_ASSIGN(auto groups, GroupByKeys(group_by, keys, objects));
  ASSERT_EQ(groups.size(), 2);
  EXPECT_EQ(groups[0].second.allocation_ids(), AllocationIdSet(alloc_a));
  EXPECT_EQ(groups[1].second.allocation_ids(), AllocationIdSet(alloc_b));
}

TEST(ObjectsGroupByTest, ByKeysStopsAtFirstError) {
  ObjectsGroupBy group_by;
  auto objs = DataSliceImpl::AllocateEmptyObjects(3);
  auto keys = DataSliceImpl::Create(CreateDenseArray<int32_t>({1, 2, 3}));
  int calls = 0;
  EXPECT_THAT(group_by.ByKeys(keys).Split(objs).ForEach(
                  [&](const DataItem& key, const DataSliceImpl& objects) {
                    ++calls;
                    return absl::InvalidArgumentError("error");
                  }),
              StatusIs(absl::StatusCode::kInvalidArgument, "error"));
  EXPECT_EQ(calls, 1);
}

TEST(ObjectsGroupByTest, ByKeysNested) {
  ObjectsGroupBy group_by;
  auto objs = DataSliceImpl::AllocateEmptyObjects(4);
  auto keys = DataSliceImpl::Create(CreateDenseArray<int32_t>({1, 2, 1, 2}));
  std::vector<DataSliceImpl> groups;
  ASSERT_OK(group_by.ByKeys(keys).Split(objs).ForEach(
      [&](const DataItem& key, DataSliceImpl objects) -> absl::Status {
        // Uses `group_by` while iterating over its groups.
        ASSIGN_OR_RETURN(auto inner_groups,
                         GroupByKeys(group_by, objects, objects));
        for (auto& [inner_key, inner_objects] : inner_groups) {
          groups.push_back(std::move(inner_objects));
        }
        return absl::OkStatus();
      }));
  EXPECT_THAT(groups, ElementsAre(ElementsAre(objs[0]), ElementsAre(objs[2]),
                                  ElementsAre(objs[1]), ElementsAre(objs[3])));
}

}  // namespace
}  // namespace koladata::internal
