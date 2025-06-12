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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {
namespace {

using ::testing::ElementsAre;

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
}

}  // namespace
}  // namespace koladata::internal
