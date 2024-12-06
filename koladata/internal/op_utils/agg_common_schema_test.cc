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
#include "koladata/internal/op_utils/agg_common_schema.h"

#include <cstdint>
#include <initializer_list>
#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/util/init_arolla.h"

namespace koladata::internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::CreateDenseArray;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

arolla::DenseArrayEdge CreateEdge(std::initializer_list<int64_t> split_points) {
  return *arolla::DenseArrayEdge::FromSplitPoints(
      arolla::CreateFullDenseArray(std::vector<int64_t>(split_points)));
}

TEST(AggCommonSchemaTest, WithCommonSchema) {
  {
    // DTypes.
    auto ds = DataSliceImpl::Create(CreateDenseArray<schema::DType>(
        {schema::kInt32, std::nullopt, schema::kInt64, std::nullopt,
         schema::kInt32}));
    auto edge = CreateEdge({0, 3, 4, 5});
    EXPECT_THAT(AggCommonSchemaOp(ds, edge),
                IsOkAndHolds(
                    ElementsAre(schema::kInt64, std::nullopt, schema::kInt32)));
  }
  {
    // Entity schemas.
    auto schema = AllocateExplicitSchema();
    auto ds = DataSliceImpl::Create(CreateDenseArray<ObjectId>(
        {schema, std::nullopt, schema, std::nullopt, schema}));
    auto edge = CreateEdge({0, 3, 4, 5});
    EXPECT_THAT(AggCommonSchemaOp(ds, edge),
                IsOkAndHolds(ElementsAre(schema, std::nullopt, schema)));
  }
  {
    // Mix.
    auto schema = AllocateExplicitSchema();
    auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>(
        {DataItem(schema::kInt32), std::nullopt, DataItem(schema::kInt64),
         std::nullopt, DataItem(schema)}));
    auto edge = CreateEdge({0, 3, 4, 5});
    EXPECT_THAT(
        AggCommonSchemaOp(ds, edge),
        IsOkAndHolds(ElementsAre(schema::kInt64, std::nullopt, schema)));
  }
}

TEST(AggCommonSchemaTest, NoCommonSchema) {
  arolla::InitArolla();
  {
    // DTypes.
    auto ds = DataSliceImpl::Create(CreateDenseArray<schema::DType>(
        {schema::kInt32, std::nullopt, schema::kItemId, std::nullopt,
         schema::kInt32}));
    auto edge = CreateEdge({0, 3, 4, 5});
    EXPECT_THAT(AggCommonSchemaOp(ds, edge),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("no common schema")));
  }
  {
    // Entity schemas.
    auto schema1 = AllocateExplicitSchema();
    auto schema2 = AllocateExplicitSchema();
     auto ds = DataSliceImpl::Create(CreateDenseArray<ObjectId>(
        {schema1, std::nullopt, schema2, std::nullopt, schema1}));
    auto edge = CreateEdge({0, 3, 4, 5});
    EXPECT_THAT(AggCommonSchemaOp(ds, edge),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("no common schema")));
  }
}

TEST(AggCommonSchemaTest, InvalidEdge) {
  auto ds =
      DataSliceImpl::Create(CreateDenseArray<schema::DType>({schema::kInt32}));
  auto edge = CreateEdge({0, 10});
  EXPECT_THAT(AggCommonSchemaOp(ds, edge),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("argument sizes mismatch")));
}

TEST(AggCommonSchemaTest, NonSchemaValue) {
  auto ds = DataSliceImpl::Create(CreateDenseArray<int>({1}));
  auto edge = CreateEdge({0, 1});
  EXPECT_THAT(AggCommonSchemaOp(ds, edge),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected Schema, got: 1")));
}

}  // namespace
}  // namespace koladata::internal
