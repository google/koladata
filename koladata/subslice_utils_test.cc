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
#include "koladata/subslice_utils.h"

#include <cstdint>
#include <initializer_list>
#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "koladata/data_slice.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"

namespace koladata::subslice {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::CreateFullDenseArray;
using ::koladata::testing::IsEquivalentTo;

DataSlice::JaggedShape::Edge CreateEdge(
    std::initializer_list<int64_t> split_points) {
  return *DataSlice::JaggedShape::Edge::FromSplitPoints(
      CreateFullDenseArray(std::vector<int64_t>(split_points)));
}

absl::StatusOr<DataSlice::JaggedShape> ShapeFromEdges(
    std::initializer_list<DataSlice::JaggedShape::Edge> edges) {
  return DataSlice::JaggedShape::FromEdges(edges);
}

// Most of the tests for Subslice are in Python, in slices_subslice_test.py.
// Here we check some basic cases.
TEST(SubsliceUtilsTest, Subslice1D) {
  auto ds = test::DataSlice<int>({1, 2, 3});
  auto indices_ds =
      test::DataSlice<int>({-4, -3, -2, -1, 0, std::nullopt, 1, 2, 3});
  auto expected_ds = test::DataSlice<int>(
      {std::nullopt, 1, 2, 3, 1, std::nullopt, 2, 3, std::nullopt});
  EXPECT_THAT(Subslice(ds, {indices_ds}),
              IsOkAndHolds(IsEquivalentTo(expected_ds)));
}

TEST(SubsliceUtilsTest, Subslice2DLastDimension) {
  ASSERT_OK_AND_ASSIGN(auto shape, ShapeFromEdges({CreateEdge({0, 3}),
                                                   CreateEdge({0, 2, 4, 7})}));
  auto ds = test::DataSlice<int>({1, 2, 3, 4, 5, 6, 7}, shape);
  ASSERT_OK_AND_ASSIGN(
      auto indices_shape,
      ShapeFromEdges({CreateEdge({0, 3}), CreateEdge({0, 6, 12, 18})}));
  auto indices_ds = test::DataSlice<int>(
      {-3, -2, -1, 0, 1, 2, 2, 1, 0, -1, -2, -3, -3, -2, std::nullopt, 0, 1, 2},
      indices_shape);
  auto expected_ds = test::DataSlice<int>(
      {std::nullopt, 1, 2, 1, 2, std::nullopt, std::nullopt, 4, 3, 4, 3,
       std::nullopt, 5, 6, std::nullopt, 5, 6, 7},
      indices_shape);
  EXPECT_THAT(Subslice(ds, {indices_ds}),
              IsOkAndHolds(IsEquivalentTo(expected_ds)));
}

TEST(SubsliceUtilsTest, Subslice2DLastDimensionWithExpand) {
  ASSERT_OK_AND_ASSIGN(auto shape, ShapeFromEdges({CreateEdge({0, 3}),
                                                   CreateEdge({0, 2, 4, 7})}));
  auto ds = test::DataSlice<int>({1, 2, 3, 4, 5, 6, 7}, shape);
  auto indices_ds = test::DataItem(-2);
  auto expected_ds = test::DataSlice<int>({1, 3, 6});
  EXPECT_THAT(Subslice(ds, {indices_ds}),
              IsOkAndHolds(IsEquivalentTo(expected_ds)));
}

TEST(SubsliceUtilsTest, Subslice2DBothDimensions) {
  ASSERT_OK_AND_ASSIGN(auto shape, ShapeFromEdges({CreateEdge({0, 3}),
                                                   CreateEdge({0, 2, 4, 7})}));
  auto ds = test::DataSlice<int>({1, 2, 3, 4, 5, 6, 7}, shape);
  auto indices1_ds = test::DataSlice<int>({2, 1});
  ASSERT_OK_AND_ASSIGN(
      auto indices2_shape,
      ShapeFromEdges({CreateEdge({0, 2}), CreateEdge({0, 6, 12})}));
  auto indices2_ds = test::DataSlice<int>(
      {-3, -2, -1, 0, 1, 2, 2, 1, 0, -1, -2, -3}, indices2_shape);
  auto expected_ds = test::DataSlice<int>(
      {5, 6, 7, 5, 6, 7, std::nullopt, 4, 3, 4, 3, std::nullopt},
      indices2_shape);
  EXPECT_THAT(Subslice(ds, {indices1_ds, indices2_ds}),
              IsOkAndHolds(IsEquivalentTo(expected_ds)));
}

TEST(SubsliceUtilsTest, Subslice2DRange) {
  ASSERT_OK_AND_ASSIGN(auto shape, ShapeFromEdges({CreateEdge({0, 3}),
                                                   CreateEdge({0, 2, 4, 7})}));
  auto ds = test::DataSlice<int>({1, 2, 3, 4, 5, 6, 7}, shape);
  auto low = test::DataItem(-2);
  auto high = test::DataItem(3);
  ASSERT_OK_AND_ASSIGN(
      auto indices2_shape,
      ShapeFromEdges({CreateEdge({0, 2}), CreateEdge({0, 6, 12})}));
  auto indices2_ds = test::DataSlice<int>(
      {-3, -2, -1, 0, 1, 2, 2, 1, 0, -1, -2, -3}, indices2_shape);
  auto expected_ds = test::DataSlice<int>(
      {std::nullopt, 3, 4, 3, 4, std::nullopt, 7, 6, 5, 7, 6, 5},
      indices2_shape);
  EXPECT_THAT(Subslice(ds, {subslice::Slice{low, high}, indices2_ds}),
              IsOkAndHolds(IsEquivalentTo(expected_ds)));
}

}  // namespace
}  // namespace koladata::subslice
