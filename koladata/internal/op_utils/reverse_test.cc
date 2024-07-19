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
#include "koladata/internal/op_utils/reverse.h"

#include <cstddef>
#include <cstdint>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/util/text.h"

namespace koladata::internal {
namespace {

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;

TEST(ReverseTest, Empty) {
  size_t size = 7;
  EXPECT_THAT(ReverseOp{}(DataSliceImpl::CreateEmptyAndUnknownType(size),
                          arolla::JaggedDenseArrayShape::FlatFromSize(size)),
              ElementsAreArray(std::vector<DataItem>(size)));
}

TEST(ReverseTest, EmptyWithTypeAndRank1) {
  EXPECT_THAT(ReverseOp{}(DataSliceImpl::CreateAllMissingObjectDataSlice(0),
                          arolla::JaggedDenseArrayShape::FlatFromSize(0)),
              ElementsAreArray(std::vector<DataItem>(0)));
}

TEST(ReverseTest, EmptyWithTypeAndRank2NoParent) {
  EXPECT_THAT(ReverseOp{}(DataSliceImpl::CreateAllMissingObjectDataSlice(0),
                          *arolla::JaggedDenseArrayShape::FromEdges(
                              {arolla::DenseArrayEdge::UnsafeFromSplitPoints(
                                   arolla::CreateDenseArray<int64_t>({0, 0})),
                               arolla::DenseArrayEdge::UnsafeFromSplitPoints(
                                   arolla::CreateDenseArray<int64_t>({0}))})),
              ElementsAreArray(std::vector<DataItem>(0)));
}

TEST(ReverseTest, OneDim) {
  EXPECT_THAT(ReverseOp{}(DataSliceImpl::Create(
                              {DataItem(1), DataItem(2), DataItem(3)}),
                          arolla::JaggedDenseArrayShape::FlatFromSize(3)),
              ElementsAre(DataItem(3), DataItem(2), DataItem(1)));
}

TEST(ReverseTest, OneDimMissing) {
  EXPECT_THAT(
      ReverseOp{}(DataSliceImpl::Create({DataItem(1), DataItem(), DataItem(3)}),
                  arolla::JaggedDenseArrayShape::FlatFromSize(3)),
      ElementsAre(DataItem(3), DataItem(), DataItem(1)));
}

TEST(ReverseTest, OneDimMixedType) {
  EXPECT_THAT(ReverseOp{}(DataSliceImpl::Create({DataItem(1), DataItem(2.0f),
                                                 DataItem(2),
                                                 DataItem(arolla::Text("a"))}),
                          arolla::JaggedDenseArrayShape::FlatFromSize(4)),
              ElementsAre(DataItem(arolla::Text("a")), DataItem(2),
                          DataItem(2.0f), DataItem(1)));
}

TEST(ReverseTest, TwoDim) {
  EXPECT_THAT(
      ReverseOp{}(DataSliceImpl::Create({DataItem(1), DataItem(2), DataItem(3),
                                         DataItem(4), DataItem(5), DataItem(6),
                                         DataItem(7)}),
                  *arolla::JaggedDenseArrayShape::FromEdges(
                      {arolla::DenseArrayEdge::UnsafeFromSplitPoints(
                           arolla::CreateDenseArray<int64_t>({0, 2})),
                       arolla::DenseArrayEdge::UnsafeFromSplitPoints(
                           arolla::CreateDenseArray<int64_t>({0, 4, 7}))})),
      ElementsAre(DataItem(4), DataItem(3), DataItem(2), DataItem(1),
                  DataItem(7), DataItem(6), DataItem(5)));
}

}  // namespace
}  // namespace koladata::internal
