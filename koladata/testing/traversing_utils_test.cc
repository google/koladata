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
#include "koladata/testing/traversing_utils.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"

namespace koladata {
namespace {

using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::MatchesRegex;
using ::koladata::testing::DeepEquivalentMismatches;

TEST(TraversingUtilsTest, ShapesAreNotEquivalent) {
  int64_t size_a = 2;
  int64_t size_b = 3;
  auto shape_a = DataSlice::JaggedShape::FlatFromSize(size_a);
  auto shape_b = DataSlice::JaggedShape::FlatFromSize(size_b);
  auto objects_a = internal::DataSliceImpl::AllocateEmptyObjects(size_a);
  auto objects_b = internal::DataSliceImpl::AllocateEmptyObjects(size_b);
  auto explicit_schema = internal::AllocateExplicitSchema();
  ASSERT_OK_AND_ASSIGN(auto ds_a,
                       DataSlice::Create(objects_a, shape_a,
                                         internal::DataItem(explicit_schema)));
  ASSERT_OK_AND_ASSIGN(auto ds_b,
                       DataSlice::Create(objects_b, shape_b,
                                         internal::DataItem(explicit_schema)));
  ASSERT_OK_AND_ASSIGN(
      auto result, DeepEquivalentMismatches(ds_a, ds_b, /*max_count=*/2, {}));
  EXPECT_THAT(result,
              ElementsAre("expected both DataSlices to be of the same shape "
                          "but got JaggedShape(2) and JaggedShape(3)"));
}

TEST(TraversingUtilsTest, SlicesAreEquivalent) {
  int64_t size = 2;
  auto shape = DataSlice::JaggedShape::FlatFromSize(size);
  auto objects_a = internal::DataSliceImpl::AllocateEmptyObjects(size);
  auto objects_b = internal::DataSliceImpl::AllocateEmptyObjects(size);
  auto explicit_schema = internal::AllocateExplicitSchema();
  ASSERT_OK_AND_ASSIGN(
      auto ds_a,
      DataSlice::Create(objects_a, shape, internal::DataItem(explicit_schema)));
  ASSERT_OK_AND_ASSIGN(
      auto ds_b,
      DataSlice::Create(objects_b, shape, internal::DataItem(explicit_schema)));
  ASSERT_OK_AND_ASSIGN(
      auto result, DeepEquivalentMismatches(ds_a, ds_b, /*max_count=*/2, {}));
  EXPECT_THAT(result, IsEmpty());
}

TEST(TraversingUtilsTest, TraversalError) {
  int64_t size = 2;
  auto shape = DataSlice::JaggedShape::FlatFromSize(size);
  auto objects_a = internal::DataSliceImpl::AllocateEmptyObjects(size);
  auto objects_b = internal::DataSliceImpl::AllocateEmptyObjects(size);
  auto explicit_schema = internal::AllocateExplicitSchema();
  ASSERT_OK_AND_ASSIGN(
      auto ds_a,
      DataSlice::Create(objects_a, shape, internal::DataItem(explicit_schema)));
  ASSERT_OK_AND_ASSIGN(
      auto ds_b,
      DataSlice::Create(objects_b, shape, internal::DataItem(schema::kObject)));
  auto result = DeepEquivalentMismatches(ds_a, ds_b, /*max_count=*/2, {});
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST(TraversingUtilsTest, SlicesMismatch) {
  int64_t size = 2;
  auto shape = DataSlice::JaggedShape::FlatFromSize(size);
  auto objects_a = internal::DataSliceImpl::Create(size, internal::DataItem(2));
  auto objects_b = internal::DataSliceImpl::Create(size, internal::DataItem(3));
  ASSERT_OK_AND_ASSIGN(
      auto ds_a,
      DataSlice::Create(objects_a, shape, internal::DataItem(schema::kObject)));
  ASSERT_OK_AND_ASSIGN(
      auto ds_b,
      DataSlice::Create(objects_b, shape, internal::DataItem(schema::kObject)));
  {
    ASSERT_OK_AND_ASSIGN(auto mismatches, DeepEquivalentMismatches(
                                              ds_a, ds_b, /*max_count=*/2, {}));
    EXPECT_THAT(
        mismatches,
        ::testing::UnorderedElementsAreArray(
            {"modified:\nexpected.S[0]:\nDataItem(3, schema: OBJECT)\n"
             "-> actual.S[0]:\nDataItem(2, schema: OBJECT)",
             "modified:\nexpected.S[1]:\nDataItem(3, schema: OBJECT)\n"
             "-> actual.S[1]:\nDataItem(2, schema: OBJECT)"}));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto mismatches, DeepEquivalentMismatches(
                                              ds_a, ds_b, /*max_count=*/1, {}));
    EXPECT_THAT(mismatches,
                ElementsAre(MatchesRegex(
                    "modified:\nexpected\\.S\\[.\\]:\nDataItem\\(3, schema: "
                    "OBJECT\\)\n-> actual\\.S\\[.\\]:\nDataItem\\(2, schema: "
                    "OBJECT\\)")));
  }
}

}  // namespace
}  // namespace koladata
