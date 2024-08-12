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
#include "koladata/internal/op_utils/equal.h"

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata::internal {
namespace {
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;

using ::arolla::CreateDenseArray;
using ::arolla::GetQType;
using ::arolla::kMissing;
using ::arolla::kPresent;
using ::arolla::Text;
using ::arolla::Unit;

constexpr size_t kLargeAllocSize = 2000;

static_assert(kLargeAllocSize > kSmallAllocMaxCapacity);

TEST(EqualTest, DataSlicePrimitiveValues) {
  {
    // Int.
    auto l = CreateDenseArray<int>({1, 2, 12, std::nullopt, std::nullopt});
    auto r = CreateDenseArray<int>({1, 3, std::nullopt, 4, std::nullopt});
    auto lds = DataSliceImpl::Create(l);
    auto rds = DataSliceImpl::Create(r);

    ASSERT_OK_AND_ASSIGN(auto res, EqualOp()(lds, rds));
    EXPECT_THAT(res.values<Unit>(),
                ElementsAre(kPresent, kMissing, kMissing, kMissing, kMissing));
  }
  {
    // Int32 and Int64.
    auto l = CreateDenseArray<int>({1, 2, 12, std::nullopt, std::nullopt});
    auto r = CreateDenseArray<int64_t>({1, 3, std::nullopt, 4, std::nullopt});
    auto lds = DataSliceImpl::Create(l);
    auto rds = DataSliceImpl::Create(r);

    ASSERT_OK_AND_ASSIGN(auto res, EqualOp()(lds, rds));
    EXPECT_THAT(res.values<Unit>(),
                ElementsAre(kPresent, kMissing, kMissing, kMissing, kMissing));

    // Same result if int64 is first argument.
    ASSERT_OK_AND_ASSIGN(res, EqualOp()(rds, lds));
    EXPECT_THAT(res.values<Unit>(),
                ElementsAre(kPresent, kMissing, kMissing, kMissing, kMissing));
  }
  {
    // Float32 and Float64.
    auto l = CreateDenseArray<float>({1, 2, 12, std::nullopt, std::nullopt});
    auto r = CreateDenseArray<double>({1, 3, std::nullopt, 4, std::nullopt});
    auto lds = DataSliceImpl::Create(l);
    auto rds = DataSliceImpl::Create(r);

    ASSERT_OK_AND_ASSIGN(auto res, EqualOp()(lds, rds));
    EXPECT_THAT(res.values<Unit>(),
                ElementsAre(kPresent, kMissing, kMissing, kMissing, kMissing));

    // Same result if Float64 is first argument.
    ASSERT_OK_AND_ASSIGN(res, EqualOp()(rds, lds));
    EXPECT_THAT(res.values<Unit>(),
                ElementsAre(kPresent, kMissing, kMissing, kMissing, kMissing));
  }
  {
    // Int32 and Float32.
    auto l = CreateDenseArray<int>({1, 2, 12, std::nullopt, std::nullopt});
    auto r = CreateDenseArray<float>({1, 3, std::nullopt, 4, std::nullopt});
    auto lds = DataSliceImpl::Create(l);
    auto rds = DataSliceImpl::Create(r);

    ASSERT_OK_AND_ASSIGN(auto res, EqualOp()(lds, rds));
    EXPECT_THAT(res.values<Unit>(),
                ElementsAre(kPresent, kMissing, kMissing, kMissing, kMissing));
  }
  {
    // Text
    auto l =
        CreateDenseArray<Text>({Text("a"), Text("b"), std::nullopt, Text("c")});
    auto r =
        CreateDenseArray<Text>({std::nullopt, Text("b"), Text("c"), Text("d")});
    auto lds = DataSliceImpl::Create(l);
    auto rds = DataSliceImpl::Create(r);

    ASSERT_OK_AND_ASSIGN(auto res, EqualOp()(lds, rds));
    EXPECT_THAT(res.values<Unit>(),
                ElementsAre(kMissing, kPresent, kMissing, kMissing));
  }
}

TEST(PresenceOrTest, DataSliceMixedPrimitiveValues) {
  {
    auto l_int =
        CreateDenseArray<int>({1, std::nullopt, std::nullopt, std::nullopt});
    auto l_float = CreateDenseArray<float>(
        {std::nullopt, std::nullopt, 2., 3.});
    auto r_int64_t =
        CreateDenseArray<int64_t>({1, std::nullopt, 2, std::nullopt});
    auto r_text = CreateDenseArray<Text>(
        {std::nullopt, Text("c"), std::nullopt, Text("d")});
    auto lds = DataSliceImpl::Create(l_int, l_float);
    auto rds = DataSliceImpl::Create(r_text, r_int64_t);

    ASSERT_OK_AND_ASSIGN(auto res, EqualOp()(lds, rds));
    EXPECT_THAT(res.values<Unit>(),
                ElementsAre(kPresent, kMissing, kPresent, kMissing));
  }
}

TEST(EqualTest, DataSliceObjectId) {
  {
    auto obj_id = Allocate(kLargeAllocSize).ObjectByOffset(1909);
    auto alloc_id_1 = AllocationId(obj_id);
    AllocationId alloc_id_2 = Allocate(2);
    auto lhs_objects =
        CreateDenseArray<ObjectId>({alloc_id_2.ObjectByOffset(0), std::nullopt,
                                    alloc_id_2.ObjectByOffset(1)});
    auto rhs_objects = CreateDenseArray<ObjectId>(
        {std::nullopt, obj_id, alloc_id_2.ObjectByOffset(1)});
    auto lhs = DataSliceImpl::CreateObjectsDataSlice(
        lhs_objects, AllocationIdSet({alloc_id_2}));
    auto rhs = DataSliceImpl::CreateObjectsDataSlice(
        rhs_objects, AllocationIdSet({alloc_id_1, alloc_id_2}));
    ASSERT_EQ(lhs.dtype(), GetQType<ObjectId>());
    ASSERT_EQ(rhs.dtype(), GetQType<ObjectId>());

    ASSERT_OK_AND_ASSIGN(auto res, EqualOp()(lhs, rhs));
    EXPECT_THAT(res.values<Unit>(), ElementsAre(kMissing, kMissing, kPresent));
  }
}

TEST(EqualTest, LengthMismatch) {
  auto l_float = CreateDenseArray<float>({3.14, 2.71});
  auto r_unit = CreateDenseArray<Unit>({kMissing, kMissing, kPresent});
  auto lds = DataSliceImpl::Create(l_float);
  auto rds = DataSliceImpl::Create(r_unit);

  EXPECT_THAT(EqualOp()(lds, rds),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "equal requires input slices to have the same size"));
}

TEST(EqualTest, DataItem) {
  {
    // Int32.
    auto litem = DataItem(5);
    auto ritem = DataItem(5);
    auto res = EqualOp()(litem, ritem);
    EXPECT_EQ(res.value<Unit>(), kPresent);
  }
  {
    // Int32 not equal.
    auto litem = DataItem(5);
    auto ritem = DataItem(3);
    auto res = EqualOp()(litem, ritem);
    EXPECT_FALSE(res.has_value());
  }
  {
    // Int32 and Float32.
    auto litem = DataItem(5);
    auto ritem = DataItem(5.);
    auto res = EqualOp()(litem, ritem);
    EXPECT_EQ(res.value<Unit>(), kPresent);
  }
  {
    // Int32 and Int64.
    auto litem = DataItem(5);
    auto ritem = DataItem(static_cast<int64_t>(5));
    auto res = EqualOp()(litem, ritem);
    EXPECT_EQ(res.value<Unit>(), kPresent);
  }
  {
    auto litem = DataItem();
    auto ritem = DataItem();
    auto res = EqualOp()(litem, ritem);
    EXPECT_FALSE(res.has_value());
  }
  {
    // Objects.
    auto litem = DataItem(AllocateSingleObject());
    auto ritem = DataItem(AllocateSingleObject());
    auto res = EqualOp()(litem, ritem);
    EXPECT_FALSE(res.has_value());
  }
}

}  // namespace
}  // namespace koladata::internal
