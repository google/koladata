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
#include "koladata/internal/data_slice.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/internal/types.h"
#include "koladata/internal/types_buffer.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"

namespace koladata::internal {
namespace {

using ::arolla::Bytes;
using ::arolla::CreateDenseArray;
using ::arolla::OptionalValue;
using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::Ge;
using ::testing::IsEmpty;
using ::testing::Lt;
using ::testing::MatchesRegex;
using ::testing::SizeIs;

struct TryCompileAssignment {
  template <typename T>
  auto operator()(T v) -> decltype(v[0] = DataItem()) {
    return v[0];
  }
};

// Test that `array[i] = value` doesn't compile (DataSlice is immutable).
static_assert(!std::is_invocable_v<TryCompileAssignment, DataSliceImpl>);

TEST(DataSliceImpl, EmptyArrayConstructionMustBeEmptyAndUnknown) {
  EXPECT_TRUE(DataSliceImpl::AllocateEmptyObjects(0).is_empty_and_unknown());
  EXPECT_TRUE(DataSliceImpl::ObjectsFromAllocation(Allocate(0), 0)
                  .is_empty_and_unknown());
  EXPECT_TRUE(
      DataSliceImpl::CreateEmptyAndUnknownType(0).is_empty_and_unknown());
  EXPECT_TRUE(
      DataSliceImpl::CreateEmptyAndUnknownType(77).is_empty_and_unknown());
  {
    SCOPED_TRACE("from empty single dense array");
    EXPECT_TRUE(
        DataSliceImpl::CreateObjectsDataSlice(
            arolla::CreateEmptyDenseArray<ObjectId>(17), AllocationIdSet())
            .is_empty_and_unknown());
    EXPECT_TRUE(
        DataSliceImpl::CreateWithAllocIds(
            AllocationIdSet(), arolla::CreateEmptyDenseArray<ObjectId>(17))
            .is_empty_and_unknown());
    EXPECT_TRUE(
        DataSliceImpl::Create(arolla::CreateEmptyDenseArray<DataItem>(17))
            .is_empty_and_unknown());
    EXPECT_TRUE(
        DataSliceImpl::Create(arolla::CreateEmptyDenseArray<ObjectId>(17))
            .is_empty_and_unknown());
    EXPECT_TRUE(
        DataSliceImpl::Create(arolla::CreateEmptyDenseArray<float>(17))
            .is_empty_and_unknown());
  }
}

TEST(DataSliceImpl, AllocateEmptyObjects) {
  {
    DataSliceImpl ds = DataSliceImpl::AllocateEmptyObjects(0);
    EXPECT_EQ(ds.size(), 0);
    EXPECT_EQ(ds.dtype(), arolla::GetNothingQType());
    EXPECT_EQ(ds.allocation_ids().size(), 0);
    EXPECT_TRUE(ds.is_empty_and_unknown());
  }
  {
    constexpr int64_t kSize = 57;
    DataSliceImpl ds = DataSliceImpl::AllocateEmptyObjects(kSize);
    EXPECT_EQ(ds.size(), kSize);
    EXPECT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
    EXPECT_EQ(ds.allocation_ids().size(), 1);
    AllocationId alloc_id = ds.allocation_ids().ids()[0];
    EXPECT_EQ(ds.values<ObjectId>().size(), kSize);
    ds.values<ObjectId>().ForEach([&](int64_t id, bool present, ObjectId obj) {
      EXPECT_TRUE(present) << id;
      EXPECT_EQ(AllocationId(obj), alloc_id) << id;
      EXPECT_EQ(obj, alloc_id.ObjectByOffset(id)) << id;
    });
  }
}

TEST(DataSliceImpl, CreateObjectsDataSlice) {
  constexpr int64_t kSize = 3;
  AllocationId alloc_id0 = Allocate(kSize);
  AllocationId alloc_id1 = Allocate(kSize);
  DataSliceImpl ds = DataSliceImpl::CreateObjectsDataSlice(
      arolla::CreateFullDenseArray<ObjectId>({alloc_id0.ObjectByOffset(0),
                                              alloc_id1.ObjectByOffset(1),
                                              alloc_id0.ObjectByOffset(2)}),
      AllocationIdSet({alloc_id0, alloc_id1}));
  EXPECT_EQ(ds.size(), kSize);
  EXPECT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(ds.allocation_ids(), ElementsAre(alloc_id0, alloc_id1));
  EXPECT_EQ(ds.values<ObjectId>().size(), kSize);
  EXPECT_THAT(ds.values<ObjectId>(), ElementsAre(alloc_id0.ObjectByOffset(0),
                                                 alloc_id1.ObjectByOffset(1),
                                                 alloc_id0.ObjectByOffset(2)));
}

TEST(DataSliceImpl, Create) {
  constexpr int64_t kSize = 3;
  auto array = arolla::CreateFullDenseArray<int64_t>({57, 75, 19});
  DataSliceImpl ds = DataSliceImpl::Create(array);
  EXPECT_EQ(ds.size(), kSize);
  EXPECT_EQ(ds.dtype(), arolla::GetQType<int64_t>());
  EXPECT_FALSE(ds.is_empty_and_unknown());
  EXPECT_THAT(ds.allocation_ids(), IsEmpty());
  EXPECT_THAT(ds.values<int64_t>(), ElementsAre(57, 75, 19));
}

TEST(DataSliceImpl, CreateWithTypesBuffer) {
  constexpr int64_t kSize = 3;
  auto array =
      arolla::CreateDenseArray<int64_t>({std::nullopt, 75, std::nullopt});
  TypesBuffer types_buffer;
  types_buffer.InitAllUnset(3);
  types_buffer.id_to_typeidx[1] =
      types_buffer.get_or_add_typeidx(ScalarTypeId<int64_t>());
  types_buffer.id_to_typeidx[2] = TypesBuffer::kRemoved;
  DataSliceImpl ds = DataSliceImpl::CreateWithTypesBuffer(
      std::move(types_buffer), AllocationIdSet(), array);
  EXPECT_EQ(ds.size(), kSize);
  EXPECT_EQ(ds.dtype(), arolla::GetQType<int64_t>());
  EXPECT_FALSE(ds.is_empty_and_unknown());
  EXPECT_THAT(ds.allocation_ids(), IsEmpty());
  EXPECT_THAT(ds, ElementsAre(DataItem(), 75, DataItem()));
  EXPECT_EQ(ds.types_buffer().id_to_typeidx[0], TypesBuffer::kUnset);
  EXPECT_EQ(ds.types_buffer().id_to_typeidx[1], 0);
  EXPECT_EQ(ds.types_buffer().id_to_typeidx[2], TypesBuffer::kRemoved);
}

TEST(DataSliceImpl, CreateFromDataItemSpan) {
  {
    auto array = std::vector<DataItem>{};
    DataSliceImpl ds = DataSliceImpl::Create(array);
    EXPECT_EQ(ds.size(), 0);
    EXPECT_TRUE(ds.is_empty_and_unknown());
  }
  {
    auto array = std::vector<DataItem>{DataItem(), DataItem()};
    DataSliceImpl ds = DataSliceImpl::Create(array);
    EXPECT_EQ(ds.size(), 2);
    EXPECT_TRUE(ds.is_empty_and_unknown());
  }
  {
    ObjectId x = AllocateSingleObject();
    ObjectId y = Allocate(1024).ObjectByOffset(6);
    auto array = std::vector<DataItem>{DataItem(x), DataItem(y)};
    DataSliceImpl ds = DataSliceImpl::Create(array);
    EXPECT_EQ(ds.size(), 2);
    EXPECT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
    EXPECT_FALSE(ds.is_empty_and_unknown());
    EXPECT_THAT(ds.allocation_ids(), ElementsAre(AllocationId(y)));
    EXPECT_TRUE(ds.allocation_ids().contains_small_allocation_id());
    EXPECT_THAT(ds, ElementsAre(DataItem{x}, DataItem{y}));
  }
  {
    auto array =
        std::vector<DataItem>{DataItem{int64_t{57}}, DataItem{int64_t{75}}};
    DataSliceImpl ds = DataSliceImpl::Create(array);
    EXPECT_EQ(ds.size(), 2);
    EXPECT_EQ(ds.dtype(), arolla::GetQType<int64_t>());
    EXPECT_FALSE(ds.is_empty_and_unknown());
    EXPECT_THAT(ds.allocation_ids(), IsEmpty());
    EXPECT_THAT(ds, ElementsAre(DataItem{57}, DataItem{75}));
  }
  {
    auto array = std::vector<DataItem>{DataItem{57}, DataItem{75.0}};
    DataSliceImpl ds = DataSliceImpl::Create(array);
    EXPECT_EQ(ds.size(), 2);
    EXPECT_EQ(ds.dtype(), arolla::GetNothingQType());
    EXPECT_FALSE(ds.is_empty_and_unknown());
    EXPECT_THAT(ds.allocation_ids(), IsEmpty());
    EXPECT_THAT(ds, ElementsAre(DataItem{57}, DataItem{75.0}));
  }
}

TEST(DataSliceImpl, CreateFromDataItemDenseArray) {
  {
    auto array = arolla::CreateEmptyDenseArray<DataItem>(0);
    DataSliceImpl ds = DataSliceImpl::Create(array);
    EXPECT_EQ(ds.size(), 0);
    EXPECT_TRUE(ds.is_empty_and_unknown());
  }
  {
    auto array = arolla::CreateEmptyDenseArray<DataItem>(6);
    DataSliceImpl ds = DataSliceImpl::Create(array);
    EXPECT_EQ(ds.size(), 6);
    EXPECT_TRUE(ds.is_empty_and_unknown());
  }
  {
    ObjectId x = AllocateSingleObject();
    ObjectId y = Allocate(1024).ObjectByOffset(6);
    auto array =
        CreateDenseArray<DataItem>({DataItem(x), std::nullopt, DataItem(y)});
    DataSliceImpl ds = DataSliceImpl::Create(array);
    EXPECT_EQ(ds.size(), 3);
    EXPECT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
    EXPECT_FALSE(ds.is_empty_and_unknown());
    EXPECT_THAT(ds.allocation_ids(), ElementsAre(AllocationId(y)));
    EXPECT_TRUE(ds.allocation_ids().contains_small_allocation_id());
    EXPECT_THAT(ds, ElementsAre(DataItem{x}, std::nullopt, DataItem{y}));
  }
  {
    auto array = CreateDenseArray<DataItem>(
        {DataItem{int64_t{57}}, std::nullopt, DataItem{int64_t{75}}});
    DataSliceImpl ds = DataSliceImpl::Create(array);
    EXPECT_EQ(ds.size(), 3);
    EXPECT_EQ(ds.dtype(), arolla::GetQType<int64_t>());
    EXPECT_TRUE(ds.is_single_dtype());
    EXPECT_THAT(ds.allocation_ids(), IsEmpty());
    EXPECT_THAT(ds, ElementsAre(DataItem{57}, std::nullopt, DataItem{75}));
  }
  {
    auto array = CreateDenseArray<DataItem>(
        {DataItem{57}, std::nullopt, DataItem{75.0}});
    DataSliceImpl ds = DataSliceImpl::Create(array);
    EXPECT_EQ(ds.size(), 3);
    EXPECT_EQ(ds.dtype(), arolla::GetNothingQType());
    EXPECT_TRUE(ds.is_mixed_dtype());
    EXPECT_THAT(ds.allocation_ids(), IsEmpty());
    EXPECT_THAT(ds, ElementsAre(DataItem{57}, std::nullopt, DataItem{75.0}));
  }
}

TEST(DataSliceImpl, CreateFromDataItemAndSize) {
  {
    auto item = DataItem(42);
    auto ds = DataSliceImpl::Create(5, item);
    EXPECT_EQ(ds.size(), 5);
    EXPECT_EQ(ds.dtype(), arolla::GetQType<int>());
    EXPECT_FALSE(ds.is_empty_and_unknown());
    EXPECT_FALSE(ds.is_mixed_dtype());
    EXPECT_THAT(ds.allocation_ids(), IsEmpty());
    EXPECT_THAT(ds, ElementsAre(DataItem{42}, DataItem{42}, DataItem{42},
                                DataItem{42}, DataItem{42}));
  }
}

TEST(DataSliceImpl, ElementsAre) {
  auto array = arolla::CreateFullDenseArray<int64_t>({57, 75, 19});
  DataSliceImpl ds = DataSliceImpl::Create(array);
  EXPECT_THAT(ds, ElementsAre(int64_t{57}, int64_t{75}, int64_t{19}));
  EXPECT_THAT(ds, ElementsAre(int64_t{57}, int{75}, int64_t{19}));
  EXPECT_THAT(ds,
              ::testing::Not(ElementsAre(int64_t{57}, float{75}, int64_t{19})));
}

TEST(DataSliceImpl, AsDataItemDenseArray) {
  {
    // Empty
    DataSliceImpl ds = DataSliceImpl::CreateEmptyAndUnknownType(6);
    auto expected = arolla::CreateEmptyDenseArray<DataItem>(6);
    EXPECT_THAT(ds.AsDataItemDenseArray(), ElementsAreArray(expected));
    EXPECT_EQ(ds.present(3), false);
    EXPECT_EQ(ds.present(4), false);
  }
  {
    // Single dtype
    auto array =
        CreateDenseArray<int>({1, 1, std::nullopt, std::nullopt, 12, 12});
    DataSliceImpl ds = DataSliceImpl::Create(array);
    EXPECT_THAT(ds.AsDataItemDenseArray(),
                ElementsAre(DataItem{1}, DataItem{1}, std::nullopt,
                            std::nullopt, DataItem{12}, DataItem{12}));
    EXPECT_EQ(ds.present(3), false);
    EXPECT_EQ(ds.present(4), true);
  }
  {
    // Mixed dtypes
    auto int_array = CreateDenseArray<int>(
        {1, 1, std::nullopt, std::nullopt, std::nullopt, 12});
    auto float_array = CreateDenseArray<float>(
        {std::nullopt, std::nullopt, 12.1f, 12.1f, std::nullopt, std::nullopt});
    DataSliceImpl ds = DataSliceImpl::Create(int_array, float_array);
    EXPECT_THAT(ds.AsDataItemDenseArray(),
                ElementsAre(DataItem{1}, DataItem{1}, DataItem{12.1f},
                            DataItem{12.1f}, std::nullopt, DataItem{12}));
    EXPECT_EQ(ds.present(3), true);
    EXPECT_EQ(ds.present(4), false);
  }
  {
    // Mixed dtypes with ObjectIds
    auto obj_id_1 = Allocate(2000).ObjectByOffset(1909);
    auto alloc_id_1 = AllocationId(obj_id_1);
    auto obj_id_2 = AllocateSingleObject();  // small allocation
    auto alloc_id_2 = AllocationId(obj_id_2);
    auto objects = CreateDenseArray<ObjectId>({std::nullopt, std::nullopt,
                                               obj_id_1, obj_id_1, obj_id_2,
                                               std::nullopt, std::nullopt});
    auto values_int = CreateDenseArray<int>(
        {1, 1, std::nullopt, std::nullopt, std::nullopt, std::nullopt, 12});
    DataSliceImpl ds = DataSliceImpl::CreateWithAllocIds(
        AllocationIdSet({alloc_id_1, alloc_id_2}), objects, values_int);
    EXPECT_THAT(ds.AsDataItemDenseArray(),
                ElementsAre(DataItem{1}, DataItem{1}, DataItem{obj_id_1},
                            DataItem{obj_id_1}, DataItem{obj_id_2},
                            std::nullopt, DataItem{12}));
    EXPECT_EQ(ds.present(3), true);
    EXPECT_EQ(ds.present(4), true);
  }
}

TEST(DataSliceImpl, CreateObjects) {
  ObjectId obj1 = AllocateSingleObject();
  AllocationId alloc1 = Allocate(25);
  AllocationId alloc2 = Allocate(25);
  auto array = arolla::CreateFullDenseArray<ObjectId>(
      {obj1, alloc1.ObjectByOffset(2), alloc2.ObjectByOffset(3),
       alloc1.ObjectByOffset(1)});
  DataSliceImpl ds = DataSliceImpl::Create(array);
  EXPECT_EQ(ds.size(), 4);
  EXPECT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_TRUE(ds.allocation_ids().contains_small_allocation_id());
  EXPECT_THAT(ds.allocation_ids().ids(), ElementsAre(alloc1, alloc2));
}

TEST(DataSliceImpl, CreatePolymorfic) {
  constexpr int64_t kSize = 3;
  arolla::meta::foreach_type<supported_primitives_list>([&](auto meta_type) {
    using T = typename decltype(meta_type)::type;
    auto array = arolla::CreateConstDenseArray<T>(kSize, T());
    ASSERT_OK_AND_ASSIGN(
        DataSliceImpl ds,
        DataSliceImpl::Create(arolla::TypedRef::FromValue(array)));
    EXPECT_EQ(ds.size(), kSize);
    EXPECT_EQ(ds.dtype(), arolla::GetQType<T>());
    EXPECT_THAT(ds.allocation_ids(), IsEmpty());
    EXPECT_THAT(ds.values<T>(), ElementsAre(T(), T(), T()));
  });

  auto array = arolla::CreateConstDenseArray<uint64_t>(kSize, 0);
  EXPECT_THAT(
      DataSliceImpl::Create(arolla::TypedRef::FromValue(array)).status(),
      ::absl_testing::StatusIs(absl::StatusCode::kInvalidArgument,
                               ::testing::HasSubstr("element type: UINT64")));

  EXPECT_THAT(
      DataSliceImpl::Create(arolla::TypedRef::FromValue(ObjectId())).status(),
      ::absl_testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          ::testing::HasSubstr("unsupported type: OBJECT_ID")));
}

TEST(DataSliceImpl, IntersectingIdsCheck) {
  auto values_1 = CreateDenseArray<int>({1, std::nullopt, std::nullopt});
  auto values_2 = CreateDenseArray<float>({std::nullopt, 2., std::nullopt});
  auto values_3 = CreateDenseArray<int64_t>({std::nullopt, std::nullopt, 3});
  EXPECT_TRUE(
      data_slice_impl::VerifyNonIntersectingIds(values_1, values_2, values_3));

  values_3 = CreateDenseArray<int64_t>({1, std::nullopt, std::nullopt});
  EXPECT_FALSE(
      data_slice_impl::VerifyNonIntersectingIds(values_1, values_2, values_3));
}


TEST(DataSliceImpl, CreateEmptyAndUnknownType) {
  constexpr size_t kSize = 57;
  DataSliceImpl ds = DataSliceImpl::CreateEmptyAndUnknownType(kSize);
  EXPECT_EQ(ds.size(), kSize);
  EXPECT_EQ(ds.dtype(), arolla::GetNothingQType());
  EXPECT_TRUE(ds.is_empty_and_unknown());
  EXPECT_EQ(ds.present(43), false);
}

TEST(DataSliceImpl, CreateMixed) {
  constexpr int64_t kSize = 3;
  auto array_f = arolla::CreateDenseArray<float>(
      std::vector<OptionalValue<float>>{7.0f, std::nullopt, std::nullopt});
  auto array_int = arolla::CreateDenseArray<int>(
      std::vector<OptionalValue<int>>{std::nullopt, 5, std::nullopt});
  auto array_bytes =
      arolla::CreateDenseArray<Bytes>(std::vector<OptionalValue<Bytes>>{
          std::nullopt, std::nullopt, Bytes("57")});
  DataSliceImpl ds = DataSliceImpl::Create(array_f, array_int, array_bytes);
  EXPECT_EQ(ds.size(), kSize);
  EXPECT_EQ(ds.dtype(), arolla::GetNothingQType());
  EXPECT_THAT(ds.allocation_ids(), IsEmpty());

  ds.VisitValues([&](const auto& array) {
    using T = typename std::decay_t<decltype(array)>::base_type;
    if constexpr (std::is_same_v<T, float>) {
      ASSERT_THAT(array, ElementsAreArray(array_f));
    } else if constexpr (std::is_same_v<T, int>) {
      ASSERT_THAT(array, ElementsAreArray(array_int));
    } else if constexpr (std::is_same_v<T, Bytes>) {
      ASSERT_THAT(array, ElementsAreArray(array_bytes));
    } else {
      ASSERT_THAT(array, IsEmpty());
    }
  });

  EXPECT_EQ(ds[0], DataItem(7.0f));
  EXPECT_EQ(ds[1], DataItem(5));
  EXPECT_EQ(ds[2], DataItem(Bytes("57")));
  EXPECT_THAT(ds, ElementsAre(7.0f, 5, Bytes("57")));
  EXPECT_EQ(ds.present(0), true);
  EXPECT_EQ(ds.present(1), true);
  EXPECT_EQ(ds.present(2), true);
}

TEST(DataSliceImpl, CreateMixedWithEmptyArrays) {
  constexpr int64_t kSize = 3;
  auto array_f = arolla::CreateDenseArray<float>(
      std::vector<OptionalValue<float>>{7.0f, std::nullopt, std::nullopt});
  auto array_int = arolla::CreateEmptyDenseArray<int>(3);
  auto array_bytes = arolla::CreateEmptyDenseArray<Bytes>(3);
  for (DataSliceImpl ds : {
           DataSliceImpl::Create(array_f, array_int, array_bytes),
           DataSliceImpl::Create(array_int, array_f, array_bytes),
           DataSliceImpl::Create(array_int, array_bytes, array_f),
       }) {
    EXPECT_EQ(ds.size(), kSize);
    EXPECT_EQ(ds.dtype(), arolla::GetQType<float>());
    EXPECT_THAT(ds.allocation_ids(), IsEmpty());
    EXPECT_THAT(ds.values<float>(),
                ElementsAre(7.0f, std::nullopt, std::nullopt));
  }
}

TEST(DataSliceImpl, CreateMixedAllEmptyArrays) {
  constexpr int64_t kSize = 3;
  auto array_f = arolla::CreateEmptyDenseArray<float>(3);
  auto array_int = arolla::CreateEmptyDenseArray<int>(3);
  auto array_bytes = arolla::CreateEmptyDenseArray<Bytes>(3);
  DataSliceImpl ds = DataSliceImpl::Create(array_f, array_int, array_bytes);
  EXPECT_EQ(ds.size(), kSize);
  EXPECT_EQ(ds.dtype(), arolla::GetNothingQType());
  EXPECT_TRUE(ds.is_empty_and_unknown());
  EXPECT_THAT(ds.allocation_ids(), IsEmpty());
  EXPECT_THAT(ds, ElementsAre(std::nullopt, std::nullopt, std::nullopt));
  EXPECT_EQ(ds.present(0), false);
  EXPECT_EQ(ds.present(1), false);
  EXPECT_EQ(ds.present(2), false);
}

TEST(DataSliceImpl, PresentCount) {
  EXPECT_EQ(DataSliceImpl().present_count(), 0);
  EXPECT_EQ(
      DataSliceImpl::Create(
          arolla::CreateDenseArray<int>({std::nullopt, std::nullopt}))
      .present_count(), 0);
  EXPECT_EQ(
      DataSliceImpl::Create(
          arolla::CreateDenseArray<int>(
              {std::nullopt, std::nullopt, 12, 42, std::nullopt}))
      .present_count(), 2);
  EXPECT_EQ(
      DataSliceImpl::Create(
          arolla::CreateDenseArray<int>({std::nullopt, std::nullopt}),
          arolla::CreateDenseArray<float>({std::nullopt, std::nullopt}))
      .present_count(), 0);
  EXPECT_EQ(
      DataSliceImpl::Create(
          arolla::CreateDenseArray<int>({std::nullopt, 42}),
          arolla::CreateDenseArray<float>({std::nullopt, std::nullopt}))
      .present_count(), 1);
  EXPECT_EQ(
      DataSliceImpl::Create(
          arolla::CreateDenseArray<int>({std::nullopt, 12}),
          arolla::CreateDenseArray<float>({3.14, std::nullopt}))
      .present_count(), 2);
  EXPECT_EQ(DataSliceImpl::AllocateEmptyObjects(12).present_count(), 12);
}

TEST(DataSliceImpl, ContainsOnlyLists) {
  EXPECT_TRUE(DataSliceImpl().ContainsOnlyLists());
  EXPECT_TRUE(
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({std::nullopt}))
          .ContainsOnlyLists());
  EXPECT_TRUE(DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
                                        std::nullopt,
                                        AllocateSingleList(),
                                        AllocateSingleList(),
                                    }))
                  .ContainsOnlyLists());
  EXPECT_FALSE(DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
                                         std::nullopt,
                                         AllocateSingleList(),
                                         AllocateSingleObject(),
                                     }))
                   .ContainsOnlyLists());
  EXPECT_FALSE(DataSliceImpl::Create(arolla::CreateDenseArray<int>({42}))
                   .ContainsOnlyLists());
  EXPECT_TRUE(
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({std::nullopt}))
          .ContainsOnlyLists());
  EXPECT_TRUE(DataSliceImpl::Create(
                  arolla::CreateDenseArray<int>({std::nullopt}),
                  arolla::CreateDenseArray<ObjectId>({AllocateSingleList()}))
                  .ContainsOnlyLists());
}

TEST(DataSliceImpl, ContainsOnlyListsBigAlloc) {
  AllocationId lists_alloc = AllocateLists(kSmallAllocMaxCapacity + 10);
  EXPECT_TRUE(DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
                                        std::nullopt,
                                        lists_alloc.ObjectByOffset(0),
                                        lists_alloc.ObjectByOffset(1),
                                        lists_alloc.ObjectByOffset(2),
                                        lists_alloc.ObjectByOffset(7),
                                    }))
                  .ContainsOnlyLists());

  AllocationId other_alloc = Allocate(kSmallAllocMaxCapacity + 10);
  EXPECT_FALSE(DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
                                         std::nullopt,
                                         lists_alloc.ObjectByOffset(0),
                                         lists_alloc.ObjectByOffset(1),
                                         lists_alloc.ObjectByOffset(2),
                                         other_alloc.ObjectByOffset(7),
                                         other_alloc.ObjectByOffset(1),
                                         other_alloc.ObjectByOffset(0),
                                         other_alloc.ObjectByOffset(9),
                                     }))
               .ContainsOnlyLists());
}

TEST(DataSliceImpl, ContainsOnlyDicts) {
  EXPECT_TRUE(DataSliceImpl().ContainsOnlyDicts());
  EXPECT_TRUE(
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({std::nullopt}))
          .ContainsOnlyDicts());
  EXPECT_TRUE(DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
                                        std::nullopt,
                                        AllocateSingleDict(),
                                        AllocateSingleDict(),
                                    }))
                  .ContainsOnlyDicts());
  EXPECT_FALSE(DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
                                         std::nullopt,
                                         AllocateSingleDict(),
                                         AllocateSingleObject(),
                                     }))
                   .ContainsOnlyDicts());
  EXPECT_FALSE(DataSliceImpl::Create(arolla::CreateDenseArray<int>({42}))
                   .ContainsOnlyDicts());
  EXPECT_TRUE(
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({std::nullopt}))
          .ContainsOnlyDicts());
  EXPECT_TRUE(DataSliceImpl::Create(
                  arolla::CreateDenseArray<int>({std::nullopt}),
                  arolla::CreateDenseArray<ObjectId>({AllocateSingleDict()}))
                  .ContainsOnlyDicts());
}

TEST(DataSliceImpl, ContainsOnlyDictsBigAlloc) {
  AllocationId dicts_alloc = AllocateDicts(kSmallAllocMaxCapacity + 10);
  EXPECT_TRUE(DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
                                        std::nullopt,
                                        dicts_alloc.ObjectByOffset(0),
                                        dicts_alloc.ObjectByOffset(1),
                                        dicts_alloc.ObjectByOffset(2),
                                        dicts_alloc.ObjectByOffset(7),
                                    }))
                  .ContainsOnlyDicts());

  AllocationId other_alloc = Allocate(kSmallAllocMaxCapacity + 10);
  EXPECT_FALSE(DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
                                         std::nullopt,
                                         dicts_alloc.ObjectByOffset(0),
                                         dicts_alloc.ObjectByOffset(1),
                                         dicts_alloc.ObjectByOffset(2),
                                         other_alloc.ObjectByOffset(7),
                                         other_alloc.ObjectByOffset(1),
                                         other_alloc.ObjectByOffset(0),
                                         other_alloc.ObjectByOffset(9),
                                     }))
               .ContainsOnlyDicts());
}

TEST(DataSliceImpl, ContainsOnlyEntities) {
  EXPECT_TRUE(DataSliceImpl().ContainsOnlyEntities());
  EXPECT_TRUE(
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({std::nullopt}))
          .ContainsOnlyEntities());
  EXPECT_TRUE(DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
                                        std::nullopt,
                                        AllocateSingleObject(),
                                        AllocateSingleObject(),
                                    }))
                  .ContainsOnlyEntities());
  EXPECT_FALSE(DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
                                         std::nullopt,
                                         AllocateSingleDict(),
                                         AllocateSingleObject(),
                                     }))
                   .ContainsOnlyEntities());
  EXPECT_FALSE(DataSliceImpl::Create(arolla::CreateDenseArray<int>({42}))
                   .ContainsOnlyEntities());
  EXPECT_TRUE(
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({std::nullopt}))
          .ContainsOnlyEntities());
  EXPECT_TRUE(DataSliceImpl::Create(
                  arolla::CreateDenseArray<int>({std::nullopt}),
                  arolla::CreateDenseArray<ObjectId>({AllocateSingleObject()}))
                  .ContainsOnlyEntities());
}

TEST(DataSliceImpl, ContainsOnlyEntitiesBigAlloc) {
  AllocationId entities_alloc = Allocate(kSmallAllocMaxCapacity + 10);
  EXPECT_TRUE(DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
                                        std::nullopt,
                                        entities_alloc.ObjectByOffset(0),
                                        entities_alloc.ObjectByOffset(1),
                                        entities_alloc.ObjectByOffset(2),
                                        entities_alloc.ObjectByOffset(7),
                                    }))
                  .ContainsOnlyEntities());

  AllocationId dicts_alloc = AllocateDicts(kSmallAllocMaxCapacity + 10);
  EXPECT_FALSE(DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
                                         std::nullopt,
                                         entities_alloc.ObjectByOffset(0),
                                         entities_alloc.ObjectByOffset(1),
                                         entities_alloc.ObjectByOffset(2),
                                         dicts_alloc.ObjectByOffset(7),
                                         dicts_alloc.ObjectByOffset(1),
                                         dicts_alloc.ObjectByOffset(0),
                                         dicts_alloc.ObjectByOffset(9),
                                     }))
                   .ContainsOnlyEntities());
}

TEST(DataSliceImpl, IsEquivalentTo) {
  auto empty = DataSliceImpl();
  auto empty_and_unknown = DataSliceImpl::CreateEmptyAndUnknownType(2);
  auto empty_and_unknown_size_3 = DataSliceImpl::CreateEmptyAndUnknownType(3);
  auto int0 = DataSliceImpl::Create(arolla::CreateDenseArray<int>({0, 0}));
  auto int64_t0 =
      DataSliceImpl::Create(arolla::CreateDenseArray<int64_t>({0, 0}));
  auto float0 = DataSliceImpl::Create(arolla::CreateDenseArray<float>({0, 0}));
  auto int_1_null =
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({1, std::nullopt}));
  auto int0_and_empty_float = DataSliceImpl::Create(
      arolla::CreateDenseArray<int>({0, 0}),
      arolla::CreateDenseArray<float>({std::nullopt, std::nullopt}));
  auto empty_int_and_float0 = DataSliceImpl::Create(
      arolla::CreateDenseArray<int>({std::nullopt, std::nullopt}),
      arolla::CreateDenseArray<float>({0, 0}));
  auto mix1_int_float =
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({0, std::nullopt}),
                            arolla::CreateDenseArray<float>({std::nullopt, 0}));
  auto mix2_int_float =
      DataSliceImpl::Create(arolla::CreateDenseArray<float>({std::nullopt, 0}),
                            arolla::CreateDenseArray<int>({0, std::nullopt}));
  auto mix_float_int =
      DataSliceImpl::Create(arolla::CreateDenseArray<float>({0, std::nullopt}),
                            arolla::CreateDenseArray<int>({std::nullopt, 0}));

  EXPECT_TRUE(empty.IsEquivalentTo(empty));
  EXPECT_FALSE(empty.IsEquivalentTo(int0));
  EXPECT_FALSE(int0.IsEquivalentTo(float0));
  EXPECT_FALSE(int0.IsEquivalentTo(int64_t0));
  EXPECT_TRUE(float0.IsEquivalentTo(float0));
  EXPECT_FALSE(int0.IsEquivalentTo(int_1_null));
  EXPECT_TRUE(int0_and_empty_float.IsEquivalentTo(int0));
  EXPECT_TRUE(int0.IsEquivalentTo(int0_and_empty_float));
  EXPECT_FALSE(int0.IsEquivalentTo(empty_int_and_float0));
  EXPECT_TRUE(float0.IsEquivalentTo(empty_int_and_float0));
  EXPECT_FALSE(int0_and_empty_float.IsEquivalentTo(mix1_int_float));
  EXPECT_TRUE(mix2_int_float.IsEquivalentTo(mix1_int_float));
  EXPECT_FALSE(mix2_int_float.IsEquivalentTo(mix_float_int));
  EXPECT_TRUE(mix_float_int.IsEquivalentTo(mix_float_int));
  EXPECT_TRUE(empty_and_unknown.IsEquivalentTo(empty_and_unknown));
  EXPECT_FALSE(empty_and_unknown.IsEquivalentTo(empty_and_unknown_size_3));
  EXPECT_FALSE(empty_and_unknown.IsEquivalentTo(mix_float_int));
}

TEST(DataSliceImpl, Constructors) {
  {
    // Empty DataSliceImpl.
    DataSliceImpl ds;
    DataSliceImpl ds_copy(ds);
    EXPECT_EQ(ds_copy.size(), ds.size());
    EXPECT_EQ(ds_copy.dtype(), ds.dtype());
    EXPECT_THAT(ds_copy.allocation_ids().ids(),
                ElementsAreArray(ds.allocation_ids().ids()));

    DataSliceImpl ds_move(std::move(ds));
    EXPECT_EQ(ds_move.size(), ds_copy.size());
    EXPECT_EQ(ds_move.dtype(), ds_copy.dtype());
    EXPECT_THAT(ds_move.allocation_ids().ids(),
                ElementsAreArray(ds_copy.allocation_ids().ids()));
  }
  {
    // Objects DataSlice
    auto ds = DataSliceImpl::AllocateEmptyObjects(3);
    DataSliceImpl ds_copy(ds);
    EXPECT_EQ(ds_copy.size(), ds.size());
    EXPECT_EQ(ds_copy.dtype(), ds.dtype());
    EXPECT_THAT(ds_copy.values<ObjectId>(),
                ElementsAreArray(ds.values<ObjectId>()));
    EXPECT_THAT(ds_copy.allocation_ids().ids(),
                ElementsAreArray(ds.allocation_ids().ids()));

    DataSliceImpl ds_move(std::move(ds));
    EXPECT_EQ(ds_move.size(), ds_copy.size());
    EXPECT_EQ(ds_move.dtype(), ds_copy.dtype());
    EXPECT_THAT(ds_move.values<ObjectId>(),
                ElementsAreArray(ds_copy.values<ObjectId>()));
    EXPECT_THAT(ds_move.allocation_ids().ids(),
                ElementsAreArray(ds_copy.allocation_ids().ids()));
  }
  {
    // Primitives DataSlice
    auto ds =
        DataSliceImpl::Create(CreateDenseArray<int>({1, std::nullopt, 3}));
    DataSliceImpl ds_copy(ds);
    EXPECT_EQ(ds_copy.size(), ds.size());
    EXPECT_EQ(ds_copy.dtype(), ds.dtype());
    EXPECT_THAT(ds_copy.values<int>(), ElementsAreArray(ds.values<int>()));
    EXPECT_THAT(ds_copy.allocation_ids().ids(),
                ElementsAreArray(ds.allocation_ids().ids()));

    DataSliceImpl ds_move(std::move(ds));
    EXPECT_EQ(ds_move.size(), ds_copy.size());
    EXPECT_EQ(ds_move.dtype(), ds_copy.dtype());
    EXPECT_THAT(ds_move.values<int>(), ElementsAreArray(ds_copy.values<int>()));
    EXPECT_THAT(ds_move.allocation_ids().ids(),
                ElementsAreArray(ds_copy.allocation_ids().ids()));
  }
}

TEST(DataSliceImpl, Assignments) {
  {
    // Empty DataSliceImpl.
    DataSliceImpl ds;
    DataSliceImpl ds_copy;
    ds_copy = ds;
    EXPECT_EQ(ds_copy.size(), ds.size());
    EXPECT_EQ(ds_copy.dtype(), ds.dtype());
    EXPECT_THAT(ds_copy.allocation_ids().ids(),
                ElementsAreArray(ds.allocation_ids().ids()));

    DataSliceImpl ds_move;
    ds_move = std::move(ds);
    EXPECT_EQ(ds_move.size(), ds_copy.size());
    EXPECT_EQ(ds_move.dtype(), ds_copy.dtype());
    EXPECT_THAT(ds_move.allocation_ids().ids(),
                ElementsAreArray(ds_copy.allocation_ids().ids()));
  }
  {
    // Objects DataSlice
    auto ds = DataSliceImpl::AllocateEmptyObjects(3);
    DataSliceImpl ds_copy;
    ds_copy = ds;
    EXPECT_EQ(ds_copy.size(), ds.size());
    EXPECT_EQ(ds_copy.dtype(), ds.dtype());
    EXPECT_THAT(ds_copy.values<ObjectId>(),
                ElementsAreArray(ds.values<ObjectId>()));
    EXPECT_THAT(ds_copy.allocation_ids().ids(),
                ElementsAreArray(ds.allocation_ids().ids()));

    DataSliceImpl ds_move;
    ds_move = std::move(ds);
    EXPECT_EQ(ds_move.size(), ds_copy.size());
    EXPECT_EQ(ds_move.dtype(), ds_copy.dtype());
    EXPECT_THAT(ds_move.values<ObjectId>(),
                ElementsAreArray(ds_copy.values<ObjectId>()));
    EXPECT_THAT(ds_move.allocation_ids().ids(),
                ElementsAreArray(ds_copy.allocation_ids().ids()));
  }
  {
    // Primitives DataSlice
    auto ds =
        DataSliceImpl::Create(CreateDenseArray<int>({1, std::nullopt, 3}));
    DataSliceImpl ds_copy;
    ds_copy = ds;
    EXPECT_EQ(ds_copy.size(), ds.size());
    EXPECT_EQ(ds_copy.dtype(), ds.dtype());
    EXPECT_THAT(ds_copy.values<int>(), ElementsAreArray(ds.values<int>()));
    EXPECT_THAT(ds_copy.allocation_ids().ids(),
                ElementsAreArray(ds.allocation_ids().ids()));

    DataSliceImpl ds_move;
    ds_move = std::move(ds);
    EXPECT_EQ(ds_move.size(), ds_copy.size());
    EXPECT_EQ(ds_move.dtype(), ds_copy.dtype());
    EXPECT_THAT(ds_move.values<int>(), ElementsAreArray(ds_copy.values<int>()));
    EXPECT_THAT(ds_move.allocation_ids().ids(),
                ElementsAreArray(ds_copy.allocation_ids().ids()));
  }
}

TEST(DataSliceImpl, FingerprintTest) {
  constexpr int64_t kSize = 3;
  auto ds_1 = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto ds_2 = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto ds_3 = DataSliceImpl::Create(CreateDenseArray<int>({1, 2, 3}));
  auto ds_4 = DataSliceImpl::Create(CreateDenseArray<int>({1, 2, 3}));
  auto ds_5 = DataSliceImpl::Create(CreateDenseArray<int64_t>({1, 2, 3}));

  EXPECT_EQ(arolla::FingerprintHasher("salt").Combine(ds_1).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_1).Finish());
  EXPECT_NE(arolla::FingerprintHasher("salt").Combine(ds_1).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_2).Finish());
  EXPECT_NE(arolla::FingerprintHasher("salt").Combine(ds_1).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_3).Finish());
  EXPECT_NE(arolla::FingerprintHasher("salt").Combine(ds_2).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_3).Finish());
  EXPECT_EQ(arolla::FingerprintHasher("salt").Combine(ds_3).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_4).Finish());
  EXPECT_NE(arolla::FingerprintHasher("salt").Combine(ds_3).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_5).Finish());

  // Allocates objects with same allocation ids.
  AllocationId alloc_id0 = Allocate(kSize);
  AllocationId alloc_id1 = Allocate(kSize);
  DataSliceImpl ds_6 = DataSliceImpl::CreateObjectsDataSlice(
      arolla::CreateFullDenseArray<ObjectId>({alloc_id0.ObjectByOffset(0),
                                              alloc_id1.ObjectByOffset(1),
                                              alloc_id0.ObjectByOffset(2)}),
      AllocationIdSet({alloc_id0, alloc_id1}));
  DataSliceImpl ds_7 = DataSliceImpl::CreateObjectsDataSlice(
      arolla::CreateFullDenseArray<ObjectId>({alloc_id0.ObjectByOffset(0),
                                              alloc_id1.ObjectByOffset(1),
                                              alloc_id0.ObjectByOffset(2)}),
      AllocationIdSet({alloc_id0, alloc_id1}));
  EXPECT_EQ(arolla::FingerprintHasher("salt").Combine(ds_6).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_7).Finish());
  EXPECT_NE(arolla::FingerprintHasher("salt").Combine(ds_1).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_6).Finish());

  // b/325602964 regression test - slices created from same arrays in different
  // order should have the same fingerprints.
  auto values_1 = CreateDenseArray<int>({1, std::nullopt, std::nullopt, 4});
  auto values_2 =
      CreateDenseArray<float>({std::nullopt, 3.14, std::nullopt, std::nullopt});
  auto values_3 = CreateDenseArray<schema::DType>(
      {std::nullopt, std::nullopt, schema::kInt32, std::nullopt});
  auto ds_8 = DataSliceImpl::Create(values_1, values_2, values_3);
  auto ds_9 = DataSliceImpl::Create(values_2, values_3, values_1);
  EXPECT_EQ(arolla::FingerprintHasher("salt").Combine(ds_8).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_9).Finish());

  auto ds_10 = DataSliceImpl::Create(
      CreateDenseArray<int>({1, std::nullopt, std::nullopt}),
      CreateDenseArray<float>({std::nullopt, std::nullopt, std::nullopt}),
      CreateDenseArray<arolla::Bytes>(
          {std::nullopt, std::nullopt, std::nullopt}),
      CreateDenseArray<double>({std::nullopt, std::nullopt, std::nullopt}),
      CreateDenseArray<bool>({std::nullopt, std::nullopt, std::nullopt}));
  auto ds_11 = DataSliceImpl::Create(
      CreateDenseArray<int>({1, std::nullopt, std::nullopt}));
  EXPECT_EQ(arolla::FingerprintHasher("salt").Combine(ds_10).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_11).Finish());

  // b/309749267 regression test - take qtype into consideration.
  auto ds_12 = DataSliceImpl::Create(CreateDenseArray<int>({0}));
  auto ds_13 = DataSliceImpl::Create(CreateDenseArray<float>({0.0}));
  EXPECT_NE(arolla::FingerprintHasher("salt").Combine(ds_12).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_13).Finish());

  // b/325575589 regression test - empty slices with the same size have the same
  // fingerprint (regardless of the types - handled at a higher-level).
  auto ds_14 = DataSliceImpl::Create(
      CreateDenseArray<int>({std::nullopt, std::nullopt}));
  auto ds_15 = SliceBuilder(2).Build();
  auto ds_16 = DataSliceImpl::Create(
      CreateDenseArray<float>({std::nullopt, std::nullopt}),
      CreateDenseArray<int>({std::nullopt, std::nullopt}));
  EXPECT_EQ(arolla::FingerprintHasher("salt").Combine(ds_14).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_15).Finish());
  EXPECT_EQ(arolla::FingerprintHasher("salt").Combine(ds_14).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_16).Finish());
}

TEST(DataSliceImpl, AbslStringify) {
  EXPECT_THAT(absl::StrCat(DataSliceImpl::CreateEmptyAndUnknownType(0)),
              Eq("[]"));
  EXPECT_THAT(absl::StrCat(DataSliceImpl::CreateEmptyAndUnknownType(1)),
              Eq("[None]"));
  EXPECT_THAT(absl::StrCat(DataSliceImpl::Create(
                  {DataItem(AllocateSingleObject()), DataItem(5), DataItem()})),
              MatchesRegex(R"(\[.*, 5, None])"));
  EXPECT_THAT(
      absl::StrCat(DataSliceImpl::AllocateEmptyObjects(10000)),
      AllOf(
          // We don't want to check the exact string length, but it should not
          // exceed the limit (1000) by a lot.
          SizeIs(Ge((1000))), SizeIs(Lt((1100))),
          MatchesRegex(R"re(\[.*, \.\.\. \(10000 elements total\)\])re")));
}

}  // namespace
}  // namespace koladata::internal
