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
#include "koladata/internal/data_bag.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <numeric>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/internal/uuid_object.h"
#include "koladata/testing/status_matchers_backport.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"

namespace koladata::internal {
namespace {

using ::arolla::DenseArrayEdge;
using ::arolla::OptionalValue;
using ::koladata::internal::testing::IsEquivalentTo;
using ::koladata::testing::IsOkAndHolds;
using ::koladata::testing::StatusIs;
using ::testing::_;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::MatchesRegex;
using ::testing::Ne;
using ::testing::Not;
using ::testing::Pair;
using ::testing::Property;
using ::testing::UnorderedElementsAre;

const internal::DataItem& GetAnySchema() {
  static const absl::NoDestructor<internal::DataItem> kAnySchema(schema::kAny);
  return *kAnySchema;
}

const internal::DataItem& GetIntSchema() {
  static const absl::NoDestructor<internal::DataItem> kIntSchema(
      schema::kInt32);
  return *kIntSchema;
}

const internal::DataItem& GetFloatSchema() {
  static const absl::NoDestructor<internal::DataItem> kFloatSchema(
      schema::kFloat32);
  return *kFloatSchema;
}

AllocationId GenerateImplicitSchemas(size_t size) {
  return AllocationId(
      CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          Allocate(size).ObjectByOffset(0),
          arolla::FingerprintHasher(schema::kImplicitSchemaSeed).Finish()));
}

TEST(DataBagTest, Empty) {
  auto empty = DataBagImpl::CreateEmptyDatabag();

  DataBagImpl::ConstDenseSourceArray dense_sources;
  DataBagImpl::ConstSparseSourceArray sparse_sources;
  empty->GetAttributeDataSources(Allocate(1), "a", dense_sources,
                                 sparse_sources);

  EXPECT_THAT(dense_sources, IsEmpty());
  EXPECT_THAT(sparse_sources, IsEmpty());
}

TEST(DataBagTest, PariallyPersistentForkNoModifications) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds = DataSliceImpl::AllocateEmptyObjects(3);
  auto ds_a = DataSliceImpl::AllocateEmptyObjects(3);

  ASSERT_OK(db->SetAttr(ds, "a", ds_a));

  // PartiallyPersistentFork shouldn't create linear structure
  // if no modifications happen.
  // Would either timeout or go out of stack without such an optimization.
  for (int i = 0; i < (1 << 20); ++i) {
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get, db->GetAttr(ds, "a"));
    EXPECT_EQ(ds_a_get.size(), 3);
    db = db->PartiallyPersistentFork();
  }
}

TEST(DataBagTest, DeepChainOfForks) {
  int64_t alloc_size = 10;
  int64_t set_size = 100;
  int64_t step = alloc_size / set_size;

  AllocationId alloc = Allocate(alloc_size);
  arolla::DenseArrayBuilder<ObjectId> objects_bldr(set_size);
  for (int64_t i = 0; i < set_size; ++i) {
    objects_bldr.Set(i, alloc.ObjectByOffset(i * step));
  }
  DataSliceImpl objects = DataSliceImpl::CreateWithAllocIds(
      AllocationIdSet(alloc), std::move(objects_bldr).Build());
  DataSliceImpl values = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<int32_t>(set_size, 57));

  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetAttr(objects, "a", values));

  for (int i = 0; i < 1000'000; ++i) {
    auto old_db = db;
    db = db->PartiallyPersistentFork();
    ASSERT_OK(db->SetAttr(objects, "a", values));
    ASSERT_THAT(db, Ne(old_db));
  }
}

TEST(DataBagTest, SetGet) {
  constexpr int64_t kSize = 13;
  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetAttr(DataSliceImpl::CreateEmptyAndUnknownType(3), "a",
                        DataSliceImpl::CreateEmptyAndUnknownType(3)));

  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto ds_a = DataSliceImpl::AllocateEmptyObjects(kSize);

  ASSERT_OK(db->SetAttr(ds, "a", ds_a));

  DataBagImpl::ConstDenseSourceArray dense_sources;
  DataBagImpl::ConstSparseSourceArray sparse_sources;
  db->GetAttributeDataSources(ds.allocation_ids().ids()[0], "a", dense_sources,
                              sparse_sources);
  EXPECT_EQ(dense_sources.size(), 1);
  EXPECT_EQ(sparse_sources.size(), 0);

  ASSERT_OK_AND_ASSIGN(DataItem ds_a_5_item,
                       db->GetAttr(DataItem(ds.values<ObjectId>()[5]), "a"));
  EXPECT_EQ(ds_a_5_item.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_EQ(ds_a_5_item.value<ObjectId>(), ds_a.values<ObjectId>()[5].value);

  ASSERT_OK_AND_ASSIGN(DataItem ds_a_null_item, db->GetAttr(DataItem(), "a"));
  EXPECT_FALSE(ds_a_null_item.has_value());

  EXPECT_THAT(db->GetAttr(DataSliceImpl::CreateEmptyAndUnknownType(3), "a"),
              IsOkAndHolds(ElementsAre(DataItem(), DataItem(), DataItem())));
  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get, db->GetAttr(ds, "a"));

  EXPECT_EQ(ds_a_get.size(), kSize);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(ds_a_get.allocation_ids(),
              ElementsAreArray(ds_a.allocation_ids()));
  EXPECT_EQ(ds_a_get.values<ObjectId>().size(), kSize);
  EXPECT_THAT(ds_a_get.values<ObjectId>(),
              ElementsAreArray(ds_a.values<ObjectId>()));
}

TEST(DataBagTest, SetGetWithFallbackObjectId) {
  for (size_t size : {1, 2, 4, 17, 126}) {
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db_f = DataBagImpl::CreateEmptyDatabag();

    auto ds = DataSliceImpl::AllocateEmptyObjects(size);

    auto ds_a = DataSliceImpl::AllocateEmptyObjects(size);
    ASSERT_OK(db->SetAttr(ds, "a", ds_a));

    {  // fallback is empty
      ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get,
                           db->GetAttr(ds, "a", {db_f.get()}));
      EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<ObjectId>());
      EXPECT_THAT(ds_a_get.allocation_ids(),
                  ElementsAreArray(ds_a.allocation_ids()));
      EXPECT_THAT(ds_a_get.values<ObjectId>(),
                  ElementsAreArray(ds_a.values<ObjectId>()));
      EXPECT_THAT(ds_a_get, IsEquivalentTo(ds_a));
    }

    {  // main is empty
      ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get,
                           db_f->GetAttr(ds, "a", {db.get()}));
      EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<ObjectId>());
      EXPECT_THAT(ds_a_get.allocation_ids(),
                  ElementsAreArray(ds_a.allocation_ids()));
      EXPECT_THAT(ds_a_get.values<ObjectId>(),
                  ElementsAreArray(ds_a.values<ObjectId>()));
      EXPECT_THAT(ds_a_get, IsEquivalentTo(ds_a));
    }

    // General case
    std::vector<arolla::OptionalValue<ObjectId>> main_b_values(size);
    std::vector<arolla::OptionalValue<ObjectId>> fallback_b_values(size);
    std::vector<arolla::OptionalValue<ObjectId>> merge_b_values(size);
    AllocationId alloc_b1 = Allocate(size);
    AllocationId alloc_b2 = Allocate(size);
    for (size_t i = 0; i < size; ++i) {
      ObjectId id1 = alloc_b1.ObjectByOffset(i);
      ObjectId id2 = alloc_b2.ObjectByOffset(i);
      if (i % 4 == 0) {
        main_b_values[i] = id1;
        merge_b_values[i] = id1;
      } else if (i % 4 == 1) {
        fallback_b_values[i] = id2;
        merge_b_values[i] = id2;
      } else if (i % 4 == 2) {
        main_b_values[i] = id1;
        fallback_b_values[i] = id2;
        merge_b_values[i] = id1;
      }
    }
    auto main_ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<ObjectId>(main_b_values));
    ASSERT_OK(db->SetAttr(ds, "b", main_ds_b));
    auto fallback_ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<ObjectId>(fallback_b_values));
    ASSERT_OK(db_f->SetAttr(ds, "b", fallback_ds_b));
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_b_get,
                         db->GetAttr(ds, "b", {db_f.get()}));
    auto merge_ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<ObjectId>(merge_b_values));

    EXPECT_EQ(ds_b_get.dtype(), arolla::GetQType<ObjectId>());
    EXPECT_THAT(ds_b_get.allocation_ids(),
                ElementsAreArray(merge_ds_b.allocation_ids()));
    EXPECT_THAT(ds_b_get.values<ObjectId>(),
                ElementsAreArray(merge_ds_b.values<ObjectId>()));
    EXPECT_THAT(ds_b_get, IsEquivalentTo(merge_ds_b));
  }
}

TEST(DataBagTest, SetGetWithFallbackPrimitive) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto db_f = DataBagImpl::CreateEmptyDatabag();

  {
    auto ds = DataSliceImpl::AllocateEmptyObjects(3);

    auto ds_a = DataSliceImpl::Create(
        arolla::CreateFullDenseArray<double>({1.0, 2.0, 3.0}));
    ASSERT_OK(db->SetAttr(ds, "a", ds_a));

    {  // fallback is empty
      ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get,
                           db->GetAttr(ds, "a", {db_f.get()}));
      EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<double>());
      EXPECT_THAT(ds_a_get.allocation_ids(), IsEmpty());
      EXPECT_THAT(ds_a_get.values<double>(),
                  ElementsAreArray(ds_a.values<double>()));
      EXPECT_THAT(ds_a_get, IsEquivalentTo(ds_a));
    }

    {  // main is empty
      ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get,
                           db_f->GetAttr(ds, "a", {db.get()}));
      EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<double>());
      EXPECT_THAT(ds_a_get.allocation_ids(), IsEmpty());
      EXPECT_THAT(ds_a_get.values<double>(),
                  ElementsAreArray(ds_a.values<double>()));
      EXPECT_THAT(ds_a_get, IsEquivalentTo(ds_a));
    }
  }

  // General case
  for (size_t size : {1, 2, 4, 17, 126}) {
    auto ds = DataSliceImpl::AllocateEmptyObjects(size);

    std::vector<arolla::OptionalValue<int64_t>> main_b_values(size);
    std::vector<arolla::OptionalValue<int64_t>> fallback_b_values(size);
    std::vector<arolla::OptionalValue<int64_t>> merge_b_values(size);
    for (size_t i = 0; i < size; ++i) {
      int64_t val1 = i + 1;
      int64_t val2 = -i - 1;
      if (i % 4 == 0) {
        main_b_values[i] = val1;
        merge_b_values[i] = val1;
      } else if (i % 4 == 1) {
        fallback_b_values[i] = val2;
        merge_b_values[i] = val2;
      } else if (i % 4 == 2) {
        main_b_values[i] = val1;
        fallback_b_values[i] = val2;
        merge_b_values[i] = val1;
      }
    }
    auto main_ds_b =
        DataSliceImpl::Create(arolla::CreateDenseArray<int64_t>(main_b_values));
    ASSERT_OK(db->SetAttr(ds, "b", main_ds_b));
    auto fallback_ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<int64_t>(fallback_b_values));
    ASSERT_OK(db_f->SetAttr(ds, "b", fallback_ds_b));
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_b_get,
                         db->GetAttr(ds, "b", {db_f.get()}));
    auto merge_ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<int64_t>(merge_b_values));

    EXPECT_EQ(ds_b_get.dtype(), arolla::GetQType<int64_t>());
    EXPECT_THAT(ds_b_get.allocation_ids(), IsEmpty());
    EXPECT_THAT(ds_b_get.values<int64_t>(),
                ElementsAreArray(merge_ds_b.values<int64_t>()));
    EXPECT_THAT(ds_b_get, IsEquivalentTo(merge_ds_b));
  }
}

TEST(DataBagTest, SetGetWithFallbackPrimitiveMixedType) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto db_f = DataBagImpl::CreateEmptyDatabag();

  for (size_t size : {1, 2, 4, 17, 126}) {
    auto ds = DataSliceImpl::AllocateEmptyObjects(size);

    std::vector<arolla::OptionalValue<int64_t>> main_b_values(size);
    std::vector<arolla::OptionalValue<double>> fallback_b_values(size);
    std::vector<arolla::OptionalValue<int64_t>> merge_b_values_int64(size);
    std::vector<arolla::OptionalValue<double>> merge_b_values_double(size);
    for (size_t i = 0; i < size; ++i) {
      int64_t val1 = i + 1;
      double val2 = -i - 1;
      if (i % 4 == 0) {
        main_b_values[i] = val1;
        merge_b_values_int64[i] = val1;
      } else if (i % 4 == 1) {
        fallback_b_values[i] = val2;
        merge_b_values_double[i] = val2;
      } else if (i % 4 == 2) {
        main_b_values[i] = val1;
        fallback_b_values[i] = val2;
        merge_b_values_int64[i] = val1;
      }
    }
    auto main_ds_b =
        DataSliceImpl::Create(arolla::CreateDenseArray<int64_t>(main_b_values));
    ASSERT_OK(db->SetAttr(ds, "b", main_ds_b));
    auto fallback_ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<double>(fallback_b_values));
    ASSERT_OK(db_f->SetAttr(ds, "b", fallback_ds_b));
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_b_get,
                         db->GetAttr(ds, "b", {db_f.get()}));
    auto merge_ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<int64_t>(merge_b_values_int64),
        arolla::CreateDenseArray<double>(merge_b_values_double));

    EXPECT_EQ(ds_b_get.is_mixed_dtype(), size > 1);
    EXPECT_EQ(ds_b_get.dtype(), size == 1 ? arolla::GetQType<int64_t>()
                                          : arolla::GetNothingQType())
        << size;
    EXPECT_THAT(ds_b_get.allocation_ids(), IsEmpty());
    EXPECT_THAT(ds_b_get, IsEquivalentTo(merge_ds_b));
  }
}

TEST(DataBagTest, SetGetEmptyAndUnknownType) {
  constexpr int kSize = 3;
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto empty_ds = DataSliceImpl::CreateEmptyAndUnknownType(kSize);
  auto int_ds = DataSliceImpl::Create(
      arolla::CreateDenseArray<int64_t>({2, std::nullopt, 42}));

  ASSERT_OK(db->SetAttr(ds, "attr", empty_ds));
  EXPECT_THAT(db->GetAttr(ds, "attr"),
              IsOkAndHolds(ElementsAre(DataItem(), DataItem(), DataItem())));

  EXPECT_THAT(db->GetAttr(DataItem(ds[0]), "attr"), IsOkAndHolds(DataItem()));

  ASSERT_OK(db->SetAttr(ds, "attr", int_ds));
  EXPECT_THAT(db->GetAttr(ds, "attr"),
              IsOkAndHolds(ElementsAre(2, DataItem(), 42)));
}

TEST(DataBagTest, SingleSparseSourceToDense) {
  constexpr size_t kAllocSize = 10000;
  auto db = DataBagImpl::CreateEmptyDatabag();

  AllocationId alloc = Allocate(kAllocSize);

  {  // Set 5 of 10000 -> create sparse source
    auto ds = DataSliceImpl::ObjectsFromAllocation(alloc, 5);
    auto ds_a =
        DataSliceImpl::Create(arolla::CreateConstDenseArray<int>(5, 57));
    ASSERT_OK(db->SetAttr(ds, "a", ds_a));
  }

  {  // Set 5000 of 10000 -> convert sparse source to dense source
    DataSliceImpl::Builder objs_bldr(kAllocSize / 2);
    DataSliceImpl::Builder values_bldr(kAllocSize / 2);
    for (size_t i = 0; i < kAllocSize / 2; ++i) {
      objs_bldr.Set(i, DataItem(alloc.ObjectByOffset(i + kAllocSize / 2)));
      values_bldr.Set(i, DataItem(static_cast<int>(i + kAllocSize / 2)));
    }
    ASSERT_OK(db->SetAttr(std::move(objs_bldr).Build(), "a",
                          std::move(values_bldr).Build()));
  }

  {
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db->GetAttributeDataSources(alloc, "a", dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 1);
    EXPECT_EQ(sparse_sources.size(), 0);
  }

  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db->GetAttr(DataItem(alloc.ObjectByOffset(3)), "a"));
    EXPECT_EQ(item, DataItem(57));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        DataItem item,
        db->GetAttr(DataItem(alloc.ObjectByOffset(kAllocSize - 1)), "a"));
    EXPECT_EQ(item, DataItem(static_cast<int>(kAllocSize - 1)));
  }
}

TEST(DataBagTest, SparseSource) {
  constexpr size_t kAllocSize = 10000;
  auto db = DataBagImpl::CreateEmptyDatabag();

  AllocationId alloc = Allocate(kAllocSize);

  {  // Set 5 of 10000 -> create sparse source
    auto ds = DataSliceImpl::ObjectsFromAllocation(alloc, 5);
    auto ds_a =
        DataSliceImpl::Create(arolla::CreateConstDenseArray<int>(5, 57));
    ASSERT_OK(db->SetAttr(ds, "a", ds_a));
  }

  {
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db->GetAttributeDataSources(alloc, "a", dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 0);
    EXPECT_EQ(sparse_sources.size(), 1);
  }

  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db->GetAttr(DataItem(alloc.ObjectByOffset(3)), "a"));
    EXPECT_EQ(item, DataItem(57));
  }
  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db->GetAttr(DataItem(alloc.ObjectByOffset(7)), "a"));
    EXPECT_EQ(item, DataItem());
  }

  auto db2 = db->PartiallyPersistentFork();

  {  // Sparse source from parent is used.
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db2->GetAttributeDataSources(alloc, "a", dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 0);
    EXPECT_EQ(sparse_sources.size(), 1);
  }

  // Set 1 of 10000 -> create sparse source
  ASSERT_OK(db2->SetAttr(DataItem(alloc.ObjectByOffset(7)), "a", DataItem(43)));

  {
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db2->GetAttributeDataSources(alloc, "a", dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 0);
    EXPECT_EQ(sparse_sources.size(), 2);
  }

  {  // Set 5000 of 10000 -> create dense source and merge sparse sources
    DataSliceImpl::Builder objs_bldr(kAllocSize / 2);
    DataSliceImpl::Builder values_bldr(kAllocSize / 2);
    for (size_t i = 0; i < kAllocSize / 2; ++i) {
      objs_bldr.Set(i, DataItem(alloc.ObjectByOffset(i + kAllocSize / 2)));
      values_bldr.Set(i, DataItem(static_cast<int>(i + kAllocSize / 2)));
    }
    ASSERT_OK(db2->SetAttr(std::move(objs_bldr).Build(), "a",
                           std::move(values_bldr).Build()));
  }

  {
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db2->GetAttributeDataSources(alloc, "a", dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 1);
    EXPECT_EQ(sparse_sources.size(), 0);
  }

  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db2->GetAttr(DataItem(alloc.ObjectByOffset(3)), "a"));
    EXPECT_EQ(item, DataItem(57));
  }
  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db2->GetAttr(DataItem(alloc.ObjectByOffset(7)), "a"));
    EXPECT_EQ(item, DataItem(43));
  }
  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db2->GetAttr(DataItem(alloc.ObjectByOffset(8)), "a"));
    EXPECT_EQ(item, DataItem());
  }
  {
    ASSERT_OK_AND_ASSIGN(
        DataItem item,
        db2->GetAttr(DataItem(alloc.ObjectByOffset(kAllocSize / 2 + 1)), "a"));
    EXPECT_EQ(item, DataItem(static_cast<int>(kAllocSize / 2 + 1)));
  }

  // Set 1 of 10000 -> use existing dense source
  ASSERT_OK(
      db2->SetAttr(DataItem(alloc.ObjectByOffset(3)), "a", DataItem(3.14f)));

  auto db3 = db2->PartiallyPersistentFork();

  {
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db3->GetAttributeDataSources(alloc, "a", dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 1);
    EXPECT_EQ(sparse_sources.size(), 0);
  }

  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db3->GetAttr(DataItem(alloc.ObjectByOffset(3)), "a"));
    EXPECT_EQ(item, DataItem(3.14f));
  }
  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db3->GetAttr(DataItem(alloc.ObjectByOffset(7)), "a"));
    EXPECT_EQ(item, DataItem(43));
  }
}

AllocationIdSet ConcatAllocations(const DataSliceImpl& ds,
                                  const DataSliceImpl& ds2) {
  std::vector<AllocationId> allocation_ids_union;
  allocation_ids_union.insert(allocation_ids_union.end(),
                              ds.allocation_ids().begin(),
                              ds.allocation_ids().end());
  allocation_ids_union.insert(allocation_ids_union.end(),
                              ds2.allocation_ids().begin(),
                              ds2.allocation_ids().end());
  return AllocationIdSet(allocation_ids_union);
}

std::vector<OptionalValue<ObjectId>> ConcatObjects(const DataSliceImpl& ds,
                                                   const DataSliceImpl& ds2) {
  std::vector<OptionalValue<ObjectId>> objects_union;
  objects_union.insert(objects_union.end(), ds.values<ObjectId>().begin(),
                       ds.values<ObjectId>().end());
  objects_union.insert(objects_union.end(), ds2.values<ObjectId>().begin(),
                       ds2.values<ObjectId>().end());
  return objects_union;
}

TEST(DataBagTest, SetGetTwoAllocationIds) {
  constexpr int64_t kSize = 3;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto ds_a = DataSliceImpl::AllocateEmptyObjects(kSize);
  ASSERT_OK(db->SetAttr(ds, "a", ds_a));

  auto ds2 = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto ds2_a = DataSliceImpl::AllocateEmptyObjects(kSize);
  ASSERT_OK(db->SetAttr(ds2, "a", ds2_a));

  auto ds_union = DataSliceImpl::CreateObjectsDataSlice(
      arolla::CreateDenseArray<ObjectId>(ConcatObjects(ds, ds2)),
      ConcatAllocations(ds, ds2));

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get, db->GetAttr(ds_union, "a"));

  auto db2 = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db2->SetAttr(ds_union, "a", ds_a_get));

  for (auto& db_cur : {db.get(), db2.get()}) {
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get_cur,
                         db_cur->GetAttr(ds_union, "a"));
    EXPECT_EQ(ds_a_get_cur.size(), kSize * 2);
    EXPECT_EQ(ds_a_get_cur.dtype(), arolla::GetQType<ObjectId>());
    EXPECT_THAT(ds_a_get_cur.allocation_ids(),
                ElementsAreArray(ConcatAllocations(ds_a, ds2_a)));
    EXPECT_EQ(ds_a_get_cur.values<ObjectId>().size(), kSize * 2);
    EXPECT_THAT(ds_a_get_cur.values<ObjectId>(),
                ElementsAreArray(ConcatObjects(ds_a, ds2_a)));
  }
}

TEST(DataBagTest, SetGetPrimitiveOverrideRemoveViaFork) {
  constexpr int64_t kSize = 3;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);

  for (int i = 0; i < 3; ++i) {
    db = db->PartiallyPersistentFork();
    auto ds_a = DataSliceImpl::Create(arolla::CreateDenseArray<int64_t>(
        std::vector{arolla::OptionalValue<int64_t>{i == 0, 1},
                    arolla::OptionalValue<int64_t>{i == 1, 2},
                    arolla::OptionalValue<int64_t>{i == 2, 3}}));
    ASSERT_OK(db->SetAttr(ds, "a", ds_a));
  }

  {
    ASSERT_OK_AND_ASSIGN(DataItem ds_a_0_item,
                         db->GetAttr(DataItem(ds.values<ObjectId>()[0]), "a"));
    EXPECT_FALSE(ds_a_0_item.has_value());
  }
  {
    ASSERT_OK_AND_ASSIGN(DataItem ds_a_2_item,
                         db->GetAttr(DataItem(ds.values<ObjectId>()[2]), "a"));
    ASSERT_EQ(ds_a_2_item.dtype(), arolla::GetQType<int64_t>());
    EXPECT_EQ(ds_a_2_item.value<int64_t>(), 3);
  }

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get, db->GetAttr(ds, "a"));

  EXPECT_EQ(ds_a_get.size(), kSize);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<int64_t>());
  EXPECT_THAT(ds_a_get.allocation_ids(), IsEmpty());
  EXPECT_THAT(ds_a_get.values<int64_t>(),
              ElementsAre(std::nullopt, std::nullopt, 3));

  {
    db = db->PartiallyPersistentFork();
    auto ds_a = DataSliceImpl::Create(arolla::CreateDenseArray<int64_t>(
        std::vector{arolla::OptionalValue<int64_t>(std::nullopt),
                    arolla::OptionalValue<int64_t>{5},
                    arolla::OptionalValue<int64_t>{7}}));
    ASSERT_OK(db->SetAttr(ds, "a", ds_a));
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db->GetAttributeDataSources(ds.allocation_ids().ids()[0], "a",
                                dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 1);
    EXPECT_EQ(sparse_sources.size(), 0);
    ASSERT_OK_AND_ASSIGN(ds_a_get, db->GetAttr(ds, "a"));
    EXPECT_THAT(ds_a_get.values<int64_t>(), ElementsAre(std::nullopt, 5, 7));
  }
}

TEST(DataBagTest, SetGetPrimitiveOverrideValueViaFork) {
  constexpr int64_t kSize = 4;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);

  for (int i = 0; i < 4; ++i) {
    db = db->PartiallyPersistentFork();
    auto ds_cur_obj = DataSliceImpl::CreateObjectsDataSlice(
        arolla::CreateDenseArray<ObjectId>(
            std::vector<arolla::OptionalValue<ObjectId>>{
                ds.values<ObjectId>()[i], ds.values<ObjectId>()[3 - i]}),
        ds.allocation_ids());
    auto ds_a =
        DataSliceImpl::Create(arolla::CreateDenseArray<int64_t>({i, i}));
    ASSERT_OK(db->SetAttr(ds_cur_obj, "a", ds_a));
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db->GetAttributeDataSources(ds.allocation_ids().ids()[0], "a",
                                dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 1);
    EXPECT_EQ(sparse_sources.size(), 0);
  }

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get, db->GetAttr(ds, "a"));

  EXPECT_EQ(ds_a_get.size(), kSize);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<int64_t>());
  EXPECT_THAT(ds_a_get.allocation_ids(), IsEmpty());
  EXPECT_THAT(ds_a_get.values<int64_t>(), ElementsAre(3, 2, 2, 3));
}

TEST(DataBagTest, SetGetObjectsOverrideRemoveViaFork) {
  constexpr int64_t kSize = 3;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  AllocationId a_objects = Allocate(8);

  for (int i = 0; i < 3; ++i) {
    db = db->PartiallyPersistentFork();
    auto ds_a = DataSliceImpl::CreateObjectsDataSlice(
        arolla::CreateDenseArray<ObjectId>(
            std::vector{arolla::OptionalValue<ObjectId>{
                            i == 0, a_objects.ObjectByOffset(0)},
                        arolla::OptionalValue<ObjectId>{
                            i == 1, a_objects.ObjectByOffset(1)},
                        arolla::OptionalValue<ObjectId>{
                            i == 2, a_objects.ObjectByOffset(2)}}),
        AllocationIdSet(a_objects));
    ASSERT_OK(db->SetAttr(ds, "a", ds_a));
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db->GetAttributeDataSources(ds.allocation_ids().ids()[0], "a",
                                dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 1);
    EXPECT_EQ(sparse_sources.size(), 0);
  }

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK_AND_ASSIGN(DataItem ds_a_item,
                         db->GetAttr(DataItem(ds.values<ObjectId>()[i]), "a"));
    if (i == 2) {
      EXPECT_EQ(ds_a_item.value<ObjectId>(), a_objects.ObjectByOffset(2)) << i;
    } else {
      EXPECT_FALSE(ds_a_item.holds_value<ObjectId>()) << i;
    }
  }

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get, db->GetAttr(ds, "a"));

  EXPECT_EQ(ds_a_get.size(), kSize);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(ds_a_get.allocation_ids(), ElementsAre(a_objects));
  EXPECT_THAT(
      ds_a_get.values<ObjectId>(),
      ElementsAre(std::nullopt, std::nullopt, a_objects.ObjectByOffset(2)));
}

TEST(DataBagTest, SetGetObjectsOverrideNewValueViaFork) {
  constexpr int64_t kSize = 4;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  AllocationId a_objects = Allocate(8);

  for (int i = 0; i < 4; ++i) {
    db = db->PartiallyPersistentFork();
    auto ds_cur_obj = DataSliceImpl::CreateObjectsDataSlice(
        arolla::CreateDenseArray<ObjectId>(
            std::vector<arolla::OptionalValue<ObjectId>>{
                ds.values<ObjectId>()[i], ds.values<ObjectId>()[3 - i]}),
        ds.allocation_ids());
    auto ds_a = DataSliceImpl::CreateObjectsDataSlice(
        arolla::CreateDenseArray<ObjectId>(
            std::vector<arolla::OptionalValue<ObjectId>>{
                a_objects.ObjectByOffset(i), a_objects.ObjectByOffset(i)}),
        AllocationIdSet(a_objects));
    ASSERT_OK(db->SetAttr(ds_cur_obj, "a", ds_a));
  }

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get, db->GetAttr(ds, "a"));

  EXPECT_EQ(ds_a_get.size(), kSize);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(ds_a_get.allocation_ids(), ElementsAre(a_objects));
  EXPECT_THAT(
      ds_a_get.values<ObjectId>(),
      ElementsAre(a_objects.ObjectByOffset(3), a_objects.ObjectByOffset(2),
                  a_objects.ObjectByOffset(2), a_objects.ObjectByOffset(3)));
}

TEST(DataBagTest, CreateObjectDataItemGet) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds_a = DataItem(AllocateSingleObject());
  auto ds_b = DataItem(57);
  auto ds_c = DataItem();
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      db->CreateObjectsFromFields(
          {"a", "b", "c"},
          {std::cref(ds_a), std::cref(ds_b), std::cref(ds_c)}));
  ASSERT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());

  AllocationId alloc(ds.value<ObjectId>());
  EXPECT_TRUE(alloc.IsSmall());

  for (auto [attr, expected_ds] :
       std::vector{std::pair{"a", ds_a}, std::pair{"b", ds_b},
                   std::pair{"c", ds_c}}) {
    ASSERT_OK_AND_ASSIGN(auto ds_get, db->GetAttr(ds, attr));
    EXPECT_EQ(ds_get, expected_ds);
  }
}

TEST(DataBagTest, CreateObjectsGet) {
  for (int64_t size : {1, 2, 13, 1079}) {
    auto db = DataBagImpl::CreateEmptyDatabag();

    auto ds_a = DataSliceImpl::AllocateEmptyObjects(size);
    auto ds_b = DataSliceImpl::AllocateEmptyObjects(size);
    auto ds_c = DataSliceImpl::CreateEmptyAndUnknownType(size);
    ASSERT_OK_AND_ASSIGN(
        auto ds,
        db->CreateObjectsFromFields(
            {"a", "b", "c"},
            {std::cref(ds_a), std::cref(ds_b), std::cref(ds_c)}));

    AllocationId alloc(ds.values<ObjectId>()[0].value);

    for (auto [attr, expected_ds, dtype] :
         std::vector{std::tuple{"a", ds_a, arolla::GetQType<ObjectId>()},
                     std::tuple{"b", ds_b, arolla::GetQType<ObjectId>()},
                     std::tuple{"c", ds_c, arolla::GetNothingQType()}}) {
      ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_get, db->GetAttr(ds, attr));

      EXPECT_EQ(ds_get.size(), size);
      EXPECT_EQ(ds_get.dtype(), dtype);
      EXPECT_EQ(ds_get.allocation_ids(), expected_ds.allocation_ids());
      EXPECT_EQ(ds_get.size(), size);
      EXPECT_THAT(ds_get, IsEquivalentTo(expected_ds));

      EXPECT_THAT(db->GetAttr(ds[0], attr), IsOkAndHolds(ds_get[0]));
    }

    // Assigning an attribute on a missing (nullptr) dense source.
    ASSERT_OK(db->SetAttr(ds, "c", DataSliceImpl::Create(size, DataItem(42))));
    EXPECT_THAT(db->GetAttr(ds[0], "c"), IsOkAndHolds(42));
  }
}

TEST(DataBagTest, CreateObjectsSetObjects) {
  for (int64_t size : {1, 2, 13, 1079}) {
    auto db = DataBagImpl::CreateEmptyDatabag();

    auto ds_empty = DataSliceImpl::AllocateEmptyObjects(0);
    auto ds_a = DataSliceImpl::AllocateEmptyObjects(size);
    auto ds_b = DataSliceImpl::AllocateEmptyObjects(size);
    auto ds_b_new = DataSliceImpl::AllocateEmptyObjects(size);
    ASSERT_OK_AND_ASSIGN(auto ds, db->CreateObjectsFromFields(
                                      {std::string("a"), std::string("b")},
                                      {std::cref(ds_a), std::cref(ds_b)}));
    ASSERT_OK(db->SetAttr(ds, "b", ds_b_new));

    AllocationId alloc(ds.values<ObjectId>()[0].value);

    {
      DataBagImpl::ConstDenseSourceArray dense_sources;
      DataBagImpl::ConstSparseSourceArray sparse_sources;
      db->GetAttributeDataSources(alloc, "a", dense_sources, sparse_sources);
      EXPECT_EQ(dense_sources.size(), size <= kSmallAllocMaxCapacity ? 0 : 1);
      EXPECT_EQ(sparse_sources.size(), size <= kSmallAllocMaxCapacity ? 1 : 0);
    }

    for (auto [attr, expected_ds, extra_alloc_ids_ds] :
         std::vector{std::tuple{"a", ds_a, ds_empty},
                     std::tuple{"b", ds_b_new, ds_b}}) {
      ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_get, db->GetAttr(ds, attr));

      EXPECT_EQ(ds_get.size(), size);
      EXPECT_EQ(ds_get.dtype(), arolla::GetQType<ObjectId>());
      {
        AllocationIdSet expected_set;
        expected_set.Insert(extra_alloc_ids_ds.allocation_ids());
        expected_set.Insert(expected_ds.allocation_ids());
        EXPECT_EQ(ds_get.allocation_ids(), expected_set);
      }
      EXPECT_EQ(ds_get.values<ObjectId>().size(), size);
      EXPECT_THAT(ds_get.values<ObjectId>(),
                  ElementsAreArray(expected_ds.values<ObjectId>()));
    }

    auto ds_b_new2 = DataSliceImpl::AllocateEmptyObjects(size);
    ASSERT_OK(db->SetAttr(ds, "b", ds_b_new2));
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_b_get, db->GetAttr(ds, "b"));
    {
      AllocationIdSet expected_set;
      expected_set.Insert(ds_b.allocation_ids());
      expected_set.Insert(ds_b_new.allocation_ids());
      expected_set.Insert(ds_b_new2.allocation_ids());
      EXPECT_EQ(ds_b_get.allocation_ids(), expected_set);
    }
    EXPECT_EQ(ds_b_get.values<ObjectId>().size(), size);
    EXPECT_THAT(ds_b_get.values<ObjectId>(),
                ElementsAreArray(ds_b_new2.values<ObjectId>()));
  }
}

TEST(DataBagTest, CreatePrimitiveSetPrimitive) {
  constexpr int64_t kSize = 13;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds_empty = DataSliceImpl::AllocateEmptyObjects(0);
  auto ds_a =
      DataSliceImpl::Create(arolla::CreateConstDenseArray<int32_t>(kSize, 75));
  auto ds_b =
      DataSliceImpl::Create(arolla::CreateConstDenseArray<int32_t>(kSize, 57));
  auto ds_b_new =
      DataSliceImpl::Create(arolla::CreateConstDenseArray<int32_t>(kSize, 37));
  ASSERT_OK_AND_ASSIGN(
      auto ds, db->CreateObjectsFromFields({std::string("a"), std::string("b")},
                                           {std::cref(ds_a), std::cref(ds_b)}));
  ASSERT_OK(db->SetAttr(ds, "b", ds_b_new));

  for (auto [attr, expected_ds] :
       std::vector{std::tuple{"a", ds_a}, std::tuple{"b", ds_b_new}}) {
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_get, db->GetAttr(ds, attr));

    EXPECT_EQ(ds_get.size(), kSize);
    EXPECT_EQ(ds_get.dtype(), arolla::GetQType<int32_t>());
    EXPECT_THAT(ds_get.allocation_ids(), IsEmpty());
    EXPECT_THAT(ds_get.values<int32_t>(),
                ElementsAreArray(expected_ds.values<int32_t>()));
  }

  auto ds_b_new2 =
      DataSliceImpl::Create(arolla::CreateConstDenseArray<int32_t>(kSize, 93));
  ASSERT_OK(db->SetAttr(ds, "b", ds_b_new2));

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_b_get, db->GetAttr(ds, "b"));
  EXPECT_THAT(ds_b_get.allocation_ids(), IsEmpty());
  EXPECT_THAT(ds_b_get.values<int32_t>(),
              ElementsAreArray(std::vector<int32_t>(kSize, 93)));
}

TEST(DataBagTest, TextAttribute) {
  constexpr int64_t kSize = 13;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds_empty = DataSliceImpl::AllocateEmptyObjects(0);
  auto ds_a = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<arolla::Text>(kSize, "aaa"));
  auto ds_b = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<arolla::Text>(kSize, "bbb"));
  auto ds_b_new = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<arolla::Text>(kSize, "ccc"));
  ASSERT_OK_AND_ASSIGN(
      auto ds, db->CreateObjectsFromFields({std::string("a"), std::string("b")},
                                           {std::cref(ds_a), std::cref(ds_b)}));
  ASSERT_OK(db->SetAttr(ds, "b", ds_b_new));

  for (auto [attr, expected_ds] :
       std::vector{std::tuple{"a", ds_a}, std::tuple{"b", ds_b_new}}) {
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_get, db->GetAttr(ds, attr));

    EXPECT_EQ(ds_get.size(), kSize);
    EXPECT_EQ(ds_get.dtype(), arolla::GetQType<arolla::Text>());
    EXPECT_THAT(ds_get.allocation_ids(), IsEmpty());
    EXPECT_THAT(ds_get.values<arolla::Text>(),
                ElementsAreArray(expected_ds.values<arolla::Text>()));
  }
}

TEST(DataBagTest, EmptySliceGetWithSparseSource) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  constexpr int64_t kSize = 4;
  auto obj1 = AllocateSingleObject();
  auto dsb = DataSliceImpl::Builder(kSize);
  dsb.GetMutableAllocationIds().Insert(AllocationId(obj1));
  auto ds = std::move(dsb).Build();
  EXPECT_TRUE(ds.allocation_ids().empty());
  EXPECT_TRUE(ds.allocation_ids().contains_small_allocation_id());

  ASSERT_OK_AND_ASSIGN(auto result, db->GetAttr(ds, "a"));

  EXPECT_TRUE(result.is_empty_and_unknown());
}

TEST(DataBagTest, EmptySliceSet) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  constexpr int64_t kSize = 4;
  auto ds = DataSliceImpl::CreateEmptyAndUnknownType(kSize);

  ASSERT_OK(db->SetAttr(ds, "a", ds));
}

TEST(DataBagTest, EmptySliceSetGetWithSparseSource) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  constexpr int64_t kSize = 4;
  auto obj1 = AllocateSingleObject();
  ASSERT_OK(db->SetAttr(DataItem(obj1), "a", DataItem(1)));

  auto dsb = DataSliceImpl::Builder(kSize);
  dsb.GetMutableAllocationIds().Insert(AllocationId(obj1));
  auto ds = std::move(dsb).Build();
  EXPECT_TRUE(ds.allocation_ids().empty());
  EXPECT_TRUE(ds.allocation_ids().contains_small_allocation_id());

  ASSERT_OK(db->SetAttr(ds, "a", ds));

  ASSERT_OK_AND_ASSIGN(auto result, db->GetAttr(ds, "a"));
  EXPECT_TRUE(result.is_empty_and_unknown());
  ASSERT_OK_AND_ASSIGN(auto obj1_a, db->GetAttr(DataItem(obj1), "a"));
  EXPECT_EQ(obj1_a.value<int>(), 1);
}

TEST(DataBagTest, SetGetDataItem) {
  auto ds_a = DataItem(57.0f);
  for (DataItem ds : {
           CreateUuidFromFields("", {{"a", std::cref(ds_a)}}),
           DataItem(AllocateSingleObject()),
           DataItem(Allocate(1).ObjectByOffset(0)),
           DataItem(Allocate(2).ObjectByOffset(1)),
           DataItem(Allocate(7777).ObjectByOffset(5555)),
       }) {
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto ds_b = DataItem(75);
    ASSERT_OK(db->SetAttr(ds, "b", ds_b));

    ASSERT_OK_AND_ASSIGN(DataItem ds_b_get, db->GetAttr(ds, "b"));
    ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<int32_t>());
    EXPECT_EQ(ds_b_get.value<int>(), 75);

    db = db->PartiallyPersistentFork();
    auto ds_c = DataItem(91.0);
    ASSERT_OK(db->SetAttr(ds, "c", ds_c));

    ASSERT_OK_AND_ASSIGN(ds_b_get, db->GetAttr(ds, "b"));
    ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<int32_t>());
    EXPECT_EQ(ds_b_get.value<int32_t>(), 75);

    ASSERT_OK_AND_ASSIGN(auto ds_c_get, db->GetAttr(ds, "c"));
    ASSERT_EQ(ds_c_get.dtype(), arolla::GetQType<double>());
    EXPECT_EQ(ds_c_get.value<double>(), 91.0);

    ASSERT_OK(db->SetAttr(ds, "b", DataItem(arolla::Text("B"))));
    ASSERT_OK_AND_ASSIGN(ds_b_get, db->GetAttr(ds, "b"));
    ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<arolla::Text>());
    EXPECT_EQ(ds_b_get.value<arolla::Text>(), arolla::Text("B"));
  }
}

TEST(DataBagTest, SetGetDataItemWithFallback) {
  auto ds_a = DataItem(57.0f);
  for (DataItem ds : {
           CreateUuidFromFields("", {{"a", std::cref(ds_a)}}),
           DataItem(AllocateSingleObject()),
           DataItem(Allocate(1).ObjectByOffset(0)),
           DataItem(Allocate(2).ObjectByOffset(1)),
           DataItem(Allocate(7777).ObjectByOffset(5555)),
       }) {
    auto db_fb = DataBagImpl::CreateEmptyDatabag();
    auto ds_b = DataItem(75);
    ASSERT_OK(db_fb->SetAttr(ds, "b", ds_b));

    auto db = DataBagImpl::CreateEmptyDatabag();

    ASSERT_OK_AND_ASSIGN(DataItem ds_b_get, db->GetAttr(ds, "b"));
    EXPECT_THAT(db->GetAttr(ds, "b", {db_fb.get()}),
                IsOkAndHolds(DataItem(75)));

    EXPECT_THAT(db->GetAttr(ds, "b"), IsOkAndHolds(DataItem()));

    ASSERT_OK(db->SetAttr(ds, "b", DataItem(arolla::Text("B"))));
    EXPECT_THAT(db->GetAttr(ds, "b"),
                IsOkAndHolds(DataItem(arolla::Text("B"))));
    EXPECT_THAT(db->GetAttr(ds, "b", {}),
                IsOkAndHolds(DataItem(arolla::Text("B"))));
    EXPECT_THAT(db->GetAttr(ds, "b", {db_fb.get()}),
                IsOkAndHolds(DataItem(arolla::Text("B"))));
  }
}

TEST(DataBagTest, SetGetDataSliceUUid) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds_a =
      DataSliceImpl::Create(arolla::CreateFullDenseArray<int>({13, 19, 57}));
  ASSERT_OK_AND_ASSIGN(auto ds, CreateUuidFromFields(
      "", {{"a", std::cref(ds_a)}}));

  auto ds_b =
      DataSliceImpl::Create(arolla::CreateFullDenseArray<int>({23, 29, 75}));

  ASSERT_OK(db->SetAttr(ds, "b", ds_b));

  ASSERT_OK_AND_ASSIGN(auto ds_b_get, db->GetAttr(ds, "b"));
  ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<int32_t>());
  EXPECT_THAT(ds_b_get.values<int>(), ElementsAre(23, 29, 75));

  db = db->PartiallyPersistentFork();
  auto ds_c =
      DataSliceImpl::Create(arolla::CreateFullDenseArray<double>({91, 39, 43}));
  ASSERT_OK(db->SetAttr(ds, "c", ds_c));

  ASSERT_OK_AND_ASSIGN(ds_b_get, db->GetAttr(ds, "b"));
  ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<int32_t>());
  EXPECT_THAT(ds_b_get.values<int>(), ElementsAre(23, 29, 75));

  ASSERT_OK_AND_ASSIGN(auto ds_c_get, db->GetAttr(ds, "c"));
  ASSERT_EQ(ds_c_get.dtype(), arolla::GetQType<double>());
  EXPECT_THAT(ds_c_get.values<double>(), ElementsAre(91, 39, 43));

  auto text_array =
      std::vector{arolla::Text("Q"), arolla::Text("B"), arolla::Text("W")};
  auto ds_b_text =
      DataSliceImpl::Create(arolla::CreateFullDenseArray<arolla::Text>(
          text_array.begin(), text_array.end()));
  ASSERT_OK(db->SetAttr(ds, "b", ds_b_text));

  ASSERT_OK_AND_ASSIGN(ds_b_get, db->GetAttr(ds, "b"));
  ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<arolla::Text>());
  EXPECT_THAT(ds_b_get.values<arolla::Text>(), ElementsAreArray(text_array));

  // remove
  ds_b = DataSliceImpl::Create(arolla::CreateDenseArray<int>(
      std::vector<OptionalValue<int>>{5, std::nullopt, 7}));
  ASSERT_OK(db->SetAttr(ds, "b", ds_b));
  ASSERT_OK_AND_ASSIGN(ds_b_get, db->GetAttr(ds, "b"));
  ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<int32_t>());
  EXPECT_THAT(ds_b_get.values<int>(), ElementsAre(5, std::nullopt, 7));

  // ObjectId
  AllocationId alloc1 = Allocate(1000);
  AllocationId alloc2 = Allocate(2000);
  AllocationId alloc3 = Allocate(1);
  auto object_array = arolla::CreateFullDenseArray<ObjectId>(
      {alloc1.ObjectByOffset(5), alloc3.ObjectByOffset(0),
       alloc2.ObjectByOffset(9)});
  ds_b = DataSliceImpl::CreateWithAllocIds(AllocationIdSet({alloc1, alloc2}),
                                           object_array);
  ASSERT_OK(db->SetAttr(ds, "b", ds_b));
  ASSERT_OK_AND_ASSIGN(ds_b_get, db->GetAttr(ds, "b"));
  ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(ds_b_get.values<ObjectId>(), ElementsAreArray(object_array));
  EXPECT_THAT(ds_b_get.allocation_ids(), UnorderedElementsAre(alloc1, alloc2));
  EXPECT_TRUE(ds_b_get.allocation_ids().contains_small_allocation_id());
}

TEST(DataBagTest, EmptyAndUnknownLists) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto empty = DataSliceImpl::CreateEmptyAndUnknownType(3);
  EXPECT_THAT(
      db->GetListSize(empty),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
  EXPECT_OK(db->AppendToList(empty, empty));
  EXPECT_THAT(
      db->GetFromLists(empty, arolla::CreateEmptyDenseArray<int64_t>(3)),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
  EXPECT_OK(
      db->SetInLists(empty, arolla::CreateEmptyDenseArray<int64_t>(3), empty));
  EXPECT_OK(db->RemoveInList(empty, arolla::CreateEmptyDenseArray<int64_t>(3)));
  EXPECT_THAT(
      db->PopFromLists(empty, arolla::CreateEmptyDenseArray<int64_t>(3)),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
  EXPECT_THAT(
      db->GetFromLists(empty, arolla::CreateEmptyDenseArray<int64_t>(3)),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
  {
    ASSERT_OK_AND_ASSIGN((auto [values, edge]), db->ExplodeLists(empty));
    EXPECT_EQ(values.size(), 0);
    EXPECT_EQ(edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 0, 0, 0));
    EXPECT_OK(db->ExtendLists(empty, values, edge));
    EXPECT_OK(
        db->ReplaceInLists(empty, DataBagImpl::ListRange(), values, edge));
  }
}

TEST(DataBagTest, AppendToLists) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc_id = AllocateLists(2);
  ObjectId list_from_other_alloc = AllocateSingleList();
  DataSliceImpl lists =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
          {alloc_id.ObjectByOffset(1), alloc_id.ObjectByOffset(0),
           list_from_other_alloc}));

  EXPECT_THAT(db->GetListSize(lists), IsOkAndHolds(ElementsAre(0, 0, 0)));
  ASSERT_OK(db->AppendToList(
      lists,
      DataSliceImpl::Create(
          arolla::CreateDenseArray<int64_t>({7, std::nullopt, std::nullopt}),
          arolla::CreateDenseArray<float>({std::nullopt, std::nullopt, 3.0}))));
  ASSERT_OK(db->AppendToList(
      DataSliceImpl::ObjectsFromAllocation(alloc_id, 1),
      DataSliceImpl::Create(arolla::CreateDenseArray<bool>({false}))));
  auto db2 = db->PartiallyPersistentFork();
  ASSERT_OK(db2->AppendToList(
      lists, DataSliceImpl::Create(
                 arolla::CreateDenseArray<int>({1, 2, std::nullopt}))));
  auto db3 = db2->PartiallyPersistentFork();

  EXPECT_THAT(db->GetListSize(lists), IsOkAndHolds(ElementsAre(1, 2, 1)));
  EXPECT_THAT(db2->GetListSize(lists), IsOkAndHolds(ElementsAre(2, 3, 2)));
  EXPECT_THAT(db3->GetListSize(lists), IsOkAndHolds(ElementsAre(2, 3, 2)));

  EXPECT_THAT(
      db2->GetFromLists(lists, arolla::CreateDenseArray<int64_t>({0, 0, 0})),
      IsOkAndHolds(ElementsAre(int64_t{7}, DataItem(), 3.0f)));
  EXPECT_THAT(
      db2->GetFromLists(lists, arolla::CreateDenseArray<int64_t>({-1, 1, 0})),
      IsOkAndHolds(ElementsAre(1, false, 3.0f)));
  EXPECT_THAT(db2->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(3.0, DataItem())));

  ASSERT_OK(
      db3->AppendToList(lists, DataSliceImpl::CreateEmptyAndUnknownType(3)));
  EXPECT_THAT(db3->GetListSize(lists), IsOkAndHolds(ElementsAre(3, 4, 3)));
}

TEST(DataBagTest, RemoveInLists) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc_id = AllocateLists(3);
  DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc_id, 3);

  ASSERT_OK(db->ExtendList(DataItem(alloc_id.ObjectByOffset(0)),
                           DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                               {1, 2, std::nullopt}))));
  ASSERT_OK(db->ExtendList(
      DataItem(alloc_id.ObjectByOffset(1)),
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({4, 6}))));
  ASSERT_OK(db->ExtendList(DataItem(alloc_id.ObjectByOffset(2)),
                           DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                               {std::nullopt, 8, 9}))));

  // Remove [-1:]
  ASSERT_OK(db->RemoveInList(lists, DataBagImpl::ListRange(-1)));
  EXPECT_THAT(db->ExplodeList(lists[0]), IsOkAndHolds(ElementsAre(1, 2)));
  EXPECT_THAT(db->ExplodeList(lists[1]), IsOkAndHolds(ElementsAre(4)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(std::nullopt, 8)));

  // Remove [1:0]
  ASSERT_OK(db->RemoveInList(lists, DataBagImpl::ListRange(1, 0)));
  EXPECT_THAT(db->ExplodeList(lists[0]), IsOkAndHolds(ElementsAre(1, 2)));
  EXPECT_THAT(db->ExplodeList(lists[1]), IsOkAndHolds(ElementsAre(4)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(std::nullopt, 8)));
}

TEST(DataBagTest, SetAndGetInLists) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc_id = AllocateLists(3);
  DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc_id, 3);

  ASSERT_OK(db->ExtendList(DataItem(alloc_id.ObjectByOffset(0)),
                           DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                               {1, 2, std::nullopt}))));
  ASSERT_OK(db->ExtendList(DataItem(alloc_id.ObjectByOffset(1)),
                           DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                               {4, std::nullopt, 6}))));
  ASSERT_OK(db->ExtendList(DataItem(alloc_id.ObjectByOffset(2)),
                           DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                               {std::nullopt, 8, 9}))));

  EXPECT_THAT(
      db->GetFromLists(lists, arolla::CreateDenseArray<int64_t>({0, 1, -1})),
      IsOkAndHolds(ElementsAre(1, DataItem(), 9)));

  ASSERT_OK(db->SetInLists(
      lists, arolla::CreateDenseArray<int64_t>({0, -2, 2}),
      DataSliceImpl::Create(
          arolla::CreateDenseArray<float>({10.0f, 11.0f, 12.0f}))));

  EXPECT_THAT(
      db->GetFromLists(lists, arolla::CreateDenseArray<int64_t>({2, 1, 0})),
      IsOkAndHolds(ElementsAre(DataItem(), 11.0f, DataItem())));

  ASSERT_OK(db->SetInLists(lists, arolla::CreateDenseArray<int64_t>({0, -2, 2}),
                           DataSliceImpl::CreateEmptyAndUnknownType(3)));

  EXPECT_THAT(
      db->GetFromLists(lists, arolla::CreateDenseArray<int64_t>({2, 1, 0})),
      IsOkAndHolds(ElementsAre(DataItem(), DataItem(), DataItem())));
}

TEST(DataBagTest, ExplodeLists) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc_id = AllocateLists(3);
  DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc_id, 3);

  ASSERT_OK(db->ExtendList(DataItem(alloc_id.ObjectByOffset(0)),
                           DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                               {1, 2, std::nullopt}))));
  ASSERT_OK(
      db->AppendToList(DataItem(alloc_id.ObjectByOffset(1)), DataItem(4)));
  ASSERT_OK(
      db->AppendToList(DataItem(alloc_id.ObjectByOffset(1)), DataItem(6.0f)));
  ASSERT_OK(db->ExtendList(DataItem(alloc_id.ObjectByOffset(2)),
                           DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                               {std::nullopt, 8, 9}))));

  {  // Slices [1:] from each list grouped together.
    ASSERT_OK_AND_ASSIGN((auto [values, edge]),
                         db->ExplodeLists(lists, DataBagImpl::ListRange(1)));

    EXPECT_THAT(values, ElementsAre(2, std::nullopt, 6.0f, 8, 9));

    ASSERT_EQ(edge.edge_type(), arolla::DenseArrayEdge::SPLIT_POINTS);
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 2, 3, 5));
  }

  {  // Slices [2:0] from each list grouped together.
    ASSERT_OK_AND_ASSIGN((auto [values, edge]),
                         db->ExplodeLists(lists, DataBagImpl::ListRange(2, 0)));

    EXPECT_THAT(values, ElementsAre());

    ASSERT_EQ(edge.edge_type(), arolla::DenseArrayEdge::SPLIT_POINTS);
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 0, 0, 0));
  }
}

TEST(DataBagTest, ExtendAndReplaceInLists) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc_id = AllocateLists(3);
  DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc_id, 3);

  {  // extend
    auto values = DataSliceImpl::Create(arolla::CreateDenseArray<int>(
        {2, std::nullopt, 8, 10, 11, 12}));
    ASSERT_OK_AND_ASSIGN(auto edge,
                         arolla::DenseArrayEdge::FromSplitPoints(
                             arolla::CreateDenseArray<int64_t>({0, 2, 3, 6})));
    ASSERT_OK(db->ExtendLists(lists, values, edge));
  }

  {  // replace [0:0]
    auto values = DataSliceImpl::Create(
        arolla::CreateDenseArray<int>({1, std::nullopt, 9}));
    ASSERT_OK_AND_ASSIGN(auto edge,
                         arolla::DenseArrayEdge::FromSplitPoints(
                             arolla::CreateDenseArray<int64_t>({0, 1, 2, 3})));
    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(0, 0), values, edge));
  }

  EXPECT_THAT(db->ExplodeList(lists[0]),
              IsOkAndHolds(ElementsAre(1, 2, std::nullopt)));
  EXPECT_THAT(db->ExplodeList(lists[1]),
              IsOkAndHolds(ElementsAre(std::nullopt, 8)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(9, 10, 11, 12)));

  {  // replace [1:3]
    auto values = DataSliceImpl::Create(
        arolla::CreateDenseArray<int>({{}, 1, {}, {}}),
        arolla::CreateDenseArray<float>({3.0f, {}, 2.0f, {}}));
    ASSERT_OK_AND_ASSIGN(auto edge,
                         arolla::DenseArrayEdge::FromSplitPoints(
                             arolla::CreateDenseArray<int64_t>({0, 1, 3, 4})));
    ASSERT_OK(db->ReplaceInLists(lists, DataBagImpl::ListRange(1, 3),
                                 values, edge));
  }

  EXPECT_THAT(db->ExplodeList(lists[0]), IsOkAndHolds(ElementsAre(1, 3.0f)));
  EXPECT_THAT(db->ExplodeList(lists[1]),
              IsOkAndHolds(ElementsAre(std::nullopt, 1, 2.0f)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(9, std::nullopt, 12)));

  {  // extend, duplicated list.
    auto lists2 = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
        {alloc_id.ObjectByOffset(2), std::nullopt,
         alloc_id.ObjectByOffset(2)}));
    auto values = DataSliceImpl::Create(arolla::CreateDenseArray<arolla::Text>(
        {arolla::Text("aaa"), arolla::Text("bbb")}));
    ASSERT_OK_AND_ASSIGN(auto edge,
                         arolla::DenseArrayEdge::FromSplitPoints(
                             arolla::CreateDenseArray<int64_t>({0, 1, 1, 2})));
    ASSERT_OK(db->ExtendLists(lists2, values, edge));
  }

  EXPECT_THAT(db->ExplodeList(lists[0]), IsOkAndHolds(ElementsAre(1, 3.0f)));
  EXPECT_THAT(db->ExplodeList(lists[1]),
              IsOkAndHolds(ElementsAre(std::nullopt, 1, 2.0f)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(9, std::nullopt, 12, arolla::Text("aaa"),
                                       arolla::Text("bbb"))));

  {  // replace [-1:] (last element)
    auto values =
        DataSliceImpl::Create(arolla::CreateConstDenseArray<int>(6, 57));
    ASSERT_OK_AND_ASSIGN(auto edge,
                         arolla::DenseArrayEdge::FromSplitPoints(
                             arolla::CreateDenseArray<int64_t>({0, 2, 4, 6})));
    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(-1), values, edge));
  }

  EXPECT_THAT(db->ExplodeList(lists[0]), IsOkAndHolds(ElementsAre(1, 57, 57)));
  EXPECT_THAT(db->ExplodeList(lists[1]),
              IsOkAndHolds(ElementsAre(std::nullopt, 1, 57, 57)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(9, std::nullopt, 12, arolla::Text("aaa"),
                                       57, 57)));

  {  // replace [1:] with 2 missing
    auto values = DataSliceImpl::CreateAllMissingObjectDataSlice(6);
    ASSERT_OK_AND_ASSIGN(auto edge,
                         arolla::DenseArrayEdge::FromSplitPoints(
                             arolla::CreateDenseArray<int64_t>({0, 2, 4, 6})));
    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(1), values, edge));
  }

  EXPECT_THAT(db->ExplodeList(lists[0]),
              IsOkAndHolds(ElementsAre(1, std::nullopt, std::nullopt)));
  EXPECT_THAT(
      db->ExplodeList(lists[1]),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(9, std::nullopt, std::nullopt)));

  {  // replace [1:0]
    auto values = DataSliceImpl::Create(
        arolla::CreateDenseArray<int>({2, 3, std::nullopt, 10}));
    ASSERT_OK_AND_ASSIGN(auto edge,
                         arolla::DenseArrayEdge::FromSplitPoints(
                             arolla::CreateDenseArray<int64_t>({0, 1, 3, 4})));
    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(1, 0), values, edge));
  }

  EXPECT_THAT(db->ExplodeList(lists[0]),
              IsOkAndHolds(ElementsAre(1, 2, std::nullopt, std::nullopt)));
  EXPECT_THAT(db->ExplodeList(lists[1]),
              IsOkAndHolds(ElementsAre(std::nullopt, 3, std::nullopt,
                                       std::nullopt, std::nullopt)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(9, 10, std::nullopt, std::nullopt)));
}

TEST(DataBagTest, ReplaceInListsEmptyAndUnknown) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc_id = AllocateLists(3);
  DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc_id, 3);
  auto empty_values = DataSliceImpl::CreateEmptyAndUnknownType(3);
  auto values = DataSliceImpl::Create(
      arolla::CreateDenseArray<int>({1, std::nullopt, 9}));
  ASSERT_OK_AND_ASSIGN(auto edge,
                       arolla::DenseArrayEdge::FromSplitPoints(
                           arolla::CreateDenseArray<int64_t>({0, 1, 2, 3})));

  {  // replace [:]
    SCOPED_TRACE("replace [:]");
    ASSERT_OK(db->ReplaceInLists(lists, DataBagImpl::ListRange(), empty_values,
                                 edge));
    for (int i = 0; i < lists.size(); ++i) {
      EXPECT_THAT(db->ExplodeList(lists[i]),
                  IsOkAndHolds(ElementsAre(std::nullopt)))
          << i;
    }

    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(), values, edge));
    ASSERT_OK(db->ReplaceInLists(lists, DataBagImpl::ListRange(), empty_values,
                                 edge));
    for (int i = 0; i < lists.size(); ++i) {
      EXPECT_THAT(db->ExplodeList(lists[i]),
                  IsOkAndHolds(ElementsAre(std::nullopt)))
          << i;
    }
  }

  {  // replace [0:1]
    SCOPED_TRACE("replace [0:1]");
    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(), values, edge));
    ASSERT_OK(db->ReplaceInLists(lists, DataBagImpl::ListRange(0, 1),
                                 empty_values, edge));
    for (int i = 0; i < lists.size(); ++i) {
      EXPECT_THAT(db->ExplodeList(lists[i]),
                  IsOkAndHolds(ElementsAre(std::nullopt)))
          << i;
    }
  }

  {  // replace [0:0]
    SCOPED_TRACE("replace [0:0]");
    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(), values, edge));
    ASSERT_OK(db->ReplaceInLists(lists, DataBagImpl::ListRange(0, 0),
                                 empty_values, edge));
    for (int i = 0; i < lists.size(); ++i) {
      EXPECT_THAT(db->ExplodeList(lists[i]),
                  IsOkAndHolds(ElementsAre(std::nullopt, values[i])))
          << i;
    }
  }

  {  // replace [1:1]
    SCOPED_TRACE("replace [1:1]");
    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(), values, edge));
    ASSERT_OK(db->ReplaceInLists(lists, DataBagImpl::ListRange(1, 1),
                                 empty_values, edge));
    for (int i = 0; i < lists.size(); ++i) {
      EXPECT_THAT(db->ExplodeList(lists[i]),
                  IsOkAndHolds(ElementsAre(values[i], std::nullopt)))
          << i;
    }
  }
}

TEST(DataBagTest, SingleListOps) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  DataItem list(AllocateSingleList());

  EXPECT_THAT(db->GetListSize(list), IsOkAndHolds(DataItem(int64_t{0})));

  ASSERT_OK(db->AppendToList(list, DataItem(1.0f)));
  ASSERT_OK(db->ExtendList(
      list,
      DataSliceImpl::Create(arolla::CreateDenseArray<float>({2.0f, 3.0f}))));

  EXPECT_THAT(db->GetListSize(list), IsOkAndHolds(DataItem(int64_t{3})));
  EXPECT_THAT(db->ExplodeList(list),
              IsOkAndHolds(ElementsAre(1.0f, 2.0f, 3.0f)));

  // Remove [2:1]
  ASSERT_OK(db->RemoveInList(list, DataBagImpl::ListRange(2, 1)));
  EXPECT_THAT(db->GetListSize(list), IsOkAndHolds(DataItem(int64_t{3})));
  EXPECT_THAT(db->ExplodeList(list),
              IsOkAndHolds(ElementsAre(1.0f, 2.0f, 3.0f)));

  // Remove [0:2]
  ASSERT_OK(db->RemoveInList(list, DataBagImpl::ListRange(0, 2)));
  EXPECT_THAT(db->GetListSize(list), IsOkAndHolds(DataItem(int64_t{1})));
  EXPECT_THAT(db->ExplodeList(list), IsOkAndHolds(ElementsAre(3.0f)));

  // Remove [1:]
  ASSERT_OK(db->RemoveInList(list, DataBagImpl::ListRange(1)));
  EXPECT_THAT(db->GetListSize(list), IsOkAndHolds(DataItem(int64_t{1})));
  EXPECT_THAT(db->ExplodeList(list), IsOkAndHolds(ElementsAre(3.0f)));

  ASSERT_OK(db->ExtendList(
      list, DataSliceImpl::Create(arolla::CreateDenseArray<int>({7, 8, 9}))));
  ASSERT_OK(db->ReplaceInList(
      list, DataBagImpl::ListRange(2, 3),
      DataSliceImpl::Create(arolla::CreateDenseArray<arolla::Text>(
          {arolla::Text("aaa"), arolla::Text("bbb")}))));

  EXPECT_THAT(db->GetListSize(list), IsOkAndHolds(DataItem(int64_t{5})));
  EXPECT_THAT(db->GetFromList(list, 2),
              IsOkAndHolds(DataItem(arolla::Text("aaa"))));
  EXPECT_THAT(db->GetFromList(list, -2),
              IsOkAndHolds(DataItem(arolla::Text("bbb"))));
  EXPECT_THAT(db->ExplodeList(list, DataBagImpl::ListRange(0, 2)),
              IsOkAndHolds(ElementsAre(3.0f, 7)));
  EXPECT_THAT(
      db->ExplodeList(list, DataBagImpl::ListRange(-3)),
      IsOkAndHolds(ElementsAre(arolla::Text("aaa"), arolla::Text("bbb"), 9)));

  ASSERT_OK(db->SetInList(list, -2, DataItem(arolla::Text("ccc"))));

  ASSERT_OK(db->ReplaceInList(
      list, DataBagImpl::ListRange(0, -2),
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({0}))));

  EXPECT_THAT(db->ExplodeList(list),
              IsOkAndHolds(ElementsAre(0, arolla::Text("ccc"), 9)));

  ASSERT_OK(db->ReplaceInList(
      list, DataBagImpl::ListRange(),
      DataSliceImpl::Create(
          arolla::CreateDenseArray<int>({0, std::nullopt, std::nullopt}),
          arolla::CreateDenseArray<float>(
              {std::nullopt, std::nullopt, 5.0f}))));

  EXPECT_THAT(db->ExplodeList(list),
              IsOkAndHolds(ElementsAre(0, DataItem(), 5.0f)));

  ASSERT_OK(db->ReplaceInList(
      list, DataBagImpl::ListRange(2, 1),
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({0}))));

  EXPECT_THAT(db->ExplodeList(list),
              IsOkAndHolds(ElementsAre(0, DataItem(), 0, 5.0f)));
}

TEST(DataBagTest, ListWithFallback) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  DataItem list(AllocateSingleList());
  auto fb_db = DataBagImpl::CreateEmptyDatabag();

  EXPECT_THAT(db->GetListSize(list, {fb_db.get()}),
              IsOkAndHolds(DataItem(int64_t{0})));
  EXPECT_THAT(db->GetFromList(list, 0, {fb_db.get()}),
              IsOkAndHolds(DataItem()));
  EXPECT_THAT(
      db->ExplodeList(list, DataBagImpl::ListRange(0, 1), {fb_db.get()}),
      IsOkAndHolds(ElementsAre()));

  ASSERT_OK(db->AppendToList(list, DataItem(1.0f)));
  {
    EXPECT_THAT(db->GetListSize(list, {fb_db.get()}),
                IsOkAndHolds(DataItem(int64_t{1})));
    EXPECT_THAT(db->GetFromList(list, 0, {fb_db.get()}),
                IsOkAndHolds(DataItem(1.0f)));
    EXPECT_THAT(db->GetFromList(list, 1, {fb_db.get()}),
                IsOkAndHolds(DataItem()));
    EXPECT_THAT(
        db->ExplodeList(list, DataBagImpl::ListRange(0, 1), {fb_db.get()}),
        IsOkAndHolds(ElementsAre(1.0f)));
  }
  {
    EXPECT_THAT(fb_db->GetListSize(list, {db.get()}),
                IsOkAndHolds(DataItem(int64_t{1})));
    EXPECT_THAT(fb_db->GetFromList(list, 0, {db.get()}),
                IsOkAndHolds(DataItem(1.0f)));
    EXPECT_THAT(fb_db->GetFromList(list, 1, {db.get()}),
                IsOkAndHolds(DataItem()));
    EXPECT_THAT(
        fb_db->ExplodeList(list, DataBagImpl::ListRange(0, 1), {db.get()}),
        IsOkAndHolds(ElementsAre(1.0f)));
  }

  ASSERT_OK(fb_db->AppendToList(list, DataItem(2.0f)));
  ASSERT_OK(fb_db->AppendToList(list, DataItem(3.0f)));
  {
    EXPECT_THAT(db->GetListSize(list, {fb_db.get()}),
                IsOkAndHolds(DataItem(int64_t{1})));
    EXPECT_THAT(db->GetFromList(list, 0, {fb_db.get()}),
                IsOkAndHolds(DataItem(1.0f)));
    // fallback is not used in case list is not empty
    EXPECT_THAT(db->GetFromList(list, 1, {fb_db.get()}),
                IsOkAndHolds(DataItem()));
    EXPECT_THAT(
        db->ExplodeList(list, DataBagImpl::ListRange(0, 2), {fb_db.get()}),
        IsOkAndHolds(ElementsAre(1.0f)));
  }
  {
    EXPECT_THAT(fb_db->GetListSize(list, {db.get()}),
                IsOkAndHolds(DataItem(int64_t{2})));
    EXPECT_THAT(fb_db->GetFromList(list, 0, {db.get()}),
                IsOkAndHolds(DataItem(2.0f)));
    EXPECT_THAT(fb_db->GetFromList(list, 1, {db.get()}),
                IsOkAndHolds(DataItem(3.0f)));
    EXPECT_THAT(fb_db->GetFromList(list, 2, {db.get()}),
                IsOkAndHolds(DataItem()));
    EXPECT_THAT(
        fb_db->ExplodeList(list, DataBagImpl::ListRange(0, 2), {db.get()}),
        IsOkAndHolds(ElementsAre(2.0f, 3.0f)));
  }
}

TEST(DataBagTest, ListBatchWithFallback) {
  constexpr int64_t kSize = 7;
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto lists =
      DataSliceImpl::ObjectsFromAllocation(AllocateLists(kSize), kSize);
  auto fb_db = DataBagImpl::CreateEmptyDatabag();

  EXPECT_THAT(db->GetListSize(lists, {fb_db.get()}),
              IsOkAndHolds(ElementsAreArray(std::vector<int64_t>(kSize, 0))));
  EXPECT_THAT(
      db->GetFromLists(lists, arolla::CreateConstDenseArray<int64_t>(kSize, 0),
                       {fb_db.get()}),
      IsOkAndHolds(ElementsAreArray(std::vector<DataItem>(kSize, DataItem()))));
  {
    ASSERT_OK_AND_ASSIGN(
        (auto [values, edge]),
        db->ExplodeLists(lists, DataBagImpl::ListRange(0, kSize),
                         {fb_db.get()}));
    EXPECT_THAT(values, ElementsAre());
    EXPECT_THAT(edge.edge_values(),
                ElementsAreArray(std::vector<int64_t>(kSize + 1, 0)));
  }

  auto alloc_0 = Allocate(kSize);
  ASSERT_OK(db->AppendToList(
      lists, DataSliceImpl::ObjectsFromAllocation(alloc_0, kSize)));
  {
    EXPECT_THAT(db->GetListSize(lists, {fb_db.get()}),
                IsOkAndHolds(ElementsAreArray(std::vector<int64_t>(kSize, 1))));
    EXPECT_THAT(db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 0),
                    {fb_db.get()}),
                IsOkAndHolds(ElementsAreArray(
                    DataSliceImpl::ObjectsFromAllocation(alloc_0, kSize))));
    EXPECT_THAT(db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 1),
                    {fb_db.get()}),
                IsOkAndHolds(ElementsAreArray(
                    std::vector<DataItem>(kSize, DataItem()))));
    ASSERT_OK_AND_ASSIGN(
        (auto [values, edge]),
        db->ExplodeLists(lists, DataBagImpl::ListRange(0, kSize),
                         {fb_db.get()}));
    EXPECT_THAT(values, ElementsAreArray(DataSliceImpl::ObjectsFromAllocation(
                            alloc_0, kSize)));
    std::vector<int64_t> expected_split_points(kSize + 1, 0);
    std::iota(expected_split_points.begin(), expected_split_points.end(), 0);
    EXPECT_THAT(edge.edge_values(), ElementsAreArray(expected_split_points));
  }
  {
    EXPECT_THAT(fb_db->GetListSize(lists, {db.get()}),
                IsOkAndHolds(ElementsAreArray(std::vector<int64_t>(kSize, 1))));
    EXPECT_THAT(fb_db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 0),
                    {db.get()}),
                IsOkAndHolds(ElementsAreArray(
                    DataSliceImpl::ObjectsFromAllocation(alloc_0, kSize))));
    EXPECT_THAT(fb_db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 1),
                    {db.get()}),
                IsOkAndHolds(ElementsAreArray(
                    std::vector<DataItem>(kSize, DataItem()))));
    ASSERT_OK_AND_ASSIGN(
        (auto [values, edge]),
        fb_db->ExplodeLists(lists, DataBagImpl::ListRange(0, kSize),
                            {db.get()}));
    EXPECT_THAT(values, ElementsAreArray(DataSliceImpl::ObjectsFromAllocation(
                            alloc_0, kSize)));
    std::vector<int64_t> expected_split_points(kSize + 1, 0);
    std::iota(expected_split_points.begin(), expected_split_points.end(), 0);
    EXPECT_THAT(edge.edge_values(), ElementsAreArray(expected_split_points));
  }

  ASSERT_OK(fb_db->AppendToList(lists[0], DataItem(2.0f)));
  ASSERT_OK(fb_db->AppendToList(lists[0], DataItem(3.0f)));
  ASSERT_OK(db->AppendToList(lists[6], DataItem(57)));
  {  // fallback is not used in case list is not empty
    auto expected_sizes = std::vector<int64_t>(kSize, 1);
    expected_sizes[6] = 2;
    EXPECT_THAT(db->GetListSize(lists, {fb_db.get()}),
                IsOkAndHolds(ElementsAreArray(expected_sizes)));
    EXPECT_THAT(db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 0),
                    {fb_db.get()}),
                IsOkAndHolds(ElementsAreArray(
                    DataSliceImpl::ObjectsFromAllocation(alloc_0, kSize))));
    auto expected_items = std::vector<DataItem>(kSize);
    expected_items[6] = DataItem(57);
    EXPECT_THAT(db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 1),
                    {fb_db.get()}),
                IsOkAndHolds(ElementsAreArray(expected_items)));
    ASSERT_OK_AND_ASSIGN(
        (auto [values, edge]),
        db->ExplodeLists(lists, DataBagImpl::ListRange(0, kSize),
                         {fb_db.get()}));
    auto alloc_0_items = DataSliceImpl::ObjectsFromAllocation(alloc_0, kSize);
    expected_items =
        std::vector<DataItem>(alloc_0_items.begin(), alloc_0_items.end());
    expected_items.push_back(DataItem(57));
    EXPECT_THAT(values, ElementsAreArray(expected_items));
    std::vector<int64_t> expected_split_points(kSize + 1, 0);
    std::iota(expected_split_points.begin(), expected_split_points.end(), 0);
    expected_split_points.back() += 1;
    EXPECT_THAT(edge.edge_values(), ElementsAreArray(expected_split_points));
  }
  {
    auto expected_sizes = std::vector<int64_t>(kSize, 1);
    expected_sizes[0] = 2;
    expected_sizes[6] = 2;
    EXPECT_THAT(fb_db->GetListSize(lists, {db.get()}),
                IsOkAndHolds(ElementsAreArray(expected_sizes)));
    auto alloc_0_items = DataSliceImpl::ObjectsFromAllocation(alloc_0, kSize);
    auto expected_items =
        std::vector<DataItem>(alloc_0_items.begin(), alloc_0_items.end());
    expected_items[0] = DataItem(2.0f);
    EXPECT_THAT(fb_db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 0),
                    {db.get()}),
                IsOkAndHolds(ElementsAreArray(expected_items)));
    expected_items = std::vector<DataItem>(kSize);
    expected_items[0] = DataItem(3.0f);
    expected_items[6] = DataItem(57);
    EXPECT_THAT(fb_db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 1),
                    {db.get()}),
                IsOkAndHolds(ElementsAreArray(expected_items)));
    ASSERT_OK_AND_ASSIGN(
        (auto [values, edge]),
        fb_db->ExplodeLists(lists, DataBagImpl::ListRange(0, kSize),
                            {db.get()}));
    expected_items =
        std::vector<DataItem>(alloc_0_items.begin(), alloc_0_items.end());
    expected_items[0] = DataItem(2.0f);
    expected_items.insert(expected_items.begin() + 1, DataItem(3.0f));
    expected_items.push_back(DataItem(57));
    EXPECT_THAT(values, ElementsAreArray(expected_items));
    // The first and the last list have size 2.
    std::vector<int64_t> expected_split_points(kSize + 1, 0);
    expected_split_points[1] = 2;
    std::iota(expected_split_points.begin() + 2, expected_split_points.end(),
              3);
    expected_split_points.back() += 1;
    EXPECT_THAT(edge.edge_values(), ElementsAreArray(expected_split_points));
  }
}

TEST(DataBagTest, Dicts) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc_id = AllocateDicts(2);
  ObjectId dict_from_other_alloc = AllocateSingleDict();
  DataSliceImpl all_dicts =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
          {alloc_id.ObjectByOffset(1), alloc_id.ObjectByOffset(0),
           dict_from_other_alloc, std::nullopt}));

  EXPECT_THAT(
      db->GetDictKeys(DataItem(dict_from_other_alloc)),
      IsOkAndHolds(Pair(ElementsAre(), Property(&DenseArrayEdge::edge_values,
                                                ElementsAre(0, 0)))));
  EXPECT_THAT(db->GetDictSize(DataItem(dict_from_other_alloc)),
              IsOkAndHolds(DataItem(int64_t{0})));

  ASSERT_OK(db->SetInDict(
      all_dicts,
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({1, 2, 3, 4})),
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({5, 6, 7, 8}))));
  EXPECT_THAT(db->SetInDict(
                  all_dicts,
                  DataSliceImpl::Create(arolla::CreateDenseArray<int>({1, 2})),
                  DataSliceImpl::Create(arolla::CreateDenseArray<int>({5, 6}))),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "dicts and keys sizes don't match: 4 vs 2"));

  auto db2 = db->PartiallyPersistentFork();

  ASSERT_OK(db2->SetInDict(
      DataSliceImpl::Create(arolla::CreateConstDenseArray<ObjectId>(
          3, alloc_id.ObjectByOffset(0))),
      DataSliceImpl::Create(arolla::CreateDenseArray<arolla::Bytes>(
          {arolla::Bytes("1.0"), arolla::Bytes("2.0"), arolla::Bytes("3.0")})),
      DataSliceImpl::Create(
          arolla::CreateDenseArray<double>({8.0, 9.0, 10.0}))));
  EXPECT_THAT(
      db2->SetInDict(
          DataSliceImpl::Create(arolla::CreateConstDenseArray<ObjectId>(
              2, alloc_id.ObjectByOffset(1))),
          DataSliceImpl::Create(arolla::CreateDenseArray<float>({1.0f, 2.0f})),
          DataSliceImpl::Create(
              arolla::CreateDenseArray<int>({1, std::nullopt}),
              arolla::CreateDenseArray<float>({std::nullopt, 2.0f}))),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "invalid key type: FLOAT32"));

  {
    ASSERT_OK_AND_ASSIGN((auto [keys, edge]), db2->GetDictKeys(all_dicts));
    EXPECT_THAT(std::vector(keys.begin(), keys.begin() + 1),
                UnorderedElementsAre(1));
    EXPECT_THAT(
        std::vector(keys.begin() + 1, keys.begin() + 5),
        UnorderedElementsAre(2, arolla::Bytes("1.0"), arolla::Bytes("2.0"),
                             arolla::Bytes("3.0")));
    EXPECT_THAT(std::vector(keys.begin() + 5, keys.end()),
                UnorderedElementsAre(3));
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 5, 6, 6));
    EXPECT_THAT(db2->GetDictSize(all_dicts),
                IsOkAndHolds(ElementsAre(1, 4, 1, std::nullopt)));
  }

  EXPECT_THAT(
      db2->GetFromDict(
          all_dicts,
          DataSliceImpl::Create(
              arolla::CreateDenseArray<int>({1, std::nullopt, std::nullopt, 4}),
              arolla::CreateDenseArray<arolla::Bytes>(
                  {std::nullopt, arolla::Bytes("2.0"), arolla::Bytes("3.0"),
                   std::nullopt}))),
      IsOkAndHolds(ElementsAre(5, 9.0, DataItem(), DataItem())));
  EXPECT_THAT(
      db2->GetFromDict(all_dicts,
                       DataSliceImpl::Create(
                           arolla::CreateDenseArray<int>({1, std::nullopt}))),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "dicts and keys sizes don't match: 4 vs 2"));

  ASSERT_OK(db2->ClearDict(all_dicts[2]));
  ASSERT_OK(db2->ClearDict(DataSliceImpl::ObjectsFromAllocation(alloc_id, 1)));
  ASSERT_OK(db2->SetInDict(DataItem(alloc_id.ObjectByOffset(1)), DataItem(1),
                           DataItem(true)));

  EXPECT_THAT(db2->GetFromDict(all_dicts[0], DataItem(1)),
              IsOkAndHolds(DataItem(true)));
  EXPECT_THAT(db2->GetFromDict(all_dicts[0], DataItem(1.0f)),
              IsOkAndHolds(DataItem()));
  EXPECT_THAT(db2->GetFromDict(all_dicts[0], DataItem(2.0f)),
              IsOkAndHolds(DataItem()));
  EXPECT_THAT(db2->GetFromDict(all_dicts[1], DataItem(2)),
              IsOkAndHolds(DataItem()));
  EXPECT_THAT(db2->GetFromDict(all_dicts[2], DataItem(3)),
              IsOkAndHolds(DataItem()));
  EXPECT_THAT(db2->GetFromDict(all_dicts[3], DataItem(4)),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "dict expected, got None"));

  ASSERT_OK(db2->SetInDict(
      all_dicts,
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({1, 1, 1, 1})),
      DataSliceImpl::CreateEmptyAndUnknownType(4)));
  EXPECT_THAT(db2->GetFromDict(all_dicts[0], DataItem(1)),
              IsOkAndHolds(DataItem()));

  EXPECT_THAT(db->GetFromDict(all_dicts[0], DataItem(1)),
              IsOkAndHolds(DataItem(5)));
  EXPECT_THAT(db->GetFromDict(all_dicts[1], DataItem(2)),
              IsOkAndHolds(DataItem(6)));
  EXPECT_THAT(db->GetFromDict(all_dicts[2], DataItem(3)),
              IsOkAndHolds(DataItem(7)));
  EXPECT_THAT(db->GetFromDict(all_dicts[3], DataItem(4)),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "dict expected, got None"));
}

TEST(DataBagTest, EmptyAndUnknownDicts) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto empty = DataSliceImpl::CreateEmptyAndUnknownType(3);
  ASSERT_OK_AND_ASSIGN((auto [keys, edge]), db->GetDictKeys(empty));
  EXPECT_EQ(keys.size(), 0);
  EXPECT_EQ(edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 0, 0, 0));
  EXPECT_OK(db->ClearDict(empty));
  EXPECT_OK(db->SetInDict(empty, empty, empty));
  EXPECT_THAT(
      db->GetDictSize(empty),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
  EXPECT_THAT(db->GetFromDict(empty, empty),
              IsOkAndHolds(ElementsAre(DataItem(), DataItem(), DataItem())));
}

TEST(DataBagTest, DictFallbacks) {
  AllocationId alloc_id = AllocateDicts(2);
  ObjectId dict_from_other_alloc = AllocateSingleDict();
  DataSliceImpl all_dicts =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
          {alloc_id.ObjectByOffset(1), alloc_id.ObjectByOffset(0),
           dict_from_other_alloc}));

  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetInDict(all_dicts[1], DataItem(2), DataItem(4.1)));
  auto fb_db = DataBagImpl::CreateEmptyDatabag();
  auto all_dict_keys =
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({1, 2, 3}));
  {
    ASSERT_OK_AND_ASSIGN((auto [keys, edge]),
                         db->GetDictKeys(all_dicts, {fb_db.get()}));
    EXPECT_THAT(keys, ElementsAre(DataItem(2)));
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 0, 1, 1));
    EXPECT_THAT(db->GetDictSize(all_dicts, {fb_db.get()}),
                IsOkAndHolds(ElementsAre(0, 1, 0)));
  }
  ASSERT_OK(fb_db->SetInDict(
      all_dicts, all_dict_keys,
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({4, 5, 6}))));

  auto expected_items =
      std::vector<DataItem>({DataItem(4), DataItem(4.1), DataItem(6)});
  EXPECT_THAT(db->GetFromDict(all_dicts[0], DataItem(1), {fb_db.get()}),
              IsOkAndHolds(expected_items[0]));
  EXPECT_THAT(db->GetFromDict(all_dicts[1], DataItem(2), {fb_db.get()}),
              IsOkAndHolds(expected_items[1]));
  EXPECT_THAT(db->GetFromDict(all_dicts[2], DataItem(3), {fb_db.get()}),
              IsOkAndHolds(expected_items[2]));

  EXPECT_THAT(db->GetFromDict(all_dicts, all_dict_keys, {fb_db.get()}),
              IsOkAndHolds(ElementsAreArray(expected_items)));

  EXPECT_THAT(db->GetDictKeys(all_dicts[0], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(1)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 1)))));
  EXPECT_THAT(db->GetDictKeys(all_dicts[1], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(2)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 1)))));
  EXPECT_THAT(db->GetDictKeys(all_dicts[2], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(3)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 1)))));
  EXPECT_THAT(db->GetDictSize(all_dicts, {fb_db.get()}),
              IsOkAndHolds(ElementsAre(1, 1, 1)));
  {
    ASSERT_OK_AND_ASSIGN((auto [keys, edge]),
                         db->GetDictKeys(all_dicts, {fb_db.get()}));
    EXPECT_THAT(keys, ElementsAre(DataItem(1), DataItem(2), DataItem(3)));
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 2, 3));
  }

  ASSERT_OK(
      fb_db->SetInDict(all_dicts[1], DataItem(arolla::Text("a")), DataItem(7)));
  EXPECT_THAT(
      db->GetDictKeys(all_dicts[1], {fb_db.get()}),
      IsOkAndHolds(
          Pair(UnorderedElementsAre(DataItem(2), DataItem(arolla::Text("a"))),
               Property(&DenseArrayEdge::edge_values, ElementsAre(0, 2)))));
  {
    ASSERT_OK_AND_ASSIGN((auto [keys, edge]),
                         db->GetDictKeys(all_dicts, {fb_db.get()}));
    ASSERT_EQ(keys.size(), 4);
    EXPECT_THAT(keys[0], Eq(DataItem(1)));
    EXPECT_THAT((std::vector{keys[1], keys[2]}),
                UnorderedElementsAre(DataItem(2), DataItem(arolla::Text("a"))));
    EXPECT_THAT(keys[3], Eq(DataItem(3)));
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 3, 4));
  }

  ASSERT_OK(db->SetInDict(all_dicts[2], DataItem(7), DataItem(-1)));
  EXPECT_THAT(db->SetInDict(all_dicts[2], DataItem(), DataItem(1)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "invalid key type: NOTHING"));
  EXPECT_THAT(db->GetDictKeys(all_dicts[2], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(3), DataItem(7)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 2)))));
  {
    ASSERT_OK_AND_ASSIGN((auto [keys, edge]),
                         db->GetDictKeys(all_dicts, {fb_db.get()}));
    ASSERT_EQ(keys.size(), 5);
    EXPECT_THAT(keys[0], Eq(DataItem(1)));
    EXPECT_THAT((std::vector{keys[1], keys[2]}),
                UnorderedElementsAre(DataItem(2), DataItem(arolla::Text("a"))));
    EXPECT_THAT((std::vector{keys[3], keys[4]}),
                UnorderedElementsAre(DataItem(3), DataItem(7)));
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 3, 5));
    EXPECT_THAT(db->GetDictSize(all_dicts, {fb_db.get()}),
                IsOkAndHolds(ElementsAre(1, 2, 2)));
    EXPECT_THAT(db->GetDictSize(all_dicts[0], {fb_db.get()}),
              IsOkAndHolds(DataItem(int64_t{1})));
    EXPECT_THAT(db->GetDictSize(all_dicts[1], {fb_db.get()}),
              IsOkAndHolds(DataItem(int64_t{2})));
    EXPECT_THAT(db->GetDictSize(all_dicts[2], {fb_db.get()}),
              IsOkAndHolds(DataItem(int64_t{2})));
  }
}

TEST(DataBagTest, EmptySchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema_item = DataItem(AllocateExplicitSchema());

  auto schemas_alloc = GenerateImplicitSchemas(3);
  auto schema_slice = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
    schemas_alloc.ObjectByOffset(0), schemas_alloc.ObjectByOffset(1),
    schemas_alloc.ObjectByOffset(2)
  }));


  ASSERT_OK_AND_ASSIGN(auto schema_attrs, db->GetSchemaAttrs(schema_item));
  EXPECT_THAT(schema_attrs, ElementsAre());
  EXPECT_EQ(schema_attrs.dtype(), arolla::GetQType<arolla::Text>());

  EXPECT_THAT(db->GetSchemaAttr(schema_item, "a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(db->GetSchemaAttrAllowMissing(schema_item, "a"),
              IsOkAndHolds(DataItem()));
  EXPECT_THAT(db->GetSchemaAttr(schema_slice, "a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(
      db->GetSchemaAttrAllowMissing(schema_slice, "a"),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
  EXPECT_THAT(
      db->GetSchemaAttrAllowMissing(DataSliceImpl::CreateEmptyAndUnknownType(3),
                                    "a"),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));

  auto fb_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(schema_attrs,
                       db->GetSchemaAttrs(schema_item, {fb_db.get()}));
  EXPECT_THAT(schema_attrs, ElementsAre());
  EXPECT_EQ(schema_attrs.dtype(), arolla::GetQType<arolla::Text>());
  EXPECT_THAT(db->GetSchemaAttr(schema_item, "a", {fb_db.get()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(db->GetSchemaAttrAllowMissing(schema_item, "a", {fb_db.get()}),
              IsOkAndHolds(DataItem()));

  EXPECT_THAT(db->GetSchemaAttr(schema_slice, "a", {fb_db.get()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(
      db->GetSchemaAttrAllowMissing(schema_slice, "a", {fb_db.get()}),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
}

TEST(DataBagTest, GetSetSchemaAttr_Item) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = DataItem(AllocateExplicitSchema());

  ASSERT_OK(db->SetSchemaAttr(schema, "a", GetIntSchema()));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"), IsOkAndHolds(GetIntSchema()));
  EXPECT_THAT(db->GetSchemaAttrs(schema),
              IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"))));

  ASSERT_OK(db->SetSchemaAttr(schema, "b", GetFloatSchema()));
  EXPECT_THAT(db->GetSchemaAttr(schema, "b"), IsOkAndHolds(GetFloatSchema()));
  EXPECT_THAT(
      db->GetSchemaAttrs(schema),
      IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"), arolla::Text("b"))));

  auto schema_2 = DataItem(AllocateExplicitSchema());
  ASSERT_OK(db->SetSchemaAttr(schema, "c", schema_2));
  EXPECT_THAT(db->GetSchemaAttr(schema, "c"), IsOkAndHolds(schema_2));
  EXPECT_THAT(db->GetSchemaAttrs(schema),
              IsOkAndHolds(UnorderedElementsAre(
                  arolla::Text("a"), arolla::Text("b"), arolla::Text("c"))));

  auto fb_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(fb_db->SetSchemaAttr(schema, "d", GetIntSchema()));
  EXPECT_THAT(db->GetSchemaAttr(schema, "c", {fb_db.get()}),
              IsOkAndHolds(schema_2));
  EXPECT_THAT(db->GetSchemaAttr(schema, "d", {fb_db.get()}),
              IsOkAndHolds(GetIntSchema()));
  EXPECT_THAT(
      db->GetSchemaAttrs(schema, {fb_db.get()}),
      IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"), arolla::Text("b"),
                                        arolla::Text("c"), arolla::Text("d"))));
}

TEST(DataBagTest, GetSetSchemaAttr_Slice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schemas_alloc = GenerateImplicitSchemas(6);
  // First item is None.
  auto schema = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
    std::nullopt, schemas_alloc.ObjectByOffset(1),
    schemas_alloc.ObjectByOffset(2)
  }));
  auto values_a = DataSliceImpl::Create(
      arolla::CreateDenseArray<schema::DType>({
        std::nullopt, schema::kInt32, std::nullopt}),
      arolla::CreateDenseArray<ObjectId>({
        std::nullopt, std::nullopt, AllocateExplicitSchema()}));

  ASSERT_OK(db->SetSchemaAttr(DataSliceImpl::CreateEmptyAndUnknownType(2), "a",
                              values_a));
  ASSERT_OK(db->SetSchemaAttr(schema, "a", values_a));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"),
              IsOkAndHolds(
                  ElementsAre(std::nullopt, GetIntSchema(), values_a[2])));
  EXPECT_THAT(db->GetSchemaAttrs(schema[1]),
              IsOkAndHolds(ElementsAre(arolla::Text("a"))));
  EXPECT_THAT(db->GetSchemaAttrs(schema[2]),
              IsOkAndHolds(ElementsAre(arolla::Text("a"))));

  // Setting a single item on a schema slice.
  ASSERT_OK(db->SetSchemaAttr(schema, "b", GetFloatSchema()));
  EXPECT_THAT(db->GetSchemaAttr(schema, "b"),
              IsOkAndHolds(ElementsAre(std::nullopt, GetFloatSchema(),
                                       GetFloatSchema())));
  EXPECT_THAT(db->GetSchemaAttrs(schema[1]),
              IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"),
                                                arolla::Text("b"))));
  EXPECT_THAT(db->GetSchemaAttrs(schema[2]),
              IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"),
                                                arolla::Text("b"))));

  auto schema_2 = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
    schemas_alloc.ObjectByOffset(3), schemas_alloc.ObjectByOffset(4),
    schemas_alloc.ObjectByOffset(5)
  }));
  ASSERT_OK(db->SetSchemaAttr(schema, "c", schema_2));
  EXPECT_THAT(
      db->GetSchemaAttr(schema, "c"),
      IsOkAndHolds(ElementsAre(std::nullopt, schema_2[1], schema_2[2])));
  EXPECT_THAT(db->GetSchemaAttrs(schema[1]),
              IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"),
                                                arolla::Text("b"),
                                                arolla::Text("c"))));
  EXPECT_THAT(db->GetSchemaAttrs(schema[2]),
              IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"),
                                                arolla::Text("b"),
                                                arolla::Text("c"))));

  auto fb_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(fb_db->SetSchemaAttr(schema, "d",
                                 DataSliceImpl::Create(3, GetIntSchema())));
  EXPECT_THAT(
      db->GetSchemaAttr(schema, "c", {fb_db.get()}),
      IsOkAndHolds(ElementsAre(std::nullopt, schema_2[1], schema_2[2])));
  EXPECT_THAT(db->GetSchemaAttr(schema, "d", {fb_db.get()}),
              IsOkAndHolds(
                  ElementsAre(std::nullopt, GetIntSchema(), GetIntSchema())));
  EXPECT_THAT(
      db->GetSchemaAttrs(schema[1], {fb_db.get()}),
      IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"), arolla::Text("b"),
                                        arolla::Text("c"), arolla::Text("d"))));
  EXPECT_THAT(
      db->GetSchemaAttrs(schema[1], {}),
      IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"),
                                        arolla::Text("b"),
                                        arolla::Text("c"))));
}

TEST(DataBagTest, SetSchemaAttrOverwrite_Item) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = DataItem(AllocateExplicitSchema());

  ASSERT_OK(db->SetSchemaAttr(schema, "a", GetIntSchema()));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"), IsOkAndHolds(GetIntSchema()));

  ASSERT_OK(db->SetSchemaAttr(schema, "a", GetAnySchema()));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"), IsOkAndHolds(GetAnySchema()));

  ASSERT_OK_AND_ASSIGN(auto ds_impl, db->GetSchemaAttrs(schema));
  EXPECT_THAT(ds_impl, UnorderedElementsAre(arolla::Text("a")));
}

TEST(DataBagTest, SetSchemaAttrOverwrite_Slice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schemas_alloc = GenerateImplicitSchemas(3);
  auto schema = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
    schemas_alloc.ObjectByOffset(0), schemas_alloc.ObjectByOffset(1),
    schemas_alloc.ObjectByOffset(2)
  }));
  auto values = DataSliceImpl::Create(
      arolla::CreateDenseArray<schema::DType>({
        std::nullopt, schema::kInt32, schema::kFloat32}),
      arolla::CreateDenseArray<ObjectId>({
        AllocateExplicitSchema(), std::nullopt, std::nullopt}));

  ASSERT_OK(db->SetSchemaAttr(schema, "a", values));
  ASSERT_OK_AND_ASSIGN(auto schema_get_a, db->GetSchemaAttr(schema, "a"));
  EXPECT_THAT(schema_get_a, IsEquivalentTo(values));

  ASSERT_OK(
      db->SetSchemaAttr(schema, "a", DataSliceImpl::Create(3, GetAnySchema())));
  ASSERT_OK_AND_ASSIGN(schema_get_a, db->GetSchemaAttr(schema, "a"));
  EXPECT_THAT(schema_get_a,
              ElementsAre(GetAnySchema(), GetAnySchema(), GetAnySchema()));

  for (int i = 0; i < schema.size(); ++i) {
    EXPECT_THAT(db->GetSchemaAttrs(schema[i]),
                IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"))));
  }
}

TEST(DataBagTest, SetSchemaAttrErrors_Item) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = DataItem(AllocateExplicitSchema());
  auto dict = DataItem(AllocateSingleDict());

  // Disallow updating schema through a non-schema API:
  EXPECT_THAT(db->SetSchemaAttr(dict, "a", GetIntSchema()),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("schema expected")));

  EXPECT_THAT(db->SetSchemaAttr(schema, "a", DataItem()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("only schemas can be assigned as attributes of"
                                 " schemas, got: None")));

  EXPECT_THAT(db->SetSchemaAttr(schema, "a", DataItem(42)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("only schemas can be assigned as attributes of"
                                 " schemas, got: 42")));

  EXPECT_THAT(db->SetSchemaAttr(schema, "a", DataItem(AllocateSingleDict())),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("only schemas can be assigned as attributes of"
                                 " schemas, got: ")));

  EXPECT_THAT(db->GetSchemaAttr(GetAnySchema(), "any_attr"),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("Cannot get or set attributes on schema "
                                 "constants: ANY")));
  EXPECT_THAT(db->SetSchemaAttr(GetIntSchema(), "any_attr", GetIntSchema()),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("Cannot get or set attributes on schema "
                                 "constants: INT32")));
}

TEST(DataBagTest, SetSchemaAttrErrors_Slice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schemas_alloc = GenerateImplicitSchemas(3);
  auto schema = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
    schemas_alloc.ObjectByOffset(0), schemas_alloc.ObjectByOffset(1),
    schemas_alloc.ObjectByOffset(2)
  }));
  auto schema_with_const_schemas = DataSliceImpl::Create(
      arolla::CreateDenseArray<schema::DType>({
        std::nullopt, std::nullopt, schema::kInt32}),
      arolla::CreateDenseArray<ObjectId>({
        schemas_alloc.ObjectByOffset(0), std::nullopt, std::nullopt}));
  auto dict = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
    AllocateSingleDict(), AllocateSingleDict(), AllocateSingleDict()}));

  auto valid_values = DataSliceImpl::Create(3, GetIntSchema());

  // Disallow updating schema through a non-schema API:
  EXPECT_THAT(
      db->SetSchemaAttr(dict, "a", valid_values),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("cannot set schema attribute of a non-schema slice [")));

  EXPECT_THAT(
      db->SetSchemaAttr(dict, "any_attr", GetIntSchema()),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("cannot set schema attribute of a non-schema slice [")));

  EXPECT_THAT(
      db->SetSchemaAttr(schema, "a", DataSliceImpl::Create(3, DataItem(42))),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "cannot set a non-schema slice [42, 42, 42] as a schema attribute"));

  EXPECT_THAT(db->SetSchemaAttr(schema, "a", DataItem(42)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("only schemas can be assigned as attributes "
                                 "of schemas, got: 42")));

  EXPECT_THAT(
      db->SetSchemaAttr(schema, "a", DataSliceImpl::AllocateEmptyObjects(3)),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          MatchesRegex(
              R"(cannot set a non-schema slice \[.*, .*, .*\] as a schema attribute)")));

  // Mixed with some non schemas.
  auto mixed_with_non_schemas = DataSliceImpl::Create(
      arolla::CreateDenseArray<schema::DType>({
        std::nullopt, std::nullopt, schema::kInt32}),
      arolla::CreateDenseArray<int>({42, std::nullopt, std::nullopt}));
  EXPECT_THAT(db->SetSchemaAttr(schema, "a", mixed_with_non_schemas),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot set a non-schema slice [42, None, INT32] as a "
                       "schema attribute"));

  // Basically a no-op.
  ASSERT_OK(
      db->SetSchemaAttr(schema, "a", DataSliceImpl::Create(3, DataItem())));

  EXPECT_THAT(
      db->GetSchemaAttr(schema_with_const_schemas, "any_attr"),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("cannot get schema attribute of a non-schema slice [")));

  EXPECT_THAT(
      db->SetSchemaAttr(schema_with_const_schemas, "any_attr", GetIntSchema()),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("cannot set schema attribute of a non-schema slice [")));

  // Result has less items than schema.
  auto values = DataSliceImpl::Create(arolla::CreateDenseArray<schema::DType>({
    schema::kInt32, std::nullopt, std::nullopt}));
  ASSERT_OK(db->SetSchemaAttr(schema, "a", values));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(
      db->GetSchemaAttrAllowMissing(schema, "a"),
      IsOkAndHolds(ElementsAre(schema::kInt32, std::nullopt, std::nullopt)));
}

TEST(DataBagTest, DelSchemaAttr_Item) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = DataItem(AllocateExplicitSchema());

  ASSERT_OK(db->SetSchemaAttr(schema, "a", GetIntSchema()));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"), IsOkAndHolds(GetIntSchema()));

  EXPECT_THAT(db->DelSchemaAttr(schema, "b"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'b' is missing")));

  ASSERT_OK(db->DelSchemaAttr(schema, "a"));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(db->GetSchemaAttrs(schema), IsOkAndHolds(ElementsAre()));
}

TEST(DataBagTest, DelSchemaAttr_Slice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schemas_alloc = GenerateImplicitSchemas(3);
  auto schema = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
    schemas_alloc.ObjectByOffset(0), schemas_alloc.ObjectByOffset(1),
    schemas_alloc.ObjectByOffset(2)
  }));
  auto values = DataSliceImpl::Create(
      arolla::CreateDenseArray<schema::DType>({
        std::nullopt, schema::kInt32, schema::kFloat32}),
      arolla::CreateDenseArray<ObjectId>({
        AllocateExplicitSchema(), std::nullopt, std::nullopt}));

  ASSERT_OK(db->SetSchemaAttr(schema, "a", values));
  ASSERT_OK_AND_ASSIGN(auto ds_get_a, db->GetSchemaAttr(schema, "a"));
  EXPECT_THAT(ds_get_a, IsEquivalentTo(values));

  EXPECT_THAT(db->DelSchemaAttr(schema, "b"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'b' is missing")));

  ASSERT_OK(db->DelSchemaAttr(schema, "a"));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  for (int i = 0; i < schema.size(); ++i) {
    EXPECT_THAT(db->GetSchemaAttrs(schema[i]), IsOkAndHolds(ElementsAre()));
  }
}

TEST(DataBagTest, CreateExplicitSchemaFromFields) {
  auto db = internal::DataBagImpl::CreateEmptyDatabag();

  std::vector<absl::string_view> attrs{"a", "b", "c"};
  auto int_s = GetIntSchema();
  auto float_s = GetFloatSchema();
  auto schema_s = DataItem(AllocateExplicitSchema());
  std::vector<std::reference_wrapper<const DataItem>> items{
    std::cref(int_s), std::cref(float_s), std::cref(schema_s)
  };

  ASSERT_OK_AND_ASSIGN(auto schema,
                       db->CreateExplicitSchemaFromFields(attrs, items));
  // Explicit Schema item is returned.
  EXPECT_TRUE(schema.value<internal::ObjectId>().IsSchema());
  EXPECT_TRUE(schema.value<internal::ObjectId>().IsExplicitSchema());

  ASSERT_OK_AND_ASSIGN(auto ds_impl, db->GetSchemaAttrs(schema));
  EXPECT_THAT(ds_impl, UnorderedElementsAre(arolla::Text("a"),
                                            arolla::Text("b"),
                                            arolla::Text("c")));

  EXPECT_THAT(db->GetSchemaAttr(schema, "a"), IsOkAndHolds(int_s));
  EXPECT_THAT(db->GetSchemaAttr(schema, "b"), IsOkAndHolds(float_s));
  EXPECT_THAT(db->GetSchemaAttr(schema, "c"), IsOkAndHolds(schema_s));

  schema_s = DataItem(42);
  std::vector<std::reference_wrapper<const DataItem>> items_error{
    std::cref(int_s), std::cref(float_s), std::cref(schema_s)
  };
  EXPECT_THAT(db->CreateExplicitSchemaFromFields(attrs, items_error),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("only schemas can be assigned as attributes of"
                                 " schemas, got: 42")));
}

TEST(DataBagTest, CreateUuSchemaFromFields) {
  auto db = internal::DataBagImpl::CreateEmptyDatabag();

  std::vector<absl::string_view> attrs{"a", "b", "c"};
  auto int_s = GetIntSchema();
  auto float_s = GetFloatSchema();
  auto schema_s = DataItem(AllocateExplicitSchema());
  std::vector<std::reference_wrapper<const DataItem>> items{
    std::cref(int_s), std::cref(float_s), std::cref(schema_s)
  };

  ASSERT_OK_AND_ASSIGN(auto schema,
                       db->CreateUuSchemaFromFields("", attrs, items));
  // Explicit UuSchema item is returned.
  EXPECT_TRUE(schema.value<internal::ObjectId>().IsSchema());
  EXPECT_TRUE(schema.value<internal::ObjectId>().IsExplicitSchema());
  EXPECT_TRUE(schema.value<internal::ObjectId>().IsUuid());

  ASSERT_OK_AND_ASSIGN(auto ds_impl, db->GetSchemaAttrs(schema));
  EXPECT_THAT(ds_impl, UnorderedElementsAre(arolla::Text("a"),
                                            arolla::Text("b"),
                                            arolla::Text("c")));

  EXPECT_THAT(db->GetSchemaAttr(schema, "a"), IsOkAndHolds(int_s));
  EXPECT_THAT(db->GetSchemaAttr(schema, "b"), IsOkAndHolds(float_s));
  EXPECT_THAT(db->GetSchemaAttr(schema, "c"), IsOkAndHolds(schema_s));

  ASSERT_OK_AND_ASSIGN(auto seeded_schema,
                       db->CreateUuSchemaFromFields("seed", attrs, items));
  EXPECT_THAT(schema, Not(IsEquivalentTo(seeded_schema)));

  ASSERT_OK_AND_ASSIGN(
      auto shuffled_schema,
      db->CreateUuSchemaFromFields(
          "", {"a", "c", "b"},
          {std::cref(int_s), std::cref(schema_s), std::cref(float_s)}));
  EXPECT_THAT(schema, IsEquivalentTo(shuffled_schema));

  schema_s = DataItem(42);
  std::vector<std::reference_wrapper<const DataItem>> items_error{
    std::cref(int_s), std::cref(float_s), std::cref(schema_s)
  };
  EXPECT_THAT(db->CreateUuSchemaFromFields("", attrs, items_error),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("only schemas can be assigned as attributes of"
                                 " schemas, got: 42")));
}

TEST(DataBagTest, OverwriteSchemaFields) {
  auto db = internal::DataBagImpl::CreateEmptyDatabag();

  std::vector<absl::string_view> attrs{"a", "b", "c"};
  auto int_s = GetIntSchema();
  auto float_s = GetFloatSchema();
  auto schema_s = DataItem(AllocateExplicitSchema());
  std::vector<std::reference_wrapper<const DataItem>> items{
    std::cref(int_s), std::cref(float_s), std::cref(schema_s)
  };

  {
    // DataItem.
    ASSERT_OK_AND_ASSIGN(
        auto schema_item,
        CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
            DataItem(AllocateSingleObject()), schema::kImplicitSchemaSeed));
    ASSERT_OK(db->OverwriteSchemaFields<DataItem>(schema_item, attrs, items));

    ASSERT_OK_AND_ASSIGN(auto ds_impl, db->GetSchemaAttrs(schema_item));
    EXPECT_THAT(ds_impl, UnorderedElementsAre(arolla::Text("a"),
                                              arolla::Text("b"),
                                              arolla::Text("c")));

    EXPECT_THAT(db->GetSchemaAttr(schema_item, "a"), IsOkAndHolds(int_s));
    EXPECT_THAT(db->GetSchemaAttr(schema_item, "b"), IsOkAndHolds(float_s));
    EXPECT_THAT(db->GetSchemaAttr(schema_item, "c"), IsOkAndHolds(schema_s));
  }
  {
    // DataSliceImpl - Schema slice is returned.
    ASSERT_OK_AND_ASSIGN(
        auto schema_slice,
        CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
            DataSliceImpl::AllocateEmptyObjects(2),
            schema::kImplicitSchemaSeed));
    ASSERT_OK(
        db->OverwriteSchemaFields<DataSliceImpl>(schema_slice, attrs, items));

    for (int i = 0; i < schema_slice.size(); ++i) {
      ASSERT_OK_AND_ASSIGN(auto ds_impl, db->GetSchemaAttrs(schema_slice[i]));
      EXPECT_THAT(ds_impl, UnorderedElementsAre(arolla::Text("a"),
                                                arolla::Text("b"),
                                                arolla::Text("c")));
    }
    EXPECT_THAT(db->GetSchemaAttr(schema_slice, "a"),
                IsOkAndHolds(ElementsAre(int_s, int_s)));
    EXPECT_THAT(db->GetSchemaAttr(schema_slice, "b"),
                IsOkAndHolds(ElementsAre(float_s, float_s)));
    EXPECT_THAT(db->GetSchemaAttr(schema_slice, "c"),
                IsOkAndHolds(ElementsAre(schema_s, schema_s)));
  }
  {
    // DataSliceImpl - overwriting existing schemas
    ASSERT_OK_AND_ASSIGN(
        auto schema_slice,
        CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
            DataSliceImpl::AllocateEmptyObjects(2),
            schema::kImplicitSchemaSeed));
    ASSERT_OK(
        db->OverwriteSchemaFields<DataSliceImpl>(schema_slice, attrs, items));
    std::vector<absl::string_view> new_attrs{"a", "b", "d"};
    ASSERT_OK(
        db->OverwriteSchemaFields<DataSliceImpl>(
            schema_slice, new_attrs, items));

    for (int i = 0; i < schema_slice.size(); ++i) {
      ASSERT_OK_AND_ASSIGN(auto ds_impl, db->GetSchemaAttrs(schema_slice[i]));
      EXPECT_THAT(ds_impl, UnorderedElementsAre(arolla::Text("a"),
                                                arolla::Text("b"),
                                                arolla::Text("d")));
    }
    EXPECT_THAT(db->GetSchemaAttr(schema_slice, "a"),
                IsOkAndHolds(ElementsAre(int_s, int_s)));
    EXPECT_THAT(db->GetSchemaAttr(schema_slice, "b"),
                IsOkAndHolds(ElementsAre(float_s, float_s)));
    EXPECT_THAT(db->GetSchemaAttr(schema_slice, "d"),
                IsOkAndHolds(ElementsAre(schema_s, schema_s)));
  }
  {
    // Error.
    ASSERT_OK_AND_ASSIGN(
        auto schema_item,
        CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
            DataItem(AllocateSingleObject()), schema::kImplicitSchemaSeed));
    schema_s = DataItem(42);
    std::vector<std::reference_wrapper<const DataItem>> items_error{
      std::cref(int_s), std::cref(float_s), std::cref(schema_s)
    };
    EXPECT_THAT(
        db->OverwriteSchemaFields<DataItem>(schema_item, attrs, items_error),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("only schemas can be assigned as attributes of "
                           "schemas, got: 42")));

    EXPECT_THAT(
        db->OverwriteSchemaFields<DataSliceImpl>(
            DataSliceImpl::AllocateEmptyObjects(2), attrs,
            {std::cref(int_s), std::cref(float_s), std::cref(int_s)}),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                 HasSubstr("dicts expected")));

    ASSERT_OK_AND_ASSIGN(
        auto schema_slice,
        CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
            DataSliceImpl::AllocateEmptyObjects(2),
            schema::kImplicitSchemaSeed));
    EXPECT_THAT(
        db->OverwriteSchemaFields<DataSliceImpl>(schema_slice, attrs,
                                                 items_error),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("only schemas can be assigned as attributes of "
                           "schemas, got: 42")));
  }
}

TEST(DataBagTest, SetSchemaFields) {
  auto db = internal::DataBagImpl::CreateEmptyDatabag();

  std::vector<absl::string_view> attrs_1{"a", "b"};
  std::vector<absl::string_view> attrs_2{"b", "c"};
  auto int_s = GetIntSchema();
  auto float_s = GetFloatSchema();
  auto schema_s = DataItem(AllocateExplicitSchema());
  std::vector<std::reference_wrapper<const DataItem>> items_1{
    std::cref(int_s), std::cref(float_s)};
  std::vector<std::reference_wrapper<const DataItem>> items_2{
    std::cref(schema_s), std::cref(float_s)};

  {
    // DataItem.
    ASSERT_OK_AND_ASSIGN(
        auto schema_item,
        CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
            DataItem(AllocateSingleObject()), schema::kImplicitSchemaSeed));
    ASSERT_OK(db->SetSchemaFields<DataItem>(schema_item, attrs_1, items_1));
    ASSERT_OK_AND_ASSIGN(auto ds_impl, db->GetSchemaAttrs(schema_item));
    EXPECT_THAT(ds_impl, UnorderedElementsAre(arolla::Text("a"),
                                              arolla::Text("b")));

    ASSERT_OK(db->SetSchemaFields<DataItem>(schema_item, attrs_2, items_2));
    ASSERT_OK_AND_ASSIGN(ds_impl, db->GetSchemaAttrs(schema_item));
    EXPECT_THAT(ds_impl,
                UnorderedElementsAre(arolla::Text("a"), arolla::Text("b"),
                                     arolla::Text("c")));

    EXPECT_THAT(db->GetSchemaAttr(schema_item, "a"), IsOkAndHolds(int_s));
    EXPECT_THAT(db->GetSchemaAttr(schema_item, "b"), IsOkAndHolds(schema_s));
    EXPECT_THAT(db->GetSchemaAttr(schema_item, "c"), IsOkAndHolds(float_s));
  }
  {
    // DataSliceImpl - Schema slice is returned.
    ASSERT_OK_AND_ASSIGN(
        auto schema_slice,
        CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
            DataSliceImpl::AllocateEmptyObjects(2),
            schema::kImplicitSchemaSeed));

    ASSERT_OK(
        db->SetSchemaFields<DataSliceImpl>(
            schema_slice, attrs_1, items_1));
    for (int i = 0; i < schema_slice.size(); ++i) {
      ASSERT_OK_AND_ASSIGN(auto ds_impl, db->GetSchemaAttrs(schema_slice[i]));
      EXPECT_THAT(ds_impl, UnorderedElementsAre(arolla::Text("a"),
                                                arolla::Text("b")));
    }

    ASSERT_OK(
        db->SetSchemaFields<DataSliceImpl>(
            schema_slice, attrs_2, items_2));
    for (int i = 0; i < schema_slice.size(); ++i) {
      ASSERT_OK_AND_ASSIGN(auto ds_impl, db->GetSchemaAttrs(schema_slice[i]));
      EXPECT_THAT(ds_impl, UnorderedElementsAre(arolla::Text("a"),
                                                arolla::Text("b"),
                                                arolla::Text("c")));
    }

    EXPECT_THAT(db->GetSchemaAttr(schema_slice, "a"),
                IsOkAndHolds(ElementsAre(int_s, int_s)));
    EXPECT_THAT(db->GetSchemaAttr(schema_slice, "b"),
                IsOkAndHolds(ElementsAre(schema_s, schema_s)));
    EXPECT_THAT(db->GetSchemaAttr(schema_slice, "c"),
                IsOkAndHolds(ElementsAre(float_s, float_s)));
  }
  {
    // Error.
    ASSERT_OK_AND_ASSIGN(
        auto schema_item,
        CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
            DataItem(AllocateSingleObject()), schema::kImplicitSchemaSeed));
    schema_s = DataItem(42);

    EXPECT_THAT(
        db->SetSchemaFields<DataItem>(schema_item, attrs_1,
                                      {std::cref(int_s), std::cref(schema_s)}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("only schemas can be assigned as attributes of "
                           "schemas, got: 42")));

    EXPECT_THAT(
        db->SetSchemaFields<DataSliceImpl>(
            DataSliceImpl::AllocateEmptyObjects(2), attrs_1,
            {std::cref(int_s), std::cref(float_s)}),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                 HasSubstr("dicts expected")));

    ASSERT_OK_AND_ASSIGN(
        auto schema_slice,
        CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
            DataSliceImpl::AllocateEmptyObjects(2),
            schema::kImplicitSchemaSeed));

    EXPECT_THAT(
        db->SetSchemaFields<DataSliceImpl>(
            schema_slice, attrs_1, {std::cref(int_s), std::cref(schema_s)}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("only schemas can be assigned as attributes of "
                           "schemas, got: 42")));
  }
}

TEST(DataBagTest, ReverseMergeOptions) {
  EXPECT_EQ(
      (MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite,
                    .schema_conflict_policy = MergeOptions::kKeepOriginal}),
      ReverseMergeOptions(
          MergeOptions{.data_conflict_policy = MergeOptions::kKeepOriginal,
                       .schema_conflict_policy = MergeOptions::kOverwrite}));
  EXPECT_EQ(
      (MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite,
                    .schema_conflict_policy = MergeOptions::kRaiseOnConflict}),
      ReverseMergeOptions(MergeOptions{
          .data_conflict_policy = MergeOptions::kKeepOriginal,
          .schema_conflict_policy = MergeOptions::kRaiseOnConflict}));
  EXPECT_EQ(MergeOptions(), ReverseMergeOptions(MergeOptions()));
}

template <typename Allocator>
struct DataBagAllocatorTest : public ::testing::Test {
  ObjectId AllocSingle() { return Allocator().AllocSingle(); }
  ObjectId AllocSingleList() { return Allocator().AllocSingleList(); }
  ObjectId AllocSingleDict() { return Allocator().AllocSingleDict(); }
};

constexpr size_t kDataBagMergeParamCount = 3;
template <typename AllocatorWithId>
struct DataBagMergeTest
    : public DataBagAllocatorTest<std::tuple_element_t<0, AllocatorWithId>> {
  MergeOptions merge_options() const {
    constexpr int opt_id = std::tuple_element_t<1, AllocatorWithId>();
    static constexpr std::array<MergeOptions, kDataBagMergeParamCount>
        merge_options = {
            MergeOptions(),
            MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite},
            MergeOptions{.data_conflict_policy = MergeOptions::kKeepOriginal}};
    return merge_options[opt_id];
  }
};

struct SmallAlloc {
  ObjectId AllocSingle() const { return AllocateSingleObject(); }
  ObjectId AllocSingleList() const { return AllocateSingleList(); }
  ObjectId AllocSingleDict() const { return AllocateSingleDict(); }
};

struct BigAlloc {
  ObjectId AllocSingle() const { return Allocate(31).ObjectByOffset(17); }
  ObjectId AllocSingleList() const {
    return AllocateLists(31).ObjectByOffset(17);
  }
  ObjectId AllocSingleDict() const {
    return AllocateDicts(31).ObjectByOffset(17);
  }
};

template <int64_t kAllocSize>
struct SameAlloc {
  template <class AllocFn>
  ObjectId AllocNext() const {
    thread_local AllocationId alloc = AllocFn()(kAllocSize);
    thread_local int64_t offset = 0;
    if (offset == kAllocSize) {
      alloc = AllocFn()(kAllocSize);
      offset = 0;
    }
    return alloc.ObjectByOffset(offset++);
  }
  ObjectId AllocSingle() const {
    auto alloc = [](int64_t size) { return Allocate(size); };
    return AllocNext<decltype(alloc)>();
  }
  ObjectId AllocSingleList() const {
    auto alloc = [](int64_t size) { return AllocateLists(size); };
    return AllocNext<decltype(alloc)>();
  }
  ObjectId AllocSingleDict() const {
    auto alloc = [](int64_t size) { return AllocateDicts(size); };
    return AllocNext<decltype(alloc)>();
  }
};

using AllocTypes = ::testing::Types<SmallAlloc, BigAlloc, SameAlloc<127>,
                                    SameAlloc<16>, SameAlloc<4>, SameAlloc<2>>;
TYPED_TEST_SUITE(DataBagAllocatorTest, AllocTypes);

template <int kRepeatSize>
struct AllocsWithIndex {
  template <int kLeftSize, int kRepeatIndex, class Alloc, class... Allocs>
  static auto repeat_impl(::testing::Types<Alloc, Allocs...> types) {
    if constexpr (kLeftSize == 0) {
      return types;
    } else if constexpr (kRepeatIndex < kRepeatSize) {
      return repeat_impl<kLeftSize, kRepeatIndex + 1>(
          ::testing::Types<
              Alloc, Allocs...,
              std::pair<Alloc, std::integral_constant<int, kRepeatIndex>>>());
    } else {
      return repeat_impl<kLeftSize - 1, 0>(::testing::Types<Allocs...>());
    }
  }

  template <class... Allocs>
  static auto repeat(::testing::Types<Allocs...> types) {
    return repeat_impl<sizeof...(Allocs), 0>(types);
  }
  using Params = decltype(repeat(AllocTypes()));
};

TYPED_TEST_SUITE(DataBagMergeTest,
                 typename AllocsWithIndex<kDataBagMergeParamCount>::Params);

TYPED_TEST(DataBagAllocatorTest, MergeObjectsOnly) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  {
    ASSERT_OK(db->MergeInplace(*db));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->MergeInplace(*db2));
  }

  auto [a, b, c] = std::array{
      DataItem(this->AllocSingle()),
      DataItem(this->AllocSingle()),
      DataItem(this->AllocSingle()),
  };

  ASSERT_OK(db->SetAttr(a, "a", DataItem(57)));
  {
    ASSERT_OK(db->MergeInplace(*db));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(DataItem(57)));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->MergeInplace(*db));
    EXPECT_THAT(db2->GetAttr(a, "a"), IsOkAndHolds(DataItem(57)));
  }

  {
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(b, "a", DataItem(37)));
    ASSERT_OK(db2->MergeInplace(*db));
    EXPECT_THAT(db2->GetAttr(a, "a"), IsOkAndHolds(DataItem(57)));
    EXPECT_THAT(db2->GetAttr(b, "a"), IsOkAndHolds(DataItem(37)));
  }

  // merging into unmodified fork
  {
    auto db_fork = db->PartiallyPersistentFork();
    ASSERT_OK(db_fork->MergeInplace(*db));
    EXPECT_THAT(db_fork->GetAttr(a, "a"), IsOkAndHolds(DataItem(57)));
  }

  // merging into modified fork
  {
    auto db_fork = db->PartiallyPersistentFork();
    ASSERT_OK(db_fork->SetAttr(b, "a", DataItem(arolla::Text("ba"))));
    ASSERT_OK(db_fork->SetAttr(b, "b", DataItem(75.0)));
    ASSERT_OK(db_fork->MergeInplace(*db));
    EXPECT_THAT(db_fork->GetAttr(a, "a"), IsOkAndHolds(DataItem(57)));
    EXPECT_THAT(db_fork->GetAttr(b, "b"), IsOkAndHolds(DataItem(75.0)));
    EXPECT_THAT(db_fork->GetAttr(b, "a"), IsOkAndHolds(arolla::Text("ba")));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->MergeInplace(*db_fork));
    EXPECT_THAT(db2->GetAttr(a, "a"), IsOkAndHolds(DataItem(57)));
    EXPECT_THAT(db2->GetAttr(b, "b"), IsOkAndHolds(DataItem(75.0)));
    EXPECT_THAT(db2->GetAttr(b, "a"), IsOkAndHolds(arolla::Text("ba")));
  }

  // merging into modified fork chain 2
  {
    auto db_fork = db->PartiallyPersistentFork();
    ASSERT_OK(db_fork->SetAttr(b, "a", DataItem(arolla::Text("ba"))));
    ASSERT_OK(db_fork->SetAttr(c, "a", DataItem(arolla::Bytes("NOT_USED"))));
    db_fork = db_fork->PartiallyPersistentFork();
    ASSERT_OK(db_fork->SetAttr(c, "a", DataItem(arolla::Bytes("ca"))));
    ASSERT_OK(db_fork->MergeInplace(*db));
    auto check_attrs = [&](auto db) {
      EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(DataItem(57)));
      EXPECT_THAT(db->GetAttr(b, "a"), IsOkAndHolds(arolla::Text("ba")));
      EXPECT_THAT(db->GetAttr(c, "a"), IsOkAndHolds(arolla::Bytes("ca")));
    };
    check_attrs(db_fork);

    {
      auto db2 = DataBagImpl::CreateEmptyDatabag();
      ASSERT_OK(db2->MergeInplace(*db_fork));
      check_attrs(db2);
    }
    {
      auto db2 = DataBagImpl::CreateEmptyDatabag();
      ASSERT_OK(db2->SetAttr(b, "a", DataItem(arolla::Text("NOT_USED"))));
      db2 = db2->PartiallyPersistentFork();
      ASSERT_OK(db2->SetAttr(b, "a", DataItem(arolla::Text("ba"))));
      ASSERT_OK(db2->MergeInplace(*db_fork));
      check_attrs(db2);
    }
  }
}

TEST(DataBagTest, MergeObjectsOnlyDenseSources) {
  constexpr int64_t kSize = 179;
  auto a = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto a_value = DataSliceImpl::AllocateEmptyObjects(kSize);

  {  // merge dense with sparse
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->MergeInplace(*db2));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
    ASSERT_OK(db2->SetAttr(a[5], "a", a_value[5]));
    ASSERT_OK(db->MergeInplace(*db2));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
  {  // merge const dense with sparse
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK_AND_ASSIGN(auto x, db->CreateObjectsFromFields({"a"}, {a_value}));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->MergeInplace(*db2));
    EXPECT_THAT(db->GetAttr(x, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
    ASSERT_OK(db2->SetAttr(x[5], "a", a_value[5]));
    ASSERT_OK(db->MergeInplace(*db2));
    EXPECT_THAT(db->GetAttr(x, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
  {  // merge sparse with dense
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(a, "a", a_value));
    ASSERT_OK(db->SetAttr(a[5], "a", a_value[5]));
    ASSERT_OK(db->MergeInplace(*db2));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
  {  // merge sparse with dense overwrite
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(a, "a", a_value));
    ASSERT_OK(db->SetAttr(a[5], "a", a_value[7]));
    EXPECT_THAT(
        db->MergeInplace(*db2),
        StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("conflict")));
    ASSERT_OK(db->MergeInplace(
        *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
  {  // merge sparse with const dense
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK_AND_ASSIGN(auto x,
                         db2->CreateObjectsFromFields({"a"}, {a_value}));
    ASSERT_OK(db->SetAttr(x[5], "a", a_value[5]));
    ASSERT_OK(db2->MergeInplace(*db));
    EXPECT_THAT(db2->GetAttr(x, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
  {  // merge dense with dense
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    ASSERT_OK(db2->SetAttr(a, "a", a_value));
    ASSERT_OK(db->MergeInplace(*db2));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
  {
    SCOPED_TRACE("merge dense with dense conflict");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    ASSERT_OK(db2->SetAttr(a, "a", a_value));
    ASSERT_OK(db2->SetAttr(a[0], "a", a_value[1]));
    EXPECT_THAT(
        db->MergeInplace(*db2),
        StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("conflict")));
    EXPECT_THAT(
        db2->MergeInplace(*db),
        StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("conflict")));
  }
  {
    SCOPED_TRACE("merge dense with dense conflict allowed");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    ASSERT_OK(db2->SetAttr(a, "a", a_value));
    ASSERT_OK(db2->SetAttr(a[0], "a", a_value[1]));
    ASSERT_OK(db->MergeInplace(
        *db2,
        MergeOptions{.data_conflict_policy = MergeOptions::kKeepOriginal}));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
    ASSERT_OK(db->MergeInplace(
        *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));
    std::vector<DataItem> a_value_modified(a_value.begin(), a_value.end());
    a_value_modified[0] = a_value[1];
    EXPECT_THAT(db->GetAttr(a, "a"),
                IsOkAndHolds(ElementsAreArray(a_value_modified)));
  }

  // merge dense with many sparse
  for (bool left : {true, false}) {
    for (int conflict_layer : {0, 1, 2, -1}) {
      for (MergeOptions merge_options :
           {MergeOptions(),
            MergeOptions{.data_conflict_policy = MergeOptions::kKeepOriginal},
            MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}}) {
        SCOPED_TRACE(absl::StrCat("merge dense with many sparse: ", left, " ",
                                  conflict_layer, " ",
                                  merge_options.data_conflict_policy));
        auto db = DataBagImpl::CreateEmptyDatabag();
        ASSERT_OK(db->SetAttr(a, "a", a_value));
        auto db2 = DataBagImpl::CreateEmptyDatabag();
        ASSERT_OK(db2->SetAttr(a[5], "a", a_value[5]));
        ASSERT_OK(db2->SetAttr(a[9], "a", a_value[0]));
        if (conflict_layer == 0) {
          ASSERT_OK(db2->SetAttr(a[0], "a", a_value[1]));
        }
        db2 = db2->PartiallyPersistentFork();
        ASSERT_OK(db2->SetAttr(a[9], "a", a_value[9]));
        ASSERT_OK(db2->SetAttr(a[14], "a", a_value[0]));
        if (conflict_layer == 1) {
          ASSERT_OK(db2->SetAttr(a[0], "a", a_value[1]));
        }
        db2 = db2->PartiallyPersistentFork();
        if (conflict_layer == 2) {
          ASSERT_OK(db2->SetAttr(a[0], "a", a_value[1]));
        }
        ASSERT_OK(db2->SetAttr(a[14], "a", a_value[14]));
        std::vector<DataItem> a_value_expected(a_value.begin(), a_value.end());
        if (conflict_layer != -1 &&
            merge_options.data_conflict_policy == MergeOptions::kOverwrite) {
          a_value_expected[0] = a_value[1];
        }
        if (left) {
          if (conflict_layer != -1 && merge_options.data_conflict_policy ==
                                          MergeOptions::kRaiseOnConflict) {
            EXPECT_THAT(db->MergeInplace(*db2, merge_options),
                        StatusIs(absl::StatusCode::kFailedPrecondition,
                                 HasSubstr("conflict")));
            continue;
          }
          ASSERT_OK(db->MergeInplace(*db2, merge_options));
          EXPECT_THAT(db->GetAttr(a, "a"),
                      IsOkAndHolds(ElementsAreArray(a_value_expected)));
        } else {
          if (conflict_layer != -1 && merge_options.data_conflict_policy ==
                                          MergeOptions::kRaiseOnConflict) {
            EXPECT_THAT(db2->MergeInplace(*db, merge_options),
                        StatusIs(absl::StatusCode::kFailedPrecondition,
                                 HasSubstr("conflict")));
            continue;
          }
          ASSERT_OK(db2->MergeInplace(*db, ReverseMergeOptions(merge_options)));
          EXPECT_THAT(db2->GetAttr(a, "a"),
                      IsOkAndHolds(ElementsAreArray(a_value_expected)));
        }
      }
    }
  }
}

TEST(DataBagTest, MergeObjectsOverwriteOnlyDenseSources) {
  constexpr int64_t kSize = 179;
  auto a = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto a_value = DataSliceImpl::AllocateEmptyObjects(kSize);

  {
    SCOPED_TRACE("merge dense with sparse overwrite");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(a[5], "a", a_value[0]));
    ASSERT_OK(db->MergeInplace(
        *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));
    std::vector<DataItem> a_value_expected(a_value.begin(), a_value.end());
    a_value_expected[5] = a_value[0];
    EXPECT_THAT(db->GetAttr(a, "a"),
                IsOkAndHolds(ElementsAreArray(a_value_expected)));
  }
  {
    SCOPED_TRACE("merge dense with sparse overwrite mixed");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(a[2], "a", DataItem(17.0)));
    ASSERT_OK(db2->SetAttr(a[5], "a", DataItem(57)));
    ASSERT_OK(db->MergeInplace(
        *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));
    std::vector<DataItem> a_value_expected(a_value.begin(), a_value.end());
    a_value_expected[2] = DataItem(17.0);
    a_value_expected[5] = DataItem(57);
    EXPECT_THAT(db->GetAttr(a, "a"),
                IsOkAndHolds(ElementsAreArray(a_value_expected)));
  }
  {
    SCOPED_TRACE("merge dense with sparse overwrite nothing");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(a[5], "a", a_value[0]));
    ASSERT_OK(db2->SetAttr(a[5], "a", DataItem()));
    ASSERT_OK(db->MergeInplace(
        *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
  {
    SCOPED_TRACE("merge dense with dense overwrite all");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    auto b_value = DataSliceImpl::AllocateEmptyObjects(kSize);
    ASSERT_OK(db2->SetAttr(a, "a", b_value));
    ASSERT_OK(db->MergeInplace(
        *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(b_value)));
  }
  {
    SCOPED_TRACE("merge dense with dense overwrite all but one");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    auto db2 = DataBagImpl::CreateEmptyDatabag();

    auto b_value = DataSliceImpl::AllocateEmptyObjects(kSize);
    ASSERT_OK(db2->SetAttr(a, "a", b_value));
    ASSERT_OK(db2->SetAttr(a[5], "a", DataItem()));
    ASSERT_OK(db->MergeInplace(
        *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));

    std::vector<DataItem> a_value_expected(b_value.begin(), b_value.end());
    a_value_expected[5] = a_value[5];
    EXPECT_THAT(db->GetAttr(a, "a"),
                IsOkAndHolds(ElementsAreArray(a_value_expected)));
  }
  {
    SCOPED_TRACE("merge dense with dense overwrite many mixed");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    auto db2 = DataBagImpl::CreateEmptyDatabag();

    auto b_value = DataSliceImpl::AllocateEmptyObjects(kSize);
    ASSERT_OK(db2->SetAttr(a, "a", b_value));
    ASSERT_OK(db2->SetAttr(a[1], "a", DataItem(27.0)));
    ASSERT_OK(db2->SetAttr(a[3], "a", DataItem(57)));
    ASSERT_OK(db2->SetAttr(a[5], "a", DataItem()));
    ASSERT_OK(db->MergeInplace(
        *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));

    std::vector<DataItem> a_value_expected(b_value.begin(), b_value.end());
    a_value_expected[1] = DataItem(27.0);
    a_value_expected[3] = DataItem(57);
    a_value_expected[5] = a_value[5];
    EXPECT_THAT(db->GetAttr(a, "a"),
                IsOkAndHolds(ElementsAreArray(a_value_expected)));
  }
  {
    SCOPED_TRACE("merge dense with dense overwrite nothing");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(a, "a", DataSliceImpl::AllocateEmptyObjects(kSize)));
    ASSERT_OK(
        db2->SetAttr(a, "a", DataSliceImpl::CreateEmptyAndUnknownType(kSize)));
    ASSERT_OK(db->MergeInplace(
        *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
}

TYPED_TEST(DataBagAllocatorTest, MergeObjectAttrsOnlyConflicts) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto a = DataItem(this->AllocSingle());
  auto db1 = db->PartiallyPersistentFork();
  ASSERT_OK(db1->SetAttr(a, "x", DataItem(57)));
  auto db2 = db->PartiallyPersistentFork();
  ASSERT_OK(db2->SetAttr(a, "x", DataItem(75.0)));
  EXPECT_THAT(
      db1->MergeInplace(*db2),
      StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("conflict")));
  // the same value is not a conflict
  ASSERT_OK(db2->SetAttr(a, "x", DataItem(57)));
  ASSERT_OK(db1->MergeInplace(*db2));
  EXPECT_THAT(db1->GetAttr(a, "x"), IsOkAndHolds(DataItem(57)));
}

TYPED_TEST(DataBagAllocatorTest, MergeObjectAttrsOnlyConflictsAllowed) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto db1 = db->PartiallyPersistentFork();
  auto db2 = db->PartiallyPersistentFork();
  std::vector<DataItem> objs;
  for (int64_t i = 0; i < 1000; ++i) {
    auto a = DataItem(this->AllocSingle());
    objs.push_back(a);
    ASSERT_OK(db1->SetAttr(a, "x", DataItem(57)));
    ASSERT_OK(db2->SetAttr(a, "x", DataItem(75.0)));
  }
  ASSERT_OK(db1->MergeInplace(
      *db2, MergeOptions{.data_conflict_policy = MergeOptions::kKeepOriginal}));
  for (int64_t i = 0; i < 1000; ++i) {
    ASSERT_THAT(db1->GetAttr(objs[i], "x"), IsOkAndHolds(DataItem(57)));
  }
  ASSERT_OK(db1->MergeInplace(
      *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));
  for (int64_t i = 0; i < 1000; ++i) {
    ASSERT_THAT(db1->GetAttr(objs[i], "x"), IsOkAndHolds(DataItem(75.0)));
  }
}

TYPED_TEST(DataBagAllocatorTest, MergeObjectAttrsOnlyLongForks) {
  constexpr int64_t kMaxForks = 20;
  std::vector<DataItem> objs_a(kMaxForks);
  std::vector<DataItem> objs_b(kMaxForks);
  for (int64_t i = 0; i < kMaxForks; ++i) {
    objs_a[i] = DataItem(this->AllocSingle());
    objs_b[i] = DataItem(this->AllocSingle());
  }
  auto x = DataItem(this->AllocSingle());
  auto y = DataItem(this->AllocSingle());
  auto db1 = DataBagImpl::CreateEmptyDatabag();
  auto db2 = DataBagImpl::CreateEmptyDatabag();
  for (int64_t i = 0; i < kMaxForks; ++i) {
    for (int64_t j = 0; j < kMaxForks - i; ++j) {
      ASSERT_OK(db1->SetAttr(objs_a[j], "a", DataItem(i)));
      ASSERT_OK(db2->SetAttr(objs_a[j], "b", DataItem(-i)));
      ASSERT_OK(db1->SetAttr(objs_b[j], "b", DataItem(i * 2)));
      ASSERT_OK(db2->SetAttr(objs_b[j], "a", DataItem(-i * 2)));
      ASSERT_OK(db2->SetAttr(i % 2 == 0 ? x : y,
                             "overwite_b" + std::to_string(j),
                             DataItem(-j * 3)));
    }
    ASSERT_OK(
        db1->SetAttr(objs_a[i], "xa" + std::to_string(i), DataItem(i * 3)));
    ASSERT_OK(
        db2->SetAttr(objs_b[i], "xb" + std::to_string(i), DataItem(-i * 3)));

    ASSERT_OK(
        db1->SetAttr(x, "overwite_b" + std::to_string(i), DataItem(i * 5)));
    ASSERT_OK(
        db1->SetAttr(y, "overwite_b" + std::to_string(i), DataItem(i * 5)));

    db1 = db1->PartiallyPersistentFork();
    db2 = db2->PartiallyPersistentFork();
  }
  for (int64_t i = 0; i < kMaxForks; ++i) {
    ASSERT_OK(
        db2->SetAttr(x, "overwite_b" + std::to_string(i), DataItem(i * 5)));
    ASSERT_OK(
        db2->SetAttr(y, "overwite_b" + std::to_string(i), DataItem(i * 5)));
  }
  ASSERT_OK(db1->MergeInplace(*db2));
  for (int64_t i = 0; i < kMaxForks; ++i) {
    int64_t expected_i = kMaxForks - i - 1;
    EXPECT_THAT(db1->GetAttr(objs_a[i], "xa" + std::to_string(i)),
                IsOkAndHolds(DataItem(i * 3)));
    EXPECT_THAT(db1->GetAttr(objs_b[i], "xb" + std::to_string(i)),
                IsOkAndHolds(DataItem(-i * 3)));
    EXPECT_THAT(db1->GetAttr(x, "overwite_b" + std::to_string(i)),
                IsOkAndHolds(DataItem(i * 5)));
    EXPECT_THAT(db1->GetAttr(y, "overwite_b" + std::to_string(i)),
                IsOkAndHolds(DataItem(i * 5)));
    EXPECT_THAT(db1->GetAttr(objs_a[i], "a"),
                IsOkAndHolds(DataItem(expected_i)));
    EXPECT_THAT(db1->GetAttr(objs_a[i], "b"),
                IsOkAndHolds(DataItem(-expected_i)));
    EXPECT_THAT(db1->GetAttr(objs_b[i], "a"),
                IsOkAndHolds(DataItem(-expected_i * 2)));
    EXPECT_THAT(db1->GetAttr(objs_b[i], "b"),
                IsOkAndHolds(DataItem(expected_i * 2)));
  }
}

TYPED_TEST(DataBagMergeTest, MergeLists) {
  constexpr int64_t kSize = 77;
  auto verify_lists = [](absl::Span<const DataItem> lists, DataBagImpl* db,
                         int64_t expected_size = 1, int64_t value_or = 0) {
    for (int64_t i = 0; i < lists.size(); ++i) {
      auto list = lists[i];
      ASSERT_THAT(db->GetListSize(list), IsOkAndHolds(DataItem(expected_size)));
      ASSERT_THAT(db->ExplodeList(list),
                  IsOkAndHolds(ElementsAreArray(
                      std::vector<int64_t>(expected_size, i | value_or))));
    }
  };
  MergeOptions merge_options = this->merge_options();
  {
    SCOPED_TRACE("merge with non existing");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> lists(kSize);
    for (int64_t i = 0; i < kSize; ++i) {
      lists[i] = DataItem(this->AllocSingleList());
      ASSERT_OK(db2->AppendToList(lists[i], DataItem(i)));
    }
    ASSERT_OK(db->MergeInplace(*db2, merge_options));
    verify_lists(lists, db.get());
  }
  {
    SCOPED_TRACE("merge existing, but empty");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> lists(kSize);
    for (int64_t i = 0; i < kSize; ++i) {
      lists[i] = DataItem(this->AllocSingleList());
      // create empty lists in db
      ASSERT_OK(db->AppendToList(lists[i], DataItem(i)));
      ASSERT_OK(db->RemoveInList(lists[i], DataBagImpl::ListRange(0)));

      ASSERT_OK(db2->AppendToList(lists[i], DataItem(i)));
    }
    ASSERT_OK(db->MergeInplace(*db2, merge_options));
    verify_lists(lists, db.get());
  }
  {
    SCOPED_TRACE("merge non existing or empty both sides");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> lists(kSize);
    for (int64_t i = 0; i < kSize; ++i) {
      lists[i] = DataItem(this->AllocSingleList());
      // create empty lists in db
      if (i % 2 == 0) {
        ASSERT_OK(db->AppendToList(lists[i], DataItem(i)));
        if (i % 6 == 0) {
          ASSERT_OK(db2->AppendToList(lists[i], DataItem(i)));
          ASSERT_OK(db2->RemoveInList(lists[i], DataBagImpl::ListRange(0)));
        }
      } else {
        ASSERT_OK(db2->AppendToList(lists[i], DataItem(i)));
        if (i % 6 == 5) {
          ASSERT_OK(db->AppendToList(lists[i], DataItem(i)));
          ASSERT_OK(db->RemoveInList(lists[i], DataBagImpl::ListRange(0)));
        }
      }
    }
    ASSERT_OK(db->MergeInplace(*db2, merge_options));
    verify_lists(lists, db.get());
  }
  {
    SCOPED_TRACE("merge the same");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> lists(kSize);
    for (int64_t i = 0; i < kSize; ++i) {
      lists[i] = DataItem(this->AllocSingleList());
      ASSERT_OK(db->AppendToList(lists[i], DataItem(i)));
      ASSERT_OK(db2->AppendToList(lists[i], DataItem(i)));
    }
    ASSERT_OK(db->MergeInplace(*db2, merge_options));
    verify_lists(lists, db.get());
  }
  {
    SCOPED_TRACE("merge with conflicting value");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> lists(kSize);
    for (int64_t i = 0; i < kSize; ++i) {
      lists[i] = DataItem(this->AllocSingleList());
      ASSERT_OK(db->AppendToList(lists[i], DataItem(i | 1)));
      ASSERT_OK(db2->AppendToList(lists[i], DataItem(i)));
    }
    auto status = db->MergeInplace(*db2, merge_options);
    if (merge_options.data_conflict_policy == MergeOptions::kRaiseOnConflict) {
      EXPECT_THAT(status,
                  StatusIs(absl::StatusCode::kFailedPrecondition,
                           HasSubstr("conflict")));
    } else if (merge_options.data_conflict_policy == MergeOptions::kOverwrite) {
      EXPECT_OK(status);
      verify_lists(lists, db.get());
    } else if (merge_options.data_conflict_policy ==
               MergeOptions::kKeepOriginal) {
      EXPECT_OK(status);
      verify_lists(lists, db.get(), /*expected_size=*/1, /*value_or=*/1);
    }
  }
  {
    SCOPED_TRACE("merge with conflicting size");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> lists(kSize);
    for (int64_t i = 0; i < kSize; ++i) {
      lists[i] = DataItem(this->AllocSingleList());
      ASSERT_OK(db->AppendToList(lists[i], DataItem(i)));
      ASSERT_OK(db->AppendToList(lists[i], DataItem(i)));
      ASSERT_OK(db2->AppendToList(lists[i], DataItem(i)));
    }
    auto status = db->MergeInplace(*db2, merge_options);
    if (merge_options.data_conflict_policy == MergeOptions::kRaiseOnConflict) {
      EXPECT_THAT(status,
                  StatusIs(absl::StatusCode::kFailedPrecondition,
                           HasSubstr("conflict")));
    } else if (merge_options.data_conflict_policy == MergeOptions::kOverwrite) {
      EXPECT_OK(status);
      verify_lists(lists, db.get());
    } else if (merge_options.data_conflict_policy ==
               MergeOptions::kKeepOriginal) {
      EXPECT_OK(status);
      verify_lists(lists, db.get(), /*expected_size=*/2);
    }
  }
  {
    SCOPED_TRACE("merge non existing with fork");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> lists(kSize);
    for (int64_t i = 0; i < kSize; ++i) {
      lists[i] = DataItem(this->AllocSingleList());
      ASSERT_OK(db2->AppendToList(lists[i], DataItem(i)));
      if (i % 5 == 4) {
        db2 = db2->PartiallyPersistentFork();
      }
    }
    ASSERT_OK(db->MergeInplace(*db2, merge_options));
    verify_lists(lists, db.get());
  }
  {
    SCOPED_TRACE("merge existing with forks");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> lists(kSize);
    for (int64_t i = 0; i < kSize; ++i) {
      lists[i] = DataItem(this->AllocSingleList());
      ASSERT_OK(db->AppendToList(lists[i], DataItem(i)));
      ASSERT_OK(db2->AppendToList(lists[i], DataItem(i)));
      if (i % 7 == 6) {
        db = db->PartiallyPersistentFork();
      }
      if (i % 5 == 4) {
        db2 = db2->PartiallyPersistentFork();
      }
    }
    ASSERT_OK(db->MergeInplace(*db2, merge_options));
    verify_lists(lists, db.get());
  }
  {
    SCOPED_TRACE("merge existing with overwrites in parents");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> lists(kSize);
    for (int64_t i = 0; i < kSize; ++i) {
      lists[i] = DataItem(this->AllocSingleList());
      ASSERT_OK(db->AppendToList(lists[i], DataItem(-i)));
      ASSERT_OK(db2->AppendToList(lists[i], DataItem(-i * 2)));
      db = db->PartiallyPersistentFork();
      if (i > 0) {
        ASSERT_OK(db->SetInList(lists[i - 1], 0, DataItem(i - 1)));
      }
      db2 = db2->PartiallyPersistentFork();
      if (i > 1) {
        ASSERT_OK(db2->SetInList(lists[i - 2], 0, DataItem(i - 2)));
      }
    }
    db = db->PartiallyPersistentFork();
    db2 = db2->PartiallyPersistentFork();
    ASSERT_OK(db->SetInList(lists.back(), 0, DataItem(kSize - 1)));
    ASSERT_OK(db2->SetInList(lists[kSize - 2], 0, DataItem(kSize - 2)));
    ASSERT_OK(db2->SetInList(lists.back(), 0, DataItem(kSize - 1)));
    ASSERT_OK(db->MergeInplace(*db2, merge_options));
    verify_lists(lists, db.get());
  }
}

TYPED_TEST(DataBagMergeTest, MergeDictsOnly) {
  MergeOptions merge_options = this->merge_options();

  auto [a, b, c, k, k2] = std::array{
      DataItem(this->AllocSingleDict()), DataItem(this->AllocSingleDict()),
      DataItem(this->AllocSingleDict()), DataItem(this->AllocSingle()),
      DataItem(this->AllocSingle()),
  };

  {
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetInDict(a, k, DataItem(57)));
    ASSERT_OK(db->MergeInplace(*db, merge_options));
    EXPECT_THAT(db->GetFromDict(a, k), IsOkAndHolds(DataItem(57)));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->MergeInplace(*db, merge_options));
    EXPECT_THAT(db2->GetFromDict(a, k), IsOkAndHolds(DataItem(57)));
  }

  {
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetInDict(a, k, DataItem(57)));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetInDict(b, k, DataItem(37)));
    ASSERT_OK(db2->MergeInplace(*db, merge_options));
    EXPECT_THAT(db2->GetFromDict(a, k), IsOkAndHolds(DataItem(57)));
    EXPECT_THAT(db2->GetFromDict(b, k), IsOkAndHolds(DataItem(37)));
  }

  {
    SCOPED_TRACE("merging into unmodified fork");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetInDict(a, k, DataItem(57)));
    auto db_fork = db->PartiallyPersistentFork();
    ASSERT_OK(db_fork->MergeInplace(*db, merge_options));
    EXPECT_THAT(db_fork->GetFromDict(a, k), IsOkAndHolds(DataItem(57)));
  }

  {
    SCOPED_TRACE("merging into modified fork");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetInDict(a, k, DataItem(57)));
    auto db_fork = db->PartiallyPersistentFork();
    ASSERT_OK(db_fork->SetInDict(b, k, DataItem(arolla::Text("ba"))));
    ASSERT_OK(db_fork->SetInDict(b, k2, DataItem(75.0)));
    ASSERT_OK(db_fork->MergeInplace(*db, merge_options));
    EXPECT_THAT(db_fork->GetFromDict(a, k), IsOkAndHolds(DataItem(57)));
    EXPECT_THAT(db_fork->GetFromDict(b, k2), IsOkAndHolds(DataItem(75.0)));
    EXPECT_THAT(db_fork->GetFromDict(b, k), IsOkAndHolds(arolla::Text("ba")));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->MergeInplace(*db_fork, merge_options));
    EXPECT_THAT(db2->GetFromDict(a, k), IsOkAndHolds(DataItem(57)));
    EXPECT_THAT(db2->GetFromDict(b, k2), IsOkAndHolds(DataItem(75.0)));
    EXPECT_THAT(db2->GetFromDict(b, k), IsOkAndHolds(arolla::Text("ba")));
  }

  {
    SCOPED_TRACE("merging conflict");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetInDict(a, k, DataItem(57)));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetInDict(a, k, DataItem(75)));
    auto status = db->MergeInplace(*db2, merge_options);
    if (merge_options.data_conflict_policy == MergeOptions::kRaiseOnConflict) {
      EXPECT_THAT(status, StatusIs(absl::StatusCode::kFailedPrecondition,
                                   HasSubstr("conflict")));
    } else if (merge_options.data_conflict_policy == MergeOptions::kOverwrite) {
      EXPECT_OK(status);
      EXPECT_THAT(db->GetFromDict(a, k), IsOkAndHolds(DataItem(75)));
    } else if (merge_options.data_conflict_policy ==
               MergeOptions::kKeepOriginal) {
      EXPECT_OK(status);
      EXPECT_THAT(db->GetFromDict(a, k), IsOkAndHolds(DataItem(57)));
    }
  }

  {
    SCOPED_TRACE("merging into modified fork chain 2");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetInDict(a, k, DataItem(57)));
    auto db_fork = db->PartiallyPersistentFork();
    ASSERT_OK(db_fork->SetInDict(b, k, DataItem(arolla::Text("ba"))));
    ASSERT_OK(db_fork->SetInDict(c, k, DataItem(arolla::Bytes("NOT_USED"))));
    db_fork = db_fork->PartiallyPersistentFork();
    ASSERT_OK(db_fork->SetInDict(c, k, DataItem(arolla::Bytes("ca"))));
    ASSERT_OK(db_fork->MergeInplace(*db, merge_options));
    auto check_dicts = [&](auto db) {
      EXPECT_THAT(db->GetFromDict(a, k), IsOkAndHolds(DataItem(57)));
      EXPECT_THAT(db->GetFromDict(b, k), IsOkAndHolds(arolla::Text("ba")));
      EXPECT_THAT(db->GetFromDict(c, k), IsOkAndHolds(arolla::Bytes("ca")));
    };
    check_dicts(db_fork);

    {
      auto db2 = DataBagImpl::CreateEmptyDatabag();
      ASSERT_OK(db2->MergeInplace(*db_fork, merge_options));
      check_dicts(db2);
    }
    {
      auto db2 = DataBagImpl::CreateEmptyDatabag();
      ASSERT_OK(db2->SetInDict(b, k, DataItem(arolla::Text("NOT_USED"))));
      db2 = db2->PartiallyPersistentFork();
      ASSERT_OK(db2->SetInDict(b, k, DataItem(arolla::Text("ba"))));
      ASSERT_OK(db2->MergeInplace(*db_fork, merge_options));
      check_dicts(db2);
    }
  }
}

TYPED_TEST(DataBagMergeTest, MergeDictsOnlyFork) {
  MergeOptions merge_options = this->merge_options();
  constexpr int64_t kSize = 37;
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto db2 = DataBagImpl::CreateEmptyDatabag();
  DataItem k = DataItem(this->AllocSingle());
  std::vector<DataItem> dicts(kSize);
  for (int64_t i = 0; i < kSize; ++i) {
    dicts[i] = DataItem(this->AllocSingleDict());
    ASSERT_OK(db->SetInDict(dicts[i], k, DataItem(-i)));
    ASSERT_OK(db->SetInDict(dicts[i], DataItem(-i), DataItem(-i)));
    ASSERT_OK(db2->SetInDict(dicts[i], k, DataItem(-i * 2)));
    ASSERT_OK(db2->SetInDict(dicts[i], DataItem(-i * 2 - 2), DataItem(-i * 2)));
    db = db->PartiallyPersistentFork();
    if (i > 0) {
      ASSERT_OK(db->SetInDict(dicts[i - 1], k, DataItem(i - 1)));
    }
    db2 = db2->PartiallyPersistentFork();
    if (i > 1) {
      ASSERT_OK(db2->SetInDict(dicts[i - 2], k, DataItem(i - 2)));
    }
  }
  db = db->PartiallyPersistentFork();
  db2 = db2->PartiallyPersistentFork();
  ASSERT_OK(db->SetInDict(dicts.back(), k, DataItem(kSize - 1)));

  ASSERT_OK(db2->SetInDict(dicts.back(), k, DataItem(kSize - 1)));
  ASSERT_OK(db2->SetInDict(dicts[kSize - 2], k, DataItem(kSize - 2)));
  ASSERT_OK(db2->SetInDict(dicts.back(), k, DataItem(kSize - 1)));
  ASSERT_OK(db->MergeInplace(*db2, merge_options));
  for (int64_t i = 0; i < kSize; ++i) {
    EXPECT_THAT(
        db->GetDictKeys(dicts[i]),
        IsOkAndHolds(Pair(
            UnorderedElementsAre(k, DataItem(-i), DataItem(-i * 2 - 2)), _)));
    EXPECT_THAT(db->GetFromDict(dicts[i], k), IsOkAndHolds(DataItem(i)));
    EXPECT_THAT(db->GetFromDict(dicts[i], DataItem(-i)),
                IsOkAndHolds(DataItem(-i)));
    EXPECT_THAT(db->GetFromDict(dicts[i], DataItem(-i * 2 - 2)),
                IsOkAndHolds(DataItem(-i * 2)));
  }
}

// There are more tests for DataBagIndex/DataBagContent in triples_test.cc
TEST(DataBagTest, DataBagIndex) {
  auto ds1 = DataSliceImpl::AllocateEmptyObjects(15);
  auto ds2 = DataSliceImpl::AllocateEmptyObjects(15);
  auto ds3 = DataSliceImpl::AllocateEmptyObjects(15);

  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetAttr(ds1, "a", ds2));
  ASSERT_OK(db->SetAttr(ds1, "b", ds2));
  ASSERT_OK(db->SetAttr(ds2, "a", ds3));

  DataBagIndex index = db->CreateIndex();
  EXPECT_TRUE(index.dicts.empty());
  EXPECT_TRUE(index.lists.empty());
  EXPECT_EQ(index.attrs.size(), 2);
  EXPECT_FALSE(index.attrs["a"].with_small_allocs);
  EXPECT_FALSE(index.attrs["b"].with_small_allocs);
  EXPECT_THAT(index.attrs["b"].allocations,
              ElementsAreArray(ds1.allocation_ids().ids()));

  // db.CreateIndex() should return allocation ids in sorted order.
  AllocationId ds1_alloc = ds1.allocation_ids().ids().front();
  AllocationId ds2_alloc = ds2.allocation_ids().ids().front();
  if (ds1_alloc < ds2_alloc) {
    EXPECT_THAT(index.attrs["a"].allocations,
                ElementsAre(ds1_alloc, ds2_alloc));
  } else {
    EXPECT_THAT(index.attrs["a"].allocations,
                ElementsAre(ds1_alloc, ds2_alloc));
  }
}

TEST(DataBagTest, MergeExplicitSchemas) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(DataItem schema, db->CreateExplicitSchemaFromFields(
                                            {"foo"}, {GetIntSchema()}));
  auto db2 = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db2->SetSchemaAttr(schema, "foo", GetFloatSchema()));
  {
    SCOPED_TRACE("overwrite schema");
    auto res = db->PartiallyPersistentFork();
    ASSERT_OK(res->MergeInplace(
        *db2,
        MergeOptions{.schema_conflict_policy = MergeOptions::kOverwrite}));
    EXPECT_THAT(res->GetSchemaAttr(schema, "foo"),
                IsOkAndHolds(GetFloatSchema()));
  }
  {
    SCOPED_TRACE("overwrite data");
    auto res = db->PartiallyPersistentFork();
    EXPECT_THAT(
        res->MergeInplace(*db2, MergeOptions{.data_conflict_policy =
                                                 MergeOptions::kOverwrite}),
        StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("conflict")));
  }
  {
    SCOPED_TRACE("keep schema");
    auto res = db->PartiallyPersistentFork();
    ASSERT_OK(res->MergeInplace(
        *db2,
        MergeOptions{.schema_conflict_policy = MergeOptions::kKeepOriginal}));
    EXPECT_THAT(res->GetSchemaAttr(schema, "foo"),
                IsOkAndHolds(GetIntSchema()));
  }
  {
    SCOPED_TRACE("keep data");
    auto res = db->PartiallyPersistentFork();
    EXPECT_THAT(
        res->MergeInplace(*db2, MergeOptions{.data_conflict_policy =
                                                 MergeOptions::kKeepOriginal}),
        StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("conflict")));
  }
  {
    SCOPED_TRACE("raise on conflict");
    auto res = db->PartiallyPersistentFork();
    EXPECT_THAT(
        res->MergeInplace(*db2, MergeOptions()),
        StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("conflict")));
  }
}

TEST(DataBagTest, MergeImplicitSchemas) {
  ASSERT_OK_AND_ASSIGN(
      DataItem schema,
      CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          DataItem(AllocateSingleObject()), schema::kImplicitSchemaSeed));
  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetSchemaAttr(schema, "foo", GetIntSchema()));
  auto db2 = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db2->SetSchemaAttr(schema, "foo", GetFloatSchema()));
  {
    SCOPED_TRACE("overwrite data");
    auto res = db->PartiallyPersistentFork();
    ASSERT_OK(res->MergeInplace(
        *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));
    EXPECT_THAT(res->GetSchemaAttr(schema, "foo"),
                IsOkAndHolds(GetFloatSchema()));
  }
  {
    SCOPED_TRACE("overwrite schema");
    auto res = db->PartiallyPersistentFork();
    EXPECT_THAT(
        res->MergeInplace(*db2, MergeOptions{.schema_conflict_policy =
                                                 MergeOptions::kOverwrite}),
        StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("conflict")));
  }
  {
    SCOPED_TRACE("keep data");
    auto res = db->PartiallyPersistentFork();
    ASSERT_OK(res->MergeInplace(
        *db2,
        MergeOptions{.data_conflict_policy = MergeOptions::kKeepOriginal}));
    EXPECT_THAT(res->GetSchemaAttr(schema, "foo"),
                IsOkAndHolds(GetIntSchema()));
  }
  {
    SCOPED_TRACE("keep schema");
    auto res = db->PartiallyPersistentFork();
    EXPECT_THAT(
        res->MergeInplace(*db2, MergeOptions{.schema_conflict_policy =
                                                 MergeOptions::kKeepOriginal}),
        StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("conflict")));
  }
  {
    SCOPED_TRACE("raise on conflict");
    auto res = db->PartiallyPersistentFork();
    EXPECT_THAT(
        res->MergeInplace(*db2, MergeOptions()),
        StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("conflict")));
  }
}

// NOTE(b/343432263): msan regression test to ensure that the DataBagImpl
// destructor does not cause use-of-uninitialized-value issues.
using DataBagMsanTest = ::testing::TestWithParam<DataBagImplPtr>;

TEST_P(DataBagMsanTest, Msan) {
  const auto& db = GetParam();
  ASSERT_NE(db, nullptr);
}

INSTANTIATE_TEST_SUITE_P(
    DataBagMsanTestSuite, DataBagMsanTest,
    ::testing::ValuesIn([]() -> std::vector<DataBagImplPtr> {
      auto db_no_parent = DataBagImpl::CreateEmptyDatabag();

      auto db = DataBagImpl::CreateEmptyDatabag();
      auto ds = DataSliceImpl::AllocateEmptyObjects(3);
      auto ds_a = DataSliceImpl::AllocateEmptyObjects(3);
      EXPECT_OK(db->SetAttr(ds, "a", ds_a));
      auto db_with_parent = db->PartiallyPersistentFork();

      return {{db_no_parent}, {db_with_parent}};
    }()));

}  // namespace
}  // namespace koladata::internal
