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
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <limits>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/bytes.h"
#include "arolla/util/status.h"
#include "arolla/util/testing/status_matchers.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/errors.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/internal/uuid_object.h"

namespace koladata::internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::PayloadIs;
using ::koladata::internal::testing::DataBagEqual;
using ::testing::_;
using ::testing::AllOf;
using ::testing::AnyOf;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::HasSubstr;
using ::testing::MatchesRegex;
using ::testing::NotNull;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

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

constexpr size_t kDataBagMergeParamCount = 4;
template <typename AllocatorWithId>
struct DataBagMergeTest
    : public DataBagAllocatorTest<std::tuple_element_t<0, AllocatorWithId>> {
  static constexpr int kOptId = std::tuple_element_t<1, AllocatorWithId>();

  absl::Status MergeInplaceWithOptions(DataBagImpl& db1,
                                       const DataBagImpl& db2) {
    if constexpr (kOptId == 3) {
      ASSIGN_OR_RETURN(auto update, db1.CreateOverwritingMergeUpdate(db2));
      return db1.MergeInplace(*update, merge_options());
    }
    return db1.MergeInplace(db2, merge_options());
  }

  MergeOptions merge_options() const {
    static constexpr std::array<MergeOptions, kDataBagMergeParamCount>
        merge_options = {
            MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite},
            MergeOptions(),
            MergeOptions{.data_conflict_policy = MergeOptions::kKeepOriginal}};
    return merge_options[kOptId == 3 ? 0 : kOptId];
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

TYPED_TEST(DataBagMergeTest, MergeObjectsOnly) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  {
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(this->MergeInplaceWithOptions(*db2, *db2));
  }

  auto [a, b, c] = std::array{
      DataItem(this->AllocSingle()),
      DataItem(this->AllocSingle()),
      DataItem(this->AllocSingle()),
  };

  ASSERT_OK(db->SetAttr(a, "a", DataItem(57)));
  {
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(DataItem(57)));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(this->MergeInplaceWithOptions(*db2, *db));
    EXPECT_THAT(db2->GetAttr(a, "a"), IsOkAndHolds(DataItem(57)));
  }

  {
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(b, "a", DataItem(37)));
    ASSERT_OK(this->MergeInplaceWithOptions(*db2, *db));
    EXPECT_THAT(db2->GetAttr(a, "a"), IsOkAndHolds(DataItem(57)));
    EXPECT_THAT(db2->GetAttr(b, "a"), IsOkAndHolds(DataItem(37)));
  }

  // merging into unmodified fork
  {
    auto db_fork = db->PartiallyPersistentFork();
    ASSERT_OK(this->MergeInplaceWithOptions(*db_fork, *db));
    EXPECT_THAT(db_fork->GetAttr(a, "a"), IsOkAndHolds(DataItem(57)));
  }

  // merging unmodified fork into self
  {
    auto db_fork = db->PartiallyPersistentFork();
    ASSERT_OK(db->MergeInplace(*db_fork));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(DataItem(57)));
  }

  // merging into modified fork
  {
    auto db_fork = db->PartiallyPersistentFork();
    ASSERT_OK(db_fork->SetAttr(b, "a", DataItem(arolla::Text("ba"))));
    ASSERT_OK(db_fork->SetAttr(b, "b", DataItem(75.0)));
    ASSERT_OK(this->MergeInplaceWithOptions(*db_fork, *db));
    EXPECT_THAT(db_fork->GetAttr(a, "a"), IsOkAndHolds(DataItem(57)));
    EXPECT_THAT(db_fork->GetAttr(b, "b"), IsOkAndHolds(DataItem(75.0)));
    EXPECT_THAT(db_fork->GetAttr(b, "a"), IsOkAndHolds(arolla::Text("ba")));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(this->MergeInplaceWithOptions(*db2, *db_fork));
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
    ASSERT_OK(this->MergeInplaceWithOptions(*db_fork, *db));
    auto check_attrs = [&](auto db) {
      EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(DataItem(57)));
      EXPECT_THAT(db->GetAttr(b, "a"), IsOkAndHolds(arolla::Text("ba")));
      EXPECT_THAT(db->GetAttr(c, "a"), IsOkAndHolds(arolla::Bytes("ca")));
    };
    check_attrs(db_fork);

    {
      auto db2 = DataBagImpl::CreateEmptyDatabag();
      ASSERT_OK(this->MergeInplaceWithOptions(*db2, *db_fork));
      check_attrs(db2);
    }
    {
      auto db2 = DataBagImpl::CreateEmptyDatabag();
      ASSERT_OK(db2->SetAttr(b, "a", DataItem(arolla::Text("NOT_USED"))));
      db2 = db2->PartiallyPersistentFork();
      ASSERT_OK(db2->SetAttr(b, "a", DataItem(arolla::Text("ba"))));
      ASSERT_OK(this->MergeInplaceWithOptions(*db2, *db_fork));
      check_attrs(db2);
    }
  }
}

TEST(DataBagTest, MergeObjectsOnlyDenseSourcesPerPolicy) {
  constexpr int64_t kSize = 179;
  auto a = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto a_value = DataSliceImpl::AllocateEmptyObjects(kSize);
  {  // merge sparse with dense overwrite
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(a, "a", a_value));
    ASSERT_OK(db->SetAttr(a[5], "a", a_value[7]));
    EXPECT_THAT(db->MergeInplace(*db2),
                AllOf(StatusIs(absl::StatusCode::kFailedPrecondition,
                               HasSubstr("conflict")),
                      PayloadIs<internal::DataBagMergeConflictError>()));

    ASSERT_OK(db->MergeInplace(
        *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
  {
    SCOPED_TRACE("merge dense with dense conflict");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    ASSERT_OK(db2->SetAttr(a, "a", a_value));
    ASSERT_OK(db2->SetAttr(a[0], "a", a_value[1]));
    EXPECT_THAT(db->MergeInplace(*db2),
                AllOf(StatusIs(absl::StatusCode::kFailedPrecondition,
                               HasSubstr("conflict")),
                      PayloadIs<internal::DataBagMergeConflictError>()));
    EXPECT_THAT(db2->MergeInplace(*db),
                AllOf(StatusIs(absl::StatusCode::kFailedPrecondition,
                               HasSubstr("conflict")),
                      PayloadIs<internal::DataBagMergeConflictError>()));
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
  for (auto [merge_options, via_update] :
       std::vector<std::pair<MergeOptions, bool>>{
           {MergeOptions(), false},
           {MergeOptions{.data_conflict_policy = MergeOptions::kKeepOriginal},
            false},
           {MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite},
            false},
           {MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite},
            true},
       }) {
    for (bool left : {true, false}) {
      if (via_update && !left) {
        continue;
      }
      for (int conflict_layer : {0, 1, 2, -1}) {
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
          if (via_update) {
            ASSERT_OK_AND_ASSIGN(auto update,
                                 db->CreateOverwritingMergeUpdate(*db2));
            ASSERT_OK(db->MergeInplace(*update, merge_options));
          } else {
            ASSERT_OK(db->MergeInplace(*db2, merge_options));
          }
          EXPECT_THAT(db->GetAttr(a, "a"),
                      IsOkAndHolds(ElementsAreArray(a_value_expected)));
        } else {
          if (conflict_layer != -1 && merge_options.data_conflict_policy ==
                                          MergeOptions::kRaiseOnConflict) {
            EXPECT_THAT(
                db2->MergeInplace(*db, merge_options),
                AllOf(StatusIs(absl::StatusCode::kFailedPrecondition,
                               HasSubstr("conflict")),
                      PayloadIs<internal::DataBagMergeConflictError>()));
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

TYPED_TEST(DataBagMergeTest, MergeObjectsOnlyDenseSources) {
  constexpr int64_t kSize = 179;
  auto a = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto a_value = DataSliceImpl::AllocateEmptyObjects(kSize);

  {  // merge dense with sparse
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
    ASSERT_OK(db2->SetAttr(a[5], "a", a_value[5]));
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
  {  // merge const dense with sparse
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK_AND_ASSIGN(auto x, db->CreateObjectsFromFields({"a"}, {a_value}));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
    EXPECT_THAT(db->GetAttr(x, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
    ASSERT_OK(db2->SetAttr(x[5], "a", a_value[5]));
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
    EXPECT_THAT(db->GetAttr(x, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
  {  // merge sparse with dense
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(a, "a", a_value));
    ASSERT_OK(db->SetAttr(a[5], "a", a_value[5]));
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
  {  // merge sparse with const dense
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK_AND_ASSIGN(auto x,
                         db2->CreateObjectsFromFields({"a"}, {a_value}));
    ASSERT_OK(db->SetAttr(x[5], "a", a_value[5]));
    ASSERT_OK(this->MergeInplaceWithOptions(*db2, *db));
    EXPECT_THAT(db2->GetAttr(x, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
  {
    SCOPED_TRACE("merge dense with dense");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    ASSERT_OK(db2->SetAttr(a, "a", a_value));
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
  {
    SCOPED_TRACE("merge empty with const dense");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK_AND_ASSIGN(
        auto obj, db2->CreateObjectsFromFields({"a"}, {std::cref(a_value)}));
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
    EXPECT_THAT(db->GetAttr(obj, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
}

TYPED_TEST(DataBagAllocatorTest, MergeObjectsOnlyViaUpdate) {
  const auto empty_db = DataBagImpl::CreateEmptyDatabag();
  {
    SCOPED_TRACE("merge empty with empty");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK_AND_ASSIGN(auto update1, db->CreateOverwritingMergeUpdate(*db));
    EXPECT_THAT(update1, DataBagEqual(empty_db));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK_AND_ASSIGN(auto update2, db->CreateOverwritingMergeUpdate(*db2));
    EXPECT_THAT(update2, DataBagEqual(empty_db));
  }
  {
    SCOPED_TRACE("merge with new attribute");
    auto obj = DataItem(this->AllocSingle());

    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(obj, "b", DataItem(42)));
    auto db_fork = db->PartiallyPersistentFork();
    ASSERT_OK(db_fork->SetAttr(obj, "a", DataItem(57)));
    ASSERT_OK_AND_ASSIGN(auto update,
                         db->CreateOverwritingMergeUpdate(*db_fork));
    EXPECT_THAT(db->GetAttr(obj, "b"), IsOkAndHolds(DataItem(42)))
        << "db should not be modified";
    auto expected_update = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(expected_update->SetAttr(obj, "a", DataItem(57)));

    EXPECT_THAT(update, DataBagEqual(expected_update));
  }
  {
    SCOPED_TRACE("merge with overwritten attribute");
    auto obj = DataItem(this->AllocSingle());

    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(obj, "b", DataItem(42)));
    auto original_db = db->PartiallyPersistentFork();
    auto db_fork = db->PartiallyPersistentFork();
    ASSERT_OK(db_fork->SetAttr(obj, "b", DataItem(57)));
    ASSERT_OK_AND_ASSIGN(auto update,
                         db->CreateOverwritingMergeUpdate(*db_fork));
    EXPECT_THAT(db->GetAttr(obj, "b"), IsOkAndHolds(DataItem(42)))
        << "db should not be modified";
    auto expected_update = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(expected_update->SetAttr(obj, "b", DataItem(57)));

    EXPECT_THAT(update, DataBagEqual(expected_update));
  }
}

TYPED_TEST(DataBagAllocatorTest, MergeListsOnlyViaUpdate) {
  constexpr int64_t kSize = 7;
  {
    SCOPED_TRACE("merge with non existing");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> lists(kSize);
    for (int64_t i = 0; i < kSize; ++i) {
      lists[i] = DataItem(this->AllocSingleList());
      ASSERT_OK(db2->AppendToList(lists[i], DataItem(i)));
    }
    ASSERT_OK_AND_ASSIGN(auto update, db->CreateOverwritingMergeUpdate(*db2));
    EXPECT_THAT(update, DataBagEqual(db2));
  }
  {
    SCOPED_TRACE("merge with existing");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> lists(kSize);
    for (int64_t i = 0; i < kSize; ++i) {
      lists[i] = DataItem(this->AllocSingleList());
      ASSERT_OK(db->AppendToList(lists[i], DataItem(i)));
      ASSERT_OK(db2->AppendToList(lists[i], DataItem(i + kSize)));
    }
    ASSERT_OK_AND_ASSIGN(auto update, db->CreateOverwritingMergeUpdate(*db2));
    EXPECT_THAT(update, DataBagEqual(db2));
  }
  {
    SCOPED_TRACE("merge with fork");
    auto db = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> lists(kSize);
    for (int64_t i = 0; i < kSize; i += 2) {
      lists[i] = DataItem(this->AllocSingleList());
      ASSERT_OK(db->AppendToList(lists[i], DataItem(i)));
    }
    auto db2 = db->PartiallyPersistentFork();
    for (int64_t i = 1; i < kSize; i += 2) {
      lists[i] = DataItem(this->AllocSingleList());
      ASSERT_OK(db2->AppendToList(lists[i], DataItem(i + kSize)));
    }
    ASSERT_OK_AND_ASSIGN(auto update, db->CreateOverwritingMergeUpdate(*db2));
    // Update must contain new data, but may contain old data as well.
    for (int64_t i = 1; i < kSize; i += 2) {
      EXPECT_THAT(update->GetListSize(lists[i]), IsOkAndHolds(DataItem(1)));
      EXPECT_THAT(update->GetFromList(lists[i], 0),
                  IsOkAndHolds(DataItem(i + kSize)));
    }
  }
}

TYPED_TEST(DataBagMergeTest, MergeToDenseAllRemoved) {
  for (int64_t size : {1, 3, 16, 37, 128, 512, 1034}) {
    auto a = DataSliceImpl::AllocateEmptyObjects(size);
    auto a_value = DataSliceImpl::CreateEmptyAndUnknownType(size);
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));

    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(a, "a", a_value));

    db2 = db2->PartiallyPersistentFork();
    // Now remove the value in sparse source.
    ASSERT_OK(db2->SetAttr(a[0], "a", a_value[0]));

    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
}

TYPED_TEST(DataBagMergeTest, MergeObjectsOverwriteOnlyDenseSources) {
  if (this->merge_options().data_conflict_policy != MergeOptions::kOverwrite) {
    GTEST_SKIP() << "Only test overwrite policy";
  }
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
    ASSERT_OK(db2->SetAttr(a[3], "a", DataItem()));
    ASSERT_OK(db2->SetAttr(a[5], "a", DataItem(57)));
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
    std::vector<DataItem> a_value_expected(a_value.begin(), a_value.end());
    a_value_expected[2] = DataItem(17.0);
    a_value_expected[3] = DataItem();
    a_value_expected[5] = DataItem(57);
    EXPECT_THAT(db->GetAttr(a, "a"),
                IsOkAndHolds(ElementsAreArray(a_value_expected)));
  }
  {
    SCOPED_TRACE("merge dense with empty sparse overwrite nothing");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(a_value)));
  }
  {
    SCOPED_TRACE("merge dense with dense overwrite all");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    auto b_value = DataSliceImpl::AllocateEmptyObjects(kSize);
    ASSERT_OK(db2->SetAttr(a, "a", b_value));
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
    EXPECT_THAT(db->GetAttr(a, "a"), IsOkAndHolds(ElementsAreArray(b_value)));
  }
  {
    SCOPED_TRACE("merge dense with dense overwrite all but one");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    auto db2 = DataBagImpl::CreateEmptyDatabag();

    auto b_value = DataSliceImpl::AllocateEmptyObjects(kSize);
    SliceBuilder a_but_one(a.size());
    for (int i = 0; i < a.size(); ++i) {
      if (i != 5) {
        a_but_one.InsertIfNotSetAndUpdateAllocIds(i, a[i]);
      }
    }
    ASSERT_OK(db2->SetAttr(std::move(a_but_one).Build(), "a", b_value));
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));

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
    SliceBuilder a_but_one(a.size());
    for (int i = 0; i < a.size(); ++i) {
      if (i != 5) {
        a_but_one.InsertIfNotSetAndUpdateAllocIds(i, a[i]);
      }
    }
    ASSERT_OK(db2->SetAttr(std::move(a_but_one).Build(), "a", b_value));
    ASSERT_OK(db2->SetAttr(a[1], "a", DataItem(27.0)));
    ASSERT_OK(db2->SetAttr(a[3], "a", DataItem(57)));
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));

    std::vector<DataItem> a_value_expected(b_value.begin(), b_value.end());
    a_value_expected[1] = DataItem(27.0);
    a_value_expected[3] = DataItem(57);
    a_value_expected[5] = a_value[5];
    EXPECT_THAT(db->GetAttr(a, "a"),
                IsOkAndHolds(ElementsAreArray(a_value_expected)));
  }
  {
    SCOPED_TRACE("merge dense with dense overwrite all removed");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", a_value));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(a, "a", DataSliceImpl::AllocateEmptyObjects(kSize)));
    ASSERT_OK(
        db2->SetAttr(a, "a", DataSliceImpl::CreateEmptyAndUnknownType(kSize)));
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
    EXPECT_THAT(db->GetAttr(a, "a"),
                IsOkAndHolds(ElementsAreArray(
                    std::vector<DataItem>(kSize, DataItem()))));
  }
}

TYPED_TEST(DataBagMergeTest,
           MergeObjectsOverwriteDenseSparseInNonForkedRhsBag) {
  if (this->merge_options().data_conflict_policy != MergeOptions::kOverwrite) {
    GTEST_SKIP() << "Only test overwrite policy";
  }
  constexpr int64_t kSize = 179;
  auto a_value = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto b_value = DataSliceImpl::AllocateEmptyObjects(kSize);

  auto db = DataBagImpl::CreateEmptyDatabag();
  auto db2 = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(
      auto a, db2->CreateObjectsFromFields({"a"}, {std::cref(b_value)}));
  ASSERT_OK(db->SetAttr(a, "a", a_value));

  ASSERT_OK(db2->SetAttr(a[5], "a", DataItem()));
  ASSERT_OK(db->MergeInplace(
      *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));

  std::vector<DataItem> a_value_expected(b_value.begin(), b_value.end());
  a_value_expected[5] = DataItem();
  EXPECT_THAT(db->GetAttr(a, "a"),
              IsOkAndHolds(ElementsAreArray(a_value_expected)));
}

TYPED_TEST(DataBagMergeTest, MergeObjectsOverwriteDenseSparseInForkedRhsBag) {
  if (this->merge_options().data_conflict_policy != MergeOptions::kOverwrite) {
    GTEST_SKIP() << "Only test overwrite policy";
  }
  constexpr int64_t kSize = 179;
  auto a = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto a_value = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto b_value = DataSliceImpl::AllocateEmptyObjects(kSize);

  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetAttr(a, "a", a_value));

  auto db2 = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db2->SetAttr(a, "a", b_value));
  db2 = db2->PartiallyPersistentFork();

  ASSERT_OK(db2->SetAttr(a[5], "a", DataItem()));
  ASSERT_OK(db->MergeInplace(
      *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));

  std::vector<DataItem> a_value_expected(b_value.begin(), b_value.end());
  a_value_expected[5] = DataItem();
  EXPECT_THAT(db->GetAttr(a, "a"),
              IsOkAndHolds(ElementsAreArray(a_value_expected)));
}

TYPED_TEST(DataBagAllocatorTest, MergeObjectAttrsOnlyConflicts) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto a = DataItem(this->AllocSingle());
  auto db1 = db->PartiallyPersistentFork();
  ASSERT_OK(db1->SetAttr(a, "x", DataItem(57)));
  auto db2 = db->PartiallyPersistentFork();
  ASSERT_OK(db2->SetAttr(a, "x", DataItem(75.0)));
  EXPECT_THAT(db1->MergeInplace(*db2),
              AllOf(StatusIs(absl::StatusCode::kFailedPrecondition,
                             HasSubstr("conflict")),
                    PayloadIs<internal::DataBagMergeConflictError>()));

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

TYPED_TEST(DataBagMergeTest, MergeObjectAttrsOnlyLongForks) {
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
  ASSERT_OK(this->MergeInplaceWithOptions(*db1, *db2));
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
  {
    SCOPED_TRACE("merge with non existing");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> lists(kSize);
    for (int64_t i = 0; i < kSize; ++i) {
      lists[i] = DataItem(this->AllocSingleList());
      ASSERT_OK(db2->AppendToList(lists[i], DataItem(i)));
    }
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
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
    MergeOptions raise_on_conflict = this->merge_options();
    raise_on_conflict.data_conflict_policy = MergeOptions::kRaiseOnConflict;
    EXPECT_THAT(db->MergeInplace(*db2, raise_on_conflict),
                StatusIs(absl::StatusCode::kFailedPrecondition,
                         HasSubstr("conflicting list sizes")));
    MergeOptions allow_override = this->merge_options();
    allow_override.data_conflict_policy = MergeOptions::kOverwrite;
    ASSERT_OK(db->MergeInplace(*db2, allow_override));
    verify_lists(lists, db.get());
  }
  {
    SCOPED_TRACE("merge non existing");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> lists(kSize);
    for (int64_t i = 0; i < kSize; ++i) {
      lists[i] = DataItem(this->AllocSingleList());
      // create empty lists in db
      if (i % 2 == 0) {
        ASSERT_OK(db->AppendToList(lists[i], DataItem(i)));
      } else {
        ASSERT_OK(db2->AppendToList(lists[i], DataItem(i)));
      }
    }
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
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
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
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
    auto status = this->MergeInplaceWithOptions(*db, *db2);
    auto merge_options = this->merge_options();
    if (merge_options.data_conflict_policy == MergeOptions::kRaiseOnConflict) {
      EXPECT_THAT(status,
                  StatusIs(absl::StatusCode::kFailedPrecondition,
                           HasSubstr("conflict")));
      EXPECT_THAT(
          arolla::GetPayload<internal::DataBagMergeConflictError>(status),
          NotNull());
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
    auto status = this->MergeInplaceWithOptions(*db, *db2);
    auto merge_options = this->merge_options();
    if (merge_options.data_conflict_policy == MergeOptions::kRaiseOnConflict) {
      EXPECT_THAT(status,
                  StatusIs(absl::StatusCode::kFailedPrecondition,
                           HasSubstr("conflict")));
      EXPECT_THAT(
          arolla::GetPayload<internal::DataBagMergeConflictError>(status),
          NotNull());
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
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
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
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
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
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
    verify_lists(lists, db.get());
  }
}

TYPED_TEST(DataBagMergeTest, MergeDictsOnly) {
  auto [a, b, c, k, k2] = std::array{
      DataItem(this->AllocSingleDict()), DataItem(this->AllocSingleDict()),
      DataItem(this->AllocSingleDict()), DataItem(this->AllocSingle()),
      DataItem(this->AllocSingle()),
  };

  {
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetInDict(a, k, DataItem(57)));
    ASSERT_OK(this->MergeInplaceWithOptions(*db, *db));
    EXPECT_THAT(db->GetFromDict(a, k), IsOkAndHolds(DataItem(57)));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(this->MergeInplaceWithOptions(*db2, *db));
    EXPECT_THAT(db2->GetFromDict(a, k), IsOkAndHolds(DataItem(57)));
  }

  {
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetInDict(a, k, DataItem(57)));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetInDict(b, k, DataItem(37)));
    ASSERT_OK(this->MergeInplaceWithOptions(*db2, *db));
    EXPECT_THAT(db2->GetFromDict(a, k), IsOkAndHolds(DataItem(57)));
    EXPECT_THAT(db2->GetFromDict(b, k), IsOkAndHolds(DataItem(37)));
  }

  {
    SCOPED_TRACE("merging into unmodified fork");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetInDict(a, k, DataItem(57)));
    auto db_fork = db->PartiallyPersistentFork();
    ASSERT_OK(this->MergeInplaceWithOptions(*db_fork, *db));
    EXPECT_THAT(db_fork->GetFromDict(a, k), IsOkAndHolds(DataItem(57)));
  }

  {
    SCOPED_TRACE("merging into modified fork");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetInDict(a, k, DataItem(57)));
    auto db_fork = db->PartiallyPersistentFork();
    ASSERT_OK(db_fork->SetInDict(b, k, DataItem(arolla::Text("ba"))));
    ASSERT_OK(db_fork->SetInDict(b, k2, DataItem(75.0)));
    ASSERT_OK(this->MergeInplaceWithOptions(*db_fork, *db));
    EXPECT_THAT(db_fork->GetFromDict(a, k), IsOkAndHolds(DataItem(57)));
    EXPECT_THAT(db_fork->GetFromDict(b, k2), IsOkAndHolds(DataItem(75.0)));
    EXPECT_THAT(db_fork->GetFromDict(b, k), IsOkAndHolds(arolla::Text("ba")));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(this->MergeInplaceWithOptions(*db2, *db_fork));
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
    auto status = this->MergeInplaceWithOptions(*db, *db2);
    auto merge_options = this->merge_options();
    if (merge_options.data_conflict_policy == MergeOptions::kRaiseOnConflict) {
      EXPECT_THAT(
          status,
          StatusIs(
              absl::StatusCode::kFailedPrecondition,
              MatchesRegex(
                  R"regex(conflicting dict values for .* key .*: 57 vs 75)regex")));
      EXPECT_THAT(
          arolla::GetPayload<internal::DataBagMergeConflictError>(status),
          NotNull());
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
    ASSERT_OK(this->MergeInplaceWithOptions(*db_fork, *db));
    auto check_dicts = [&](auto db) {
      EXPECT_THAT(db->GetFromDict(a, k), IsOkAndHolds(DataItem(57)));
      EXPECT_THAT(db->GetFromDict(b, k), IsOkAndHolds(arolla::Text("ba")));
      EXPECT_THAT(db->GetFromDict(c, k), IsOkAndHolds(arolla::Bytes("ca")));
    };
    check_dicts(db_fork);

    {
      auto db2 = DataBagImpl::CreateEmptyDatabag();
      ASSERT_OK(this->MergeInplaceWithOptions(*db2, *db_fork));
      check_dicts(db2);
    }
    {
      auto db2 = DataBagImpl::CreateEmptyDatabag();
      ASSERT_OK(db2->SetInDict(b, k, DataItem(arolla::Text("NOT_USED"))));
      db2 = db2->PartiallyPersistentFork();
      ASSERT_OK(db2->SetInDict(b, k, DataItem(arolla::Text("ba"))));
      ASSERT_OK(this->MergeInplaceWithOptions(*db2, *db_fork));
      check_dicts(db2);
    }
  }
}

TYPED_TEST(DataBagAllocatorTest, MergeDictsOnlyViaUpdate) {
  auto [d, k, k2] =
      std::array{DataItem(this->AllocSingleDict()),
                 DataItem(this->AllocSingle()), DataItem(this->AllocSingle())};
  {
    SCOPED_TRACE("merge with non existing");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetInDict(d, k, DataItem(57)));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK_AND_ASSIGN(auto update, db2->CreateOverwritingMergeUpdate(*db));
    EXPECT_THAT(update, DataBagEqual(db));
  }
  {
    SCOPED_TRACE("merge with existing");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetInDict(d, k, DataItem(57)));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetInDict(d, k2, DataItem(37)));
    ASSERT_OK_AND_ASSIGN(auto update, db2->CreateOverwritingMergeUpdate(*db));
    EXPECT_THAT(update, DataBagEqual(db));
  }
  {
    constexpr int64_t kSize = 7;
    SCOPED_TRACE("merge with fork");
    auto db = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> dicts(kSize);
    for (int64_t i = 0; i < kSize; i += 2) {
      dicts[i] = DataItem(this->AllocSingleDict());
      ASSERT_OK(db->SetInDict(dicts[i], k, DataItem(i)));
    }
    auto db2 = db->PartiallyPersistentFork();
    for (int64_t i = 1; i < kSize; i += 2) {
      dicts[i] = DataItem(this->AllocSingleDict());
      ASSERT_OK(db2->SetInDict(dicts[i], k, DataItem(i)));
    }
    ASSERT_OK_AND_ASSIGN(auto update, db->CreateOverwritingMergeUpdate(*db2));
    for (int64_t i = 1; i < kSize; i += 2) {
      EXPECT_THAT(update->GetFromDict(dicts[i], k), IsOkAndHolds(DataItem(i)));
    }
  }
}

TYPED_TEST(DataBagMergeTest, MergeDictsOnlyFork) {
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
  ASSERT_OK(this->MergeInplaceWithOptions(*db, *db2));
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
    absl::Status status = res->MergeInplace(
        *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite});
    EXPECT_THAT(status, StatusIs(absl::StatusCode::kFailedPrecondition,
                                 HasSubstr("conflict")));

    EXPECT_THAT(arolla::GetPayload<internal::DataBagMergeConflictError>(status),
                NotNull());
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
    absl::Status status = res->MergeInplace(
        *db2,
        MergeOptions{.data_conflict_policy = MergeOptions::kKeepOriginal});
    EXPECT_THAT(status, StatusIs(absl::StatusCode::kFailedPrecondition,
                                 HasSubstr("conflict")));
    EXPECT_THAT(arolla::GetPayload<internal::DataBagMergeConflictError>(status),
                NotNull());
  }
  {
    SCOPED_TRACE("raise on conflict");
    auto res = db->PartiallyPersistentFork();
    absl::Status status = res->MergeInplace(*db2, MergeOptions());
    EXPECT_THAT(status, StatusIs(absl::StatusCode::kFailedPrecondition,
                                 HasSubstr("conflict")));
    EXPECT_THAT(arolla::GetPayload<internal::DataBagMergeConflictError>(status),
                NotNull());
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

template <typename ItemSet>
struct DataBagNaNMergeTest : public ::testing::Test {
  DataItem BaseItem() { return ItemSet().BaseItem(); }
  DataItem OtherItem() { return ItemSet().OtherItem(); }
  void Verify(const absl::Status& status) {
    if (ItemSet().IsConflict()) {
      ASSERT_THAT(status, StatusIs(absl::StatusCode::kFailedPrecondition,
                                   HasSubstr("conflict")));
      EXPECT_THAT(
          arolla::GetPayload<internal::DataBagMergeConflictError>(status),
          NotNull());
    } else {
      ASSERT_OK(status);
    }
  }
};

struct TwoNaNsDoubleSet {
  DataItem BaseItem() {
    return DataItem(std::numeric_limits<double>::quiet_NaN());
  }
  DataItem OtherItem() {
    return DataItem(std::numeric_limits<double>::signaling_NaN());
  }
  bool IsConflict() { return false; }
};

struct NaNVsZeroDoubleSet {
  DataItem BaseItem() {
    return DataItem(std::numeric_limits<double>::quiet_NaN());
  }
  DataItem OtherItem() { return DataItem(0.0); }
  bool IsConflict() { return true; }
};

struct ZeroVsNaNDoubleSet {
  DataItem BaseItem() { return DataItem(0.0); }
  DataItem OtherItem() {
    return DataItem(std::numeric_limits<double>::quiet_NaN());
  }
  bool IsConflict() { return true; }
};

struct TwoNaNsFloatSet {
  DataItem BaseItem() {
    return DataItem(std::numeric_limits<float>::quiet_NaN());
  }
  DataItem OtherItem() {
    return DataItem(std::numeric_limits<float>::signaling_NaN());
  }
  bool IsConflict() { return false; }
};

// Currently a float vs double is a conflict in dense mode, but not a conflict
// in sparse mode. Assuming it is considered undefined behavior, not testing
// it for now.
// struct TwoNaNsMixedSet {
//   DataItem BaseItem() {
//     return DataItem(std::numeric_limits<double>::quiet_NaN());
//   }
//   DataItem OtherItem() {
//     return DataItem(std::numeric_limits<float>::signaling_NaN());
//   }
//   bool IsConflict() { return false; }
// };

using ItemSets = ::testing::Types<TwoNaNsDoubleSet, NaNVsZeroDoubleSet,
                                  ZeroVsNaNDoubleSet, TwoNaNsFloatSet>;
TYPED_TEST_SUITE(DataBagNaNMergeTest, ItemSets);

TYPED_TEST(DataBagNaNMergeTest, NaNs) {
  constexpr int64_t kSize = 179;
  auto a = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto base_value = DataSliceImpl::Create(kSize, this->BaseItem());
  auto other_value = DataSliceImpl::Create(kSize, this->OtherItem());
  auto other_type_value = DataSliceImpl::Create(kSize, DataItem("foo"));

  {
    SCOPED_TRACE("merge sparse with sparse");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a[5], "a", base_value[5]));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(a[5], "a", other_value[5]));
    this->Verify(db->MergeInplace(*db2));
    this->Verify(db2->MergeInplace(*db));
  }
  {
    SCOPED_TRACE("merge dense with sparse");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", base_value));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(a[5], "a", other_value[5]));
    this->Verify(db->MergeInplace(*db2));
    this->Verify(db2->MergeInplace(*db));
  }
  {
    SCOPED_TRACE("merge dense with dense");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", base_value));
    ASSERT_OK(db2->SetAttr(a, "a", base_value));
    ASSERT_OK(db2->SetAttr(a[0], "a", other_value[0]));
    this->Verify(db->MergeInplace(*db2));
    this->Verify(db2->MergeInplace(*db));
  }
  {
    SCOPED_TRACE("merge multitype dense with dense");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", base_value));
    ASSERT_OK(db2->SetAttr(a, "a", other_value));
    ASSERT_OK(db->SetAttr(a[5], "a", other_type_value[5]));
    ASSERT_OK(db2->SetAttr(a[5], "a", other_type_value[5]));
    this->Verify(db->MergeInplace(*db2));
    this->Verify(db2->MergeInplace(*db));
  }
  {
    SCOPED_TRACE("merge multitype dense with dense");
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", base_value));
    ASSERT_OK(db2->SetAttr(a, "a", base_value));
    ASSERT_OK(db->SetAttr(a[5], "a", other_type_value[5]));
    ASSERT_OK(db2->SetAttr(a[5], "a", other_type_value[5]));
    ASSERT_OK(db2->SetAttr(a[0], "a", other_value[0]));
    this->Verify(db->MergeInplace(*db2));
    this->Verify(db2->MergeInplace(*db));
  }
  {
    SCOPED_TRACE("merge dense with dense+sparse");
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(a, "a", base_value));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(a, "a", base_value));
    auto db3 = db2->PartiallyPersistentFork();
    ASSERT_OK(db3->SetAttr(a[5], "a", other_value[5]));
    this->Verify(db->MergeInplace(*db3));
    this->Verify(db3->MergeInplace(*db));
  }
  {
    SCOPED_TRACE("merge small with small");
    auto b = DataItem(AllocateSingleObject());
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetAttr(b, "a", base_value[0]));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetAttr(b, "a", other_value[0]));
    this->Verify(db->MergeInplace(*db2));
    this->Verify(db2->MergeInplace(*db));
  }
  {
    SCOPED_TRACE("merge lists");
    auto b = DataItem(AllocateSingleList());
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->AppendToList(b, base_value[0]));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->AppendToList(b, other_value[0]));
    this->Verify(db->MergeInplace(*db2));
    this->Verify(db2->MergeInplace(*db));
  }
  {
    SCOPED_TRACE("merge dicts");
    auto b = DataItem(AllocateSingleDict());
    auto db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db->SetInDict(b, DataItem(0), base_value[0]));
    auto db2 = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(db2->SetInDict(b, DataItem(0), other_value[0]));
    this->Verify(db->MergeInplace(*db2));
    this->Verify(db2->MergeInplace(*db));
  }
}

TEST(DataBagTest, DataBagSmallRemoveOverwriteTest) {
  auto b = DataItem(AllocateSingleObject());
  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetAttr(b, "a", DataItem(1)));
  auto db2 = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db2->SetAttr(b, "a", DataItem()));
  ASSERT_OK(db->MergeInplace(
      *db2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));
  EXPECT_THAT(db->GetAttr(b, "a"), IsOkAndHolds(DataItem()));
}

TYPED_TEST(DataBagMergeTest, MergeSiblingForks) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto a = DataItem(this->AllocSingle());
  auto b = DataItem(this->AllocSingle());

  ASSERT_OK(db->SetAttr(a, "x", DataItem(57)));
  ASSERT_OK(db->SetAttr(b, "y", DataItem(75.0)));

  // Two forks of the same parent.
  auto fork1 = db->PartiallyPersistentFork();
  auto fork2 = db->PartiallyPersistentFork();

  // Merging unmodified sibling fork should be a no-op.
  {
    ASSERT_OK(this->MergeInplaceWithOptions(*fork1, *fork2));
    EXPECT_THAT(fork1->GetAttr(a, "x"), IsOkAndHolds(DataItem(57)));
    EXPECT_THAT(fork1->GetAttr(b, "y"), IsOkAndHolds(DataItem(75.0)));
  }

  // Modify fork2 and merge into fork1 — should pick up the new data.
  {
    ASSERT_OK(fork2->SetAttr(a, "z", DataItem(99)));
    ASSERT_OK(this->MergeInplaceWithOptions(*fork1, *fork2));
    EXPECT_THAT(fork1->GetAttr(a, "x"), IsOkAndHolds(DataItem(57)));
    EXPECT_THAT(fork1->GetAttr(a, "z"), IsOkAndHolds(DataItem(99)));
    EXPECT_THAT(fork1->GetAttr(b, "y"), IsOkAndHolds(DataItem(75.0)));
  }

  // Merge modified fork1 into a fresh fork3 (unmodified sibling of fork2).
  {
    auto fork3 = db->PartiallyPersistentFork();
    ASSERT_OK(this->MergeInplaceWithOptions(*fork3, *fork1));
    EXPECT_THAT(fork3->GetAttr(a, "x"), IsOkAndHolds(DataItem(57)));
    EXPECT_THAT(fork3->GetAttr(a, "z"), IsOkAndHolds(DataItem(99)));
    EXPECT_THAT(fork3->GetAttr(b, "y"), IsOkAndHolds(DataItem(75.0)));
  }
}

TYPED_TEST(DataBagAllocatorTest, MergeModifiedSiblingForks_Modified) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto object_f1 = DataItem(this->AllocSingle());
  auto object_f2 = DataItem(this->AllocSingle());
  auto object_none = DataItem(this->AllocSingle());

  auto list_f1 = DataItem(this->AllocSingleList());
  auto list_f2 = DataItem(this->AllocSingleList());
  auto list_none = DataItem(this->AllocSingleList());

  auto dict_f1 = DataItem(this->AllocSingleDict());
  auto dict_f2 = DataItem(this->AllocSingleDict());
  auto dict_none = DataItem(this->AllocSingleDict());

  ASSERT_OK(db->SetAttr(object_f1, "val", DataItem(1)));
  ASSERT_OK(db->SetAttr(object_f2, "val", DataItem(2)));
  ASSERT_OK(db->SetAttr(object_none, "val", DataItem(3)));

  ASSERT_OK(db->AppendToList(list_f1, DataItem(1)));
  ASSERT_OK(db->AppendToList(list_f2, DataItem(2)));
  ASSERT_OK(db->AppendToList(list_none, DataItem(3)));

  ASSERT_OK(db->SetInDict(dict_f1, DataItem("k1"), DataItem(1)));
  ASSERT_OK(db->SetInDict(dict_f2, DataItem("k2"), DataItem(2)));
  ASSERT_OK(db->SetInDict(dict_none, DataItem("k3"), DataItem(3)));

  auto fork1 = db->PartiallyPersistentFork();
  auto fork2 = db->PartiallyPersistentFork();

  // Modify *f1 objects in fork1
  ASSERT_OK(fork1->SetAttr(object_f1, "val2", DataItem(11)));
  ASSERT_OK(fork1->AppendToList(list_f1, DataItem(11)));
  ASSERT_OK(fork1->SetInDict(dict_f1, DataItem("k11"), DataItem(11)));

  // Add completely new objects in fork1
  auto new_obj_f1 = DataItem(this->AllocSingle());
  auto new_list_f1 = DataItem(this->AllocSingleList());
  auto new_dict_f1 = DataItem(this->AllocSingleDict());
  ASSERT_OK(fork1->SetAttr(new_obj_f1, "val", DataItem(111)));
  ASSERT_OK(fork1->AppendToList(new_list_f1, DataItem(111)));
  ASSERT_OK(fork1->SetInDict(new_dict_f1, DataItem("x"), DataItem(111)));

  // Modify *f2 objects in fork2
  ASSERT_OK(fork2->SetAttr(object_f2, "val2", DataItem(22)));
  ASSERT_OK(fork2->AppendToList(list_f2, DataItem(22)));
  ASSERT_OK(fork2->SetInDict(dict_f2, DataItem("k22"), DataItem(22)));

  // Add completely new objects in fork2
  auto new_obj_f2 = DataItem(this->AllocSingle());
  auto new_list_f2 = DataItem(this->AllocSingleList());
  auto new_dict_f2 = DataItem(this->AllocSingleDict());
  ASSERT_OK(fork2->SetAttr(new_obj_f2, "val", DataItem(222)));
  ASSERT_OK(fork2->AppendToList(new_list_f2, DataItem(222)));
  ASSERT_OK(fork2->SetInDict(new_dict_f2, DataItem("y"), DataItem(222)));

  // Merge fork2 into fork1 with Overwrite to bypass SameAlloc list conflicts
  ASSERT_OK(fork1->MergeInplace(
      *fork2, MergeOptions{.data_conflict_policy = MergeOptions::kOverwrite}));

  // Assert unmodified objects are broadly unchanged
  EXPECT_THAT(fork1->GetAttr(object_none, "val"), IsOkAndHolds(DataItem(3)));
  EXPECT_THAT(fork1->GetAttr(object_none, "val2"), IsOkAndHolds(DataItem()));
  EXPECT_THAT(fork1->GetFromDict(dict_none, DataItem("k3")),
              IsOkAndHolds(DataItem(3)));

  // Assert objects updated in fork1 are still there (Objects merge granularly)
  EXPECT_THAT(fork1->GetAttr(object_f1, "val"), IsOkAndHolds(DataItem(1)));
  EXPECT_THAT(fork1->GetAttr(object_f1, "val2"), IsOkAndHolds(DataItem(11)));
  EXPECT_THAT(fork1->GetAttr(new_obj_f1, "val"), IsOkAndHolds(DataItem(111)));
  EXPECT_THAT(fork1->GetListSize(new_list_f1),
              AnyOf(IsOkAndHolds(DataItem(1)), IsOkAndHolds(DataItem(2))));
  EXPECT_THAT(fork1->GetFromDict(new_dict_f1, DataItem("x")),
              IsOkAndHolds(DataItem(111)));

  // Assert objects updated in fork2 were successfully merged into fork1
  EXPECT_THAT(fork1->GetAttr(object_f2, "val"), IsOkAndHolds(DataItem(2)));
  EXPECT_THAT(fork1->GetAttr(object_f2, "val2"), IsOkAndHolds(DataItem(22)));
  EXPECT_THAT(fork1->GetListSize(list_f2),
              AnyOf(IsOkAndHolds(DataItem(1)), IsOkAndHolds(DataItem(2))));
  EXPECT_THAT(fork1->GetFromDict(dict_f2, DataItem("k22")),
              IsOkAndHolds(DataItem(22)));
  EXPECT_THAT(fork1->GetAttr(new_obj_f2, "val"), IsOkAndHolds(DataItem(222)));
}

}  // namespace
}  // namespace koladata::internal
