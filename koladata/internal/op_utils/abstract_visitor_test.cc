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
#include "koladata/internal/op_utils/abstract_visitor.h"

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qtype/qtype_traits.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::CreateDenseArray;
using ::testing::ElementsAre;
using ::testing::Pair;

// A visitor that relies on the default previsit implementations.
class NoOpVisitor : public AbstractVisitor {
 public:
  absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                    const DataItem& schema) override {
    return item;
  }

  absl::Status VisitList(const DataItem& list, const DataItem& schema,
                         bool is_object_schema,
                         const DataSliceImpl& items) override {
    return absl::OkStatus();
  }

  absl::Status VisitDict(const DataItem& dict, const DataItem& schema,
                         bool is_object_schema, const DataSliceImpl& keys,
                         const DataSliceImpl& values) override {
    return absl::OkStatus();
  }

  absl::Status VisitObject(const DataItem& object, const DataItem& schema,
                           bool is_object_schema,
                           absl::Span<const absl::string_view> attr_names,
                           absl::Span<const DataItem> attr_values) override {
    return absl::OkStatus();
  }

  absl::Status VisitSchema(const DataItem& item, const DataItem& schema,
                           bool is_object_schema,
                           absl::Span<const absl::string_view> attr_names,
                           absl::Span<const DataItem> attr_schema) override {
    return absl::OkStatus();
  }
};

TEST(SameTokenForAllObjectsTest, ObjectIds) {
  auto ds = DataSliceImpl::AllocateEmptyObjects(3);
  ASSERT_OK_AND_ASSIGN(auto tokens, SameTokenForAllObjects(ds));
  EXPECT_EQ(tokens.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(tokens, ElementsAre(ds[0], ds[0], ds[0]));
}

TEST(SameTokenForAllObjectsTest, SparseObjectIds) {
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {DataItem(), obj_ids[1], DataItem(), obj_ids[2], obj_ids[0]}));
  ASSERT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
  ASSERT_OK_AND_ASSIGN(auto tokens, SameTokenForAllObjects(ds));
  EXPECT_EQ(tokens.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(tokens, ElementsAre(DataItem(), obj_ids[1], DataItem(),
                                  obj_ids[1], obj_ids[1]));
}

TEST(SameTokenForAllObjectsTest, MixedSlice) {
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
  auto ds = DataSliceImpl::Create(
      CreateDenseArray<DataItem>({DataItem(), DataItem(3), obj_ids[1],
                                  DataItem(schema::kInt32), obj_ids[2]}));
  ASSERT_TRUE(ds.is_mixed_dtype());
  ASSERT_OK_AND_ASSIGN(auto tokens, SameTokenForAllObjects(ds));
  EXPECT_EQ(tokens.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(tokens, ElementsAre(DataItem(), DataItem(), obj_ids[1],
                                  DataItem(), obj_ids[1]));
}

TEST(SameTokenForAllObjectsTest, NoObjectIds) {
  ASSERT_OK_AND_ASSIGN(
      auto tokens,
      SameTokenForAllObjects(DataSliceImpl::Create(CreateDenseArray<DataItem>(
          {DataItem(1), DataItem(), DataItem("a")}))));
  EXPECT_EQ(tokens.size(), 3);
  EXPECT_EQ(tokens.present_count(), 0);

  ASSERT_OK_AND_ASSIGN(
      auto empty_tokens,
      SameTokenForAllObjects(DataSliceImpl::CreateEmptyAndUnknownType(2)));
  EXPECT_EQ(empty_tokens.size(), 2);
  EXPECT_EQ(empty_tokens.present_count(), 0);
}

TEST(SameTokenForObjectsIfTest, CallsPredicateForPresentItemsInOrder) {
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(2);
  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {obj_ids[0], DataItem(), DataItem(5), obj_ids[1]}));
  std::vector<std::pair<int64_t, DataItem>> calls;
  ASSERT_OK(SameTokenForObjectsIf(
      ds, [&](int64_t i, const DataItem& item) -> absl::StatusOr<bool> {
        calls.push_back({i, item});
        return true;
      }));
  EXPECT_THAT(calls, ElementsAre(Pair(0, obj_ids[0]), Pair(2, DataItem(5)),
                                 Pair(3, obj_ids[1])));
}

TEST(SameTokenForObjectsIfTest, TokenOfFirstTraversedObjectId) {
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {obj_ids[0], DataItem(5), DataItem(), obj_ids[1], obj_ids[2]}));
  ASSERT_OK_AND_ASSIGN(
      auto tokens,
      SameTokenForObjectsIf(
          ds, [&](int64_t i, const DataItem& item) -> absl::StatusOr<bool> {
            return item != obj_ids[0];
          }));
  EXPECT_EQ(tokens.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(tokens, ElementsAre(DataItem(), DataItem(), DataItem(),
                                  obj_ids[1], obj_ids[1]));
}

TEST(SameTokenForObjectsIfTest, NothingTraversed) {
  auto ds = DataSliceImpl::AllocateEmptyObjects(3);
  ASSERT_OK_AND_ASSIGN(
      auto tokens,
      SameTokenForObjectsIf(
          ds, [](int64_t i, const DataItem& item) -> absl::StatusOr<bool> {
            return false;
          }));
  EXPECT_EQ(tokens.size(), 3);
  EXPECT_EQ(tokens.present_count(), 0);
}

TEST(SameTokenForObjectsIfTest, PropagatesErrors) {
  auto ds = DataSliceImpl::AllocateEmptyObjects(3);
  std::vector<int64_t> calls;
  EXPECT_THAT(SameTokenForObjectsIf(
                  ds,
                  [&](int64_t i, const DataItem& item) -> absl::StatusOr<bool> {
                    calls.push_back(i);
                    if (i == 1) {
                      return absl::InvalidArgumentError("fail");
                    }
                    return true;
                  }),
              StatusIs(absl::StatusCode::kInvalidArgument, "fail"));
  EXPECT_THAT(calls, ElementsAre(0, 1));
}

TEST(AbstractVisitorTest, DefaultPrevisit) {
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(2);
  NoOpVisitor visitor;
  EXPECT_THAT(visitor.Previsit(obj_ids[0], DataItem(schema::kObject),
                               /*transition_key=*/std::nullopt, obj_ids[1],
                               DataItem(schema::kObject)),
              IsOkAndHolds(true));
}

TEST(AbstractVisitorTest, DefaultPrevisitSlice) {
  DataItem schema_1(AllocateExplicitSchema());
  DataItem schema_2(AllocateExplicitSchema());
  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {DataItem(schema::kInt32), schema_1, schema_2}));
  NoOpVisitor visitor;
  ASSERT_OK_AND_ASSIGN(
      auto tokens, visitor.PrevisitSlice(
                       ds, DataItem(schema::kSchema),
                       AbstractVisitor::TransitionKey{
                           .type = AbstractVisitor::TransitionType::kSliceItem},
                       /*parent_token=*/DataItem()));
  EXPECT_THAT(tokens, ElementsAre(DataItem(), schema_1, schema_1));
}

TEST(AbstractVisitorTest, DefaultPrevisitObjectSchemas) {
  auto objects = DataSliceImpl::AllocateEmptyObjects(2);
  auto schemas = DataSliceImpl::Create(CreateDenseArray<ObjectId>(
      {AllocateExplicitSchema(), AllocateExplicitSchema()}));
  DataItem token(AllocateSingleObject());
  NoOpVisitor visitor;
  ASSERT_OK_AND_ASSIGN(auto tokens,
                       visitor.PrevisitObjectSchemas(objects, schemas, token));
  EXPECT_EQ(tokens.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(tokens, ElementsAre(token, token));
}

class RecordingGetValueVisitor : public NoOpVisitor {
 public:
  absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                    const DataItem& schema) override {
    calls_.emplace_back(item, schema);
    if (item.holds_value<int32_t>()) {
      return DataItem(item.value<int32_t>() + 10);
    }
    return absl::InvalidArgumentError("unexpected item");
  }

  const std::vector<std::pair<DataItem, DataItem>>& calls() const {
    return calls_;
  }

 private:
  std::vector<std::pair<DataItem, DataItem>> calls_;
};

TEST(AbstractVisitorTest, DefaultGetValuesSingleSchema) {
  DataItem schema(schema::kInt32);

  {
    RecordingGetValueVisitor visitor;
    ASSERT_OK_AND_ASSIGN(
        DataSliceImpl res,
        visitor.GetValues(DataSliceImpl::CreateEmptyAndUnknownType(3), schema));
    EXPECT_EQ(res.size(), 3);
    EXPECT_EQ(res.present_count(), 0);
    EXPECT_TRUE(visitor.calls().empty());
  }

  {
    // Dense all-equal slice calls GetValue once.
    RecordingGetValueVisitor visitor;
    auto items = DataSliceImpl::Create(CreateDenseArray<int32_t>({5, 5, 5}));
    ASSERT_OK_AND_ASSIGN(DataSliceImpl res, visitor.GetValues(items, schema));
    EXPECT_THAT(res, ElementsAre(DataItem(15), DataItem(15), DataItem(15)));
    EXPECT_THAT(visitor.calls(), ElementsAre(Pair(DataItem(5), schema)));
  }

  {
    // Sparse slice calls GetValue for each present item (even if all present
    // items are equal).
    RecordingGetValueVisitor visitor;
    auto items = DataSliceImpl::Create(
        CreateDenseArray<int32_t>({5, std::nullopt, 5, 2}));
    ASSERT_OK_AND_ASSIGN(DataSliceImpl res, visitor.GetValues(items, schema));
    EXPECT_THAT(res,
                ElementsAre(DataItem(15), DataItem(), DataItem(15),
                            DataItem(12)));
    EXPECT_THAT(visitor.calls(),
                ElementsAre(Pair(DataItem(5), schema),
                            Pair(DataItem(5), schema),
                            Pair(DataItem(2), schema)));
  }

  {
    // Sparse slice with all-equal present items preserves missing elements.
    RecordingGetValueVisitor visitor;
    auto items =
        DataSliceImpl::Create(CreateDenseArray<int32_t>({5, std::nullopt, 5}));
    ASSERT_OK_AND_ASSIGN(DataSliceImpl res, visitor.GetValues(items, schema));
    EXPECT_THAT(res, ElementsAre(DataItem(15), DataItem(), DataItem(15)));
  }

  {
    RecordingGetValueVisitor visitor;
    auto items = DataSliceImpl::AllocateEmptyObjects(1);
    EXPECT_THAT(visitor.GetValues(items, schema),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

class SingleSchemaOverrideVisitor : public RecordingGetValueVisitor {
 public:
  using AbstractVisitor::GetValues;

  absl::StatusOr<DataSliceImpl> GetValues(const DataSliceImpl& items,
                                          const DataItem& schema) override {
    single_schema_calls_.push_back(schema);
    return items;
  }

  const std::vector<DataItem>& single_schema_calls() const {
    return single_schema_calls_;
  }

 private:
  std::vector<DataItem> single_schema_calls_;
};

TEST(AbstractVisitorTest, DefaultGetValuesPerItemSchemas) {
  DataItem s0(AllocateExplicitSchema());
  DataItem s1(AllocateExplicitSchema());

  {
    RecordingGetValueVisitor visitor;
    auto schemas = DataSliceImpl::Create(CreateDenseArray<DataItem>({s0, s1}));
    ASSERT_OK_AND_ASSIGN(
        DataSliceImpl res,
        visitor.GetValues(DataSliceImpl::CreateEmptyAndUnknownType(2),
                          schemas));
    EXPECT_EQ(res.size(), 2);
    EXPECT_EQ(res.present_count(), 0);
    EXPECT_TRUE(visitor.calls().empty());
  }

  {
    // All-equal schemas delegate to GetValues(items, DataItem).
    SingleSchemaOverrideVisitor visitor;
    auto items = DataSliceImpl::Create(CreateDenseArray<int32_t>({1, 2}));
    auto schemas = DataSliceImpl::Create(CreateDenseArray<DataItem>({s0, s0}));
    ASSERT_OK_AND_ASSIGN(DataSliceImpl res, visitor.GetValues(items, schemas));
    EXPECT_THAT(res, ElementsAre(DataItem(1), DataItem(2)));
    EXPECT_THAT(visitor.single_schema_calls(), ElementsAre(s0));
    EXPECT_TRUE(visitor.calls().empty());
  }

  {
    // Distinct schemas call GetValue(items[i], schemas[i]) per present item.
    RecordingGetValueVisitor visitor;
    auto items =
        DataSliceImpl::Create(CreateDenseArray<int32_t>({1, std::nullopt, 2}));
    auto schemas =
        DataSliceImpl::Create(CreateDenseArray<DataItem>({s0, DataItem(), s1}));
    ASSERT_OK_AND_ASSIGN(DataSliceImpl res, visitor.GetValues(items, schemas));
    EXPECT_THAT(res, ElementsAre(DataItem(11), DataItem(), DataItem(12)));
    EXPECT_THAT(visitor.calls(),
                ElementsAre(Pair(DataItem(1), s0), Pair(DataItem(2), s1)));
  }
}

}  // namespace
}  // namespace koladata::internal
