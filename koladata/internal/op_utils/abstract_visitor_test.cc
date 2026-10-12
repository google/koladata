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

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::CreateDenseArray;
using ::testing::ElementsAre;

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

  absl::Status VisitObject(
      const DataItem& object, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_values) override {
    return absl::OkStatus();
  }

  absl::Status VisitSchema(
      const DataItem& item, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_schema) override {
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

}  // namespace
}  // namespace koladata::internal
