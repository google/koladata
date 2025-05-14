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
#include "koladata/test_utils.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/testing/matchers.h"

namespace koladata {
namespace {

using ::arolla::Bytes;
using ::arolla::CreateDenseArray;
using ::arolla::Text;
using ::koladata::internal::DataItem;
using ::koladata::internal::DataSliceImpl;
using ::koladata::internal::ObjectId;
using ::koladata::testing::IsEquivalentTo;

TEST(TestUtils, DataItem_Int) {
  auto item = test::DataItem(42);
  ASSERT_OK_AND_ASSIGN(
      auto expected_item,
      DataSlice::Create(DataItem(42), DataItem(schema::kInt32)));
  EXPECT_THAT(item, IsEquivalentTo(expected_item));

  item = test::DataItem(42, schema::kObject);
  ASSERT_OK_AND_ASSIGN(
      expected_item,
      DataSlice::Create(DataItem(42), DataItem(schema::kObject)));
  EXPECT_THAT(item, IsEquivalentTo(expected_item));
}

TEST(TestUtils, DataItem_Text) {
  auto item = test::DataItem("abc");
  ASSERT_OK_AND_ASSIGN(
      auto expected_item,
      DataSlice::Create(DataItem(Text("abc")), DataItem(schema::kString)));
  EXPECT_THAT(item, IsEquivalentTo(expected_item));

  item = test::DataItem("abc", schema::kObject);
  ASSERT_OK_AND_ASSIGN(
      expected_item,
      DataSlice::Create(DataItem(Text("abc")), DataItem(schema::kObject)));
  EXPECT_THAT(item, IsEquivalentTo(expected_item));
}

TEST(TestUtils, DataItem_Bytes) {
  auto item = test::DataItem(Bytes("abc"));
  ASSERT_OK_AND_ASSIGN(
      auto expected_item,
      DataSlice::Create(DataItem(Bytes("abc")), DataItem(schema::kBytes)));
  EXPECT_THAT(item, IsEquivalentTo(expected_item));

  item = test::DataItem(Bytes("abc"), schema::kObject);
  ASSERT_OK_AND_ASSIGN(
      expected_item,
      DataSlice::Create(DataItem(Bytes("abc")), DataItem(schema::kObject)));
  EXPECT_THAT(item, IsEquivalentTo(expected_item));
}

TEST(TestUtils, DataItem_Object) {
  auto object = internal::AllocateSingleObject();
  auto item = test::DataItem(object);
  ASSERT_OK_AND_ASSIGN(
      auto expected_item,
      DataSlice::Create(DataItem(object), DataItem(schema::kObject)));
  EXPECT_THAT(item, IsEquivalentTo(expected_item));

  auto alloc_schema = internal::AllocateExplicitSchema();
  auto db = DataBag::Empty();
  item = test::DataItem(object, alloc_schema, db);
  ASSERT_OK_AND_ASSIGN(
      expected_item,
      DataSlice::Create(DataItem(object), DataItem(alloc_schema), db));
  EXPECT_THAT(item, IsEquivalentTo(expected_item));
}

TEST(TestUtils, Schema_DType) {
  auto item = test::Schema(schema::kInt32);
  ASSERT_OK_AND_ASSIGN(
      auto expected_item,
      DataSlice::Create(DataItem(schema::kInt32), DataItem(schema::kSchema)));
  EXPECT_THAT(item, IsEquivalentTo(expected_item));
}

TEST(TestUtils, Schema_ObjectId) {
  auto alloc_schema = internal::AllocateExplicitSchema();
  auto item = test::Schema(alloc_schema);
  ASSERT_OK_AND_ASSIGN(
      auto expected_item,
      DataSlice::Create(DataItem(alloc_schema), DataItem(schema::kSchema)));
  EXPECT_THAT(item, IsEquivalentTo(expected_item));
}

TEST(TestUtils, Schema_DataItem) {
  auto data_item = internal::DataItem(internal::AllocateExplicitSchema());
  auto item = test::Schema(data_item);
  ASSERT_OK_AND_ASSIGN(
      auto expected_item,
      DataSlice::Create(DataItem(data_item), DataItem(schema::kSchema)));
  EXPECT_THAT(item, IsEquivalentTo(expected_item));
}

TEST(TestUtils, DataSlice_SpanInt) {
  auto ds = test::DataSlice<int>({42, std::nullopt, 12});
  ASSERT_OK_AND_ASSIGN(
      auto expected_ds,
      DataSlice::Create(
          DataSliceImpl::Create(CreateDenseArray<int>({42, std::nullopt, 12})),
          DataSlice::JaggedShape::FlatFromSize(3),
          DataItem(schema::kInt32)));
  EXPECT_THAT(ds, IsEquivalentTo(expected_ds));

  ds = test::DataSlice<int>({42, std::nullopt, 12}, schema::kObject);
  ASSERT_OK_AND_ASSIGN(
      expected_ds,
      DataSlice::Create(
          DataSliceImpl::Create(CreateDenseArray<int>({42, std::nullopt, 12})),
          DataSlice::JaggedShape::FlatFromSize(3),
          DataItem(schema::kObject)));
  EXPECT_THAT(ds, IsEquivalentTo(expected_ds));
}

TEST(TestUtils, DataSlice_SpanText) {
  auto ds = test::DataSlice<arolla::Text>({"abc", std::nullopt, "xyz"});
  ASSERT_OK_AND_ASSIGN(
      auto expected_ds,
      DataSlice::Create(
          DataSliceImpl::Create(
              CreateDenseArray<Text>({Text("abc"), std::nullopt, Text("xyz")})),
          DataSlice::JaggedShape::FlatFromSize(3), DataItem(schema::kString)));
  EXPECT_THAT(ds, IsEquivalentTo(expected_ds));
}

TEST(TestUtils, DataSlice_SpanBytes) {
  auto ds = test::DataSlice<arolla::Bytes>({"abc", std::nullopt, "xyz"});
  ASSERT_OK_AND_ASSIGN(
      auto expected_ds,
      DataSlice::Create(
          DataSliceImpl::Create(CreateDenseArray<Bytes>(
              {Bytes("abc"), std::nullopt, Bytes("xyz")})),
          DataSlice::JaggedShape::FlatFromSize(3),
          DataItem(schema::kBytes)));
  EXPECT_THAT(ds, IsEquivalentTo(expected_ds));
}

TEST(TestUtils, DataSlice_Object_ObjectSchema) {
  auto alloc_id = internal::Allocate(3);
  auto ds = test::DataSlice<ObjectId>(
      {alloc_id.ObjectByOffset(0), std::nullopt, alloc_id.ObjectByOffset(1)},
      schema::kObject);
  ASSERT_OK_AND_ASSIGN(
      auto expected_ds,
      DataSlice::Create(
          DataSliceImpl::Create(CreateDenseArray<ObjectId>(
              {alloc_id.ObjectByOffset(0), std::nullopt,
               alloc_id.ObjectByOffset(1)})),
          DataSlice::JaggedShape::FlatFromSize(3),
          DataItem(schema::kObject)));
  EXPECT_THAT(ds, IsEquivalentTo(expected_ds));

  // With DataBag.
  auto db = DataBag::Empty();
  ds = test::DataSlice<ObjectId>(
      {alloc_id.ObjectByOffset(0), std::nullopt, alloc_id.ObjectByOffset(1)},
      schema::kObject, db);
  EXPECT_THAT(ds, IsEquivalentTo(expected_ds.WithBag(db)));
}

TEST(TestUtils, DataSlice_Entity) {
  auto alloc_id = internal::Allocate(3);
  auto alloc_schema = internal::AllocateExplicitSchema();
  auto ds = test::DataSlice<ObjectId>(
      {alloc_id.ObjectByOffset(0), std::nullopt, alloc_id.ObjectByOffset(1)},
      alloc_schema);
  ASSERT_OK_AND_ASSIGN(
      auto expected_ds,
      DataSlice::Create(
          DataSliceImpl::Create(CreateDenseArray<ObjectId>(
              {alloc_id.ObjectByOffset(0), std::nullopt,
               alloc_id.ObjectByOffset(1)})),
          DataSlice::JaggedShape::FlatFromSize(3),
          DataItem(alloc_schema)));
  EXPECT_THAT(ds, IsEquivalentTo(expected_ds));

  // With DataBag.
  auto db = DataBag::Empty();
  ds = test::DataSlice<ObjectId>(
      {alloc_id.ObjectByOffset(0), std::nullopt, alloc_id.ObjectByOffset(1)},
      alloc_schema, db);
  EXPECT_THAT(ds, IsEquivalentTo(expected_ds.WithBag(db)));
}

TEST(TestUtils, DataSlice_Mixed) {
  auto ds = test::MixedDataSlice<int, Text>(
      {42, std::nullopt, std::nullopt},
      {std::nullopt, "abc", std::nullopt},
      schema::kObject);
  ASSERT_OK_AND_ASSIGN(
      auto expected_ds,
      DataSlice::Create(
          DataSliceImpl::Create(
              CreateDenseArray<int>({42, std::nullopt, std::nullopt}),
              CreateDenseArray<Text>({std::nullopt, Text("abc"),
                                      std::nullopt})),
          DataSlice::JaggedShape::FlatFromSize(3),
          DataItem(schema::kObject)));
  EXPECT_THAT(ds, IsEquivalentTo(expected_ds));

  ds = test::MixedDataSlice<int, Text, Bytes>(
      {42, std::nullopt, std::nullopt},
      {std::nullopt, "abc", std::nullopt},
      {std::nullopt, std::nullopt, "&52%"});
  ASSERT_OK_AND_ASSIGN(
      expected_ds,
      DataSlice::Create(
          DataSliceImpl::Create(
              CreateDenseArray<int>({42, std::nullopt, std::nullopt}),
              CreateDenseArray<Text>({std::nullopt, Text("abc"), std::nullopt}),
              CreateDenseArray<Bytes>({std::nullopt, std::nullopt,
                                       Bytes("&52%")})),
          DataSlice::JaggedShape::FlatFromSize(3),
          DataItem(schema::kObject)));
  EXPECT_THAT(ds, IsEquivalentTo(expected_ds));
}

TEST(TestUtils, DataSlice_Empty) {
  auto ds = test::EmptyDataSlice(3, schema::kObject);
  ASSERT_OK_AND_ASSIGN(
      auto expected_ds,
      DataSlice::Create(
          internal::DataSliceImpl::CreateEmptyAndUnknownType(3),
          DataSlice::JaggedShape::FlatFromSize(3),
          internal::DataItem(schema::kObject)));
  EXPECT_THAT(ds, IsEquivalentTo(expected_ds));
}

TEST(TestUtils, DataSlice_Allocate) {
  auto ds = test::AllocateDataSlice(3, schema::kObject);
  ASSERT_OK_AND_ASSIGN(
      auto expected_ds,
      DataSlice::Create(ds.slice(), DataSlice::JaggedShape::FlatFromSize(3),
                        internal::DataItem(schema::kObject)));
  EXPECT_THAT(ds, IsEquivalentTo(expected_ds));
}

}  // namespace
}  // namespace koladata
