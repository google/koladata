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
#include "koladata/internal/dense_source.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/quote.h"
#include "arolla/expr/testing/test_operators.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"

namespace koladata::internal {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::DenseArray;
using ::arolla::Unit;
using ::arolla::bitmap::Word;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

TEST(DenseSourceTest, ObjectAttrSimple) {
  AllocationId alloc = Allocate(3);
  AllocationId attr_alloc = Allocate(3);
  DataSliceImpl attr = DataSliceImpl::ObjectsFromAllocation(attr_alloc, 3);
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<const DenseSource> ds,
                       DenseSource::CreateReadonly(alloc, attr));

  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(i)), attr[i]);
  }

  auto objs = arolla::CreateDenseArray<ObjectId>(
      std::vector<arolla::OptionalValue<ObjectId>>{
          alloc.ObjectByOffset(0), std::nullopt, alloc.ObjectByOffset(2),
          alloc.ObjectByOffset(0)});

  EXPECT_THAT(
      ds->Get(objs, /*check_alloc_id`*/false).values<ObjectId>(),
      ElementsAre(attr_alloc.ObjectByOffset(0), std::nullopt,
                  attr_alloc.ObjectByOffset(2), attr_alloc.ObjectByOffset(0)));
}

TEST(DenseSourceTest, MutableObjectAttrSimple) {
  AllocationId alloc = Allocate(3);
  AllocationId attr_alloc = Allocate(3);
  ASSERT_OK_AND_ASSIGN(auto ds, DenseSource::CreateMutable(
                                    alloc, 3, arolla::GetQType<ObjectId>()));
  ASSERT_OK(ds->Set(
      arolla::CreateFullDenseArray<ObjectId>({alloc.ObjectByOffset(0),
                                              alloc.ObjectByOffset(1),
                                              alloc.ObjectByOffset(2)}),
      DataSliceImpl::CreateWithAllocIds(
          AllocationIdSet(attr_alloc),
          arolla::CreateFullDenseArray<ObjectId>(
              {attr_alloc.ObjectByOffset(0), attr_alloc.ObjectByOffset(1),
               attr_alloc.ObjectByOffset(2)}))));

  {
    for (int i = 0; i < 3; ++i) {
      EXPECT_EQ(ds->Get(alloc.ObjectByOffset(i)),
                DataItem(attr_alloc.ObjectByOffset(i)));
    }

    auto objs = arolla::CreateDenseArray<ObjectId>(
        std::vector<arolla::OptionalValue<ObjectId>>{
            alloc.ObjectByOffset(0), std::nullopt, alloc.ObjectByOffset(2),
            alloc.ObjectByOffset(0)});

    EXPECT_THAT(ds->Get(objs, /*check_alloc_id`*/false).values<ObjectId>(),
                ElementsAre(attr_alloc.ObjectByOffset(0), std::nullopt,
                            attr_alloc.ObjectByOffset(2),
                            attr_alloc.ObjectByOffset(0)));
  }

  AllocationId attr_alloc2 = Allocate(3);

  // Reassign 0 and remove 1.
  ASSERT_OK(ds->Set(arolla::CreateFullDenseArray<ObjectId>(
                        {alloc.ObjectByOffset(0), alloc.ObjectByOffset(1)}),
                    DataSliceImpl::CreateWithAllocIds(
                        AllocationIdSet(attr_alloc2),
                        arolla::CreateDenseArray<ObjectId>(
                            {attr_alloc2.ObjectByOffset(1), std::nullopt}))));
  // Reassign 0 again, 1 should remain removed.
  ASSERT_OK(ds->Set(
      arolla::CreateFullDenseArray<ObjectId>({alloc.ObjectByOffset(0)}),
      DataSliceImpl::CreateWithAllocIds(AllocationIdSet(attr_alloc2),
                                        arolla::CreateFullDenseArray<ObjectId>(
                                            {attr_alloc2.ObjectByOffset(0)}))));

  {
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(0)),
              DataItem(attr_alloc2.ObjectByOffset(0)));
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(1)), DataItem());
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(2)),
              DataItem(attr_alloc.ObjectByOffset(2)));

    auto objs = arolla::CreateDenseArray<ObjectId>(
        std::vector<arolla::OptionalValue<ObjectId>>{
            alloc.ObjectByOffset(0), std::nullopt, attr_alloc.ObjectByOffset(2),
            alloc.ObjectByOffset(1)});

    SliceBuilder slice_bldr(objs.size());
    slice_bldr.ApplyMask(objs.ToMask());
    ds->Get(objs.values.span(), slice_bldr);
    EXPECT_THAT(std::move(slice_bldr).Build().values<ObjectId>(),
                ElementsAre(attr_alloc2.ObjectByOffset(0), std::nullopt,
                            std::nullopt, std::nullopt));
  }

  // Reassign previously removed 1.
  ASSERT_OK(ds->Set(alloc.ObjectByOffset(1),
                    DataItem(attr_alloc2.ObjectByOffset(1))));
  // Remove 2.
  ASSERT_OK(ds->Set(alloc.ObjectByOffset(2), DataItem()));

  {
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(0)),
              DataItem(attr_alloc2.ObjectByOffset(0)));
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(1)),
              DataItem(attr_alloc2.ObjectByOffset(1)));
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(2)), DataItem());

    auto objs = arolla::CreateDenseArray<ObjectId>(
        std::vector<arolla::OptionalValue<ObjectId>>{
            alloc.ObjectByOffset(0), std::nullopt, alloc.ObjectByOffset(2),
            alloc.ObjectByOffset(1)});

    EXPECT_THAT(ds->Get(objs).values<ObjectId>(),
                ElementsAre(attr_alloc2.ObjectByOffset(0), std::nullopt,
                            std::nullopt, attr_alloc2.ObjectByOffset(1)));
  }
}

TEST(DenseSourceTest, PrimitiveAttrSimple) {
  AllocationId alloc = Allocate(3);
  arolla::DenseArray<int32_t> attr_value =
      arolla::CreateFullDenseArray<int32_t>({3, 7, 9});
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<const DenseSource> ds,
      DenseSource::CreateReadonly(alloc, DataSliceImpl::Create(attr_value)));

  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(i)), DataItem(attr_value[i]));
  }

  auto objs = arolla::CreateDenseArray<ObjectId>(
      std::vector<arolla::OptionalValue<ObjectId>>{
          alloc.ObjectByOffset(0), std::nullopt, alloc.ObjectByOffset(2),
          alloc.ObjectByOffset(0)});

  EXPECT_THAT(ds->Get(objs, /*check_alloc_id`*/false).values<int32_t>(),
              ElementsAre(3, std::nullopt, 9, 3));
}

TEST(DenseSourceTest, ImmutableUnitAttr) {
  AllocationId alloc = Allocate(300);
  arolla::DenseArray<Unit> attr_value = arolla::CreateDenseArray<Unit>(
      {Unit(), std::nullopt, Unit(), std::nullopt});
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<const DenseSource> ds,
      DenseSource::CreateReadonly(alloc, DataSliceImpl::Create(attr_value)));

  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(i)), DataItem(attr_value[i]));
  }

  auto objs = arolla::CreateDenseArray<ObjectId>(
      std::vector<arolla::OptionalValue<ObjectId>>{
          alloc.ObjectByOffset(0), std::nullopt, alloc.ObjectByOffset(1),
          alloc.ObjectByOffset(0), Allocate(1).ObjectByOffset(0)});

  EXPECT_THAT(
      ds->Get(objs).values<Unit>(),
      ElementsAre(Unit(), std::nullopt, std::nullopt, Unit(), std::nullopt));
}

TEST(DenseSourceTest, ImmutableBytesAttr) {
  using Bytes = arolla::Bytes;
  using OB = arolla::OptionalValue<Bytes>;
  AllocationId alloc = Allocate(3);
  arolla::DenseArray<Bytes> attr_value =
      arolla::CreateDenseArray<Bytes>({OB("3"), OB("7"), OB("9")});
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<const DenseSource> ds,
      DenseSource::CreateReadonly(alloc, DataSliceImpl::Create(attr_value)));

  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(0)), DataItem(Bytes("3")));
  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(1)), DataItem(Bytes("7")));
  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(2)), DataItem(Bytes("9")));

  auto objs = arolla::CreateDenseArray<ObjectId>(
      std::vector<arolla::OptionalValue<ObjectId>>{
          alloc.ObjectByOffset(0), std::nullopt, alloc.ObjectByOffset(2),
          alloc.ObjectByOffset(0)});

  EXPECT_THAT(ds->Get(objs, /*check_alloc_id`*/false).values<Bytes>(),
              ElementsAre("3", std::nullopt, "9", "3"));
}

TEST(DenseSourceTest, MutableTextAttr) {
  using Text = arolla::Text;
  using OT = arolla::OptionalValue<Text>;
  AllocationId alloc = Allocate(3);
  ASSERT_OK_AND_ASSIGN(
      auto ds, DenseSource::CreateMutable(alloc, 3, arolla::GetQType<Text>()));

  ASSERT_OK(ds->Set(arolla::CreateFullDenseArray<ObjectId>(
                        {alloc.ObjectByOffset(0), alloc.ObjectByOffset(1),
                         alloc.ObjectByOffset(2)}),
                    DataSliceImpl::Create(arolla::CreateDenseArray<Text>(
                        {OT("abaca"), OT("acaba"), OT("bacaba")}))));

  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(0)), DataItem(Text("abaca")));
  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(1)), DataItem(Text("acaba")));
  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(2)), DataItem(Text("bacaba")));

  auto objs = arolla::CreateDenseArray<ObjectId>(
      std::vector<arolla::OptionalValue<ObjectId>>{
          alloc.ObjectByOffset(0), std::nullopt, alloc.ObjectByOffset(2),
          alloc.ObjectByOffset(0)});

  EXPECT_THAT(ds->Get(objs).values<Text>(),
              ElementsAre("abaca", std::nullopt, "bacaba", "abaca"));

  // Reassign 0 and remove 1.
  ASSERT_OK(ds->Set(arolla::CreateFullDenseArray<ObjectId>(
                        {alloc.ObjectByOffset(0), alloc.ObjectByOffset(1)}),
                    DataSliceImpl::Create(arolla::CreateDenseArray<Text>(
                        {OT("bbb"), std::nullopt}))));

  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(0)), DataItem(Text("bbb")));
  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(1)), DataItem());
  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(2)), DataItem(Text("bacaba")));

  // Reassign previously removed 1.
  ASSERT_OK(ds->Set(alloc.ObjectByOffset(1), DataItem(Text("aaa"))));
  // Remove 2.
  ASSERT_OK(ds->Set(alloc.ObjectByOffset(2), DataItem()));

  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(0)), DataItem(Text("bbb")));
  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(1)), DataItem(Text("aaa")));
  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(2)), DataItem());
}

TEST(DenseSourceTest, SimpleValueArrayWithComplexAllocDealloc) {
  using ExprQuote = arolla::expr::ExprQuote;
  arolla::expr::ExprOperatorPtr op =
      std::make_shared<arolla::expr::testing::DummyOp>(
          "op", arolla::expr::ExprOperatorSignature::MakeVariadicArgs());
  ASSERT_OK_AND_ASSIGN(auto expr_1,
                       arolla::expr::CallOp(op, {arolla::expr::Leaf("x")}));
  ASSERT_OK_AND_ASSIGN(auto expr_2,
                       arolla::expr::CallOp(op, {arolla::expr::Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(auto expr_3,
                       arolla::expr::CallOp(op, {arolla::expr::Leaf("z")}));
  ASSERT_OK_AND_ASSIGN(auto expr_4,
                       arolla::expr::CallOp(op, {arolla::expr::Leaf("w")}));

  {
    // Immutable DenseSource of ExprQuote.
    AllocationId alloc = Allocate(300);
    auto attr_value = arolla::CreateDenseArray<ExprQuote>(
        {ExprQuote(expr_1), std::nullopt, ExprQuote(expr_2), std::nullopt});
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<const DenseSource> ds,
        DenseSource::CreateReadonly(alloc, DataSliceImpl::Create(attr_value)));

    for (int i = 0; i < 3; ++i) {
      EXPECT_EQ(ds->Get(alloc.ObjectByOffset(i)), DataItem(attr_value[i]));
    }

    auto objs = arolla::CreateDenseArray<ObjectId>(
        std::vector<arolla::OptionalValue<ObjectId>>{
            alloc.ObjectByOffset(0), std::nullopt, alloc.ObjectByOffset(1),
            alloc.ObjectByOffset(2), Allocate(1).ObjectByOffset(0)});

    EXPECT_THAT(ds->Get(objs).values<ExprQuote>(),
                ElementsAre(ExprQuote(expr_1), std::nullopt, std::nullopt,
                            ExprQuote(expr_2), std::nullopt));

    EXPECT_THAT(
        const_cast<DenseSource&>(*ds).Set(alloc.ObjectByOffset(1),
                                          DataItem(ExprQuote(expr_4))),
        StatusIs(
            absl::StatusCode::kFailedPrecondition,
            HasSubstr("SetAttr is not allowed for an immutable DenseSource.")));
  }
  {
    // Mutable DenseSource of ExprQuote.
    AllocationId alloc = Allocate(3);
    ASSERT_OK_AND_ASSIGN(auto ds, DenseSource::CreateMutable(
                                      alloc, 3, arolla::GetQType<ExprQuote>()));

    ASSERT_OK(ds->Set(
        arolla::CreateFullDenseArray<ObjectId>({alloc.ObjectByOffset(0),
                                                alloc.ObjectByOffset(1),
                                                alloc.ObjectByOffset(2)}),
        DataSliceImpl::Create(arolla::CreateDenseArray<ExprQuote>(
            {ExprQuote(expr_1), ExprQuote(expr_2), ExprQuote(expr_3)}))));

    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(0)), DataItem(ExprQuote(expr_1)));
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(1)), DataItem(ExprQuote(expr_2)));
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(2)), DataItem(ExprQuote(expr_3)));

    auto objs = arolla::CreateDenseArray<ObjectId>(
        std::vector<arolla::OptionalValue<ObjectId>>{
            alloc.ObjectByOffset(0), std::nullopt, alloc.ObjectByOffset(2),
            alloc.ObjectByOffset(0)});

    EXPECT_THAT(ds->Get(objs).values<ExprQuote>(),
                ElementsAre(ExprQuote(expr_1), std::nullopt, ExprQuote(expr_3),
                            ExprQuote(expr_1)));

    // Reassign 0 and remove 1.
    ASSERT_OK(ds->Set(arolla::CreateFullDenseArray<ObjectId>(
                          {alloc.ObjectByOffset(0), alloc.ObjectByOffset(1)}),
                      DataSliceImpl::Create(arolla::CreateDenseArray<ExprQuote>(
                          {ExprQuote(expr_4), std::nullopt}))));

    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(0)), DataItem(ExprQuote(expr_4)));
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(1)), DataItem());
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(2)), DataItem(ExprQuote(expr_3)));

    // Reassign previously removed 1.
    ASSERT_OK(ds->Set(alloc.ObjectByOffset(1), DataItem(ExprQuote(expr_4))));
    // Remove 2.
    ASSERT_OK(ds->Set(alloc.ObjectByOffset(2), DataItem()));

    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(0)), DataItem(ExprQuote(expr_4)));
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(1)), DataItem(ExprQuote(expr_4)));
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(2)), DataItem());
  }
}

TEST(DenseSourceTest, ImmutableWithMixedTypes) {
  AllocationId alloc = Allocate(7);
  SliceBuilder bldr(alloc.Capacity());
  bldr.InsertIfNotSet(0, 5);
  bldr.InsertIfNotSetAndUpdateAllocIds(1, DataItem(alloc.ObjectByOffset(2)));
  bldr.InsertIfNotSet(3, arolla::Bytes("abc"));
  bldr.InsertIfNotSet(4, 7);
  bldr.InsertIfNotSet(5, Unit());
  bldr.InsertIfNotSet(6, schema::kFloat32);
  DataSliceImpl attr = std::move(bldr).Build();
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<const DenseSource> ds,
                       DenseSource::CreateReadonly(alloc, attr));

  for (int i = 0; i < alloc.Capacity(); ++i) {
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(i)), attr[i]);
  }

  {
    auto objs = arolla::CreateDenseArray<ObjectId>(
        std::vector<arolla::OptionalValue<ObjectId>>{alloc.ObjectByOffset(0),
                                                     alloc.ObjectByOffset(4)});
    DataSliceImpl attr_slice = ds->Get(objs);
    EXPECT_EQ(attr.allocation_ids(), attr_slice.allocation_ids());
    EXPECT_THAT(attr_slice, ElementsAre(5, 7));
  }
  {
    auto objs = arolla::CreateDenseArray<ObjectId>(
        std::vector<arolla::OptionalValue<ObjectId>>{
            alloc.ObjectByOffset(3), std::nullopt, alloc.ObjectByOffset(2),
            alloc.ObjectByOffset(2), alloc.ObjectByOffset(1),
            alloc.ObjectByOffset(0), alloc.ObjectByOffset(5),
            alloc.ObjectByOffset(6)});
    DataSliceImpl attr_slice = ds->Get(objs);
    EXPECT_EQ(attr.allocation_ids(), attr_slice.allocation_ids());
    EXPECT_THAT(attr_slice,
                ElementsAre(arolla::Bytes("abc"), std::nullopt, std::nullopt,
                            std::nullopt, alloc.ObjectByOffset(2), 5, Unit(),
                            schema::kFloat32));
  }
}

TEST(DenseSourceTest, MutableCopyOfImmutableWithoutBitmap) {
  DenseArray<int> arr = arolla::CreateDenseArray<int>({1, 2, 3});
  EXPECT_TRUE(arr.bitmap.empty());

  AllocationId alloc = Allocate(3);
  ASSERT_OK_AND_ASSIGN(
      auto source,
      DenseSource::CreateReadonly(alloc, DataSliceImpl::Create(arr)));

  auto source_copy = source->CreateMutableCopy();
  ASSERT_OK(source_copy->Set(alloc.ObjectByOffset(2), DataItem(7)));

  EXPECT_EQ(source->Get(alloc.ObjectByOffset(0)), DataItem(1));
  EXPECT_EQ(source->Get(alloc.ObjectByOffset(1)), DataItem(2));
  EXPECT_EQ(source->Get(alloc.ObjectByOffset(2)), DataItem(3));

  EXPECT_EQ(source_copy->Get(alloc.ObjectByOffset(0)), DataItem(1));
  EXPECT_EQ(source_copy->Get(alloc.ObjectByOffset(1)), DataItem(2));
  EXPECT_EQ(source_copy->Get(alloc.ObjectByOffset(2)), DataItem(7));
}

TEST(DenseSourceTest, MutableCopyOfImmutableWithoutBitmapUnit) {
  DenseArray<Unit> arr = arolla::CreateDenseArray<Unit>(
      {Unit(), Unit(), Unit()});
  EXPECT_TRUE(arr.bitmap.empty());

  AllocationId alloc = Allocate(3);
  ASSERT_OK_AND_ASSIGN(
      auto source,
      DenseSource::CreateReadonly(alloc, DataSliceImpl::Create(arr)));

  auto source_copy = source->CreateMutableCopy();
  ASSERT_OK(source_copy->Set(alloc.ObjectByOffset(2), DataItem()));

  EXPECT_EQ(source->Get(alloc.ObjectByOffset(0)), DataItem(Unit()));
  EXPECT_EQ(source->Get(alloc.ObjectByOffset(1)), DataItem(Unit()));
  EXPECT_EQ(source->Get(alloc.ObjectByOffset(2)), DataItem(Unit()));

  EXPECT_EQ(source_copy->Get(alloc.ObjectByOffset(0)), DataItem(Unit()));
  EXPECT_EQ(source_copy->Get(alloc.ObjectByOffset(1)), DataItem(Unit()));
  EXPECT_EQ(source_copy->Get(alloc.ObjectByOffset(2)), DataItem());
}

TEST(DenseSourceTest, MutableCopyOfImmutableWithMixedTypes) {
  AllocationId alloc = Allocate(7);
  SliceBuilder bldr(alloc.Capacity());
  bldr.InsertIfNotSet(0, 5);
  bldr.InsertIfNotSetAndUpdateAllocIds(1, DataItem(alloc.ObjectByOffset(2)));
  bldr.InsertIfNotSet(3, arolla::Bytes("abc"));
  bldr.InsertIfNotSet(4, 7);
  bldr.InsertIfNotSet(5, Unit());
  arolla::expr::ExprOperatorPtr op =
      std::make_shared<arolla::expr::testing::DummyOp>(
          "op", arolla::expr::ExprOperatorSignature::MakeVariadicArgs());
  ASSERT_OK_AND_ASSIGN(auto expr_1,
                       arolla::expr::CallOp(op, {arolla::expr::Leaf("x")}));
  bldr.InsertIfNotSet(6, DataItem(arolla::expr::ExprQuote(expr_1)));
  DataSliceImpl attr = std::move(bldr).Build();
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<const DenseSource> immutable_ds,
                       DenseSource::CreateReadonly(alloc, attr));

  std::shared_ptr<DenseSource> ds = immutable_ds->CreateMutableCopy();

  ASSERT_OK(ds->Set(alloc.ObjectByOffset(0), DataItem(arolla::Bytes("def"))));
  ASSERT_OK_AND_ASSIGN(auto expr_2,
                       arolla::expr::CallOp(op, {arolla::expr::Leaf("y")}));
  ASSERT_OK(ds->Set(alloc.ObjectByOffset(4),
                    DataItem(arolla::expr::ExprQuote(expr_2))));
  ASSERT_OK(ds->Set(alloc.ObjectByOffset(5), DataItem()));

  // Original is not changed.
  EXPECT_EQ(immutable_ds->Get(alloc.ObjectByOffset(0)), DataItem(5));
  EXPECT_EQ(immutable_ds->Get(alloc.ObjectByOffset(3)),
            DataItem(arolla::Bytes("abc")));
  EXPECT_EQ(immutable_ds->Get(alloc.ObjectByOffset(4)), DataItem(7));
  EXPECT_EQ(immutable_ds->Get(alloc.ObjectByOffset(5)), DataItem(Unit()));
  EXPECT_EQ(immutable_ds->Get(alloc.ObjectByOffset(6)),
            DataItem(arolla::expr::ExprQuote(expr_1)));

  // Unmodified value is copied from original.
  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(3)), DataItem(arolla::Bytes("abc")));
  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(5)), DataItem());

  // Copy is changed.
  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(0)), DataItem(arolla::Bytes("def")));
  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(4)),
            DataItem(arolla::expr::ExprQuote(expr_2)));
  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(5)), DataItem());
}

TEST(DenseSourceTest, MutableWithMixedTypes) {
  AllocationId alloc = Allocate(5);
  ASSERT_OK_AND_ASSIGN(auto source_with_main_type,
                       DenseSource::CreateMutable(
                           alloc, 5, /*main_type=*/arolla::GetQType<int>()));
  ASSERT_OK_AND_ASSIGN(
      auto source_without_main_type,
      DenseSource::CreateMutable(alloc, 5, /*main_type=*/nullptr));

  for (auto& source : {source_with_main_type, source_without_main_type}) {
    // set ints
    ASSERT_OK(source->Set(
        arolla::CreateFullDenseArray<ObjectId>({alloc.ObjectByOffset(0),
                                                alloc.ObjectByOffset(1),
                                                alloc.ObjectByOffset(3)}),
        DataSliceImpl::Create(
            arolla::CreateFullDenseArray<int>({10, 11, 12}))));
    // set floats
    ASSERT_OK(source->Set(
        arolla::CreateFullDenseArray<ObjectId>({alloc.ObjectByOffset(1),
                                                alloc.ObjectByOffset(2),
                                                alloc.ObjectByOffset(4)}),
        DataSliceImpl::Create(
            arolla::CreateFullDenseArray<float>({1.0f, 1.1f, 1.2f}))));
    // set several types at once
    {
      SliceBuilder bldr(3);
      bldr.InsertIfNotSet(0, arolla::Bytes("bytes"));
      bldr.InsertIfNotSet(1, arolla::Text("text"));
      bldr.InsertIfNotSet(2, Unit());
      ASSERT_OK(
          source->Set(arolla::CreateFullDenseArray<ObjectId>(
                          {alloc.ObjectByOffset(0), alloc.ObjectByOffset(2),
                           alloc.ObjectByOffset(3)}),
                      std::move(bldr).Build()));
    }
    // set single DataItem
    ASSERT_OK(source->Set(alloc.ObjectByOffset(1), DataItem(true)));

    // get all
    ASSERT_EQ(source->size(), 5);
    EXPECT_EQ(source->Get(alloc.ObjectByOffset(0)),
              DataItem(arolla::Bytes("bytes")));
    EXPECT_EQ(source->Get(alloc.ObjectByOffset(1)), DataItem(true));
    EXPECT_EQ(source->Get(alloc.ObjectByOffset(2)),
              DataItem(arolla::Text("text")));
    EXPECT_EQ(source->Get(alloc.ObjectByOffset(3)), DataItem(Unit()));
    EXPECT_EQ(source->Get(alloc.ObjectByOffset(4)), DataItem(1.2f));
  }
}

TEST(DenseSourceTest, ReadonlyFromEmptyAndUnknownSlice) {
  AllocationId alloc = Allocate(3);
  DataSliceImpl attr = DataSliceImpl::CreateEmptyAndUnknownType(3);
  EXPECT_THAT(DenseSource::CreateReadonly(alloc, attr),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("Empty and unknown slices")));
}

TEST(DenseSourceTest, SetUnitAttrAndReturnMissingObjectsInternal) {
  auto oth0 = AllocateSingleObject();
  auto oth1 = AllocateSingleObject();
  auto oth2 = Allocate(1027).ObjectByOffset(77);

  AllocationId alloc = Allocate(4);
  ASSERT_OK_AND_ASSIGN(
      auto ds, DenseSource::CreateMutable(alloc, 4, arolla::GetQType<Unit>()));

  auto a0 = alloc.ObjectByOffset(0);
  auto a1 = alloc.ObjectByOffset(1);
  auto a2 = alloc.ObjectByOffset(2);
  auto a3 = alloc.ObjectByOffset(3);

  std::vector<ObjectId> missing_objects;
  EXPECT_OK(ds->SetUnitAndUpdateMissingObjects(
      arolla::CreateDenseArray<ObjectId>({}), missing_objects));
  EXPECT_TRUE(missing_objects.empty());

  EXPECT_OK(ds->SetUnitAndUpdateMissingObjects(
      arolla::CreateDenseArray<ObjectId>(
          {a0, a2, a2, std::nullopt, std::nullopt, a0, oth0}),
      missing_objects));
  EXPECT_THAT(missing_objects, ElementsAre(a0, a2));

  missing_objects.clear();
  EXPECT_OK(ds->SetUnitAndUpdateMissingObjects(
      arolla::CreateDenseArray<ObjectId>({oth0, oth1, oth2, a3, a2, a1, a0}),
      missing_objects));
  EXPECT_THAT(missing_objects, ElementsAre(a3, a1));
}

TEST(DenseSourceTest, Merge) {
  auto gen_data = [&]<typename T>(T value, int size, int step, int offset) {
    arolla::DenseArrayBuilder<T> bldr(size);
    for (int i = offset; i < size; i += step) {
      bldr.Set(i, value);
    }
    return std::move(bldr).Build();
  };
  int size = 25;
  AllocationId alloc = Allocate(size);
  ASSERT_OK_AND_ASSIGN(
      auto src1,
      DenseSource::CreateReadonly(
          alloc, DataSliceImpl::Create(gen_data(int{1}, size, 2, 0))));
  ASSERT_OK_AND_ASSIGN(
      auto src2,
      DenseSource::CreateReadonly(
          alloc, DataSliceImpl::Create(gen_data(int{2}, /*size=*/23, 4, 2))));
  ASSERT_OK_AND_ASSIGN(
      auto src3,
      DenseSource::CreateReadonly(alloc, DataSliceImpl::Create(gen_data(
                                             float{.5f}, size, 3, 0))));
  ASSERT_OK_AND_ASSIGN(
      auto src4,
      DenseSource::CreateReadonly(
          alloc, DataSliceImpl::Create(gen_data(arolla::Text("a"), size, 7, 1),
                                       gen_data(int64_t{42}, size, 7, 2))));

  DataItem i1(int{1});
  DataItem i2(int{2});
  DataItem f(float{.5f});
  DataItem t(arolla::Text("a"));
  DataItem i42(int64_t{42});
  DataItem None;
  auto ids =
      DataSliceImpl::ObjectsFromAllocation(alloc, size).values<ObjectId>();

  {  // kOverwrite
    ASSERT_OK_AND_ASSIGN(
        auto dst, DenseSource::CreateMutable(
                      alloc, size, /*main_type=*/arolla::GetQType<int>()));
    ASSERT_OK(
        dst->Merge(*src1, DenseSource::ConflictHandlingOption::kOverwrite));
    ASSERT_OK(
        dst->Merge(*src2, DenseSource::ConflictHandlingOption::kOverwrite));
    ASSERT_OK(
        dst->Merge(*src3, DenseSource::ConflictHandlingOption::kOverwrite));
    ASSERT_OK(
        dst->Merge(*src4, DenseSource::ConflictHandlingOption::kOverwrite));

    EXPECT_THAT(dst->Get(ids), ElementsAre(f, t, i42, f, i1, None, f, None, t,
                                           i42, i2, None, f, None, i2, t, i42,
                                           None, f, None, i1, f, t, i42, f));
  }
  {  // kKeepOriginal
    ASSERT_OK_AND_ASSIGN(
        auto dst, DenseSource::CreateMutable(
                      alloc, size, /*main_type=*/arolla::GetQType<int>()));
    ASSERT_OK(
        dst->Merge(*src1, DenseSource::ConflictHandlingOption::kKeepOriginal));
    ASSERT_OK(
        dst->Merge(*src2, DenseSource::ConflictHandlingOption::kKeepOriginal));
    ASSERT_OK(
        dst->Merge(*src3, DenseSource::ConflictHandlingOption::kKeepOriginal));
    ASSERT_OK(
        dst->Merge(*src4, DenseSource::ConflictHandlingOption::kKeepOriginal));

    EXPECT_THAT(dst->Get(ids), ElementsAre(i1, t, i1, f, i1, None, i1, None, i1,
                                           f, i1, None, i1, None, i1, f, i1,
                                           None, i1, None, i1, f, i1, i42, i1));
  }
  {  // kRaiseOnConflict, single type
    ASSERT_OK_AND_ASSIGN(
        auto dst, DenseSource::CreateMutable(
                      alloc, size, /*main_type=*/arolla::GetQType<int>()));
    ASSERT_OK(dst->Merge(
        *src1, DenseSource::ConflictHandlingOption::kRaiseOnConflict));
    EXPECT_THAT(
        dst->Merge(*src2,
                   DenseSource::ConflictHandlingOption::kRaiseOnConflict),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                 HasSubstr("merge conflict: 1 != 2")));
  }
  {  // kRaiseOnConflict, multitype
    ASSERT_OK_AND_ASSIGN(
        auto dst, DenseSource::CreateMutable(
                      alloc, size, /*main_type=*/arolla::GetQType<int>()));
    ASSERT_OK(
        dst->Merge(*src1, DenseSource::ConflictHandlingOption::kOverwrite));
    ASSERT_OK(
        dst->Merge(*src2, DenseSource::ConflictHandlingOption::kKeepOriginal));
    ASSERT_OK(
        dst->Merge(*src3, DenseSource::ConflictHandlingOption::kOverwrite));
    EXPECT_THAT(
        dst->Merge(*src4,
                   DenseSource::ConflictHandlingOption::kRaiseOnConflict),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                 HasSubstr("merge conflict")));
  }
}

TEST(DenseSourceTest, MergeUnit) {
  int size = 7;
  AllocationId alloc = Allocate(size);
  ASSERT_OK_AND_ASSIGN(
      auto src1,
      DenseSource::CreateReadonly(
          alloc, DataSliceImpl::Create(arolla::CreateDenseArray<Unit>(
                     {Unit(), std::nullopt, Unit(), Unit(), std::nullopt,
                      Unit(), Unit()}))));
  ASSERT_OK_AND_ASSIGN(
      auto src2,
      DenseSource::CreateReadonly(
          alloc, DataSliceImpl::Create(arolla::CreateDenseArray<Unit>(
                     {Unit(), Unit(), std::nullopt, Unit(), std::nullopt,
                      Unit(), std::nullopt}))));

  auto ids =
      DataSliceImpl::ObjectsFromAllocation(alloc, size).values<ObjectId>();

  auto options = {DenseSource::ConflictHandlingOption::kOverwrite,
                  DenseSource::ConflictHandlingOption::kKeepOriginal,
                  DenseSource::ConflictHandlingOption::kRaiseOnConflict};

  for (auto option1 : options) {
    for (auto option2 : options) {
      SCOPED_TRACE(absl::StrCat("option1: ", option1, " option2: ", option2));
      ASSERT_OK_AND_ASSIGN(
          auto dst, DenseSource::CreateMutable(
                        alloc, size, /*main_type=*/arolla::GetQType<Unit>()));
      ASSERT_OK(dst->Merge(*src1, option1));
      EXPECT_THAT(dst->Get(ids),
                  ElementsAre(Unit(), std::nullopt, Unit(), Unit(),
                              std::nullopt, Unit(), Unit()));
      ASSERT_OK(dst->Merge(*src2, option2));
      EXPECT_THAT(dst->Get(ids), ElementsAre(Unit(), Unit(), Unit(), Unit(),
                                             std::nullopt, Unit(), Unit()));
    }
  }
}

TEST(DenseSourceTest, MergeFullReadOnly) {
  int size = 7;
  AllocationId alloc = Allocate(size);
  ASSERT_OK_AND_ASSIGN(
      auto src,
      DenseSource::CreateReadonly(
          alloc, DataSliceImpl::Create(arolla::CreateFullDenseArray<int>(
                     {1, 2, 3, 4, 5, 6, 7}))));
  ASSERT_OK_AND_ASSIGN(
      auto src_inverse,
      DenseSource::CreateReadonly(
          alloc, DataSliceImpl::Create(arolla::CreateFullDenseArray<int>(
                     {7, 6, 5, 4, 3, 2, 1}))));

  auto ids =
      DataSliceImpl::ObjectsFromAllocation(alloc, size).values<ObjectId>();

  auto options = {DenseSource::ConflictHandlingOption::kOverwrite,
                  DenseSource::ConflictHandlingOption::kKeepOriginal,
                  DenseSource::ConflictHandlingOption::kRaiseOnConflict};

  for (auto option : options) {
    SCOPED_TRACE(absl::StrCat("option: ", option));
    ASSERT_OK_AND_ASSIGN(
        auto dst, DenseSource::CreateMutable(
                      alloc, size, /*main_type=*/arolla::GetQType<int>()));
    ASSERT_OK(dst->Merge(*src, option));
    EXPECT_THAT(dst->Get(ids), ElementsAre(1, 2, 3, 4, 5, 6, 7));
    ASSERT_OK(dst->Merge(*src_inverse,
                         DenseSource::ConflictHandlingOption::kKeepOriginal));
    EXPECT_THAT(dst->Get(ids), ElementsAre(1, 2, 3, 4, 5, 6, 7));
    ASSERT_OK(dst->Merge(*src_inverse,
                         DenseSource::ConflictHandlingOption::kOverwrite));
    EXPECT_THAT(dst->Get(ids), ElementsAre(7, 6, 5, 4, 3, 2, 1));
  }
}

TEST(DenseSourceTest, TypedAfterEmptySet) {
  int size = 100;
  AllocationId alloc = Allocate(size);
  auto objs =
      DataSliceImpl::ObjectsFromAllocation(alloc, size).values<ObjectId>();
  ASSERT_OK_AND_ASSIGN(
      auto source, DenseSource::CreateMutable(
                       alloc, size, /*main_type=*/arolla::GetQType<Unit>()));
  ASSERT_OK(source->Set(objs, DataSliceImpl::CreateEmptyAndUnknownType(size)));
  auto missing = std::vector<ObjectId>();
  // Call a method that would fail on a multitype dense source.
  ASSERT_OK(source->SetUnitAndUpdateMissingObjects(objs, missing));
}

TEST(DenseSourceTest, GetMultitype) {
  int size = 100;
  AllocationId alloc = Allocate(size);

  // Prepare multitype dense source
  auto value_fn = [](int i) -> DataItem {
    if (i % 3 == 0) {
      return DataItem(static_cast<float>(0.5 - i));
    } else if (i % 2 == 0) {
      return DataItem(i);
    } else {
      return DataItem();
    }
  };
  ASSERT_OK_AND_ASSIGN(auto source, DenseSource::CreateMutable(alloc, size));
  for (int i = 0; i < size; ++i) {
    ASSERT_OK(source->Set(alloc.ObjectByOffset(i), value_fn(i)));
  }

  // Prepare a non-trivial list of object ids to request.
  int request_size = size / 2;
  arolla::DenseArrayBuilder<ObjectId> objs_bldr(request_size);
  for (int i = 0; i < request_size; ++i) {
    if (i % 11 == 0) {
      continue;
    }
    objs_bldr.Set(i, alloc.ObjectByOffset((i * 7) % size));
  }
  DenseArray<ObjectId> objs = std::move(objs_bldr).Build();

  // Get operation
  SliceBuilder bldr(request_size);
  bldr.ApplyMask(objs.ToMask());
  source->Get(objs.values.span(), bldr);
  DataSliceImpl slice = std::move(bldr).Build();

  // Validate result
  for (int i = 0; i < request_size; ++i) {
    arolla::OptionalValue<ObjectId> obj = objs[i];
    if (obj.present) {
      EXPECT_EQ(slice[i], value_fn(obj.value.Offset())) << "at i = " << i;
    } else {
      EXPECT_EQ(slice[i], DataItem()) << "at i = " << i;
    }
  }
}

// Test that boolean values are initialized and we do not have undefined
// behavior in ForEachPresent.
TEST(DenseSourceTest, MergeSelfBoolInitialized) {
  AllocationId alloc = Allocate(3);
  ASSERT_OK_AND_ASSIGN(
      auto ds, DenseSource::CreateMutable(alloc, 3, arolla::GetQType<bool>()));
  ASSERT_OK(ds->Set(alloc.ObjectByOffset(1), DataItem(true)));
  ASSERT_OK(
      ds->Merge(*ds, DenseSource::ConflictHandlingOption::kRaiseOnConflict));

  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(0)), DataItem());
  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(1)), DataItem(true));
  EXPECT_EQ(ds->Get(alloc.ObjectByOffset(2)), DataItem());
}

}  // namespace
}  // namespace koladata::internal
