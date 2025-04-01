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
#include "koladata/expr/expr_operators.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/test_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_slice_qtype.h"  // IWYU pragma: keep
#include "arolla/util/text.h"
#include "absl/status/status_matchers.h"

namespace koladata::expr {

namespace {

using ::absl_testing::IsOkAndHolds;
using testing::UnorderedElementsAre;

TEST(LiteralOperatorTest, DatabagLiteral) {
  // Literal freezes databag.
  {
    auto db = DataBag::Empty();
    auto literal_op = expr::LiteralOperator::MakeLiteralOperator(
        arolla::TypedValue::FromValue(db));

    EXPECT_NE(db, literal_op->value().UnsafeAs<DataBagPtr>());
    EXPECT_FALSE(literal_op->value().UnsafeAs<DataBagPtr>()->IsMutable());
    ASSERT_OK_AND_ASSIGN(auto attr, literal_op->InferAttributes({}));
    EXPECT_TRUE(attr.IsIdenticalTo(
        arolla::expr::ExprAttributes(literal_op->value())));
  }
  // Frozen Databag remains unchanged.
  {
    auto db = DataBag::Empty();
    db = db->Freeze();
    auto literal_op = expr::LiteralOperator::MakeLiteralOperator(
        arolla::TypedValue::FromValue(db));

    EXPECT_EQ(db, literal_op->value().UnsafeAs<DataBagPtr>());
    EXPECT_FALSE(literal_op->value().UnsafeAs<DataBagPtr>()->IsMutable());
    ASSERT_OK_AND_ASSIGN(auto attr, literal_op->InferAttributes({}));
    EXPECT_TRUE(attr.IsIdenticalTo(
        arolla::expr::ExprAttributes(literal_op->value())));
  }
}

TEST(LiteralOperatorTest, DataSliceLiteral) {
  // Literal freezes DataSlice.
  {
    auto db = DataBag::Empty();
    auto ds = test::DataSlice<int>({1, 2, 3}, db);
    auto literal_op = expr::LiteralOperator::MakeLiteralOperator(
        arolla::TypedValue::FromValue(ds));
    EXPECT_NE(db, literal_op->value().UnsafeAs<DataSlice>().GetBag());
    EXPECT_FALSE(
        literal_op->value().UnsafeAs<DataSlice>().GetBag()->IsMutable());
    ASSERT_OK_AND_ASSIGN(auto attr, literal_op->InferAttributes({}));
    EXPECT_TRUE(attr.IsIdenticalTo(
        arolla::expr::ExprAttributes(literal_op->value())));
  }
  // Frozen DataSlice remains unchanged.
  {
    auto db = DataBag::Empty();
    auto ds = test::DataSlice<int>({1, 2, 3}, db);
    ds = ds.FreezeBag();
    auto literal_op = expr::LiteralOperator::MakeLiteralOperator(
        arolla::TypedValue::FromValue(ds));
    EXPECT_EQ(ds.GetBag(), literal_op->value().UnsafeAs<DataSlice>().GetBag());
    EXPECT_FALSE(
        literal_op->value().UnsafeAs<DataSlice>().GetBag()->IsMutable());
    ASSERT_OK_AND_ASSIGN(auto attr, literal_op->InferAttributes({}));
    EXPECT_TRUE(attr.IsIdenticalTo(
        arolla::expr::ExprAttributes(literal_op->value())));
  }
}

TEST(LiteralOperatorTest, RegularLiteral) {
  auto literal_op = expr::LiteralOperator::MakeLiteralOperator(
      arolla::TypedValue::FromValue(int64_t{1}));
  EXPECT_EQ(1, literal_op->value().UnsafeAs<int64_t>());
  ASSERT_OK_AND_ASSIGN(auto attr, literal_op->InferAttributes({}));
  EXPECT_TRUE(attr.IsIdenticalTo(
      arolla::expr::ExprAttributes(literal_op->value())));
}

TEST(LiteralOperatorTest, FingerprintProperties) {
  // A literal constructed from a mutable databag has the same fingerprint as
  // a literal constructed from the resulting frozen databag.
  auto literal_op = expr::LiteralOperator::MakeLiteralOperator(
      arolla::TypedValue::FromValue(DataBag::Empty()));
  auto literal_op_from_frozen = expr::LiteralOperator::MakeLiteralOperator(
      literal_op->value());
  EXPECT_EQ(literal_op->fingerprint(), literal_op_from_frozen->fingerprint());

  // A literal constructed from a different databag has a different fingerprint.
  auto literal_from_second_bag = expr::LiteralOperator::MakeLiteralOperator(
      arolla::TypedValue::FromValue(DataBag::Empty()));
  EXPECT_NE(literal_op->fingerprint(), literal_from_second_bag->fingerprint());

  // Same as above but for DataSlices.
  auto literal_op_from_ds =
      expr::LiteralOperator::MakeLiteralOperator(arolla::TypedValue::FromValue(
          test::DataSlice<int>({1, 2, 3}, DataBag::Empty())));
  auto literal_op_from_frozen_ds = expr::LiteralOperator::MakeLiteralOperator(
      literal_op_from_ds->value());
  EXPECT_EQ(literal_op_from_ds->fingerprint(),
            literal_op_from_frozen_ds->fingerprint());
  auto literal_op_from_second_ds = expr::LiteralOperator::MakeLiteralOperator(
      arolla::TypedValue::FromValue(test::DataSlice<int>({1, 2, 3},
                                                         DataBag::Empty())));
  EXPECT_NE(literal_op_from_ds->fingerprint(),
            literal_op_from_second_ds->fingerprint());

  // Values different than db and ds.
  EXPECT_EQ(expr::LiteralOperator::MakeLiteralOperator(
      arolla::TypedValue::FromValue(int64_t{1}))->fingerprint(),
      expr::LiteralOperator::MakeLiteralOperator(
          arolla::TypedValue::FromValue(int64_t{1}))->fingerprint()
      );
}

TEST(LiteralOperatorTest, IsLiteral) {
  auto literal_op = expr::LiteralOperator::MakeLiteralOperator(
    arolla::TypedValue::FromValue(int64_t{1}));
  ASSERT_OK_AND_ASSIGN(auto koda_literal,
                       arolla::expr::MakeOpNode(literal_op, {}));
  auto arolla_literal = arolla::expr::Literal(int64_t{1});
  EXPECT_TRUE(IsLiteral(koda_literal));
  EXPECT_TRUE(IsLiteral(arolla_literal));
  EXPECT_FALSE(IsLiteral(arolla::expr::Leaf("x")));
}

TEST(InputContainerTest, Container) {
  ASSERT_OK_AND_ASSIGN(auto container, InputContainer::Create("V"));
  ASSERT_OK_AND_ASSIGN(auto a, container.CreateInput("a"));
  ASSERT_OK_AND_ASSIGN(auto b, container.CreateInput("b"));
  ASSERT_OK_AND_ASSIGN(auto sum, arolla::expr::CallOp("math.add", {a, b}));

  EXPECT_TRUE(IsInput(a));
  EXPECT_TRUE(IsInput(b));
  EXPECT_FALSE(IsInput(arolla::expr::Leaf("x")));
  EXPECT_FALSE(IsInput(sum));
  EXPECT_THAT(container.ExtractInputNames(sum),
              IsOkAndHolds(UnorderedElementsAre("a", "b")));

  ASSERT_OK_AND_ASSIGN(
      auto expected_a,
      arolla::expr::CallOp(
          "koda_internal.input",
          {MakeLiteral(arolla::TypedValue::FromValue(arolla::Text("V"))),
           MakeLiteral(arolla::TypedValue::FromValue(arolla::Text("a")))}));
  EXPECT_EQ(a->fingerprint(), expected_a->fingerprint());
}

}  // namespace

}  // namespace koladata::expr

