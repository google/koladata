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
#include "koladata/expr/expr_operators.h"

#include <cstdint>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/text.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice_qtype.h"  // IWYU pragma: keep
#include "koladata/test_utils.h"

namespace koladata::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::HasSubstr;
using testing::UnorderedElementsAre;

TEST(MakeLiteralTest, Basics) {
  constexpr auto check = [](arolla::TypedValue value) {
    ASSERT_OK_AND_ASSIGN(auto literal, MakeLiteral(value));
    EXPECT_TRUE(
        literal->attr().IsIdenticalTo(arolla::expr::ExprAttributes(value)));
    auto literal_op = dynamic_cast<const LiteralOperator*>(literal->op().get());
    ASSERT_NE(literal_op, nullptr);
    EXPECT_EQ(literal_op->value().GetFingerprint(), value.GetFingerprint());
  };

  check(arolla::TypedValue::FromValue(DataBag::Empty()));
  check(arolla::TypedValue::FromValue(DataBagPtr{nullptr}));
  check(arolla::TypedValue::FromValue(
      test::DataSlice<int>({1, 2, 3}, DataBag::Empty())));
  check(arolla::TypedValue::FromValue(int64_t{1}));

  {  // Mutable DataBag is not supported.
    auto db = arolla::TypedValue::FromValue(DataBag::EmptyMutable());
    EXPECT_THAT(MakeLiteral(db), StatusIs(absl::StatusCode::kInvalidArgument,
                                          HasSubstr("DataBag is not frozen")));
  }
  {  // Mutable DataSlice is not supported.
    auto ds = arolla::TypedValue::FromValue(
        test::DataSlice<int>({1, 2, 3}, DataBag::EmptyMutable()));
    EXPECT_THAT(LiteralOperator::Make(ds),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("DataSlice is not frozen")));
  }
}

TEST(MakeLiteralTest, FingerprintProperties) {
  auto db_1 = arolla::TypedValue::FromValue(DataBag::Empty());
  auto db_2 = arolla::TypedValue::FromValue(DataBag::Empty());
  ASSERT_OK_AND_ASSIGN(auto literal_1_1, MakeLiteral(db_1));
  ASSERT_OK_AND_ASSIGN(auto literal_1_2, MakeLiteral(db_1));
  ASSERT_OK_AND_ASSIGN(auto literal_2, MakeLiteral(db_2));
  EXPECT_EQ(literal_1_1->fingerprint(), literal_1_2->fingerprint());
  EXPECT_NE(literal_1_1->fingerprint(), literal_2->fingerprint());
}

TEST(MakeLiteralTest, IsLiteral) {
  auto value = arolla::TypedValue::FromValue(int64_t{1});
  ASSERT_OK_AND_ASSIGN(auto koda_literal, MakeLiteral(value));
  auto arolla_literal = arolla::expr::Literal(value);
  EXPECT_TRUE(IsLiteral(koda_literal));
  EXPECT_TRUE(IsLiteral(arolla_literal));
  EXPECT_FALSE(IsLiteral(arolla::expr::Leaf("x")));
  {
    ASSERT_OK_AND_ASSIGN(
        auto reg_op,
        arolla::expr::RegisterOperator(
            "koladata_testing.make_literal_test.literal_operator",
            LiteralOperator::Make(arolla::TypedValue::FromValue(int64_t{1}))));
    ASSERT_OK_AND_ASSIGN(auto reg_literal, arolla::expr::CallOp(reg_op, {}));
    EXPECT_FALSE(IsLiteral(reg_literal));
  }
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

  EXPECT_THAT(container.GetInputName(a), IsOkAndHolds("a"));
  EXPECT_THAT(container.GetInputName(b), IsOkAndHolds("b"));
  EXPECT_THAT(container.GetInputName(sum), IsOkAndHolds(std::nullopt));
  EXPECT_THAT(container.GetInputName(arolla::expr::Leaf("x")),
              IsOkAndHolds(std::nullopt));

  ASSERT_OK_AND_ASSIGN(
      auto expected_a,
      arolla::expr::CallOp(
          "koda_internal.input",
          {MakeLiteral(arolla::TypedValue::FromValue(arolla::Text("V"))),
           MakeLiteral(arolla::TypedValue::FromValue(arolla::Text("a")))}));
  EXPECT_EQ(a->fingerprint(), expected_a->fingerprint());

  // For historical reasons in some places we might get arolla literals rather
  // than koladata::expr::LiteralOperator. Here we test that `GetInputName`
  // doesn't fail in such cases.
  ASSERT_OK_AND_ASSIGN(
      auto c, arolla::expr::CallOp("koda_internal.input",
                                   {arolla::expr::Literal(arolla::Text("V")),
                                    arolla::expr::Literal(arolla::Text("c"))}));
  EXPECT_THAT(container.GetInputName(c), IsOkAndHolds("c"));
}

}  // namespace
}  // namespace koladata::expr
