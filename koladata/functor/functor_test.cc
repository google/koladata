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
#include "koladata/functor/functor.h"

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/quote.h"
#include "arolla/util/text.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/expr/constants.h"
#include "koladata/functor/signature_utils.h"
#include "koladata/functor_storage.h"
#include "koladata/object_factories.h"
#include "koladata/signature.h"
#include "koladata/signature_storage.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"

namespace koladata::functor {

namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::koladata::testing::IsEquivalentTo;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::FieldsAre;
using ::testing::HasSubstr;
using ::testing::Optional;

absl::StatusOr<arolla::expr::ExprNodePtr> CreateInput(absl::string_view name) {
  return arolla::expr::CallOp("koda_internal.input",
                              {arolla::expr::Literal(arolla::Text("I")),
                               arolla::expr::Literal(arolla::Text(name))});
}

absl::StatusOr<arolla::expr::ExprNodePtr> CreateVariable(
    absl::string_view name) {
  return arolla::expr::CallOp("koda_internal.input",
                              {arolla::expr::Literal(arolla::Text("V")),
                               arolla::expr::Literal(arolla::Text(name))});
}

absl::StatusOr<DataSlice> WrapExpr(
    absl::StatusOr<arolla::expr::ExprNodePtr> expr_or_error) {
  ASSIGN_OR_RETURN(auto expr, expr_or_error);
  return test::DataItem(arolla::expr::ExprQuote(std::move(expr)));
}

TEST(CreateFunctorTest, Basic) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(
      auto returns_expr,
      WrapExpr(arolla::expr::CallOp("math.multiply",
                                    {CreateInput("a"), CreateVariable("a")})));
  ASSERT_OK_AND_ASSIGN(auto var_a_expr, WrapExpr(CreateInput("b")));
  auto slice_57 = test::DataItem(57);
  ASSERT_OK_AND_ASSIGN(
      auto my_obj,
      ObjectCreator::FromAttrs(DataBag::EmptyMutable(), {"a"}, {slice_57}));
  ASSERT_OK_AND_ASSIGN(
      auto fn, CreateFunctor(returns_expr, koda_signature, {"a", "my_obj"},
                             {var_a_expr, my_obj}));
  EXPECT_FALSE(fn.GetBag()->IsMutable());
  EXPECT_THAT(fn.GetAttr(kReturnsAttrName),
              IsOkAndHolds(IsEquivalentTo(returns_expr.WithBag(fn.GetBag()))));
  EXPECT_THAT(fn.GetAttr("a"),
              IsOkAndHolds(IsEquivalentTo(var_a_expr.WithBag(fn.GetBag()))));
  EXPECT_THAT(
      fn.GetAttr(kSignatureAttrName),
      IsOkAndHolds(IsEquivalentTo(koda_signature.WithBag(fn.GetBag()))));
  // Make sure the signature was adopted.
  ASSERT_OK_AND_ASSIGN(auto fn_signature, fn.GetAttr(kSignatureAttrName));
  EXPECT_OK(KodaSignatureToCppSignature(fn_signature).status());
  // Make sure my_obj was adopted.
  ASSERT_OK_AND_ASSIGN(auto my_obj_in_fn, fn.GetAttr("my_obj"));
  EXPECT_THAT(my_obj_in_fn.GetAttr("a"),
              IsOkAndHolds(IsEquivalentTo(slice_57.WithBag(fn.GetBag()))));
}

TEST(CreateFunctorTest, DefaultSignature) {
  using enum Signature::Parameter::Kind;
  ASSERT_OK_AND_ASSIGN(
      auto returns_expr,
      WrapExpr(arolla::expr::CallOp("math.multiply",
                                    {CreateInput("a"), CreateVariable("a")})));
  ASSERT_OK_AND_ASSIGN(auto var_a_expr, WrapExpr(CreateInput("b")));
  auto slice_57 = test::DataItem(57);
  ASSERT_OK_AND_ASSIGN(
      auto fn, CreateFunctor(returns_expr, DataSlice(), {"a"}, {var_a_expr}));
  EXPECT_FALSE(fn.GetBag()->IsMutable());
  ASSERT_OK_AND_ASSIGN(auto koda_signature, fn.GetAttr(kSignatureAttrName));
  ASSERT_OK_AND_ASSIGN(auto signature,
                       KodaSignatureToCppSignature(koda_signature));
  EXPECT_THAT(
      signature.parameters(),
      ElementsAre(
          FieldsAre(
              "self", kPositionalOnly,
              Optional(IsEquivalentTo(expr::UnspecifiedSelfInput().WithBag(
                  koda_signature.GetBag())))),
          FieldsAre("a", kKeywordOnly, Eq(std::nullopt)),
          FieldsAre("b", kKeywordOnly, Eq(std::nullopt)),
          FieldsAre("__extra_inputs__", kVarKeyword, Eq(std::nullopt))));
}

TEST(CreateFunctorTest, NonExprReturns) {
  auto slice_57 = test::DataItem(57);
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto fn,
                       CreateFunctor(slice_57, koda_signature, {}, {}));
  EXPECT_FALSE(fn.GetBag()->IsMutable());
  EXPECT_THAT(fn.GetAttr(kReturnsAttrName),
              IsOkAndHolds(IsEquivalentTo(slice_57.WithBag(fn.GetBag()))));
}

TEST(CreateFunctorTest, Non0RankReturns) {
  auto slice_57 = test::DataItem(57);
  ASSERT_OK_AND_ASSIGN(
      auto slice_57_1dim,
      slice_57.Reshape(DataSlice::JaggedShape::FlatFromSize(1)));
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  EXPECT_THAT(
      CreateFunctor(slice_57_1dim, koda_signature, {}, {}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "returns must be a data item, but has shape: JaggedShape(1)"));
}

TEST(CreateFunctorTest, InvalidSignature) {
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(arolla::expr::Literal(57)));
  auto slice_57 = test::DataItem(57);
  EXPECT_THAT(CreateFunctor(returns_expr, slice_57, {}, {}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("failed to get attribute 'parameters'")));
}

TEST(CreateFunctorTest, VariablesWithNon0Rank) {
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(arolla::expr::Literal(57)));
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  auto slice_57 = test::DataItem(57);
  ASSERT_OK_AND_ASSIGN(
      auto slice_57_with_dim,
      slice_57.Reshape(DataSlice::JaggedShape::FlatFromSize(1)));
  EXPECT_THAT(
      CreateFunctor(returns_expr, koda_signature, {"a"}, {slice_57_with_dim}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "variable [a] must be a data item, but has shape: JaggedShape(1)"));
}

TEST(CreateFunctorTest, UndefinedVariable) {
  // returns = V.a, but 'a' is not defined.
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(CreateVariable("a")));
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  EXPECT_THAT(CreateFunctor(returns_expr, koda_signature, {}, {}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("undefined variables: a")));
}

TEST(CreateFunctorTest, MultipleUndefinedVariables) {
  // returns = V.a + V.b, neither defined.
  ASSERT_OK_AND_ASSIGN(
      auto returns_expr,
      WrapExpr(arolla::expr::CallOp(
          "math.add", {CreateVariable("a"), CreateVariable("b")})));
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  EXPECT_THAT(CreateFunctor(returns_expr, koda_signature, {}, {}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("undefined variables: a, b")));
}

TEST(CreateFunctorTest, CycleDetection) {
  // returns = V.a, a = V.b, b = V.a (cycle between a and b).
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(CreateVariable("a")));
  ASSERT_OK_AND_ASSIGN(auto var_a_expr, WrapExpr(CreateVariable("b")));
  ASSERT_OK_AND_ASSIGN(auto var_b_expr, WrapExpr(CreateVariable("a")));
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  EXPECT_THAT(
      CreateFunctor(returns_expr, koda_signature, {"a", "b"},
                    {var_a_expr, var_b_expr}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("has a dependency cycle")));
}

TEST(CreateFunctorTest, SelfCycle) {
  // returns = V.a, a = V.a (self-cycle).
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(CreateVariable("a")));
  ASSERT_OK_AND_ASSIGN(auto var_a_expr, WrapExpr(CreateVariable("a")));
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  EXPECT_THAT(CreateFunctor(returns_expr, koda_signature, {"a"}, {var_a_expr}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("variable [a] has a dependency cycle")));
}

TEST(ForEachReachableVariableTest, TopologicalOrder) {
  // returns = V.a, a = V.b, b = 42.
  // Expected topological order: b, a, returns.
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(CreateVariable("a")));
  ASSERT_OK_AND_ASSIGN(auto var_a_expr, WrapExpr(CreateVariable("b")));
  auto var_b = test::DataItem(42);
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(
      auto fn,
      CreateFunctor(returns_expr, koda_signature, {"a", "b"},
                    {var_a_expr, var_b}));

  std::vector<std::string> visited;
  ASSERT_OK(ForEachReachableVariable(
      fn, [&visited](absl::string_view name, const DataSlice&) {
        visited.push_back(std::string(name));
        return absl::OkStatus();
      }));
  EXPECT_THAT(visited, ElementsAre("b", "a", "returns"));
}

TEST(ForEachReachableVariableTest, UndefinedVariablesAreLeaves) {
  // Manually construct a functor where returns = V.a, but 'a' is not defined.
  // ForEachReachableVariable should still succeed — it treats undefined
  // variables as leaves with missing values.
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(CreateVariable("a")));
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  DataBagPtr db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(
      auto fn,
      ObjectCreator::FromAttrs(
          db, {"returns", "__signature__"}, {returns_expr, koda_signature}));
  db->UnsafeMakeImmutable();

  std::vector<std::string> visited;
  ASSERT_OK(ForEachReachableVariable(
      fn, [&visited](absl::string_view name, const DataSlice&) {
        visited.push_back(std::string(name));
        return absl::OkStatus();
      }));
  EXPECT_THAT(visited, ElementsAre("a", "returns"));
}

TEST(ForEachReachableVariableTest, CycleDetection) {
  // Manually construct a functor with a cycle: returns = V.a, a = V.a.
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(CreateVariable("a")));
  ASSERT_OK_AND_ASSIGN(auto var_a_expr, WrapExpr(CreateVariable("a")));
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  DataBagPtr db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(
      auto fn, ObjectCreator::FromAttrs(
                   db, {"returns", "a", "__signature__"},
                   {returns_expr, var_a_expr, koda_signature}));
  db->UnsafeMakeImmutable();

  EXPECT_THAT(ForEachReachableVariable(
                  fn, [](absl::string_view, const DataSlice&) {
                    return absl::OkStatus();
                  }),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("variable [a] has a dependency cycle")));
}

TEST(ForEachReachableVariableTest, NonFunctor) {
  auto non_functor = test::DataItem(42);
  EXPECT_THAT(ForEachReachableVariable(
                  non_functor, [](absl::string_view, const DataSlice&) {
                    return absl::OkStatus();
                  }),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("functor expected")));
}

}  // namespace

}  // namespace koladata::functor
