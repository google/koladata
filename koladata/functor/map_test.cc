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
// This file contains only basic tests, more comprehensive tests are in Python,
// in particular because most of the useful operators are defined in Python.

#include "koladata/functor/map.h"

#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/quote.h"
#include "arolla/jagged_shape/testing/matchers.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/text.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/signature.h"
#include "koladata/functor/signature_storage.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::CancellationContext;
using ::koladata::testing::IsEquivalentTo;

absl::StatusOr<arolla::expr::ExprNodePtr> CreateInput(absl::string_view name) {
  return arolla::expr::CallOp("koda_internal.input",
                              {arolla::expr::Literal(arolla::Text("I")),
                               arolla::expr::Literal(arolla::Text(name))});
}

absl::StatusOr<DataSlice> WrapExpr(
    absl::StatusOr<arolla::expr::ExprNodePtr> expr_or_error) {
  ASSIGN_OR_RETURN(auto expr, expr_or_error);
  return test::DataItem(arolla::expr::ExprQuote(std::move(expr)));
}

TEST(MapTest, Basic) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
  };
  Signature::Parameter p2 = {
      .name = "b",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1, p2}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto returns_a_expr, WrapExpr(CreateInput("a")));
  ASSERT_OK_AND_ASSIGN(auto returns_b_expr, WrapExpr(CreateInput("b")));
  ASSERT_OK_AND_ASSIGN(auto fn_a,
                       CreateFunctor(returns_a_expr, koda_signature, {}, {}));
  ASSERT_OK_AND_ASSIGN(auto fn_b,
                       CreateFunctor(returns_b_expr, koda_signature, {}, {}));
  auto merged_bag = DataBag::CommonDataBag({fn_a.GetBag(), fn_b.GetBag()});
  auto fn = test::DataSlice<internal::DataItem>(
      {fn_a.item(), fn_b.item(), internal::DataItem(), fn_a.item()},
      merged_bag);
  auto a_input = test::DataSlice<int>({1, 2, 3, 4});
  auto b_input = test::DataSlice<int>({5, 6, 7, std::nullopt});
  auto expected1 = test::DataSlice<int>({1, 6, std::nullopt, std::nullopt});
  EXPECT_THAT(MapFunctorWithCompilationCache(fn, /*args=*/{a_input, b_input},
                                             /*kwnames=*/{"a", "b"},
                                             /*include_missing=*/false),
              IsOkAndHolds(IsEquivalentTo(expected1)));
  EXPECT_THAT(MapFunctorWithCompilationCache(fn, /*args=*/{a_input, b_input},
                                             /*kwnames=*/{"b"},
                                             /*include_missing=*/false),
              IsOkAndHolds(IsEquivalentTo(expected1)));
  EXPECT_THAT(MapFunctorWithCompilationCache(fn, /*args=*/{a_input, b_input},
                                             /*kwnames=*/{},
                                             /*include_missing=*/false),
              IsOkAndHolds(IsEquivalentTo(expected1)));

  auto expected2 = test::DataSlice<int>({1, 6, std::nullopt, 4});
  EXPECT_THAT(MapFunctorWithCompilationCache(fn, /*args=*/{a_input, b_input},
                                             /*kwnames=*/{"b"},
                                             /*include_missing=*/true),
              IsOkAndHolds(IsEquivalentTo(expected2)));
}

TEST(MapTest, Alignment) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
  };
  Signature::Parameter p2 = {
      .name = "b",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1, p2}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto returns_a_expr, WrapExpr(CreateInput("a")));
  ASSERT_OK_AND_ASSIGN(auto returns_b_expr, WrapExpr(CreateInput("b")));
  ASSERT_OK_AND_ASSIGN(auto fn_a,
                       CreateFunctor(returns_a_expr, koda_signature, {}, {}));
  ASSERT_OK_AND_ASSIGN(auto fn_b,
                       CreateFunctor(returns_b_expr, koda_signature, {}, {}));
  auto merged_bag = DataBag::CommonDataBag({fn_a.GetBag(), fn_b.GetBag()});
  auto fn = test::DataSlice<internal::DataItem>({fn_a.item(), fn_b.item()},
                                                merged_bag);
  auto shape = test::ShapeFromSplitPoints({{0, 2}, {0, 3, 7}});
  auto a_input = test::DataSlice<int>({1, 2, 3, 4, 5, 6, 7}, shape);
  auto b_input = test::DataItem(8);
  auto expected = test::DataSlice<int>({1, 2, 3, 8, 8, 8, 8}, shape);
  EXPECT_THAT(MapFunctorWithCompilationCache(fn, /*args=*/{a_input, b_input},
                                             /*kwnames=*/{"b"},
                                             /*include_missing=*/false),
              IsOkAndHolds(IsEquivalentTo(expected)));
}

TEST(MapTest, Cancellation) {
  ASSERT_OK_AND_ASSIGN(
      auto signature,
      Signature::Create({
          {.name = "a",
           .kind = Signature::Parameter::Kind::kPositionalOrKeyword},
      }));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto returns_expr,
                       WrapExpr(arolla::expr::CallOp(
                           "core._identity_with_cancel", {CreateInput("a")})));
  ASSERT_OK_AND_ASSIGN(auto fn,
                       CreateFunctor(returns_expr, koda_signature, {}, {}));

  std::vector<int> items = {0, 1};
  ASSERT_OK_AND_ASSIGN(
      auto test_slice,
      DataSlice::Create(
          internal::DataSliceImpl::Create(arolla::CreateFullDenseArray(items)),
          DataSlice::JaggedShape::FlatFromSize(items.size()),
          internal::DataItem(schema::GetDType<int>())));
  {  // Without cancellation scope.
    EXPECT_THAT(MapFunctorWithCompilationCache(fn, /*args=*/{test_slice},
                                               /*kwnames=*/{},
                                               /*include_missing=*/false),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {  // With cancellation scope.
    CancellationContext::ScopeGuard cancellation_scope;
    EXPECT_THAT(MapFunctorWithCompilationCache(fn, /*args=*/{test_slice},
                                               /*kwnames=*/{},
                                               /*include_missing=*/false),
                StatusIs(absl::StatusCode::kCancelled));
  }
}

}  // namespace
}  // namespace koladata::functor
