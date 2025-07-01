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
#include "koladata/functor/functor_storage.h"

#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/quote.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/signature.h"
#include "koladata/functor/signature_utils.h"
#include "koladata/internal/dtype.h"
#include "koladata/test_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

namespace {

using ::absl_testing::IsOkAndHolds;

absl::StatusOr<DataSlice> WrapExpr(
    absl::StatusOr<arolla::expr::ExprNodePtr> expr_or_error) {
  ASSIGN_OR_RETURN(auto expr, expr_or_error);
  return test::DataItem(arolla::expr::ExprQuote(std::move(expr)));
}

TEST(IsFunctorTest, Basic) {
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(arolla::expr::Literal(57)));
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto fn,
                       CreateFunctor(returns_expr, koda_signature, {}, {}));
  EXPECT_THAT(IsFunctor(fn), IsOkAndHolds(true));
  ASSERT_OK_AND_ASSIGN(auto fn2, fn.ForkBag());
  ASSERT_OK(fn2.DelAttr(kReturnsAttrName));
  EXPECT_THAT(IsFunctor(fn2), IsOkAndHolds(false));
  ASSERT_OK_AND_ASSIGN(auto fn3, fn.ForkBag());
  ASSERT_OK(fn3.DelAttr(kSignatureAttrName));
  EXPECT_THAT(IsFunctor(fn3), IsOkAndHolds(false));
  EXPECT_OK(IsFunctor(fn.WithBag(nullptr)));
}

TEST(IsFunctorTest, PrimitiveWithBag) {
  auto x = test::DataItem(1).WithBag(DataBag::Empty());
  EXPECT_THAT(IsFunctor(x), IsOkAndHolds(false));
  EXPECT_THAT(IsFunctor(*x.WithSchema(test::Schema(schema::kObject))),
              IsOkAndHolds(false));
}

}  // namespace

}  // namespace koladata::functor
