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
#include "koladata/functor/while.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/sequence.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/unit.h"
#include "koladata/data_slice.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/cpp_function_bridge.h"
#include "koladata/functor/signature_utils.h"
#include "koladata/internal/dtype.h"
#include "koladata/signature.h"
#include "koladata/test_utils.h"

namespace koladata::functor {
namespace {

using ::absl_testing::StatusIs;

TEST(WhileTest, FactorialNoYields) {
  ASSERT_OK_AND_ASSIGN(
      auto condition,
      CreateFunctorFromFunction(
          [](const DataSlice& n,
             const DataSlice& returns) -> absl::StatusOr<DataSlice> {
            if (n.item().value<int64_t>() > 0) {
              return test::DataItem(arolla::kUnit);
            } else {
              return test::DataItem(std::nullopt, schema::kMask);
            }
          },
          "condition", "n, returns"));
  ASSERT_OK_AND_ASSIGN(
      auto body,
      CreateFunctorFromStdFunction(
          [](absl::Span<const arolla::TypedRef> args)
              -> absl::StatusOr<arolla::TypedValue> {
            const int64_t n =
                args[0].UnsafeAs<DataSlice>().item().value<int64_t>();
            const int64_t returns =
                args[1].UnsafeAs<DataSlice>().item().value<int64_t>();
            auto n_1 = arolla::TypedValue::FromValue(test::DataItem(n - 1));
            auto returns_1 =
                arolla::TypedValue::FromValue(test::DataItem(returns * n));
            return arolla::MakeNamedTuple({"n", "returns"},
                                          {n_1.AsRef(), returns_1.AsRef()});
          },
          "body", "n, returns",
          *arolla::MakeNamedTupleQType(
              {"n", "returns"},
              arolla::MakeTupleQType({arolla::GetQType<DataSlice>(),
                                      arolla::GetQType<DataSlice>()}))));

  std::vector<arolla::TypedValue> var_values = {
      arolla::TypedValue::FromValue(test::DataItem(static_cast<int64_t>(1))),
      arolla::TypedValue::FromValue(test::DataItem(static_cast<int64_t>(5))),
  };
  std::vector<std::string> var_names = {"returns", "n"};

  ASSERT_OK_AND_ASSIGN(var_values,
                       WhileWithCompilationCache(condition, body, var_names,
                                                 std::move(var_values)));
  EXPECT_EQ(var_values[0].UnsafeAs<DataSlice>().item().value<int64_t>(), 120);
  EXPECT_EQ(var_values[1].UnsafeAs<DataSlice>().item().value<int64_t>(), 0);
}

TEST(WhileTest, FactorialYields) {
  ASSERT_OK_AND_ASSIGN(
      auto condition,
      CreateFunctorFromFunction(
          [](const DataSlice& n,
             const DataSlice& result) -> absl::StatusOr<DataSlice> {
            if (n.item().value<int64_t>() > 0) {
              return test::DataItem(arolla::kUnit);
            } else {
              return test::DataItem(std::nullopt, schema::kMask);
            }
          },
          "condition", "n, result"));
  ASSERT_OK_AND_ASSIGN(
      auto body,
      CreateFunctorFromStdFunction(
          [](absl::Span<const arolla::TypedRef> args)
              -> absl::StatusOr<arolla::TypedValue> {
            const int64_t n =
                args[0].UnsafeAs<DataSlice>().item().value<int64_t>();
            const int64_t result =
                args[1].UnsafeAs<DataSlice>().item().value<int64_t>();
            auto n_1 = arolla::TypedValue::FromValue(test::DataItem(n - 1));
            auto result_1 =
                arolla::TypedValue::FromValue(test::DataItem(result * n));
            return arolla::MakeNamedTuple(
                {"n", "result", "yields"},
                {n_1.AsRef(), result_1.AsRef(), result_1.AsRef()});
          },
          "body", "n, result",
          *arolla::MakeNamedTupleQType(
              {"n", "result", "yields"},
              arolla::MakeTupleQType({arolla::GetQType<DataSlice>(),
                                      arolla::GetQType<DataSlice>(),
                                      arolla::GetQType<DataSlice>()}))));

  std::vector<arolla::TypedValue> var_values = {
      arolla::TypedValue::FromValue(
          test::DataItem(std::nullopt, schema::kInt64)),
      arolla::TypedValue::FromValue(test::DataItem(static_cast<int64_t>(5))),
      arolla::TypedValue::FromValue(test::DataItem(static_cast<int64_t>(1))),
  };
  std::vector<std::string> var_names = {"yields", "n", "result"};

  ASSERT_OK_AND_ASSIGN(var_values,
                       WhileWithCompilationCache(condition, body, var_names,
                                                 std::move(var_values), 1));

  ASSERT_OK_AND_ASSIGN(const auto& yields_seq,
                       var_values[0].As<arolla::Sequence>());
  EXPECT_EQ(yields_seq.get().value_qtype(), arolla::GetQType<DataSlice>());
  EXPECT_EQ(yields_seq.get().size(), 6);
  auto yields_span = yields_seq.get().UnsafeSpan<DataSlice>();
  EXPECT_FALSE(yields_span[0].item().has_value());
  EXPECT_EQ(yields_span[1].item().value<int64_t>(), 5);
  EXPECT_EQ(yields_span[2].item().value<int64_t>(), 20);
  EXPECT_EQ(yields_span[3].item().value<int64_t>(), 60);
  EXPECT_EQ(yields_span[4].item().value<int64_t>(), 120);
  EXPECT_EQ(yields_span[5].item().value<int64_t>(), 120);

  EXPECT_EQ(var_values[1].UnsafeAs<DataSlice>().item().value<int64_t>(), 0);
  EXPECT_EQ(var_values[2].UnsafeAs<DataSlice>().item().value<int64_t>(), 120);
}

TEST(WhileTest, InvalidVarArguments) {
  ASSERT_OK_AND_ASSIGN(auto condition, CreateFunctorFromFunction(
                                           []() -> absl::StatusOr<DataSlice> {
                                             return test::DataItem(
                                                 std::nullopt, schema::kMask);
                                           },
                                           "condition", ""));
  ASSERT_OK_AND_ASSIGN(
      auto body, CreateFunctorFromStdFunction(
                     [](absl::Span<const arolla::TypedRef> args)
                         -> absl::StatusOr<arolla::TypedValue> {
                       return arolla::MakeEmptyNamedTuple();
                     },
                     "body", "", arolla::MakeEmptyNamedTuple().GetType()));

  {
    std::vector<std::string> var_names = {"x", "y"};
    std::vector<arolla::TypedValue> var_values = {arolla::MakeEmptyTuple()};
    auto result =
        WhileWithCompilationCache(condition, body, var_names, var_values);
    EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                                 "the number of variable values must match the "
                                 "number of variable names, got 1 != 2"));
  }
  {
    std::vector<std::string> var_names = {"x", "x"};
    std::vector<arolla::TypedValue> var_values = {arolla::MakeEmptyTuple(),
                                                  arolla::MakeEmptyTuple()};
    auto result =
        WhileWithCompilationCache(condition, body, var_names, var_values);
    EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                                 "duplicate variable name: `x`"));
  }
  {
    std::vector<std::string> var_names = {"x", "y"};
    std::vector<arolla::TypedValue> var_values = {arolla::MakeEmptyTuple(),
                                                  arolla::MakeEmptyTuple()};
    auto result =
        WhileWithCompilationCache(condition, body, var_names, var_values, 3);
    EXPECT_THAT(
        result,
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "invalid number of yields vars: 3 not between 0 and 2"));
  }
}

TEST(WhileTest, CancelEmptyFunctors) {
  arolla::CancellationContext::ScopeGuard cancellation_scope;

  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  auto condition_returns = test::DataItem(arolla::kUnit);
  ASSERT_OK_AND_ASSIGN(
      auto condition, CreateFunctor(condition_returns, koda_signature, {}, {}));

  auto body_returns = test::DataItem(arolla::expr::ExprQuote(
      arolla::expr::Literal(arolla::MakeEmptyNamedTuple())));
  ASSERT_OK_AND_ASSIGN(auto body,
                       CreateFunctor(body_returns, koda_signature, {}, {}));

  arolla::CancellationContext::ScopeGuard::
  current_cancellation_context()
      ->Cancel();

  auto result = WhileWithCompilationCache(condition, body, {}, {});
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kCancelled));
}

TEST(WhileTest, CancelAfterFirstBody) {
  arolla::CancellationContext::ScopeGuard cancellation_scope;

  ASSERT_OK_AND_ASSIGN(auto condition,
                       CreateFunctorFromFunction(
                           []() -> absl::StatusOr<DataSlice> {
                             return test::DataItem(arolla::kUnit);
                           },
                           "condition", ""));
  ASSERT_OK_AND_ASSIGN(
      auto body, CreateFunctorFromStdFunction(
                     [](absl::Span<const arolla::TypedRef> args)
                         -> absl::StatusOr<arolla::TypedValue> {
                       arolla::CancellationContext::ScopeGuard::
                           current_cancellation_context()
                               ->Cancel();
                       return arolla::MakeEmptyNamedTuple();
                     },
                     "body", "", arolla::MakeEmptyNamedTuple().GetType()));

  auto result = WhileWithCompilationCache(condition, body, {}, {});
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kCancelled));
}

}  // namespace
}  // namespace koladata::functor
