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
#include "koladata/operators/unary_op.h"

#include <cstdint>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/status/status_matchers.h"
#include "arolla/memory/optional_value.h"
#include "koladata/internal/dtype.h"
#include "koladata/operators/utils.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"

namespace koladata::ops {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::koladata::testing::IsEquivalentTo;

struct TestOp {
  int64_t operator()(int64_t x) const { return x + 1; }
  arolla::OptionalValue<double> operator()(double x) const {
    if (x > 0) {
      return x * 2.0;
    }
    return std::nullopt;
  }
  absl::StatusOr<int64_t> operator()(int32_t x) const {
    if (x < 0) {
      return absl::InvalidArgumentError("negative input");
    }
    return x * 3;
  }
};

TEST(UnaryOpTest, Int64) {
  auto arg = test::DataSlice<int64_t>({1, 2, std::nullopt, 4});
  auto expected = test::DataSlice<int64_t>({2, 3, std::nullopt, 5});
  EXPECT_THAT(UnaryOpEval<TestOp>(arg), IsOkAndHolds(IsEquivalentTo(expected)));

  auto arg_item = test::DataItem<int64_t>(1);
  auto expected_item = test::DataItem<int64_t>(2);
  EXPECT_THAT(UnaryOpEval<TestOp>(arg_item),
              IsOkAndHolds(IsEquivalentTo(expected_item)));
}

TEST(UnaryOpTest, Double) {
  auto arg = test::DataSlice<double>({1.0, -2.0, std::nullopt, 4.0});
  auto expected =
      test::DataSlice<double>({2.0, std::nullopt, std::nullopt, 8.0});
  EXPECT_THAT(UnaryOpEval<TestOp>(arg), IsOkAndHolds(IsEquivalentTo(expected)));

  EXPECT_THAT(UnaryOpEval<TestOp>(test::DataItem<double>(-1.0)),
              IsOkAndHolds(IsEquivalentTo(
                  test::DataItem(std::nullopt, schema::kFloat64))));
  EXPECT_THAT(UnaryOpEval<TestOp>(test::DataItem<double>(3.0)),
              IsOkAndHolds(IsEquivalentTo(test::DataItem<double>(6.0))));
}

TEST(UnaryOpTest, Int32) {
  auto arg = test::DataSlice<int32_t>({1, 2, std::nullopt, 4});
  auto expected = test::DataSlice<int64_t>({3, 6, std::nullopt, 12});
  EXPECT_THAT(UnaryOpEval<TestOp>(arg), IsOkAndHolds(IsEquivalentTo(expected)));

  auto arg_item = test::DataItem<int32_t>(-1);
  EXPECT_THAT(UnaryOpEval<TestOp>(arg_item),
              StatusIs(absl::StatusCode::kInvalidArgument, "negative input"));
}

TEST(UnaryOpTest, OutputDType) {
  auto arg = test::DataSlice<int64_t>({1, 2, std::nullopt, 4});
  auto expected = test::DataSlice<int64_t>({2, 3, std::nullopt, 5});
  EXPECT_THAT(UnaryOpEval<TestOp>(arg, IntegerArgs("x"), schema::kInt64),
              IsOkAndHolds(IsEquivalentTo(expected)));

  EXPECT_THAT(
      UnaryOpEval<TestOp>(arg, IntegerArgs("x"), schema::kInt32),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               "Fn output type is not consistent with output schema: INT64 vs "
               "INT32"));
}

TEST(UnaryOpTest, IsInvocable) {
  auto arg_i64 = test::DataSlice<int64_t>({1});
  auto expected_i64 = test::DataSlice<int64_t>({2});
  EXPECT_THAT((UnaryOpEval<TestOp>(arg_i64, IntegerArgs("x"))),
              IsOkAndHolds(IsEquivalentTo(expected_i64)));

  auto arg_f64 = test::DataSlice<double>({1.0});
  EXPECT_THAT((UnaryOpEval<TestOp>(arg_f64, IntegerArgs("x"))),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `x` must be a slice of integer values, got a "
                       "slice of FLOAT64"));
}

}  // namespace
}  // namespace koladata::ops
