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
#include "koladata/internal/op_utils/trampoline_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"

namespace koladata::internal {
namespace {

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;

absl::Status Factorial(int x, TrampolineExecutor& executor, int& result) {
  if (x > 0) {
    auto y = std::make_unique<int>();
    executor.Enqueue([&executor, x, y_ptr = y.get()]() {
      return Factorial(x - 1, executor, *y_ptr);
    });
    executor.Enqueue([x, y = std::move(y), &result]() {
      result = x * (*y);
      return absl::OkStatus();
    });
  } else {
    result = 1;
  }
  return absl::OkStatus();
}

TEST(TrampolineExecutorTest, FactorialExample) {
  int result;

  ASSERT_OK(TrampolineExecutor::Run([&result](auto& executor) {
    return Factorial(5, executor, result);
  }));
  EXPECT_EQ(result, 120);

  ASSERT_OK(TrampolineExecutor::Run([&result](auto& executor) {
    return Factorial(11, executor, result);
  }));
  EXPECT_EQ(result, 39916800);
}

// Appends consecutive integers in the interval `[start, start + 4**depth)` to
// `values` in a roundabout way.
absl::Status FourTree(int depth, int start, TrampolineExecutor& executor,
                      std::vector<int>& values) {
  if (depth == 0) {
    values.push_back(start);
    return absl::OkStatus();
  }

  // step = 4 ** (depth - 1)
  const int step = 1 << (2 * (depth - 1));
  for (int i = 0; i < 4; ++i) {
    executor.Enqueue([&executor, depth, start, step, i, &values]() {
      return FourTree(depth - 1, start + step * i, executor, values);
    });
  }
  return absl::OkStatus();
}

TEST(TrampolineExecutorTest, ExecutionOrder) {
  std::vector<int> values;
  ASSERT_OK(TrampolineExecutor::Run(
      [&values](auto& executor) { return FourTree(2, 0, executor, values); }));

  EXPECT_THAT(values, ElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
                                  14, 15));
}

TEST(TrampolineExecutorTest, ErrorHandling) {
  int result;
  auto status = TrampolineExecutor::Run([&result](auto& executor) {
    result = 1;
    executor.Enqueue([&result]() {
      result = 2;
      return absl::OkStatus();
    });
    executor.Enqueue([&result]() {
      result = 3;
      return absl::InternalError("");
    });
    executor.Enqueue([&result]() {
      result = 4;
      return absl::OkStatus();
    });
    return absl::OkStatus();
  });

  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal));
  EXPECT_EQ(result, 3);
}

// Sets result to the sum from 1 to n inclusive (the hard way).
absl::Status SumSeries(int x, TrampolineExecutor& executor, int& result) {
  if (x > 0) {
    auto y = std::make_unique<int>();
    executor.Enqueue([&executor, x, y_ptr = y.get()]() {
      return SumSeries(x - 1, executor, *y_ptr);
    });
    executor.Enqueue([x, y = std::move(y), &result]() {
      result = x + (*y);
      return absl::OkStatus();
    });
  } else {
    result = 0;
  }
  return absl::OkStatus();
}

TEST(TrampolineExecutorTest, DeepRecursion) {
  int result;

  ASSERT_OK(TrampolineExecutor::Run([&result](auto& executor) {
    return SumSeries(5, executor, result);
  }));
  EXPECT_EQ(result, (5 * 6) / 2);

  ASSERT_OK(TrampolineExecutor::Run([&result](auto& executor) {
    return SumSeries(10000, executor, result);
  }));
  EXPECT_EQ(result, (10000 * 10001) / 2);
}


}  // namespace
}  // namespace koladata::internal
