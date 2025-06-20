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
#include "koladata/internal/op_utils/error.h"

#include <functional>
#include <type_traits>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/util/status.h"
#include "arolla/util/testing/status_matchers.h"
#include "koladata/internal/error_utils.h"

namespace koladata::internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::CausedBy;
using ::arolla::testing::PayloadIs;
using ::testing::_;
using ::testing::AllOf;
using ::testing::Eq;
using ::testing::Field;
using ::testing::IsNull;
using ::testing::Not;

TEST(OperatorEvalError, NoCause) {
  absl::Status status = OperatorEvalError("op_name", "error_message");
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               "op_name: error_message"));
}

TEST(OperatorEvalError, WithStatus) {
  absl::Status status = absl::InvalidArgumentError("Test error");
  absl::Status new_status = OperatorEvalError(status, "op_name");
  EXPECT_THAT(new_status, AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                                         "op_name: Test error"),
                                Not(CausedBy(_))));
}

TEST(OperatorEvalError, EmptyOperatorName) {
  absl::Status status = absl::InvalidArgumentError("Test error");
  absl::Status new_status = OperatorEvalError(status, "");
  EXPECT_THAT(new_status,
              StatusIs(absl::StatusCode::kInvalidArgument, "Test error"));
}

TEST(OperatorEvalError, WithStatusAndErrorMessage) {
  absl::Status status = absl::InvalidArgumentError("error cause");
  EXPECT_THAT(OperatorEvalError(status, "op_name", "error message"),
              AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                             "op_name: error message"),
                    CausedBy(StatusIs(absl::StatusCode::kInvalidArgument,
                                      "error cause"))));
}

TEST(OperatorEvalError, WithStatusContainingCause) {
  absl::Status cause = absl::InvalidArgumentError("cause 1");
  absl::Status status =
      arolla::WithCause(absl::InvalidArgumentError("cause 2"), cause);

  absl::Status new_status = OperatorEvalError(status, "op_name", "error");
  EXPECT_THAT(
      new_status,
      AllOf(StatusIs(absl::StatusCode::kInvalidArgument, "op_name: error"),
            CausedBy(StatusIs(absl::StatusCode::kInvalidArgument, "cause 2")),
            CausedBy(CausedBy(
                StatusIs(absl::StatusCode::kInvalidArgument, "cause 1")))));
}

struct DummyPayload {};

TEST(OperatorEvalError, WithStatusContainingNonKodaPayload) {
  absl::Status status =
      arolla::WithPayload(absl::InvalidArgumentError("cause"), DummyPayload{});

  absl::Status new_status = OperatorEvalError(status, "op_name", "error");
  EXPECT_THAT(
      new_status,
      AllOf(
          StatusIs(absl::StatusCode::kInvalidArgument, "op_name: error"),
          CausedBy(AllOf(StatusIs(absl::StatusCode::kInvalidArgument, "cause"),
                         PayloadIs<DummyPayload>()))));
}

TEST(OperatorEvalError, SubsequentCalls) {
  absl::Status status = OperatorEvalError(
      OperatorEvalError("op_name", "error_message"), "op_name");
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               "op_name: error_message"));
  EXPECT_THAT(arolla::GetCause(status), IsNull());
}

absl::StatusOr<int> ReturnsErrorOr(int x, int y) {
  return absl::InvalidArgumentError(absl::StrFormat("test error %d%d", x, y));
};

TEST(ReturnsOperatorEvalError, WrapsStatusOr) {
  auto wrapped_fn = ReturnsOperatorEvalError("op_name", ReturnsErrorOr);
  auto status = wrapped_fn(5, 7).status();
  EXPECT_THAT(status, AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                                     "op_name: test error 57"),
                            Not(CausedBy(_))));
}

absl::Status ReturnsError(int x, int y) {
  return absl::InvalidArgumentError(absl::StrFormat("test error %d%d", x, y));
};

TEST(ReturnsOperatorEvalError, WrapsStatus) {
  auto wrapped_fn = ReturnsOperatorEvalError("op_name", ReturnsError);
  auto status = wrapped_fn(5, 7);
  EXPECT_THAT(status, AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                                     "op_name: test error 57"),
                            Not(CausedBy(_))));
}

TEST(ReturnsOperatorEvalError, WithLambda) {
  auto fn = [](int x, int y) -> absl::StatusOr<int> {
    return absl::InvalidArgumentError(absl::StrFormat("test error %d%d", x, y));
  };
  auto wrapped_fn = ReturnsOperatorEvalError("op_name", fn);
  auto status = wrapped_fn(5, 7).status();
  EXPECT_THAT(status, AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                                     "op_name: test error 57"),
                            Not(CausedBy(_))));
}

// Counts the number of times the object is copied or moved.
struct CopyCounter {
 public:
  CopyCounter() = default;
  CopyCounter(CopyCounter&& other)
      : copy_count(other.copy_count), move_count(other.move_count + 1) {};
  CopyCounter& operator=(CopyCounter&& other) {
    copy_count = other.copy_count;
    move_count = other.move_count + 1;
    return *this;
  }
  CopyCounter(const CopyCounter& other)
      : copy_count(other.copy_count + 1), move_count(other.move_count) {}
  CopyCounter& operator=(const CopyCounter& other) {
    copy_count = other.copy_count + 1;
    move_count = other.move_count;
    return *this;
  }
  int copy_count = 0;
  int move_count = 0;
};

TEST(ReturnsOperatorEvalError, CopyCounter) {
  CopyCounter counter;
  EXPECT_THAT(counter, Field(&CopyCounter::copy_count, Eq(0)));
  EXPECT_THAT(CopyCounter(counter),
              AllOf(Field(&CopyCounter::copy_count, Eq(1)),
                    Field(&CopyCounter::move_count, Eq(0))));
  EXPECT_THAT(CopyCounter(std::move(counter)),
              AllOf(Field(&CopyCounter::copy_count, Eq(0)),
                    Field(&CopyCounter::move_count, Eq(1))));
}

absl::StatusOr<int> AcceptsCopyCounters(CopyCounter by_value,
                                        const CopyCounter& by_const_ref,
                                        CopyCounter&& by_rvalue,
                                        CopyCounter& by_ref) {
  return absl::InvalidArgumentError(absl::StrFormat(
      "by_value: %d/%d, by_const_ref: %d/%d, by_rvalue: %d/%d, by_ref: %d/%d",
      by_value.copy_count, by_value.move_count, by_const_ref.copy_count,
      by_const_ref.move_count, by_rvalue.copy_count, by_rvalue.move_count,
      by_ref.copy_count, by_ref.move_count));
};

// Check that the wrapped function has exact the same signature as the original
// function.
static_assert(std::is_same_v<decltype(std::function(AcceptsCopyCounters)),
                             decltype(std::function(ReturnsOperatorEvalError(
                                 "foo", AcceptsCopyCounters)))>);

TEST(ReturnsOperatorEvalError, NoExtraInputCopies) {
  // Test the original function.
  {
    CopyCounter counter1;
    CopyCounter counter2;
    CopyCounter counter3;
    CopyCounter counter4;
    EXPECT_THAT(
        AcceptsCopyCounters(counter1, counter2, std::move(counter3), counter4),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            "by_value: 1/0, by_const_ref: 0/0, by_rvalue: 0/0, by_ref: 0/0"));
  }
  // Test the wrapped function.
  {
    CopyCounter counter1;
    CopyCounter counter2;
    CopyCounter counter3;
    CopyCounter counter4;
    auto wrapped_fn = ReturnsOperatorEvalError("op_name", AcceptsCopyCounters);
    auto status =
        wrapped_fn(counter1, counter2, std::move(counter3), counter4).status();
    EXPECT_THAT(status,
                AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                               "op_name: by_value: 1/1, by_const_ref: 0/0, "
                               "by_rvalue: 0/0, by_ref: 0/0"),
                      Not(CausedBy(_))));
  }
}

absl::StatusOr<CopyCounter> ReturnsCopyCounter() { return CopyCounter(); };

TEST(ReturnsOperatorEvalError, NoExtraResultCopies) {
  // Test the original function.
  EXPECT_THAT(ReturnsCopyCounter(),
              IsOkAndHolds(AllOf(Field(&CopyCounter::copy_count, Eq(0)),
                                 Field(&CopyCounter::move_count, Eq(1)))));
  // Test the wrapped function.
  auto wrapped_fn = ReturnsOperatorEvalError("op_name", ReturnsCopyCounter);
  EXPECT_THAT(wrapped_fn(),
              IsOkAndHolds(AllOf(Field(&CopyCounter::copy_count, Eq(0)),
                                 Field(&CopyCounter::move_count, Eq(2)))));
}

}  // namespace
}  // namespace koladata::internal
