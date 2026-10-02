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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/util/status.h"
#include "arolla/util/testing/status_matchers.h"

namespace koladata::internal {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::CauseIs;
using ::arolla::testing::PayloadIs;
using ::testing::_;
using ::testing::AllOf;
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
                                Not(CauseIs(_))));
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
                    CauseIs(StatusIs(absl::StatusCode::kInvalidArgument,
                                     "error cause"))));
}

TEST(OperatorEvalError, WithStatusContainingCause) {
  absl::Status cause = absl::InvalidArgumentError("cause 1");
  absl::Status status = arolla::Error(absl::InvalidArgumentError("cause 2"),
                                      arolla::CausedBy(cause));

  absl::Status new_status = OperatorEvalError(status, "op_name", "error");
  EXPECT_THAT(
      new_status,
      AllOf(StatusIs(absl::StatusCode::kInvalidArgument, "op_name: error"),
            CauseIs(StatusIs(absl::StatusCode::kInvalidArgument, "cause 2")),
            CauseIs(CauseIs(
                StatusIs(absl::StatusCode::kInvalidArgument, "cause 1")))));
}

struct DummyPayload {};

TEST(OperatorEvalError, WithStatusContainingNonKodaPayload) {
  absl::Status status =
      arolla::Error(absl::InvalidArgumentError("cause"), DummyPayload{});

  absl::Status new_status = OperatorEvalError(status, "op_name", "error");
  EXPECT_THAT(
      new_status,
      AllOf(StatusIs(absl::StatusCode::kInvalidArgument, "op_name: error"),
            CauseIs(AllOf(StatusIs(absl::StatusCode::kInvalidArgument, "cause"),
                          PayloadIs<DummyPayload>()))));
}

TEST(OperatorEvalError, SubsequentCalls) {
  absl::Status status = OperatorEvalError(
      OperatorEvalError("op_name", "error_message"), "op_name");
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               "op_name: error_message"));
  EXPECT_THAT(arolla::GetCause(status), IsNull());
}

}  // namespace
}  // namespace koladata::internal
