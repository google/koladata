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
#include "koladata/internal/op_utils/utils.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"

namespace koladata::internal {
namespace {

using ::absl_testing::StatusIs;
using ::testing::StrEq;

TEST(OperatorEvalError, NoCause) {
  absl::Status status = OperatorEvalError("op_name", "error_message");
  EXPECT_THAT(status,
              StatusIs(absl::StatusCode::kInvalidArgument, "error_message"));
  std::optional<internal::Error> payload =
      internal::GetErrorPayload(status);
  EXPECT_TRUE(payload.has_value());
  EXPECT_THAT(
      payload->error_message(),
      StrEq("operator op_name failed during evaluation: error_message"));
  EXPECT_FALSE(payload->has_cause());
}

TEST(OperatorEvalError, WithStatus) {
  absl::Status stutus = absl::InvalidArgumentError("Test error");
  absl::Status new_status =
      OperatorEvalError(stutus, "op_name", "error_message");
  EXPECT_THAT(new_status,
              StatusIs(absl::StatusCode::kInvalidArgument, "Test error"));
  std::optional<internal::Error> payload =
      internal::GetErrorPayload(new_status);
  EXPECT_TRUE(payload.has_value());
  EXPECT_THAT(
      payload->error_message(),
      StrEq("operator op_name failed during evaluation: error_message"));
  EXPECT_THAT(payload->cause().error_message(), StrEq("Test error"));
}

TEST(OperatorEvalError, WithStatusContainingCause) {
  internal::Error error;
  error.set_error_message("cause");
  absl::Status stutus = absl::InvalidArgumentError("Test error");
  stutus = internal::WithErrorPayload(stutus, error);

  absl::Status new_status =
      OperatorEvalError(stutus, "op_name", "error_message");
  EXPECT_THAT(new_status,
              StatusIs(absl::StatusCode::kInvalidArgument, "Test error"));
  std::optional<internal::Error> payload =
      internal::GetErrorPayload(new_status);
  EXPECT_TRUE(payload.has_value());
  EXPECT_THAT(
      payload->error_message(),
      StrEq("operator op_name failed during evaluation: error_message"));
  EXPECT_THAT(payload->cause().error_message(), StrEq("cause"));
}

}  // namespace
}  // namespace koladata::internal
