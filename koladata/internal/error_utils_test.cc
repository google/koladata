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
#include "koladata/internal/error_utils.h"

#include <optional>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error.pb.h"
#include "koladata/s11n/codec.pb.h"
#include "arolla/util/status.h"
#include "arolla/util/testing/status_matchers.h"

namespace koladata::internal {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::CausedBy;
using ::koladata::schema::DType;
using ::koladata::schema::GetDType;
using ::koladata::schema::ItemIdDType;
using ::koladata::schema::ObjectDType;
using ::koladata::schema::SchemaDType;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::NotNull;
using ::testing::Property;
using ::testing::StrEq;

TEST(ErrorUtilsTest, GetEmptyPayload) {
  absl::Status status = absl::UnimplementedError("Test error");

  EXPECT_EQ(GetErrorPayload(status), std::nullopt);
}

TEST(ErrorUtilsTest, SetAndGetPayload) {
  Error error;
  error.set_error_message("test error message");

  absl::Status status = absl::UnimplementedError("Test error");

  absl::Status status_with_payload = WithErrorPayload(std::move(status), error);

  auto error_payload = GetErrorPayload(status_with_payload);
  ASSERT_TRUE(error_payload.has_value());
  EXPECT_THAT(error_payload->error_message(), StrEq("test error message"));

  absl::Status ok_status_with_payload =
      WithErrorPayload(absl::OkStatus(), error);
  EXPECT_EQ(GetErrorPayload(ok_status_with_payload), std::nullopt);
}

TEST(ErrorUtilsTest, WithErrorPayloadHandleError) {
  absl::Status status =
      WithErrorPayload(absl::UnimplementedError("Test error"),
                       absl::InternalError("Create error proto error"));

  EXPECT_THAT(
      status,
      StatusIs(absl::StatusCode::kUnimplemented,
               HasSubstr("; Error when creating KodaError")));
}

TEST(ErrorUtilsTest, AsKodaError) {
  absl::Status status = absl::UnimplementedError("test error");
  absl::Status koda_status = AsKodaError(status);

  EXPECT_THAT(koda_status.message(), Eq(status.message()));

  auto error_payload = GetErrorPayload(koda_status);
  ASSERT_TRUE(error_payload.has_value());
  EXPECT_THAT(error_payload->error_message(), StrEq("test error"));
}

TEST(ErrorUtilsTest, AsKodaError_OkStatus) {
  EXPECT_THAT(AsKodaError(absl::OkStatus()), IsOk());
}

struct DummyPayload {};

TEST(ErrorUtilsTest, KodaErrorFromCause) {
  absl::Status cause = arolla::WithPayload(
      absl::UnimplementedError("error cause"), DummyPayload{});
  absl::Status koda_status = KodaErrorFromCause("new error", cause);

  EXPECT_THAT(koda_status.message(), Eq("new error"));
  EXPECT_THAT(arolla::GetPayload<internal::Error>(koda_status),
              Property(&internal::Error::error_message, StrEq("new error")));
  EXPECT_THAT(
      koda_status,
      CausedBy(AllOf(StatusIs(absl::StatusCode::kUnimplemented, "error cause"),
                     ResultOf(&arolla::GetPayload<DummyPayload>, NotNull()))));
}

TEST(ErrorUtilsTest, KodaErrorFromCause_OkStatus) {
  EXPECT_THAT(KodaErrorFromCause("got an error", absl::OkStatus()), IsOk());
}

}  // namespace
}  // namespace koladata::internal
