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
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/object_id.h"
#include "koladata/s11n/codec.pb.h"
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"
#include "arolla/util/testing/equals_proto.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata::internal {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsProto;
using ::koladata::schema::AnyDType;
using ::koladata::schema::DType;
using ::koladata::schema::GetDType;
using ::koladata::schema::ItemIdDType;
using ::koladata::schema::ObjectDType;
using ::koladata::schema::SchemaDType;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::Optional;

TEST(ErrorUtilsTest, TestEncodeDataItem) {
  std::vector<DataItem> items{DataItem(1),
                              DataItem(2.f),
                              DataItem(3l),
                              DataItem(3.5),
                              DataItem(),
                              DataItem(internal::AllocateSingleObject()),
                              DataItem(arolla::kUnit),
                              DataItem(arolla::Text("abc")),
                              DataItem(arolla::Bytes("cba")),
                              DataItem(schema::kBytes)};
  for (const auto& item : items) {
    ASSERT_OK_AND_ASSIGN(auto item_proto, EncodeDataItem(item));
    EXPECT_THAT(DecodeDataItem(item_proto), IsOkAndHolds(item));
  }
}

TEST(ErrorUtilsTest, TestEncodeDtype) {
  arolla::meta::foreach_type(schema::supported_dtype_values(), [](auto tpe) {
    using T = typename decltype(tpe)::type;
    DType dtype = GetDType<T>();

    ASSERT_OK_AND_ASSIGN(auto item_proto, EncodeDataItem(DataItem(dtype)));
    EXPECT_THAT(DecodeDataItem(item_proto), IsOkAndHolds(DataItem(dtype)));
  });
}

TEST(ErrorUtilsTest, GetEmptyPayload) {
  absl::Status status = absl::UnimplementedError("Test error");

  EXPECT_EQ(GetErrorPayload(status), std::nullopt);
}

TEST(ErrorUtilsTest, SetAndGetPayload) {
  Error error;
  error.set_error_message("test error message");

  absl::Status status = absl::UnimplementedError("Test error");

  absl::Status status_with_payload = WithErrorPayload(std::move(status), error);

  EXPECT_THAT(
      GetErrorPayload(status_with_payload),
      Optional(EqualsProto(R"pb(error_message: "test error message")pb")));

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
  EXPECT_THAT(GetErrorPayload(koda_status),
              Optional(EqualsProto(R"pb(error_message: "test error")pb")));
}

TEST(ErrorUtilsTest, AsKodaError_OkStatus) {
  EXPECT_THAT(AsKodaError(absl::OkStatus()), IsOk());
}

TEST(ErrorUtilsTest, KodaErrorFromCause) {
  absl::Status status = absl::UnimplementedError("test error");
  absl::Status koda_status = KodaErrorFromCause("got an error", status);

  EXPECT_THAT(koda_status.message(), Eq(status.message()));
  EXPECT_THAT(
      GetErrorPayload(koda_status),
      Optional(EqualsProto(R"pb(error_message: "got an error"
                                cause { error_message: "test error" })pb")));
}

TEST(ErrorUtilsTest, KodaErrorFromCause_OkStatus) {
  EXPECT_THAT(KodaErrorFromCause("got an error", absl::OkStatus()), IsOk());
}

}  // namespace
}  // namespace koladata::internal
