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
#include "absl/strings/str_format.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/object_id.h"
#include "koladata/s11n/codec.pb.h"
#include "arolla/util/meta.h"
#include "arolla/util/testing/equals_proto.h"

namespace koladata::internal {
namespace {

using ::arolla::testing::EqualsProto;
using ::koladata::internal::ObjectId;
using ::koladata::s11n::KodaV1Proto;
using ::koladata::schema::AnyDType;
using ::koladata::schema::DType;
using ::koladata::schema::GetDType;
using ::koladata::schema::ItemIdDType;
using ::koladata::schema::NoneDType;
using ::koladata::schema::ObjectDType;
using ::koladata::schema::SchemaDType;
using ::testing::Optional;

TEST(ErrorUtilsTest, ObjectId) {
  ObjectId alloc_schema = internal::AllocateExplicitSchema();

  KodaV1Proto::DataItemProto obj_proto = EncodeSchema(DataItem(alloc_schema));

  EXPECT_THAT(obj_proto,
              EqualsProto(absl::StrFormat(R"pb(object_id: { hi: %d lo: %d })pb",
                                          alloc_schema.InternalHigh64(),
                                          alloc_schema.InternalLow64())));
}

TEST(ErrorUtilsTest, TestEncodeDtype) {
  arolla::meta::foreach_type(schema::supported_dtype_values(), [](auto tpe) {
    using T = typename decltype(tpe)::type;
    DType dtype = GetDType<T>();
    KodaV1Proto::DataItemProto item_proto = EncodeSchema(DataItem(dtype));
    EXPECT_THAT(item_proto, EqualsProto(absl::StrFormat(R"pb(dtype: %d)pb",
                                                        dtype.type_id())));
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

}  // namespace
}  // namespace koladata::internal
