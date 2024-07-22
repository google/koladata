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
#include "koladata/repr_utils.h"

#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/object_id.h"
#include "koladata/object_factories.h"
#include "koladata/test_utils.h"
#include "koladata/testing/status_matchers_backport.h"
#include "arolla/util/testing/equals_proto.h"

namespace koladata {
namespace {

using ::arolla::testing::EqualsProto;
using ::koladata::internal::Error;
using ::koladata::internal::ObjectId;
using ::koladata::testing::StatusIs;
using ::testing::MatchesRegex;

TEST(ReprUtilTest, TestAssembleError) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");

  ASSERT_OK_AND_ASSIGN(
      DataSlice entity,
      EntityCreator::FromAttrs(bag, {"a", "b"}, {value_1, value_2}));
  schema::DType dtype = schema::GetDType<int>();

  Error error;
  internal::NoCommonSchema* no_common_schema = error.mutable_no_common_schema();
  *no_common_schema->mutable_common_schema() =
      internal::EncodeSchema(entity.GetSchemaImpl());
  *no_common_schema->mutable_conflicting_schema() =
      internal::EncodeSchema(internal::DataItem(dtype));

  internal::ObjectId schema_obj =
      entity.GetSchemaImpl().value<internal::ObjectId>();

  absl::Status status = AssembleErrorMessage(
      internal::WithErrorPayload(absl::InvalidArgumentError("error"), error),
      {bag});
  std::optional<Error> payload = internal::GetErrorPayload(status);
  EXPECT_THAT(payload->cause(),
              EqualsProto(absl::StrFormat(
                  R"pb(no_common_schema {
                         common_schema { object_id { hi: %d lo: %d } }
                         conflicting_schema { dtype: %d }
                       })pb",
                  schema_obj.InternalHigh64(), schema_obj.InternalLow64(),
                  dtype.type_id())));
  EXPECT_THAT(
      payload->error_message(),
      AllOf(
          MatchesRegex(
              R"regex((.|\n)*cannot find a common schema for provided schemas(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*the common schema\(s\) [0-9a-f]{32}:0: SCHEMA\(a=INT32, b=TEXT\)(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*the first conflicting schema INT32: INT32(.|\n)*)regex")));
}

TEST(ReprUtilTest, TestAssembleErrorMissingContextData) {
  Error error;
  internal::NoCommonSchema* no_common_schema = error.mutable_no_common_schema();
  *no_common_schema->mutable_conflicting_schema() =
      internal::EncodeSchema(internal::DataItem(schema::GetDType<int>()));
  EXPECT_THAT(
      AssembleErrorMessage(
          internal::WithErrorPayload(absl::InternalError("error"), error), {}),
      StatusIs(absl::StatusCode::kInvalidArgument, "db is missing"));
}

TEST(ReprUtilTest, TestAssembleErrorNotHandlingOkStatus) {
  EXPECT_TRUE(AssembleErrorMessage(absl::OkStatus(), {}).ok());
}

}  // namespace
}  // namespace koladata
