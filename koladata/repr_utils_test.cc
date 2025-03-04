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
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/errors.h"
#include "koladata/internal/object_id.h"
#include "koladata/object_factories.h"
#include "koladata/test_utils.h"
#include "arolla/util/status.h"

namespace koladata {
namespace {

using ::absl_testing::StatusIs;
using ::koladata::internal::Error;
using ::testing::MatchesRegex;
using ::testing::Pointee;
using ::testing::Property;
using ::testing::StrEq;

TEST(ReprUtilTest, TestAssembleError_NoCommonSchema) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");

  ASSERT_OK_AND_ASSIGN(
      DataSlice entity,
      EntityCreator::FromAttrs(bag, {"a", "b"}, {value_1, value_2}));
  schema::DType dtype = schema::GetDType<int>();

  internal::NoCommonSchemaError error = {
      .common_schema = entity.GetSchemaImpl(),
      .conflicting_schema = internal::DataItem(dtype)};

  absl::Status status = AssembleErrorMessage(
      arolla::WithPayload(absl::InvalidArgumentError("error"),
                          std::move(error)),
      {bag});
  std::optional<Error> payload = internal::GetErrorPayload(status);
  EXPECT_TRUE(payload.has_value());
  EXPECT_THAT(
      payload->error_message(),
      AllOf(
          MatchesRegex(
              R"regex((.|\n)*cannot find a common schema(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*the common schema\(s\) \$[0-9a-zA-Z]{22}: SCHEMA\(a=INT32, b=STRING\)(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*the first conflicting schema INT32: INT32(.|\n)*)regex")));
}

TEST(ReprUtilTest, TestAssembleError_IncompatibleSchema) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      DataSlice schema_1,
      CreateEntitySchema(bag, {"y"}, {test::Schema(schema::kInt32)}));
  ASSERT_OK_AND_ASSIGN(
      DataSlice schema_2,
      CreateEntitySchema(bag, {"y"}, {test::Schema(schema::kInt64)}));

  Error error;
  internal::IncompatibleSchema* incompatible_schema =
      error.mutable_incompatible_schema();
  incompatible_schema->set_attr("x");
  ASSERT_OK_AND_ASSIGN(*incompatible_schema->mutable_expected_schema(),
                       internal::EncodeDataItem(schema_1.item()));
  ASSERT_OK_AND_ASSIGN(*incompatible_schema->mutable_assigned_schema(),
                       internal::EncodeDataItem(schema_2.item()));

  absl::Status status = AssembleErrorMessage(
      internal::WithErrorPayload(absl::InvalidArgumentError("error"), error),
      {bag});
  std::optional<Error> payload = internal::GetErrorPayload(status);
  EXPECT_TRUE(payload.has_value());
  EXPECT_TRUE(payload->has_incompatible_schema());
  EXPECT_THAT(
      payload->error_message(),
      AllOf(
          MatchesRegex(
              R"regex((.|\n)*the schema for attribute 'x' is incompatible(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*Expected schema for 'x': SCHEMA\(y=INT32\)(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*Assigned schema for 'x': SCHEMA\(y=INT64\)(.|\n)*)regex")));
}

TEST(ReprUtilTest, TestAssembleError_IncompatibleSchema_SameContent_DiffId) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      DataSlice schema_1,
      CreateEntitySchema(bag, {"y"}, {test::Schema(schema::kInt32)}));
  ASSERT_OK_AND_ASSIGN(
      DataSlice schema_2,
      CreateEntitySchema(bag, {"y"}, {test::Schema(schema::kInt32)}));

  Error error;
  internal::IncompatibleSchema* incompatible_schema =
      error.mutable_incompatible_schema();
  incompatible_schema->set_attr("x");
  ASSERT_OK_AND_ASSIGN(*incompatible_schema->mutable_expected_schema(),
                       internal::EncodeDataItem(schema_1.item()));
  ASSERT_OK_AND_ASSIGN(*incompatible_schema->mutable_assigned_schema(),
                       internal::EncodeDataItem(schema_2.item()));

  absl::Status status = AssembleErrorMessage(
      internal::WithErrorPayload(absl::InvalidArgumentError("error"), error),
      {bag});
  std::optional<Error> payload = internal::GetErrorPayload(status);
  EXPECT_TRUE(payload.has_value());
  EXPECT_TRUE(payload->has_incompatible_schema());
  EXPECT_THAT(
      payload->error_message(),
      AllOf(
          MatchesRegex(
              R"regex((.|\n)*the schema for attribute 'x' is incompatible(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*Expected schema for 'x': SCHEMA\(y=INT32\) with ItemId \$[0-9a-zA-Z]{22}(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*Assigned schema for 'x': SCHEMA\(y=INT32\) with ItemId \$[0-9a-zA-Z]{22}(.|\n)*)regex")));
}

TEST(ReprUtilTest, TestAssembleErrorMissingContextData) {
  internal::NoCommonSchemaError error = {
      .common_schema = internal::DataItem(internal::AllocateSingleObject()),
      .conflicting_schema = internal::DataItem(schema::GetDType<int>())};

  absl::Status status = AssembleErrorMessage(
      arolla::WithPayload(absl::InvalidArgumentError("error"),
                          std::move(error)),
      {});

  std::optional<Error> payload = internal::GetErrorPayload(status);
  EXPECT_TRUE(payload.has_value());
  EXPECT_THAT(
      payload->error_message(),
      AllOf(
          MatchesRegex(R"regex((.|\n)*conflicting schema INT32(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*the common schema\(s\) \$[0-9a-zA-Z]{22}(.|\n)*)regex")));

  internal::MissingObjectSchemaError error2 = {
      .missing_schema_item =
          internal::DataItem(internal::AllocateSingleObject())};
  EXPECT_THAT(
      AssembleErrorMessage(
          arolla::WithPayload(absl::InternalError("error"), error2), {}),
      StatusIs(absl::StatusCode::kInvalidArgument, "missing data slice"));
}

TEST(ReprUtilTest, TestAssembleErrorNotHandlingOkStatus) {
  EXPECT_TRUE(
      AssembleErrorMessage(absl::OkStatus(), {.db = DataBag::Empty()}).ok());
}

TEST(ReprUtilTest, TestCreateItemCreationError) {
  DataSlice value = test::DataItem(schema::kInt32);

  absl::Status status =
      CreateItemCreationError(absl::InvalidArgumentError("error"), value);

  EXPECT_THAT(
      status,
      StatusIs(absl::StatusCode::kInvalidArgument,
               "cannot create Item(s) with the provided schema: INT32"));
  std::optional<Error> payload = internal::GetErrorPayload(status);
  EXPECT_TRUE(payload.has_value());
  EXPECT_THAT(payload->error_message(),
              StrEq("cannot create Item(s) with the provided schema: INT32"));
  EXPECT_THAT(arolla::GetCause(status),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument, "error")));
  Error error;
  error.set_error_message("cause");
  status = CreateItemCreationError(
      internal::WithErrorPayload(absl::InvalidArgumentError("error"), error),
      value);
  EXPECT_THAT(
      status,
      StatusIs(absl::StatusCode::kInvalidArgument,
               "cannot create Item(s) with the provided schema: INT32"));
  payload = internal::GetErrorPayload(status);
  EXPECT_TRUE(payload.has_value());
  EXPECT_THAT(payload->error_message(),
              StrEq("cannot create Item(s) with the provided schema: INT32"));
  EXPECT_THAT(
      arolla::GetCause(status),
      Pointee(AllOf(StatusIs(absl::StatusCode::kInvalidArgument, "error"),
                    ResultOf(&arolla::GetPayload<internal::Error>,
                             Property(&internal::Error::error_message,
                                      StrEq("cause"))))));

  status = CreateItemCreationError(
      internal::WithErrorPayload(absl::InvalidArgumentError("error"), error),
      std::nullopt);
  payload = internal::GetErrorPayload(status);
  EXPECT_TRUE(payload.has_value());
  EXPECT_THAT(payload->error_message(),
              ::testing::StrEq("cannot create Item(s)"));
}

}  // namespace
}  // namespace koladata
