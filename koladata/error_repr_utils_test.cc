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
#include "koladata/error_repr_utils.h"

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/string_view.h"
#include "arolla/util/status.h"
#include "arolla/util/testing/status_matchers.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/errors.h"
#include "koladata/internal/object_id.h"
#include "koladata/object_factories.h"
#include "koladata/test_utils.h"

namespace koladata {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::CausedBy;
using ::testing::AllOf;
using ::testing::MatchesRegex;

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

  absl::Status status = KodaErrorCausedByNoCommonSchemaError(
      arolla::WithPayload(absl::InvalidArgumentError("error"),
                          std::move(error)),
      {bag});
  EXPECT_THAT(
      status.message(),
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

  internal::IncompatibleSchemaError error = {
      .attr = "x",
      .expected_schema = schema_1.item(),
      .assigned_schema = schema_2.item(),
  };

  absl::Status status = KodaErrorCausedByIncompableSchemaError(
      arolla::WithPayload(absl::InvalidArgumentError("error"),
                          std::move(error)),
      bag, bag, schema_1);
  EXPECT_THAT(
      status.message(),
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

  internal::IncompatibleSchemaError error = {
      .attr = "x",
      .expected_schema = schema_1.item(),
      .assigned_schema = schema_2.item(),
  };

  absl::Status status = KodaErrorCausedByIncompableSchemaError(
      arolla::WithPayload(absl::InvalidArgumentError("error"),
                          std::move(error)),
      bag, bag, schema_1);
  EXPECT_THAT(
      status.message(),
      AllOf(
          MatchesRegex(
              R"regex((.|\n)*the schema for attribute 'x' is incompatible(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*Expected schema for 'x': SCHEMA\(y=INT32\) with ItemId \$[0-9a-zA-Z]{22}(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*Assigned schema for 'x': SCHEMA\(y=INT32\) with ItemId \$[0-9a-zA-Z]{22}(.|\n)*)regex")));
}

TEST(ReprUtilTest, TestKodaErrorCausedByNoCommonSchemaErrorMissingContextData) {
  internal::NoCommonSchemaError error = {
      .common_schema = internal::DataItem(internal::AllocateSingleObject()),
      .conflicting_schema = internal::DataItem(schema::GetDType<int>())};

  absl::Status status = KodaErrorCausedByNoCommonSchemaError(
      arolla::WithPayload(absl::InvalidArgumentError("error"),
                          std::move(error)),
      {});

  EXPECT_THAT(
      status.message(),
      AllOf(
          MatchesRegex(R"regex((.|\n)*conflicting schema INT32(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*the common schema\(s\) \$[0-9a-zA-Z]{22}(.|\n)*)regex")));
}

TEST(ReprUtilTest, TestKodaErrorCausedByNoCommonSchemaErrorOkStatus) {
  EXPECT_TRUE(
      KodaErrorCausedByNoCommonSchemaError(absl::OkStatus(), DataBag::Empty())
          .ok());
}

struct DummyPayload {};

TEST(ReprUtilTest, TestCreateItemCreationError) {
  DataSlice value = test::DataItem(schema::kInt32);

  absl::Status status =
      CreateItemCreationError(absl::InvalidArgumentError("error"), value);

  EXPECT_THAT(
      status,
      AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                     "cannot create Item(s) with the provided schema: INT32"),
            CausedBy(StatusIs(absl::StatusCode::kInvalidArgument, "error"))));
  status = CreateItemCreationError(
      arolla::WithPayload(absl::InvalidArgumentError("error"), DummyPayload()),
      value);
  EXPECT_THAT(
      status,
      AllOf(
          StatusIs(absl::StatusCode::kInvalidArgument,
                   "cannot create Item(s) with the provided schema: INT32"),
          CausedBy(AllOf(StatusIs(absl::StatusCode::kInvalidArgument, "error"),
                         ResultOf(&arolla::GetPayload<DummyPayload>,
                                  testing::NotNull())))));

  status = CreateItemCreationError(
      arolla::WithPayload(absl::InvalidArgumentError("error"), DummyPayload()),
      std::nullopt);
  EXPECT_THAT(status.message(), ::testing::StrEq("cannot create Item(s)"));
}

TEST(ReprUtilTest, TestKodaErrorCausedByMergeConflictError) {
  DataBagPtr bag = DataBag::Empty();
  DataSlice value_1 = test::DataItem(1);
  ASSERT_OK_AND_ASSIGN(DataSlice obj,
                       ObjectCreator::FromAttrs(bag, {"a"}, {value_1}));
  internal::DataBagMergeConflictError error = {
      .conflict = internal::DataBagMergeConflictError::EntityObjectConflict{
          .object_id = obj.item(), .attr_name = "a"}};
  absl::Status status =
      KodaErrorCausedByMergeConflictError(bag, bag)(arolla::WithPayload(
          absl::InvalidArgumentError("error"), std::move(error)));
  EXPECT_THAT(status.message(),
              testing::StartsWith("cannot merge DataBags due to an exception "
                                  "encountered when merging entities"));
}

TEST(ReprUtilTest, TestShapeAlignmentError) {
  std::vector<absl::string_view> attr_names = {"x", "y"};
  DataSlice ds_x = test::DataSlice<float>({3.0, 4.0, 5.0});
  DataSlice ds_y = test::DataSlice<int32_t>({1, 2});
  std::vector<DataSlice> values = {ds_x, ds_y};

  absl::Status basic_status = absl::InvalidArgumentError("basic error");
  absl::Status status_with_payload = arolla::WithPayload(
      std::move(basic_status), internal::ShapeAlignmentError{
                                   .common_shape_id = 0,
                                   .incompatible_shape_id = 1,
                               });

  absl::Status status = KodaErrorCausedByShapeAlignmentError(
      status_with_payload, attr_names, values);

  EXPECT_THAT(
      status,
      AllOf(
          StatusIs(
              absl::StatusCode::kInvalidArgument,
              "cannot align shapes due to a shape not being broadcastable "
              "to the common shape candidate.\n\n"
              "Common shape belonging to attribute 'x': JaggedShape(3)\n"
              "Incompatible shape belonging to attribute 'y': JaggedShape(2)"),
          CausedBy(
              StatusIs(absl::StatusCode::kInvalidArgument, "basic error"))));
}

TEST(ReprUtilTest, TestShapeAlignmentError_NoPayload) {
  std::vector<absl::string_view> attr_names = {"x", "y"};
  DataSlice ds_x = test::DataSlice<float>({3.0, 4.0, 5.0});
  DataSlice ds_y = test::DataSlice<int32_t>({1, 2});
  std::vector<DataSlice> values = {ds_x, ds_y};

  absl::Status basic_status = absl::InvalidArgumentError("basic error");

  absl::Status status =
      KodaErrorCausedByShapeAlignmentError(basic_status, attr_names, values);

  EXPECT_THAT(status,
              StatusIs(absl::StatusCode::kInvalidArgument, "basic error"));
}

}  // namespace
}  // namespace koladata
