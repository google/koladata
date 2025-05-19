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
#include "koladata/functor/stack_trace.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/testing/status_matchers.h"
#include "arolla/util/text.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/object_factories.h"
#include "koladata/test_utils.h"

namespace koladata::functor {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::arolla::testing::CausedBy;
using ::arolla::testing::PayloadIs;
using ::testing::AllOf;
using ::testing::Eq;
using ::testing::Field;
using ::testing::IsEmpty;
using ::testing::Optional;
using ::testing::StrEq;

TEST(ReadStackTraceFrameTest, ReadsFullFrame) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice frame_slice,
      ObjectCreator::FromAttrs(
          db, {"function_name", "file_name", "line_text", "line_number"},
          {test::DataItem(arolla::Text("my_func")),
           test::DataItem(arolla::Text("my_file.cc")),
           test::DataItem(arolla::Text("z = x + y")), test::DataItem(57)}));

  EXPECT_THAT(
      ReadStackTraceFrame(frame_slice),
      Optional(AllOf(Field(&StackTraceFrame::function_name, Eq("my_func")),
                     Field(&StackTraceFrame::file_name, Eq("my_file.cc")),
                     Field(&StackTraceFrame::line_number, Eq(57)),
                     Field(&StackTraceFrame::line_text, Eq("z = x + y")))));
}

TEST(ReadStackTraceFrameTest, ReadsPartialFrame) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice frame_slice,
      ObjectCreator::FromAttrs(db, {"function_name"},
                               {test::DataItem(arolla::Text("my_func"))}));

  EXPECT_THAT(
      ReadStackTraceFrame(frame_slice),
      Optional(AllOf(Field(&StackTraceFrame::function_name, Eq("my_func")),
                     Field(&StackTraceFrame::file_name, IsEmpty()),
                     Field(&StackTraceFrame::line_number, Eq(0)))));
}

TEST(ReadStackTraceFrameTest, ReturnsNullOptForMissingAttributes) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice frame_slice,
      ObjectCreator::FromAttrs(db, {"other_attr"}, {test::DataItem(57)}));

  EXPECT_THAT(ReadStackTraceFrame(frame_slice), Eq(std::nullopt));
}

TEST(ReadStackTraceFrameTest, ReturnsNullOptForEmptyAttributes) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice frame_slice,
      ObjectCreator::FromAttrs(
          db, {"function_name", "file_name", "line_number"},
          {test::DataItem(arolla::Text("")), test::DataItem(arolla::Text("")),
           test::DataItem(0)}));

  EXPECT_THAT(ReadStackTraceFrame(frame_slice), Eq(std::nullopt));
}

TEST(ReadStackTraceFrameTest, ReturnsNullOptForIncorrectTypes) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice frame_slice,
      ObjectCreator::FromAttrs(db, {"line_number"},
                               {test::DataItem(arolla::Text("not_a_number"))}));
  EXPECT_THAT(ReadStackTraceFrame(frame_slice), Eq(std::nullopt));
}

TEST(MaybeAddStackTraceFrameTest, AddsFrameToStatus) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice frame_slice,
      ObjectCreator::FromAttrs(db, {"function_name"},
                               {test::DataItem(arolla::Text("my_func"))}));
  ASSERT_OK_AND_ASSIGN(
      DataSlice another_frame_slice,
      ObjectCreator::FromAttrs(
          db, {"function_name", "file_name", "line_text", "line_number"},
          {test::DataItem(arolla::Text("their_func")),
           test::DataItem(arolla::Text("their_file.cc")),
           test::DataItem(arolla::Text("z = x + y")), test::DataItem(57)}));

  EXPECT_THAT(
      MaybeAddStackTraceFrame(
          MaybeAddStackTraceFrame(absl::InvalidArgumentError("initial error"),
                                  frame_slice),
          another_frame_slice),
      AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                     StrEq("initial error\n"
                           "\n"
                           "in my_func\n"
                           "\n"
                           "their_file.cc:57, in their_func\n"
                           "z = x + y")),
            PayloadIs<StackTraceFrame>(
                Field(&StackTraceFrame::function_name, "their_func")),
            CausedBy(AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                                    StrEq("initial error\n"
                                          "\n"
                                          "in my_func")),
                           PayloadIs<StackTraceFrame>(Field(
                               &StackTraceFrame::function_name, Eq("my_func"))),
                           CausedBy(StatusIs(absl::StatusCode::kInvalidArgument,
                                             StrEq("initial error")))))));
}

TEST(MaybeAddStackTraceFrameTest, ReturnsOriginalStatusForOkStatus) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice frame_slice,
      ObjectCreator::FromAttrs(db, {"function_name"},
                               {test::DataItem(arolla::Text("error_func"))}));
  EXPECT_THAT(MaybeAddStackTraceFrame(absl::OkStatus(), frame_slice), IsOk());
}

TEST(MaybeAddStackTraceFrameTest, ReturnsOriginalStatusForInvalidFrame) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice frame_slice,
      ObjectCreator::FromAttrs(db, {"other_attr"}, {test::DataItem(57)}));

  absl::Status initial_error = absl::InvalidArgumentError("initial error");
  EXPECT_THAT(MaybeAddStackTraceFrame(initial_error, frame_slice),
              Eq(initial_error));
}

}  // namespace
}  // namespace koladata::functor
