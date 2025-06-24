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
#include "koladata/functor/parallel/stream_loop_internal.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/matchers.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/unit.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_qtype.h"
#include "koladata/internal/dtype.h"
#include "koladata/test_utils.h"

namespace koladata::functor::parallel::stream_loop_internal {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::QValueWith;
using ::testing::ElementsAre;
using ::testing::Pointer;

TEST(ParseLoopConditionDataSliceTest, Positive) {
  for (auto ds : {
           test::DataItem(arolla::Unit(), schema::kMask),
           test::DataItem(arolla::Unit(), schema::kObject),
       }) {
    ASSERT_OK_AND_ASSIGN(auto parsed_condition,
                         ParseLoopConditionDataSlice(ds));
    EXPECT_TRUE(parsed_condition.value);
    EXPECT_EQ(parsed_condition.reader, nullptr);
  }
}

TEST(ParseLoopConditionDataSliceTest, Negative) {
  for (auto ds : {
           test::DataItem(std::nullopt, schema::kMask),
           test::DataItem(std::nullopt, schema::kObject),
       }) {
    ASSERT_OK_AND_ASSIGN(auto parsed_condition,
                         ParseLoopConditionDataSlice(ds));
    EXPECT_FALSE(parsed_condition.value);
    EXPECT_EQ(parsed_condition.reader, nullptr);
  }
}

TEST(ParseLoopConditionDataSliceTest, Error) {
  EXPECT_THAT(ParseLoopConditionDataSlice(test::DataItem(1)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the condition value must be a data-item with schema "
                       "MASK, got DataItem(1, schema: INT32)"));
  EXPECT_THAT(
      ParseLoopConditionDataSlice(test::DataItem(std::nullopt, schema::kNone)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "the condition value must be a data-item with schema "
               "MASK, got DataItem(None, schema: NONE)"));
}

TEST(ParseLoopConditionStreamTest, Positive) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>(), 1);
  writer->Write(arolla::TypedRef::FromValue(test::DataItem(arolla::kUnit)));
  std::move(*writer).Close();
  ASSERT_OK_AND_ASSIGN(auto parsed_condition,
                       ParseLoopConditionStream(stream->MakeReader()));
  EXPECT_TRUE(parsed_condition.value);
  EXPECT_EQ(parsed_condition.reader, nullptr);
}

TEST(ParseLoopConditionStreamTest, Negative) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>(), 1);
  writer->Write(
      arolla::TypedRef::FromValue(test::DataItem(std::nullopt, schema::kMask)));
  std::move(*writer).Close();
  ASSERT_OK_AND_ASSIGN(auto parsed_condition,
                       ParseLoopConditionStream(stream->MakeReader()));
  EXPECT_FALSE(parsed_condition.value);
  EXPECT_EQ(parsed_condition.reader, nullptr);
}

TEST(ParseLoopConditionStreamTest, Pending) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>(), 1);
  auto reader = stream->MakeReader();
  auto* reader_original = reader.get();
  ASSERT_OK_AND_ASSIGN(auto parsed_condition,
                       ParseLoopConditionStream(std::move(reader)));
  EXPECT_FALSE(parsed_condition.value);
  EXPECT_THAT(parsed_condition.reader, Pointer(reader_original));
}

TEST(ParseLoopConditionStream, Error) {
  {
    auto [stream, writer] = MakeStream(arolla::GetQType<int>(), 1);
    writer->Write(arolla::TypedRef::FromValue(1));
    std::move(*writer).Close();
    EXPECT_THAT(ParseLoopConditionStream(stream->MakeReader()),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "the condition functor must return a DATA_SLICE or a "
                         "STREAM[DATA_SLICE], but got STREAM[INT32]"));
  }
  {
    auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>(), 1);
    std::move(*writer).Close();
    EXPECT_THAT(ParseLoopConditionStream(stream->MakeReader()),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "the condition functor returned an empty stream"));
  }
  {
    auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>(), 1);
    std::move(*writer).Close(absl::CancelledError("Boom!"));
    EXPECT_THAT(ParseLoopConditionStream(stream->MakeReader()),
                StatusIs(absl::StatusCode::kCancelled, "Boom!"));
  }
  {
    auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>(), 1);
    writer->Write(arolla::TypedRef::FromValue(test::DataItem(1)));
    std::move(*writer).Close();
    EXPECT_THAT(ParseLoopConditionStream(stream->MakeReader()),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "the condition value must be a data-item with schema "
                         "MASK, got DataItem(1, schema: INT32)"));
  }
}

TEST(ParseLoopConditionTest, Positive) {
  auto ds = test::DataItem(arolla::Unit(), schema::kMask);
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  writer->Write(arolla::TypedRef::FromValue(ds));
  std::move(*writer).Close();
  for (arolla::TypedRef condition : {
           arolla::TypedRef::FromValue(ds),
           MakeStreamQValueRef(stream),
       }) {
    ASSERT_OK_AND_ASSIGN(auto parsed_condition, ParseLoopCondition(condition));
    EXPECT_TRUE(parsed_condition.value);
    EXPECT_EQ(parsed_condition.reader, nullptr);
  }
}

TEST(ParseLoopConditionTest, Negative) {
  auto ds = test::DataItem(std::nullopt, schema::kMask);
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  writer->Write(arolla::TypedRef::FromValue(ds));
  std::move(*writer).Close();
  for (arolla::TypedRef condition : {
           arolla::TypedRef::FromValue(ds),
           MakeStreamQValueRef(stream),
       }) {
    ASSERT_OK_AND_ASSIGN(auto parsed_condition, ParseLoopCondition(condition));
    EXPECT_FALSE(parsed_condition.value);
    EXPECT_EQ(parsed_condition.reader, nullptr);
  }
}

TEST(ParseLoopConditionTest, Pending) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>(), 1);
  ASSERT_OK_AND_ASSIGN(auto parsed_condition,
                       ParseLoopCondition(MakeStreamQValueRef(stream)));
  EXPECT_FALSE(parsed_condition.value);
  EXPECT_NE(parsed_condition.reader, nullptr);
}

TEST(ParseLoopConditionTest, Error) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>(), 1);
  EXPECT_THAT(ParseLoopCondition(MakeStreamQValueRef(std::move(stream))),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the condition functor must return a DATA_SLICE or a "
                       "STREAM[DATA_SLICE], but got STREAM[INT32]"));
}

TEST(VarsTest, Empty) {
  Vars vars({}, {});
  EXPECT_TRUE(vars.kwnames().empty());
  EXPECT_TRUE(vars.values().empty());
  EXPECT_TRUE(vars.mutable_values().empty());
}

TEST(VarsTest, NonEmpty) {
  Vars vars({arolla::TypedRef::FromValue(1),
             arolla::TypedRef::FromValue(std::string("value"))},
            {"first", "second"});
  EXPECT_THAT(vars.kwnames(), ElementsAre("first", "second"));
  EXPECT_THAT(vars.values(), ElementsAre(QValueWith<int>(1),
                                         QValueWith<std::string>("value")));
  EXPECT_EQ(vars.mutable_values().data(), vars.values().data());
}

TEST(VarsTest, UnnamedVariable) {
  Vars vars({arolla::TypedRef::FromValue(std::string("foo"))}, {});
  arolla::TypedRef initial_value_ref = vars.values()[0];
  EXPECT_THAT(vars.kwnames(), ElementsAre());
  EXPECT_THAT(vars.values(), ElementsAre(QValueWith<std::string>("foo")));
  arolla::TypedValue new_value = arolla::TypedValue::FromValue(1);
  vars.mutable_values()[0] = new_value.AsRef();
  EXPECT_THAT(vars.values(), ElementsAre(QValueWith<int>(1)));
  // The reference to the initial value is guaranteed to remain valid throughout
  // the lifetime of `vars`.
  EXPECT_THAT(initial_value_ref, QValueWith<std::string>("foo"));
}

TEST(VarsTest, Update) {
  Vars vars(
      {
          arolla::TypedRef::FromValue(1.5),                   // <unnamed>
          arolla::TypedRef::FromValue(1),                     // x
          arolla::TypedRef::FromValue(std::string("value")),  // y
      },
      {
          "x",
          "y",
      });
  {
    ASSERT_OK(vars.Update(
        *arolla::MakeNamedTuple({"x"}, {arolla::TypedRef::FromValue(2)})));
    EXPECT_THAT(vars.values(),
                ElementsAre(QValueWith<double>(1.5), QValueWith<int>(2),
                            QValueWith<std::string>("value")));
  }
  {
    ASSERT_OK(vars.Update(*arolla::MakeNamedTuple(
        {"y"}, {arolla::TypedRef::FromValue(std::string("new_value"))})));
    EXPECT_THAT(vars.values(),
                ElementsAre(QValueWith<double>(1.5), QValueWith<int>(2),
                            QValueWith<std::string>("new_value")));
  }
}

TEST(VarsTest, UpdateFails) {
  Vars vars(
      {
          arolla::TypedRef::FromValue(1.5),                   // <unnamed>
          arolla::TypedRef::FromValue(1),                     // x
          arolla::TypedRef::FromValue(std::string("value")),  // y
      },
      {
          "x",
          "y",
      });
  EXPECT_THAT(vars.Update(arolla::TypedValue::FromValue(0)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected a namedtupe with a subset of initial "
                       "variables, got type INT32"));
  EXPECT_THAT(
      vars.Update(
          *arolla::MakeNamedTuple({"z"}, {arolla::TypedRef::FromValue(2)})),
      StatusIs(absl::StatusCode::kInvalidArgument, "unexpected variable 'z'"));
  EXPECT_THAT(vars.Update(*arolla::MakeNamedTuple(
                  {"y"}, {arolla::TypedRef::FromValue(5)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "variable 'y' has type BYTES, but the provided value "
                       "has type INT32"));
}

}  // namespace
}  // namespace koladata::functor::parallel::stream_loop_internal
