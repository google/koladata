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
#include "py/koladata/fstring/fstring_processor.h"

#include <cstdint>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/testing/matchers.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::koladata::internal::DataItem;
using ::koladata::schema::kFloat64;
using ::koladata::schema::kInt64;
using ::koladata::schema::kText;
using ::koladata::testing::IsEquivalentTo;

auto TextToDs(absl::string_view text) {
  return DataSlice::Create(DataItem(arolla::Text(text)), DataItem(kText))
      .value();
}

auto Int64ToDs(int64_t x) {
  return DataSlice::Create(DataItem(x), DataItem(kInt64)).value();
}

auto Float64ToDs(double x) {
  return DataSlice::Create(DataItem(x), DataItem(kFloat64)).value();
}

absl::StatusOr<DataSlice> EvalFString(absl::string_view fstring) {
  ASSIGN_OR_RETURN(auto expr, fstring::CreateFStringExpr(fstring));
  ASSIGN_OR_RETURN(auto tv, arolla::expr::Invoke(expr, {}));
  return tv.As<DataSlice>();
}

TEST(FStringProcessorTest, Single) {
  auto ds = TextToDs("abcx");
  ASSERT_OK_AND_ASSIGN(std::string fstring,
                       fstring::ToDataSlicePlaceholder(ds, "s"));
  EXPECT_THAT(EvalFString(fstring), IsOkAndHolds(IsEquivalentTo(ds)));
}

TEST(FStringProcessorTest, SingleWithSurrounding) {
  ASSERT_OK_AND_ASSIGN(std::string fstring,
                       fstring::ToDataSlicePlaceholder(TextToDs("World"), "s"));
  fstring = absl::StrCat("Hello, ", fstring, "!");
  EXPECT_THAT(EvalFString(fstring),
              IsOkAndHolds(IsEquivalentTo(TextToDs("Hello, World!"))));
}

TEST(FStringProcessorTest, SeveralWithSurrounding) {
  ASSERT_OK_AND_ASSIGN(std::string fstring1,
                       fstring::ToDataSlicePlaceholder(TextToDs("Mr."), "s"));
  ASSERT_OK_AND_ASSIGN(std::string fstring2,
                       fstring::ToDataSlicePlaceholder(TextToDs("X"), "s"));
  std::string fstring = absl::StrCat("Hello, ", fstring1, " ", fstring2, "!");
  EXPECT_THAT(EvalFString(fstring),
              IsOkAndHolds(IsEquivalentTo(TextToDs("Hello, Mr. X!"))));
}

TEST(FStringProcessorTest, SeveralWithSurroundingMultiType) {
  ASSERT_OK_AND_ASSIGN(std::string fstring1,
                       fstring::ToDataSlicePlaceholder((Int64ToDs(79)), "s"));
  ASSERT_OK_AND_ASSIGN(std::string fstring2, fstring::ToDataSlicePlaceholder(
                                                 (Float64ToDs(177.5)), "s"));
  std::string fstring =
      absl::StrCat("weight: ", fstring1, ", height: ", fstring2, ".");
  EXPECT_THAT(
      EvalFString(fstring),
      IsOkAndHolds(IsEquivalentTo(TextToDs("weight: 79, height: 177.5."))));
}

TEST(FStringProcessorTest, SingleErrorAtToDataSlicePlaceholder) {
  ASSERT_OK_AND_ASSIGN(
      auto fstring,
      fstring::ToDataSlicePlaceholder(
          DataSlice::Create(DataItem(internal::AllocateSingleObject()),
                            DataItem(schema::kAny))
              .value(),
          "s"));
  EXPECT_THAT(EvalFString(fstring).status(),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(FStringProcessorTest, NothingToFormatError) {
  EXPECT_THAT(EvalFString("Hello!"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace koladata::python
