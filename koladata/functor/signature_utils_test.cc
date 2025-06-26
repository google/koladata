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
#include "koladata/functor/signature_utils.h"

#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "koladata/data_slice.h"
#include "koladata/functor/signature.h"
#include "koladata/internal/dtype.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"

namespace koladata::functor {

namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::koladata::testing::IsEquivalentTo;
using ::testing::HasSubstr;

TEST(BindArgumentsTest, Basic) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
  };
  Signature::Parameter p2 = {
      .name = "b",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
      .default_value = test::DataItem(1),
  };
  Signature::Parameter p3 = {
      .name = "c",
      .kind = Signature::Parameter::Kind::kVarPositional,
  };
  Signature::Parameter p4 = {
      .name = "d",
      .kind = Signature::Parameter::Kind::kKeywordOnly,
      .default_value = test::DataItem(std::nullopt, schema::kInt32),
  };
  Signature::Parameter p5 = {
      .name = "e",
      .kind = Signature::Parameter::Kind::kVarKeyword,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1, p2, p3, p4, p5}));
  std::vector<DataSlice> slices = {test::DataItem(0), test::DataItem(1),
                                   test::DataItem(2), test::DataItem(3),
                                   test::DataItem(4), test::DataItem(5)};
  ASSERT_OK_AND_ASSIGN(auto bound_arguments,
                       BindArguments(signature,
                                     {arolla::TypedRef::FromValue(slices[0]),
                                      arolla::TypedRef::FromValue(slices[1]),
                                      arolla::TypedRef::FromValue(slices[2]),
                                      arolla::TypedRef::FromValue(slices[3]),
                                      arolla::TypedRef::FromValue(slices[4]),
                                      arolla::TypedRef::FromValue(slices[5])},
                                     {"f", "g"}));
  auto expected_var_args =
      arolla::MakeTuple({arolla::TypedRef::FromValue(slices[2]),
                         arolla::TypedRef::FromValue(slices[3])});
  ASSERT_OK_AND_ASSIGN(
      auto expected_var_kwargs,
      arolla::MakeNamedTuple({"f", "g"},
                             {arolla::TypedRef::FromValue(slices[4]),
                              arolla::TypedRef::FromValue(slices[5])}));
  ASSERT_EQ(bound_arguments.size(), 5);
  EXPECT_THAT(bound_arguments[0].As<DataSlice>(),
              IsOkAndHolds(IsEquivalentTo(slices[0])));
  EXPECT_THAT(bound_arguments[1].As<DataSlice>(),
              IsOkAndHolds(IsEquivalentTo(slices[1])));
  EXPECT_THAT(bound_arguments[2].GetFingerprint(),
              expected_var_args.GetFingerprint());
  EXPECT_THAT(bound_arguments[3].As<DataSlice>(),
              IsOkAndHolds(IsEquivalentTo(p4.default_value.value())));
  EXPECT_THAT(bound_arguments[4].GetFingerprint(),
              expected_var_kwargs.GetFingerprint());
}

TEST(BindArgumentsTest, NotValidIdentifiers) {
  Signature::Parameter p1 = {
      .name = " ",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
  };
  Signature::Parameter p2 = {
      .name = "?",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
  };
  Signature::Parameter p3 = {
      .name = "-",
      .kind = Signature::Parameter::Kind::kVarPositional,
  };
  Signature::Parameter p4 = {
      .name = "!",
      .kind = Signature::Parameter::Kind::kKeywordOnly,
  };
  Signature::Parameter p5 = {
      .name = "=",
      .kind = Signature::Parameter::Kind::kVarKeyword,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1, p2, p3, p4, p5}));
  std::vector<DataSlice> slices = {test::DataItem(0), test::DataItem(1),
                                   test::DataItem(2), test::DataItem(3),
                                   test::DataItem(4), test::DataItem(5)};
  ASSERT_OK_AND_ASSIGN(auto bound_arguments,
                       BindArguments(signature,
                                     {arolla::TypedRef::FromValue(slices[0]),
                                      arolla::TypedRef::FromValue(slices[1]),
                                      arolla::TypedRef::FromValue(slices[2]),
                                      arolla::TypedRef::FromValue(slices[3]),
                                      arolla::TypedRef::FromValue(slices[4]),
                                      arolla::TypedRef::FromValue(slices[5])},
                                     {"", "!"}));
  auto expected_var_args =
      arolla::MakeTuple({arolla::TypedRef::FromValue(slices[2]),
                         arolla::TypedRef::FromValue(slices[3])});
  ASSERT_OK_AND_ASSIGN(
      auto expected_var_kwargs,
      arolla::MakeNamedTuple({""}, {arolla::TypedRef::FromValue(slices[4])}));
  ASSERT_EQ(bound_arguments.size(), 5);
  EXPECT_THAT(bound_arguments[0].As<DataSlice>(),
              IsOkAndHolds(IsEquivalentTo(slices[0])));
  EXPECT_THAT(bound_arguments[1].As<DataSlice>(),
              IsOkAndHolds(IsEquivalentTo(slices[1])));
  EXPECT_EQ(bound_arguments[2].GetFingerprint(),
            expected_var_args.GetFingerprint());
  EXPECT_THAT(bound_arguments[3].As<DataSlice>(),
              IsOkAndHolds(IsEquivalentTo(slices[5])));
  EXPECT_EQ(bound_arguments[4].GetFingerprint(),
            expected_var_kwargs.GetFingerprint());
}

TEST(BindArgumentsTest, PositionalOnlyAndKeywordSameName) {
  Signature::Parameter p1 = {
      .name = "foo",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
      .default_value = test::DataItem(57),
  };
  Signature::Parameter p2 = {
      .name = "kwargs",
      .kind = Signature::Parameter::Kind::kVarKeyword,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1, p2}));
  auto input_slice = test::DataItem(43);
  ASSERT_OK_AND_ASSIGN(
      auto bound_arguments,
      BindArguments(signature, {arolla::TypedRef::FromValue(input_slice)},
                    {"foo"}));
  ASSERT_EQ(bound_arguments.size(), 2);
  EXPECT_THAT(bound_arguments[0].As<DataSlice>(),
              IsOkAndHolds(IsEquivalentTo(p1.default_value.value())));
  ASSERT_OK_AND_ASSIGN(
      auto expected_kwargs,
      arolla::MakeNamedTuple({"foo"},
                             {arolla::TypedRef::FromValue(input_slice)}));
  EXPECT_EQ(bound_arguments[1].GetFingerprint(),
            expected_kwargs.GetFingerprint());
}

TEST(BindArgumentsTest, PositionalOnlyAndKeywordSameNameErrorMessage) {
  Signature::Parameter p1 = {
      .name = "foo",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
  };
  Signature::Parameter p2 = {
      .name = "kwargs",
      .kind = Signature::Parameter::Kind::kVarKeyword,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1, p2}));
  auto input_slice = test::DataItem(43);
  EXPECT_THAT(
      BindArguments(signature, {arolla::TypedRef::FromValue(input_slice)},
                    {"foo"}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "no value provided for positional only parameter [foo]"));
}

TEST(BindArgumentsTest, SpecifyingSameBothAsPositionalAndKeyword) {
  Signature::Parameter p1 = {
      .name = "foo",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1}));
  auto input_slice = test::DataItem(43);
  EXPECT_THAT(BindArguments(signature,
                            {arolla::TypedRef::FromValue(input_slice),
                             arolla::TypedRef::FromValue(input_slice)},
                            {"foo"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "parameter [foo] specified twice"));
}

TEST(BindArgumentsTest, SpecifyingSameAsKeywordTwice) {
  Signature::Parameter p1 = {
      .name = "foo",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1}));
  auto input_slice = test::DataItem(43);
  EXPECT_THAT(BindArguments(signature,
                            {arolla::TypedRef::FromValue(input_slice),
                             arolla::TypedRef::FromValue(input_slice)},
                            {"foo", "foo"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "parameter [foo] specified twice"));
}

TEST(BindArgumentsTest, SpecifyingSameUnknownAsKeywordTwice) {
  Signature::Parameter p1 = {
      .name = "foo",
      .kind = Signature::Parameter::Kind::kVarKeyword,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1}));
  auto input_slice = test::DataItem(43);
  EXPECT_THAT(BindArguments(signature,
                            {arolla::TypedRef::FromValue(input_slice),
                             arolla::TypedRef::FromValue(input_slice)},
                            {"foo", "foo"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       // We can improve this error message if needed.
                       HasSubstr("field name foo is duplicated")));
}

TEST(BindArgumentsTest, TooFewPositional) {
  Signature::Parameter p1 = {
      .name = "foo",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1}));
  EXPECT_THAT(
      BindArguments(signature, {}, {}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "no value provided for positional only parameter [foo]"));
}

TEST(BindArgumentsTest, TooFewPositionalOkWithDefaultValue) {
  Signature::Parameter p1 = {
      .name = "foo",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
      .default_value = test::DataItem(57),
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1}));
  ASSERT_OK_AND_ASSIGN(auto bound_arguments, BindArguments(signature, {}, {}));
  EXPECT_THAT(bound_arguments[0].As<DataSlice>(),
              IsOkAndHolds(IsEquivalentTo(p1.default_value.value())));
}

TEST(BindArgumentsTest, TooManyPositional) {
  Signature::Parameter p1 = {
      .name = "foo",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1}));
  auto input_slice = test::DataItem(43);
  EXPECT_THAT(BindArguments(signature,
                            {arolla::TypedRef::FromValue(input_slice),
                             arolla::TypedRef::FromValue(input_slice),
                             arolla::TypedRef::FromValue(input_slice)},
                            {}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "too many positional arguments passed (2 extra)"));
}

TEST(BindArgumentsTest, UnknownKeyword) {
  Signature::Parameter p1 = {
      .name = "foo",
      .kind = Signature::Parameter::Kind::kVarPositional,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1}));
  auto input_slice = test::DataItem(43);
  EXPECT_THAT(BindArguments(signature,
                            {arolla::TypedRef::FromValue(input_slice),
                             arolla::TypedRef::FromValue(input_slice)},
                            {"b", "a"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "unknown keyword arguments: [b, a]"));
}

TEST(BindArgumentsTest, InvalidArgsKwnamesSizes) {
  Signature::Parameter p1 = {
      .name = "foo",
      .kind = Signature::Parameter::Kind::kVarPositional,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1}));
  EXPECT_THAT(BindArguments(signature, {}, {"foo"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "args.size < kwnames.size()"));
}

}  // namespace

}  // namespace koladata::functor
