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
#include "koladata/signature.h"

#include <cstdint>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/internal/dtype.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"

namespace koladata::functor {

namespace {

using ::absl_testing::StatusIs;
using ::koladata::testing::IsEquivalentTo;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::FieldsAre;
using ::testing::Optional;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

TEST(SignatureTest, Basic) {
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
  EXPECT_THAT(
      signature.parameters(),
      ElementsAre(FieldsAre(p1.name, p1.kind, Eq(std::nullopt)),
                  FieldsAre(p2.name, p2.kind,
                            Optional(IsEquivalentTo(p2.default_value.value()))),
                  FieldsAre(p3.name, p3.kind, Eq(std::nullopt)),
                  FieldsAre(p4.name, p4.kind,
                            Optional(IsEquivalentTo(p4.default_value.value()))),
                  FieldsAre(p5.name, p5.kind, Eq(std::nullopt))));
  EXPECT_THAT(signature.keyword_parameter_index(),
              UnorderedElementsAre(Pair("b", 1), Pair("d", 3)));
}

TEST(SignatureTest, DuplicateName) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
  };
  Signature::Parameter p2 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kVarKeyword,
  };
  EXPECT_THAT(Signature::Create({p1, p2}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "duplicate parameter name: [a]"));
}

TEST(SignatureTest, DefaultValueForVarPositional) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kVarPositional,
      .default_value = test::DataItem(1),
  };
  EXPECT_THAT(Signature::Create({p1}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "default value is not allowed for a variadic positional "
                       "parameter [a]"));
}

TEST(SignatureTest, DefaultValueForVarKeyword) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kVarKeyword,
      .default_value = test::DataItem(1),
  };
  EXPECT_THAT(Signature::Create({p1}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "default value is not allowed for a variadic keyword "
                       "parameter [a]"));
}

TEST(SignatureTest, SliceAsDefaultValue) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
      .default_value = test::DataSlice<int32_t>({1, 2, 3}),
  };
  EXPECT_THAT(Signature::Create({p1}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "default value for parameter [a] must be a data item, "
                       "but has rank 1"));
}

TEST(SignatureTest, WrongOrder) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
  };
  Signature::Parameter p2 = {
      .name = "b",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
  };
  EXPECT_THAT(Signature::Create({p1, p2}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "a positional only parameter [b] cannot follow a "
                       "positional or keyword parameter [a]"));
}

TEST(SignatureTest, TwoVarPositional) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kVarPositional,
  };
  Signature::Parameter p2 = {
      .name = "b",
      .kind = Signature::Parameter::Kind::kVarPositional,
  };
  EXPECT_THAT(Signature::Create({p1, p2}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "at most one variadic positional parameter is allowed"));
}

TEST(SignatureTest, TwoVarKeyword) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kVarKeyword,
  };
  Signature::Parameter p2 = {
      .name = "b",
      .kind = Signature::Parameter::Kind::kVarKeyword,
  };
  EXPECT_THAT(Signature::Create({p1, p2}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "at most one variadic keyword parameter is allowed"));
}

TEST(SignatureTest, DefaultsMustBeASuffix) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
      .default_value = test::DataItem(1),
  };
  Signature::Parameter p2 = {
      .name = "b",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
  };
  EXPECT_THAT(Signature::Create({p1, p2}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "a positional parameter [b] without a default value "
                       "cannot follow [a] with a default value"));
}

}  // namespace

}  // namespace koladata::functor
