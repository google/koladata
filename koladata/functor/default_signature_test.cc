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
#include "koladata/functor/default_signature.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/expr/constants.h"
#include "koladata/signature.h"
#include "koladata/signature_storage.h"
#include "koladata/testing/matchers.h"

namespace koladata::functor {

namespace {

using ::koladata::testing::IsEquivalentTo;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::FieldsAre;
using ::testing::Optional;

TEST(DefaultSignatureTest, Works) {
  using enum Signature::Parameter::Kind;
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       DefaultKodaSignatureFromInputs({"a", "b"}));
  ASSERT_OK_AND_ASSIGN(auto signature,
                       KodaSignatureToCppSignature(koda_signature));
  EXPECT_THAT(
      signature.parameters(),
      ElementsAre(
          FieldsAre(
              "self", kPositionalOnly,
              Optional(IsEquivalentTo(expr::UnspecifiedSelfInput().WithBag(
                  koda_signature.GetBag())))),
          FieldsAre("a", kKeywordOnly, Eq(std::nullopt)),
          FieldsAre("b", kKeywordOnly, Eq(std::nullopt)),
          FieldsAre("__extra_inputs__", kVarKeyword, Eq(std::nullopt))));
}

TEST(DefaultSignatureTest, SelfPassed) {
  using enum Signature::Parameter::Kind;
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       DefaultKodaSignatureFromInputs({"a", "b", "self"}));
  ASSERT_OK_AND_ASSIGN(auto signature,
                       KodaSignatureToCppSignature(koda_signature));
  EXPECT_THAT(
      signature.parameters(),
      ElementsAre(
          FieldsAre(
              "self", kPositionalOnly,
              Optional(IsEquivalentTo(expr::UnspecifiedSelfInput().WithBag(
                  koda_signature.GetBag())))),
          FieldsAre("a", kKeywordOnly, Eq(std::nullopt)),
          FieldsAre("b", kKeywordOnly, Eq(std::nullopt)),
          FieldsAre("__extra_inputs__", kVarKeyword, Eq(std::nullopt))));
}

}  // namespace

}  // namespace koladata::functor
