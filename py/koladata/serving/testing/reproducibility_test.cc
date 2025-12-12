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
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/testing/matchers.h"
#include "py/koladata/serving/testing/reproducible_item_a.h"
#include "py/koladata/serving/testing/reproducible_item_b.h"

namespace reproducibility_test {
namespace {

using ::koladata::testing::IsDeepEquivalentTo;
using ::koladata::testing::IsEquivalentTo;
using ::testing::Not;

// Note: Reproducibility for items A and B is tested using the corresponding
// golden tests:
//  * py/koladata/serving/testing:reproducible_item_a_cc_golden_test
//  * py/koladata/serving/testing:reproducible_item_b_cc_golden_test

TEST(ReproducibilityTest, ItemsAreEquivalentButDifferent) {
  ASSERT_OK_AND_ASSIGN(auto item_a, ReproducibleItemA("item"));
  ASSERT_OK_AND_ASSIGN(auto item_b, ReproducibleItemB("item"));
  EXPECT_THAT(item_a, IsDeepEquivalentTo(item_b));
  EXPECT_THAT(item_a, Not(IsEquivalentTo(item_b)));
}

}  // namespace
}  // namespace reproducibility_test
