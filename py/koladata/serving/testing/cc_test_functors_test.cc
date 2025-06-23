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
#include "py/koladata/serving/testing/cc_test_functors.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/qtype/testing/matchers.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/functor/call.h"
#include "koladata/functor/cpp_function_bridge.h"
#include "koladata/serving/slice_registry.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"

namespace koladata_serving_test {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::QValueWith;
using ::koladata::test::DataSlice;
using ::koladata::testing::IsEquivalentTo;
using ::testing::_;
using ::testing::Pair;
using ::testing::Pointee;
using ::testing::UnorderedElementsAre;

TEST(TestFunctorsTest, PlusOne) {
  ASSERT_OK_AND_ASSIGN(auto plus_one, TestFunctors_plus_one());
  EXPECT_THAT(TestFunctors("plus_one"), IsOkAndHolds(IsEquivalentTo(plus_one)));
  EXPECT_THAT(TestFunctors(),
              IsOkAndHolds(UnorderedElementsAre(
                  Pair("plus_one", IsEquivalentTo(plus_one)),
                  Pair("ask_about_serving", _), Pair("TEST_DS", _))));
  auto input = DataSlice<int64_t>({1, 2, 3});
  EXPECT_THAT(koladata::functor::CallFunctorWithCompilationCache(
                  plus_one, {arolla::TypedRef::FromValue(input)}, {}),
              IsOkAndHolds(QValueWith<koladata::DataSlice>(
                  IsEquivalentTo(DataSlice<int64_t>({2, 3, 4})))));
}

TEST(TestFunctorsTest, AskAboutServing) {
  ASSERT_OK_AND_ASSIGN(auto ask_about_serving,
                       TestFunctors_ask_about_serving());
  ASSERT_OK_AND_ASSIGN(
      koladata::DataSlice external_fn,
      koladata::functor::CreateFunctorFromFunction(
          [](const koladata::DataSlice& input) { return input; },
          "call_external_fn", "input"));

  EXPECT_THAT(
      koladata::functor::CallFunctorWithCompilationCache(
          ask_about_serving, {arolla::TypedRef::FromValue(external_fn)}, {}),
      IsOkAndHolds(QValueWith<koladata::DataSlice>(
          IsEquivalentTo(koladata::test::DataItem(
              arolla::Text("How to serve Koda functors?"))))));
}

TEST(TestFunctorsTest, GlobalRegistry) {
  ASSERT_OK_AND_ASSIGN(auto plus_one, TestFunctors_plus_one());
  EXPECT_THAT(koladata::serving::GetRegisteredSlice(
                  "//py/koladata/serving/"
                  "testing:cc_test_functors@plus_one"),
              Pointee(IsEquivalentTo(plus_one)));
}

TEST(TestFunctorsTest, TestDS) {
  ASSERT_OK_AND_ASSIGN(auto test_ds, TestFunctors_TEST_DS());
  EXPECT_THAT(test_ds, IsEquivalentTo(DataSlice<int>({1, 2, 3})));
}

}  // namespace
}  // namespace koladata_serving_test
