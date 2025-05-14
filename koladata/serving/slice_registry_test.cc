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
#include "koladata/serving/slice_registry.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "arolla/qtype/base_types.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"

namespace koladata::serving {
namespace {

using ::absl_testing::StatusIs;
using ::koladata::testing::IsEquivalentTo;
using ::testing::IsNull;
using ::testing::NotNull;
using ::testing::StrEq;

TEST(SliceRegistryTest, RegisterNewSlice) {
  EXPECT_THAT(GetRegisteredSlice("SliceRegistryTest.RegisterNewSlice"),
              IsNull());
  ASSERT_OK(RegisterSlice("SliceRegistryTest.RegisterNewSlice",
                          test::DataSlice<int32_t>({1, 2, 3})));
  EXPECT_THAT(RegisterSlice("SliceRegistryTest.RegisterNewSlice",
                            test::DataSlice<int32_t>({4, 5, 6})),
              StatusIs(absl::StatusCode::kAlreadyExists,
                       StrEq("slice SliceRegistryTest.RegisterNewSlice is "
                             "already registered")));

  auto registered_slice =
      GetRegisteredSlice("SliceRegistryTest.RegisterNewSlice");
  ASSERT_THAT(registered_slice, NotNull());
  EXPECT_THAT(*registered_slice,
              IsEquivalentTo(test::DataSlice<int32_t>({1, 2, 3})));

  // Test that the slice pointer is not invalidated after registering more
  // slices (we rely on asan here).
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(
        RegisterSlice(absl::StrCat("SliceRegistryTest.RegisterNewSlice_", i),
                      test::DataSlice<int32_t>({i})));
  }
  EXPECT_THAT(*registered_slice,
              IsEquivalentTo(test::DataSlice<int32_t>({1, 2, 3})));
}

TEST(SliceRegistryTest, GetNonRegisteredSlice) {
  EXPECT_THAT(GetRegisteredSlice("SliceRegistryTest.GetNonRegisteredSlice"),
              IsNull());
}

}  // namespace
}  // namespace koladata::serving
