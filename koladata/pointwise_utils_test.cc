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
#include "koladata/pointwise_utils.h"

#include <cstdint>
#include <optional>
#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/view_types.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/test_utils.h"

namespace koladata {
namespace {

using ::testing::ElementsAre;

TEST(ApplyUnaryPointwiseFnTest, MissingItem) {
  auto missing_item = test::DataItem(internal::DataItem());
  ASSERT_OK_AND_ASSIGN(
      auto result_item,
      ApplyUnaryPointwiseFn(
          missing_item,
          [&]<typename T>(arolla::meta::type<T>, arolla::view_type_t<T> value)
              -> absl::StatusOr<internal::DataItem> {
            return internal::DataItem(T(value));
          },
          internal::DataItem(schema::kInt32)));
  EXPECT_TRUE(result_item.is_item());
  EXPECT_EQ(result_item.present_count(), 0);
}

TEST(ApplyUnaryPointwiseFnTest, PresentItem) {
  auto item = test::DataItem<int32_t>(1);
  ASSERT_OK_AND_ASSIGN(
      auto result_item,
      ApplyUnaryPointwiseFn(
          item,
          [&]<typename T>(arolla::meta::type<T>, arolla::view_type_t<T> value)
              -> absl::StatusOr<internal::DataItem> {
            if constexpr (std::is_same_v<T, int32_t>) {
              return internal::DataItem(value + 1);
            } else {
              return absl::InvalidArgumentError("unexpected type");
            }
          },
          internal::DataItem(schema::kInt32)));
  EXPECT_TRUE(result_item.is_item());
  EXPECT_EQ(result_item.present_count(), 1);
  EXPECT_EQ(result_item.item().value<int32_t>(), 2);
}

TEST(ApplyUnaryPointwiseFnTest, Slice) {
  auto slice = test::DataSlice<int32_t>({1, std::nullopt, 2, 3});
  ASSERT_OK_AND_ASSIGN(
      auto result,
      ApplyUnaryPointwiseFn(
          slice,
          [&]<typename T>(arolla::meta::type<T>, arolla::view_type_t<T> value)
              -> absl::StatusOr<internal::DataItem> {
            if constexpr (std::is_same_v<T, int32_t>) {
              if (value > 2) {
                return internal::DataItem();
              }
              return internal::DataItem(value + 1);
            } else {
              return absl::InvalidArgumentError("unexpected type");
            }
          },
          internal::DataItem(schema::kInt32)));
  EXPECT_THAT(result.slice().values<int32_t>(),
              ElementsAre(2, std::nullopt, 3, std::nullopt));
}

TEST(ApplyUnaryPointwiseFnTest, MixedDtype) {
  auto slice = test::MixedDataSlice<int32_t, arolla::Bytes>(
      {1, std::nullopt, 2, 3, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, "x"});
  ASSERT_OK_AND_ASSIGN(
      auto result,
      ApplyUnaryPointwiseFn(
          slice,
          [&]<typename T>(arolla::meta::type<T>, arolla::view_type_t<T> value)
              -> absl::StatusOr<internal::DataItem> {
            if constexpr (std::is_same_v<T, int32_t>) {
              if (value > 2) {
                return internal::DataItem();
              }
              return internal::DataItem(value + 1);
            } else if constexpr (std::is_same_v<T, arolla::Bytes>) {
              return internal::DataItem(arolla::Text(value));
            } else {
              return absl::InvalidArgumentError("unexpected type");
            }
          },
          internal::DataItem(schema::kObject)));
  EXPECT_THAT(result.slice(),
              ElementsAre(internal::DataItem(2), internal::DataItem(),
                          internal::DataItem(3), internal::DataItem(),
                          internal::DataItem(arolla::Text("x"))));
}

}  // namespace
}  // namespace koladata
