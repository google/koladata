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
#include "koladata/internal/op_utils/base62.h"

#include "gtest/gtest.h"
#include "absl/numeric/int128.h"
#include "arolla/util/text.h"

namespace koladata::internal {
namespace {

TEST(EncodeBase62, Basic) {
  EXPECT_EQ(EncodeBase62(0), arolla::Text("0"));
  EXPECT_EQ(EncodeBase62(1), arolla::Text("1"));
  EXPECT_EQ(EncodeBase62(10), arolla::Text("A"));
  EXPECT_EQ(EncodeBase62(36), arolla::Text("a"));
  EXPECT_EQ(EncodeBase62(61), arolla::Text("z"));
  EXPECT_EQ(EncodeBase62(62), arolla::Text("10"));
  EXPECT_EQ(EncodeBase62(123), arolla::Text("1z"));
  EXPECT_EQ(EncodeBase62(absl::Uint128Max()),
            arolla::Text("7n42DGM5Tflk9n8mt7Fhc7"));
}

TEST(DecodeBase62, Basic) {
  EXPECT_EQ(DecodeBase62("0"), 0);
  EXPECT_EQ(DecodeBase62("1"), 1);
  EXPECT_EQ(DecodeBase62("A"), 10);
  EXPECT_EQ(DecodeBase62("a"), 36);
  EXPECT_EQ(DecodeBase62("z"), 61);
  EXPECT_EQ(DecodeBase62("10"), 62);
  EXPECT_EQ(DecodeBase62("7n42DGM5Tflk9n8mt7Fhc7"), absl::Uint128Max());
}

}  // namespace
}  // namespace koladata::internal
