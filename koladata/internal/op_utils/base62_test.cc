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
#include "koladata/internal/op_utils/base62.h"

#include "gtest/gtest.h"
#include "absl/numeric/int128.h"
#include "arolla/util/text.h"

namespace koladata::internal {
namespace {

TEST(EncodeBase62, Basic) {
  EXPECT_EQ(EncodeBase62(0), arolla::Text("0000000000000000000000"));
  EXPECT_EQ(EncodeBase62(1), arolla::Text("0000000000000000000001"));
  EXPECT_EQ(EncodeBase62(10), arolla::Text("000000000000000000000A"));
  EXPECT_EQ(EncodeBase62(36), arolla::Text("000000000000000000000a"));
  EXPECT_EQ(EncodeBase62(61), arolla::Text("000000000000000000000z"));
  EXPECT_EQ(EncodeBase62(62), arolla::Text("0000000000000000000010"));
  EXPECT_EQ(EncodeBase62(123), arolla::Text("000000000000000000001z"));
  EXPECT_EQ(EncodeBase62(absl::Uint128Max() / 3),
            arolla::Text("2b1LP5S1pDvFNviGIN5EXN"));
  EXPECT_EQ(EncodeBase62(absl::Uint128Max()),
            arolla::Text("7n42DGM5Tflk9n8mt7Fhc7"));
}

TEST(DecodeBase62, Basic) {
  EXPECT_EQ(DecodeBase62("0000000000000000000000"), 0);
  EXPECT_EQ(DecodeBase62("0000000000000000000001"), 1);
  EXPECT_EQ(DecodeBase62("000000000000000000000A"), 10);
  EXPECT_EQ(DecodeBase62("000000000000000000000a"), 36);
  EXPECT_EQ(DecodeBase62("000000000000000000000z"), 61);
  EXPECT_EQ(DecodeBase62("0000000000000000000010"), 62);
  EXPECT_EQ(DecodeBase62("2b1LP5S1pDvFNviGIN5EXN"), absl::Uint128Max() / 3);
  EXPECT_EQ(DecodeBase62("7n42DGM5Tflk9n8mt7Fhc7"), absl::Uint128Max());
}

}  // namespace
}  // namespace koladata::internal
