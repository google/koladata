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
#include "koladata/internal/types_buffer.h"

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bits.h"
#include "koladata/internal/types.h"

namespace koladata::internal {
namespace {

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;

TEST(TypesBufferTest, Basic) {
  TypesBuffer b;
  b.types.push_back(ScalarTypeId<int>());
  b.types.push_back(ScalarTypeId<float>());
  b.id_to_typeidx = {1, TypesBuffer::kUnset, 0, TypesBuffer::kRemoved, 1};

  EXPECT_EQ(b.size(), 5);
  EXPECT_EQ(b.type_count(), 2);
  EXPECT_EQ(b.id_to_type(0), arolla::GetQType<float>());
  EXPECT_EQ(b.id_to_type(1), nullptr);
  EXPECT_EQ(b.id_to_type(2), arolla::GetQType<int>());
  EXPECT_EQ(b.id_to_type(3), nullptr);
  EXPECT_EQ(b.id_to_type(4), arolla::GetQType<float>());

  EXPECT_THAT(b.ToBitmap(0), ElementsAre(0b00100));
  EXPECT_THAT(b.ToBitmap(1), ElementsAre(0b10001));
  EXPECT_THAT(b.ToBitmap(TypesBuffer::kRemoved), ElementsAre(0b01000));
  EXPECT_THAT(b.ToInvertedBitmap(0), ElementsAre(0b11011));
  EXPECT_THAT(b.ToInvertedBitmap(1), ElementsAre(0b01110));
  EXPECT_THAT(b.ToInvertedBitmap(TypesBuffer::kRemoved), ElementsAre(0b10111));
  EXPECT_THAT(b.ToNotRemovedBitmap(), ElementsAre(0b10111));
  EXPECT_THAT(b.ToSetBitmap(), ElementsAre(0b11101));
  EXPECT_THAT(b.ToPresenceBitmap(), ElementsAre(0b10101));
}

TEST(TypesBufferTest, ToBitmap64Elements) {
  TypesBuffer b;
  b.types.push_back(ScalarTypeId<int>());
  b.id_to_typeidx.resize(64, 0);
  EXPECT_THAT(b.ToBitmap(0),
              ElementsAre(~arolla::bitmap::Word{0}, ~arolla::bitmap::Word{0}));
  EXPECT_THAT(b.ToInvertedBitmap(0), ElementsAre(0, 0));
}

TEST(TypesBufferTest, ToBitmap40Elements) {
  TypesBuffer b;
  b.types.push_back(ScalarTypeId<int>());
  b.types.push_back(ScalarTypeId<float>());
  b.id_to_typeidx.resize(40, 0);
  for (int i = 32; i < 40; ++i) b.id_to_typeidx[i] = 1;
  EXPECT_THAT(b.ToBitmap(0),
              ElementsAre(~arolla::bitmap::Word{0}, arolla::bitmap::Word{0}));
  EXPECT_THAT(b.ToBitmap(1),
              ElementsAre(arolla::bitmap::Word{0}, 0b11111111));
  EXPECT_THAT(b.ToInvertedBitmap(0),
              ElementsAre(arolla::bitmap::Word{0}, 0b11111111));
  EXPECT_THAT(b.ToInvertedBitmap(1),
              ElementsAre(~arolla::bitmap::Word{0}, arolla::bitmap::Word{0}));
}

TEST(TypesBufferTest, ToBitmapVarSize) {
  for (int size = 0; size < 100; ++size) {
    TypesBuffer b;
    b.types.push_back(ScalarTypeId<int>());
    b.types.push_back(ScalarTypeId<float>());
    b.id_to_typeidx.resize(size);
    for (int i = 0; i < size; ++i) {
      b.id_to_typeidx[i] = i % 2;
    }
    {
      std::vector<arolla::bitmap::Word> expected0(
          arolla::bitmap::BitmapSize(size));
      for (int i = 0; i < size; i += 2) {
        arolla::SetBit(expected0.data(), i);
      }
      EXPECT_THAT(b.ToBitmap(0), ElementsAreArray(expected0));
    }
    {
      std::vector<arolla::bitmap::Word> expected1(
          arolla::bitmap::BitmapSize(size));
      for (int i = 1; i < size; i += 2) {
        arolla::SetBit(expected1.data(), i);
      }
      EXPECT_THAT(b.ToBitmap(1), ElementsAreArray(expected1));
    }
  }
}

}  // namespace
}  // namespace koladata::internal
