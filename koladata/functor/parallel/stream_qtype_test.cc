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
#include "koladata/functor/parallel/stream_qtype.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/testing/repr_token_eq.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {
namespace {

using ::arolla::testing::ReprTokenEq;

TEST(StreamQTypeTest, Basics) {
  auto qtype = GetStreamQType<arolla::QTypePtr>();
  EXPECT_EQ(qtype, GetStreamQType<arolla::QTypePtr>());
  EXPECT_EQ(qtype->name(), "STREAM[QTYPE]");
  EXPECT_EQ(qtype->type_info(), typeid(StreamPtr));
  EXPECT_EQ(qtype->type_layout().AllocSize(), sizeof(StreamPtr));
  EXPECT_EQ(qtype->type_layout().AllocAlignment().value, alignof(StreamPtr));
  EXPECT_TRUE(qtype->type_fields().empty());
  EXPECT_EQ(qtype->value_qtype(), arolla::GetQTypeQType());
  EXPECT_EQ(qtype->qtype_specialization_key(),
            "::koladata::functor::parallel::StreamQType");
}

TEST(StreamQTypeTest, IsStreamQType) {
  EXPECT_TRUE(IsStreamQType(GetStreamQType<arolla::QTypePtr>()));
  EXPECT_TRUE(IsStreamQType(GetStreamQType<int32_t>()));
  EXPECT_TRUE(IsStreamQType(GetStreamQType<float>()));
  EXPECT_FALSE(IsStreamQType(arolla::GetQTypeQType()));
  EXPECT_FALSE(IsStreamQType(arolla::GetQType<int32_t>()));
  EXPECT_FALSE(IsStreamQType(arolla::GetQType<float>()));
}

TEST(StreamQTypeTest, MakeStreamQValue) {
  auto [stream, weak_writer] = MakeStream(arolla::GetQType<int32_t>());
  auto qvalue = MakeStreamQValue(stream);
  EXPECT_THAT(qvalue.GenReprToken(), ReprTokenEq("stream[INT32]"));
  ASSERT_EQ(qvalue.GetType(), GetStreamQType<int32_t>());
  EXPECT_EQ(qvalue.GetFingerprint(), MakeStreamQValue(stream).GetFingerprint());
  auto [stream2, weak_writer2] = MakeStream(arolla::GetQType<int>());
  EXPECT_NE(qvalue.GetFingerprint(),
            MakeStreamQValue(stream2).GetFingerprint());
}

TEST(StreamQTypeTest, MakeStreamQValueRef) {
  auto [stream, weak_writer] = MakeStream(arolla::GetQType<int32_t>());
  auto qvalue = MakeStreamQValueRef(stream);
  EXPECT_THAT(qvalue.GenReprToken(), ReprTokenEq("stream[INT32]"));
  ASSERT_EQ(qvalue.GetType(), GetStreamQType<int32_t>());
  ASSERT_EQ(qvalue.GetRawPointer(), &stream);
}

}  // namespace
}  // namespace koladata::functor::parallel
