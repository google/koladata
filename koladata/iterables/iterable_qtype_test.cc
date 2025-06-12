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
#include "koladata/iterables/iterable_qtype.h"

#include <cstdint>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/mutable_sequence.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace koladata::iterables {
namespace {

using ::arolla::testing::ReprTokenEq;

TEST(IterableQTypeTest, Basics) {
  const auto* qtype = GetIterableQType<arolla::QTypePtr>();
  EXPECT_EQ(qtype->name(), "ITERABLE[QTYPE]");
  EXPECT_TRUE(qtype->type_fields().empty());
  EXPECT_EQ(qtype->value_qtype(), arolla::GetQTypeQType());
  EXPECT_EQ(qtype->qtype_specialization_key(),
            "::koladata::iterables::IterableQType");
  auto derived_qtype_interface =
      dynamic_cast<const arolla::DerivedQTypeInterface*>(qtype);
  ASSERT_NE(derived_qtype_interface, nullptr);
  EXPECT_EQ(derived_qtype_interface->GetBaseQType(),
            arolla::GetSequenceQType<arolla::QTypePtr>());
}

TEST(IterableQTypeTest, Registry) {
  EXPECT_EQ(GetIterableQType<int32_t>(),
            GetIterableQType(arolla::GetQType<int32_t>()));
  EXPECT_NE(GetIterableQType<int32_t>(), GetIterableQType<int64_t>());
}

TEST(IterableQTypeTest, IsIterableQType) {
  EXPECT_TRUE(IsIterableQType(GetIterableQType<arolla::QTypePtr>()));
  EXPECT_TRUE(IsIterableQType(GetIterableQType<int32_t>()));
  EXPECT_TRUE(IsIterableQType(GetIterableQType<float>()));
  EXPECT_FALSE(IsIterableQType(arolla::GetQTypeQType()));
  EXPECT_FALSE(IsIterableQType(arolla::GetQType<int32_t>()));
  EXPECT_FALSE(IsIterableQType(arolla::GetQType<float>()));
  EXPECT_FALSE(IsIterableQType(arolla::GetSequenceQType<float>()));
}

TEST(IterableQTypeTest, TypedValue) {
  ASSERT_OK_AND_ASSIGN(auto mutable_seq, arolla::MutableSequence::Make(
                                             arolla::GetQType<int32_t>(), 3));
  auto mutable_span = mutable_seq.UnsafeSpan<int32_t>();
  mutable_span[0] = 1;
  mutable_span[1] = 2;
  mutable_span[2] = 3;
  ASSERT_OK_AND_ASSIGN(auto seq_typed_value,
                       arolla::TypedValue::FromValueWithQType(
                           std::move(mutable_seq).Finish(),
                           arolla::GetSequenceQType<int32_t>()));
  auto typed_value = arolla::UnsafeDowncastDerivedQValue(
      GetIterableQType<int32_t>(), seq_typed_value);
  EXPECT_EQ(typed_value.GetType()->name(), "ITERABLE[INT32]");
  // Maybe improve this later?
  EXPECT_THAT(
      typed_value.GenReprToken(),
      ReprTokenEq("ITERABLE[INT32]{sequence(1, 2, 3, value_qtype=INT32)}"));
}

}  // namespace
}  // namespace koladata::iterables
