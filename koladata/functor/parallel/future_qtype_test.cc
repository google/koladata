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
#include "koladata/functor/parallel/future_qtype.h"

#include <cstdint>
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "koladata/functor/parallel/future.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::ReprTokenEq;
using ::arolla::testing::TypedValueWith;

TEST(FutureQTypeTest, Basics) {
  auto qtype = GetFutureQType<arolla::QTypePtr>();
  EXPECT_EQ(qtype, GetFutureQType<arolla::QTypePtr>());
  EXPECT_EQ(qtype->name(), "FUTURE[QTYPE]");
  EXPECT_EQ(qtype->type_info(), typeid(FuturePtr));
  EXPECT_EQ(qtype->type_layout().AllocSize(), sizeof(FuturePtr));
  EXPECT_EQ(qtype->type_layout().AllocAlignment().value, alignof(FuturePtr));
  EXPECT_TRUE(qtype->type_fields().empty());
  EXPECT_EQ(qtype->value_qtype(), arolla::GetQTypeQType());
  EXPECT_EQ(qtype->qtype_specialization_key(), "");
}

TEST(FutureQTypeTest, IsFutureQType) {
  EXPECT_TRUE(IsFutureQType(GetFutureQType<arolla::QTypePtr>()));
  EXPECT_TRUE(IsFutureQType(GetFutureQType<int32_t>()));
  EXPECT_TRUE(IsFutureQType(GetFutureQType<float>()));
  EXPECT_FALSE(IsFutureQType(arolla::GetQTypeQType()));
  EXPECT_FALSE(IsFutureQType(arolla::GetQType<int32_t>()));
  EXPECT_FALSE(IsFutureQType(arolla::GetQType<float>()));
}

TEST(FutureQTypeTest, MakeFutureQValue) {
  FuturePtr future = std::make_shared<Future>(arolla::GetQType<int>());
  EXPECT_OK(future->SetValue(arolla::TypedValue::FromValue(1)));
  auto qvalue = MakeFutureQValue(future);
  EXPECT_THAT(qvalue.GenReprToken(), ReprTokenEq("future[INT32]"));
  ASSERT_EQ(qvalue.GetType(), GetFutureQType<int>());
  EXPECT_THAT(qvalue.UnsafeAs<FuturePtr>()->GetValueForTesting(),
              IsOkAndHolds(TypedValueWith<int>(1)));
  EXPECT_EQ(qvalue.GetFingerprint(), MakeFutureQValue(future).GetFingerprint());
  FuturePtr future2 = std::make_shared<Future>(arolla::GetQType<int>());
  EXPECT_NE(qvalue.GetFingerprint(),
            MakeFutureQValue(future2).GetFingerprint());
}

}  // namespace
}  // namespace koladata::functor::parallel
