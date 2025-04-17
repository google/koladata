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
#include "koladata/functor/parallel/future.h"

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::TypedValueWith;

TEST(FutureTest, Basic) {
  FuturePtr x = std::make_shared<Future>(arolla::GetQType<int>());
  EXPECT_EQ(x->value_qtype(), arolla::GetQType<int>());
  EXPECT_EQ(arolla::Repr(x), "future[INT32]");
  EXPECT_THAT(
      x->GetValueForTesting(),
      StatusIs(absl::StatusCode::kInvalidArgument, "future has no value"));
  EXPECT_OK(x->SetValue(arolla::TypedValue::FromValue(1)));
  EXPECT_THAT(x->GetValueForTesting(), IsOkAndHolds(TypedValueWith<int>(1)));
}

TEST(FutureTest, FutureValueFingerprint) {
  FuturePtr x = std::make_shared<Future>(arolla::GetQType<int>());
  FuturePtr y = std::make_shared<Future>(arolla::GetQType<int>());
  auto x_fingerprint = arolla::FingerprintHasher("salt").Combine(x).Finish();
  EXPECT_EQ(x_fingerprint,
            arolla::FingerprintHasher("salt").Combine(x).Finish());
  EXPECT_NE(x_fingerprint,
            arolla::FingerprintHasher("salt").Combine(y).Finish());
  EXPECT_OK(x->SetValue(arolla::TypedValue::FromValue(1)));
  EXPECT_EQ(x_fingerprint,
            arolla::FingerprintHasher("salt").Combine(x).Finish());
}

TEST(FutureTest, Nullptr) {
  FuturePtr x;
  EXPECT_EQ(arolla::Repr(x), "future{nullptr}");
  EXPECT_EQ(arolla::FingerprintHasher("salt").Combine(x).Finish(),
            arolla::FingerprintHasher("salt").Combine(x).Finish());
}

TEST(FutureTest, ConsumersWithValue) {
  FuturePtr x = std::make_shared<Future>(arolla::GetQType<int>());
  int calls = 0;
  auto consumer = [&calls](absl::StatusOr<arolla::TypedValue> value) {
    ++calls;
    ASSERT_THAT(value, IsOkAndHolds(TypedValueWith<int>(1)));
  };
  x->AddConsumer(consumer);
  EXPECT_EQ(calls, 0);
  EXPECT_OK(x->SetValue(arolla::TypedValue::FromValue(1)));
  EXPECT_EQ(calls, 1);
  x->AddConsumer(consumer);
  EXPECT_EQ(calls, 2);
}

TEST(FutureTest, ConsumersWithErrorStatus) {
  FuturePtr x = std::make_shared<Future>(arolla::GetQType<int>());
  int calls = 0;
  auto consumer = [&calls](absl::StatusOr<arolla::TypedValue> value) {
    ++calls;
    ASSERT_THAT(value, StatusIs(absl::StatusCode::kOutOfRange, "test error"));
  };
  x->AddConsumer(consumer);
  EXPECT_EQ(calls, 0);
  EXPECT_OK(x->SetValue(absl::OutOfRangeError("test error")));
  EXPECT_EQ(calls, 1);
  x->AddConsumer(consumer);
  EXPECT_EQ(calls, 2);
}

TEST(FutureTest, SetValueTwice) {
  FuturePtr x = std::make_shared<Future>(arolla::GetQType<int>());
  EXPECT_OK(x->SetValue(arolla::TypedValue::FromValue(1)));
  EXPECT_THAT(x->SetValue(arolla::TypedValue::FromValue(2)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "future already has a value"));
}

TEST(FutureTest, SetValueThenError) {
  FuturePtr x = std::make_shared<Future>(arolla::GetQType<int>());
  EXPECT_OK(x->SetValue(arolla::TypedValue::FromValue(1)));
  EXPECT_THAT(x->SetValue(absl::OutOfRangeError("test error")),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "future already has a value"));
}

TEST(FutureTest, SetErrorThenValue) {
  FuturePtr x = std::make_shared<Future>(arolla::GetQType<int>());
  EXPECT_OK(x->SetValue(absl::OutOfRangeError("test error")));
  EXPECT_THAT(x->SetValue(arolla::TypedValue::FromValue(1)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "future already has a value"));
}

TEST(FutureTest, SetValueWrongType) {
  FuturePtr x = std::make_shared<Future>(arolla::GetQType<int>());
  EXPECT_THAT(x->SetValue(arolla::TypedValue::FromValue(1.0)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "value type FLOAT64 does not match future type INT32"));
}

TEST(FutureTest, GetValueForTestingOnError) {
  FuturePtr x = std::make_shared<Future>(arolla::GetQType<int>());
  EXPECT_OK(x->SetValue(absl::OutOfRangeError("test error")));
  EXPECT_THAT(x->GetValueForTesting(),
              StatusIs(absl::StatusCode::kOutOfRange, "test error"));
}

// TODO: Add multithreaded integration tests once we have thread
// pool executors implemented.

}  // namespace
}  // namespace koladata::functor::parallel
