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
#include "koladata/functor/parallel/eager_executor.h"

#include "gtest/gtest.h"
#include "koladata/functor/parallel/executor.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/repr.h"

namespace koladata::functor::parallel {
namespace {

TEST(EagerExecutorTest, Execution) {
  ExecutorPtr executor = GetEagerExecutor();
  int execute_count = 0;
  auto callback = [&]() { ++execute_count; };
  executor->Schedule(callback);
  EXPECT_EQ(execute_count, 1);
}

TEST(EagerExecutorTest, QValue) {
  ExecutorPtr executor = GetEagerExecutor();
  ExecutorPtr executor2 = GetEagerExecutor();
  ExecutorPtr null_executor = nullptr;
  EXPECT_EQ(arolla::Repr(executor), "eager_executor");
  EXPECT_EQ(arolla::TypedValue::FromValue(executor).GetFingerprint(),
            arolla::TypedValue::FromValue(executor2).GetFingerprint());
  EXPECT_NE(arolla::TypedValue::FromValue(executor).GetFingerprint(),
            arolla::TypedValue::FromValue(null_executor).GetFingerprint());
}

}  // namespace
}  // namespace koladata::functor::parallel
