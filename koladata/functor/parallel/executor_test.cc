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
#include "koladata/functor/parallel/executor.h"

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/repr.h"

namespace koladata::functor::parallel {
namespace {

class TestExecutor : public Executor {
 public:
  void Schedule(TaskFn task_fn) override {}

  std::string Repr() const override { return "test_executor"; }
};

TEST(ExecutorTest, Nullptr) {
  ExecutorPtr executor = nullptr;
  EXPECT_EQ(arolla::Repr(executor), "executor{nullptr}");
  EXPECT_EQ(arolla::TypedValue::FromValue(executor).GetFingerprint(),
            arolla::TypedValue::FromValue(executor).GetFingerprint());
}

TEST(ExecutorTest, Repr) {
  ExecutorPtr executor = std::make_shared<TestExecutor>();
  EXPECT_EQ(arolla::Repr(executor), "test_executor");
}

TEST(ExecutorTest, Fingerprint) {
  ExecutorPtr executor1 = std::make_shared<TestExecutor>();
  ExecutorPtr executor2 = std::make_shared<TestExecutor>();
  EXPECT_NE(arolla::TypedValue::FromValue(executor1).GetFingerprint(),
            arolla::TypedValue::FromValue(executor2).GetFingerprint());
}

}  // namespace
}  // namespace koladata::functor::parallel
