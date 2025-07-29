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
#include "koladata/functor/parallel/get_default_executor.h"

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/synchronization/barrier.h"
#include "koladata/functor/parallel/executor.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;

TEST(GetDefaultExecutorTest, GetDefaultExecutor) {
  const ExecutorPtr& executor = GetDefaultExecutor();
  ASSERT_NE(executor, nullptr);

  auto barrier = std::make_shared<absl::Barrier>(2);
  executor->Schedule([barrier] { barrier->Block(); });
  barrier->Block();
}

TEST(GetDefaultExecutorTest, ProvideDefaultCurrentExecutorScopeGuard) {
  EXPECT_THAT(CurrentExecutor(), StatusIs(absl::StatusCode::kInvalidArgument));
  {
    ProvideDefaultCurrentExecutorScopeGuard guard;
    EXPECT_THAT(CurrentExecutor(), IsOkAndHolds(GetDefaultExecutor()));
  }
  EXPECT_THAT(CurrentExecutor(), StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace koladata::functor::parallel
