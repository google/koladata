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
#include "koladata/functor/parallel/default_asio_executor.h"

#include <memory>

#include "gtest/gtest.h"
#include "absl/synchronization/barrier.h"
#include "koladata/functor/parallel/executor.h"

namespace koladata::functor::parallel {
namespace {

TEST(DefaultAsioExecutorTest, Basic) {
  const ExecutorPtr& executor = GetDefaultAsioExecutor();
  ASSERT_NE(executor, nullptr);

  auto barrier = std::make_shared<absl::Barrier>(2);
  executor->Schedule([barrier] { barrier->Block(); });
  barrier->Block();
}

}  // namespace
}  // namespace koladata::functor::parallel
