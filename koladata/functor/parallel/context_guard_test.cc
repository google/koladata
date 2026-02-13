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
#include "koladata/functor/parallel/context_guard.h"

#include <functional>
#include <utility>

#include "gtest/gtest.h"

namespace koladata::functor::parallel {
namespace {

class TestScopeGuard {
 public:
  TestScopeGuard(std::function<void()> on_construct_fn,
                 std::function<void()> on_destruct_fn)
      : on_construct_fn_(std::move(on_construct_fn)),
        on_destruct_fn_(std::move(on_destruct_fn)) {
    on_construct_fn_();
  }

  ~TestScopeGuard() noexcept { on_destruct_fn_(); }

 private:
  std::function<void()> on_construct_fn_;
  std::function<void()> on_destruct_fn_;
};

TEST(ContextGuardTest, TestScopeGuardInplaceCtor) {
  bool constructed = false;
  bool destructed = false;
  std::function<void()> on_construct = [&] { constructed = true; };
  std::function<void()> on_destruct = [&] { destructed = true; };
  {
    ContextGuard context_guard(std::in_place_type_t<TestScopeGuard>(),
                               on_construct, on_destruct);
    EXPECT_TRUE(constructed);
    EXPECT_FALSE(destructed);
  }
  EXPECT_TRUE(destructed);
}

TEST(ContextGuardTest, TestScopeGuardInit) {
  bool constructed = false;
  bool destructed = false;
  std::function<void()> on_construct = [&] { constructed = true; };
  std::function<void()> on_destruct = [&] { destructed = true; };
  {
    ContextGuard context_guard;
    context_guard.init<TestScopeGuard>(on_construct, on_destruct);
    EXPECT_TRUE(constructed);
    EXPECT_FALSE(destructed);
  }
  EXPECT_TRUE(destructed);
}

TEST(ContextGuardTest, Uninitialized) {
  // Test that uninitialized ContextGuards don't cause any problems.
  ContextGuard context_guard;
}

TEST(ContextGuardTest, SmallScopeGuard) {
  static int counter = 0;
  struct ScopeGuard {
    ScopeGuard() { ++counter; }
    ~ScopeGuard() { --counter; }
  };
  {
    ContextGuard context_guard;
    context_guard.init<ScopeGuard>();
    EXPECT_EQ(counter, 1);
  }
  EXPECT_EQ(counter, 0);
}

TEST(ContextGuardTest, LargeScopeGuard) {
  static int counter = 0;
  struct ScopeGuard {
    ScopeGuard() { ++counter; }
    ~ScopeGuard() { --counter; }
    char buffer[1024] = {'h', 'e', 'l', 'l', 'o'};
  };
  {
    ContextGuard context_guard;
    context_guard.init<ScopeGuard>();
    EXPECT_EQ(counter, 1);
  }
  EXPECT_EQ(counter, 0);
}

}  // namespace
}  // namespace koladata::functor::parallel
