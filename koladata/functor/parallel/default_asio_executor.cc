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
#include "koladata/functor/parallel/default_asio_executor.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "koladata/functor/parallel/executor.h"

#include "boost/asio/post.hpp"
#include "boost/asio/thread_pool.hpp"

namespace koladata::functor::parallel {
namespace {

class DefaultAsioExecutor final : public Executor {
 public:
  void Schedule(TaskFn task_fn) final {
    boost::asio::post(thread_pool_, std::move(task_fn));
  }

  std::string Repr() const final { return "default_asio_executor"; }

 private:
  boost::asio::thread_pool thread_pool_;
};

}  // namespace

const ExecutorPtr /*absl_nonnull*/& GetDefaultAsioExecutor() {
  static absl::NoDestructor<ExecutorPtr> executor(
      std::make_shared<DefaultAsioExecutor>());
  return *executor;
}

}  // namespace koladata::functor::parallel
