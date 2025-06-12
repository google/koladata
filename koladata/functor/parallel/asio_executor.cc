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
#include "koladata/functor/parallel/asio_executor.h"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/nullability.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/get_default_executor.h"

#include "boost/asio/post.hpp"
#include "boost/asio/thread_pool.hpp"

namespace koladata::functor::parallel {
namespace {

class AsioExecutor final : public Executor {
 public:
  explicit AsioExecutor(size_t num_threads)
      : thread_pool_(
            num_threads > 0
                ? std::make_unique<boost::asio::thread_pool>(num_threads)
                : std::make_unique<boost::asio::thread_pool>()) {}

  ~AsioExecutor() final {
    GetDefaultExecutor()->Schedule([thread_pool = std::move(thread_pool_)] {
      thread_pool->stop();
      thread_pool->join();
    });
  }

  void Schedule(TaskFn task_fn) final {
    boost::asio::post(*thread_pool_, std::move(task_fn));
  }

  std::string Repr() const final { return "asio_executor"; }

 private:
  // Note: Use unique_ptr to have additional flexibility within the destructor.
  std::unique_ptr<boost::asio::thread_pool> absl_nonnull thread_pool_;
};

}  // namespace

ExecutorPtr absl_nonnull MakeAsioExecutor(size_t num_threads) {
  return std::make_shared<AsioExecutor>(num_threads);
}

}  // namespace koladata::functor::parallel
