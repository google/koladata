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
#ifndef THIRD_PARTY_KOLA_DATA_FUNCTOR_PARALLEL_ASIO_EXECUTOR_H_
#define THIRD_PARTY_KOLA_DATA_FUNCTOR_PARALLEL_ASIO_EXECUTOR_H_

#include <cstddef>

#include "absl/base/nullability.h"
#include "koladata/functor/parallel/executor.h"

namespace koladata::functor::parallel {

// Creates an executor that uses boost::asio::thread_pool.
//
// If `num_threads` is 0, then the default number of threads is used. (See:
// https://live.boost.org/doc/libs/1_85_0/doc/html/boost_asio/reference/thread_pool/thread_pool.html)
ExecutorPtr /*absl_nonnull*/ MakeAsioExecutor(size_t num_threads = 0);

}  // namespace koladata::functor::parallel

#endif  // THIRD_PARTY_KOLA_DATA_FUNCTOR_PARALLEL_ASIO_EXECUTOR_H_
