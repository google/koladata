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
#ifndef THIRD_PARTY_KOLA_DATA_FUNCTOR_PARALLEL_DEFAULT_ASIO_EXECUTOR_H_
#define THIRD_PARTY_KOLA_DATA_FUNCTOR_PARALLEL_DEFAULT_ASIO_EXECUTOR_H_

#include <cstddef>

#include "koladata/functor/parallel/executor.h"

namespace koladata::functor::parallel {

// Returns the default asio executor.
const ExecutorPtr& GetDefaultAsioExecutor();

}  // namespace koladata::functor::parallel

#endif  // THIRD_PARTY_KOLA_DATA_FUNCTOR_PARALLEL_DEFAULT_ASIO_EXECUTOR_H_
