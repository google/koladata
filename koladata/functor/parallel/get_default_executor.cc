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

#include "absl/base/nullability.h"
#include "koladata/functor/parallel/executor.h"

#include "koladata/functor/parallel/default_asio_executor.h"

namespace koladata::functor::parallel {

const ExecutorPtr absl_nonnull& GetDefaultExecutor() {
  return GetDefaultAsioExecutor();
}

}  // namespace koladata::functor::parallel
