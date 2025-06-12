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
#include "koladata/functor/parallel/executor.h"

#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {

void FingerprintHasherTraits<koladata::functor::parallel::ExecutorPtr>::
operator()(FingerprintHasher* hasher,
           const koladata::functor::parallel::ExecutorPtr& value) const {
  if (value != nullptr) {
    hasher->Combine(value->uuid());
  }
}

ReprToken ReprTraits<koladata::functor::parallel::ExecutorPtr>::operator()(
    const koladata::functor::parallel::ExecutorPtr& value) const {
  if (value == nullptr) {
    return ReprToken{"executor{nullptr}"};
  }
  return ReprToken{value->Repr()};
}

AROLLA_DEFINE_SIMPLE_QTYPE(EXECUTOR, koladata::functor::parallel::ExecutorPtr);

}  // namespace arolla
