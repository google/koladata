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
#ifndef KOLADATA_FUNCTOR_PARALLEL_FUTURE_QTYPE_H_
#define KOLADATA_FUNCTOR_PARALLEL_FUTURE_QTYPE_H_

#include "absl/base/no_destructor.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/functor/parallel/future.h"

namespace koladata::functor::parallel {

// Returns true for future qtypes.
bool IsFutureQType(arolla::QTypePtr qtype);

// Returns a future qtype with the given value_qtype.
// We might want to disallow futures to futures, as those cases usually
// indicate a bug, but we allow it for now to avoid special cases.
arolla::QTypePtr GetFutureQType(arolla::QTypePtr value_qtype);

template <typename T>
arolla::QTypePtr GetFutureQType() {
  static const absl::NoDestructor result(GetFutureQType(arolla::GetQType<T>()));
  return *result;
}

arolla::TypedValue MakeFutureQValue(FuturePtr future);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_FUTURE_QTYPE_H_
