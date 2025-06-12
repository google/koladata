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
#ifndef KOLADATA_ITERABLES_ITERABLE_QTYPE_H_
#define KOLADATA_ITERABLES_ITERABLE_QTYPE_H_

#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace koladata::iterables {

// Returns true if the given qtype corresponds to Iterable.
bool IsIterableQType(const arolla::QType* qtype);

// Returns an iterable qtype with the given value_qtype.
arolla::QTypePtr GetIterableQType(arolla::QTypePtr value_qtype);

template <typename T>
arolla::QTypePtr GetIterableQType() {
  return GetIterableQType(arolla::GetQType<T>());
}

}  // namespace koladata::iterables

#endif  // KOLADATA_ITERABLES_ITERABLE_QTYPE_H_
