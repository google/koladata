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
#ifndef KOLADATA_FUNCTOR_PARALLEL_STREAM_QTYPE_H_
#define KOLADATA_FUNCTOR_PARALLEL_STREAM_QTYPE_H_

#include "absl/base/attributes.h"
#include "absl/base/no_destructor.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {

// Returns true for stream qtypes.
bool IsStreamQType(arolla::QTypePtr qtype);

// Returns a stream qtype with the given value_qtype.
arolla::QTypePtr GetStreamQType(arolla::QTypePtr value_qtype);

// Returns a stream qtype for the given value type.
template <typename T>
arolla::QTypePtr GetStreamQType() {
  static const absl::NoDestructor result(GetStreamQType(arolla::GetQType<T>()));
  return *result;
}

// Wraps the given stream into a qvalue.
arolla::TypedValue MakeStreamQValue(StreamPtr stream);

// Wraps the given stream into a qvalue.
arolla::TypedRef MakeStreamQValueRef(
    const StreamPtr& stream ABSL_ATTRIBUTE_LIFETIME_BOUND);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_STREAM_QTYPE_H_
