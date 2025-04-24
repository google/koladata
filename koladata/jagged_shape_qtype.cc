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
#include "koladata/jagged_shape_qtype.h"

#include "absl/base/no_destructor.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"  // IWYU pragma: keep
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/repr.h"

namespace koladata {

namespace {

// QType for DataSlice shape. It is a wrapper around JaggedDenseArrayShape.
class JaggedShapeQType final : public arolla::BasicDerivedQType {
 public:
  explicit JaggedShapeQType()
      : arolla::BasicDerivedQType(ConstructorArgs{
            .name = "JAGGED_SHAPE",
            .base_qtype = arolla::GetQType<arolla::JaggedDenseArrayShape>(),
            .qtype_specialization_key = "::koladata::JaggedShapeQType",
        }) {}

  arolla::ReprToken UnsafeReprToken(const void* source) const override {
    // TODO: JaggedDenseArrayShape UnsafeReprToken should be
    // changed to JaggedDenseArrayShape, and then JaggedShapeQType should not
    // rely on it.
    return arolla::GetQType<arolla::JaggedDenseArrayShape>()->UnsafeReprToken(
        source);
  }
};

}  // namespace

arolla::QTypePtr GetJaggedShapeQType() {
  static const absl::NoDestructor<JaggedShapeQType> result;
  return result.get();
}

}  // namespace koladata
