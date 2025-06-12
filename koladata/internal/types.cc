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
#include "koladata/internal/types.h"

#include <cstdint>

#include "arolla/expr/quote.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {

const arolla::QType* ScalarTypeIdToQType(int8_t id) {
  switch (id) {
    case ScalarTypeId<ObjectId>():
      return arolla::GetQType<ObjectId>();
    case ScalarTypeId<int32_t>():
      return arolla::GetQType<int32_t>();
    case ScalarTypeId<int64_t>():
      return arolla::GetQType<int64_t>();
    case ScalarTypeId<float>():
      return arolla::GetQType<float>();
    case ScalarTypeId<double>():
      return arolla::GetQType<double>();
    case ScalarTypeId<bool>():
      return arolla::GetQType<bool>();
    case ScalarTypeId<arolla::Unit>():
      return arolla::GetQType<arolla::Unit>();
    case ScalarTypeId<arolla::Text>():
      return arolla::GetQType<arolla::Text>();
    case ScalarTypeId<arolla::Bytes>():
      return arolla::GetQType<arolla::Bytes>();
    case ScalarTypeId<arolla::expr::ExprQuote>():
      return arolla::GetQType<arolla::expr::ExprQuote>();
    case ScalarTypeId<schema::DType>():
      return arolla::GetQType<schema::DType>();
    default: return nullptr;
  }
}

}  // namespace koladata::internal
