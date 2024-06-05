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
#include "koladata/internal/dtype.h"

#include <array>
#include <tuple>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/meta.h"

namespace koladata::schema {

const std::array<arolla::QTypePtr, kNextDTypeId>& DType::type_id_to_qtype() {
  static const std::array<arolla::QTypePtr, kNextDTypeId> type_id_to_qtype =
      []() {
        std::array<arolla::QTypePtr, kNextDTypeId> res{};
        arolla::meta::foreach_type(
            schema::supported_dtype_values(), [&](auto tpe) {
              using T = typename decltype(tpe)::type;
              // NOTE: ANY, ITEMID, OBJECT, SCHEMA and NONE all return NOTHING
              // as a QType.
              res[GetDTypeId<T>()] = arolla::GetNothingQType();
            });
        arolla::meta::foreach_type(
            schema::supported_primitive_dtypes(), [&](auto tpe) {
              using T = typename decltype(tpe)::type;
              res[GetDTypeId<T>()] = arolla::GetQType<T>();
            });
        return res;
      }();
  return type_id_to_qtype;
}

const absl::flat_hash_map<arolla::QTypePtr, DTypeId>&
DType::qtype_to_type_id() {
  static const absl::NoDestructor<
      absl::flat_hash_map<arolla::QTypePtr, DTypeId>>
      qtype_to_type_id([]() {
        absl::flat_hash_map<arolla::QTypePtr, DTypeId> res;
        res.reserve(std::tuple_size_v<schema::supported_dtype_values::tuple>);
        arolla::meta::foreach_type(
            schema::supported_primitive_dtypes(), [&](auto tpe) {
              using T = typename decltype(tpe)::type;
              res[arolla::GetQType<T>()] = GetDType<T>().type_id();
            });
        return res;
      }());
  return *qtype_to_type_id;
}

}  // namespace koladata::schema

namespace arolla {

AROLLA_DEFINE_SIMPLE_QTYPE(DTYPE, ::koladata::schema::DType);
AROLLA_DEFINE_OPTIONAL_QTYPE(DTYPE, ::koladata::schema::DType);
AROLLA_DEFINE_DENSE_ARRAY_QTYPE(DTYPE, ::koladata::schema::DType);

}  // namespace arolla
