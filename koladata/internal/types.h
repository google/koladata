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
#ifndef KOLADATA_INTERNAL_TYPES_H_
#define KOLADATA_INTERNAL_TYPES_H_

#include <cstdint>
#include <variant>

#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {

using supported_primitives_list =
    arolla::meta::concat_t<schema::supported_primitive_dtypes,
                           arolla::meta::type_list<schema::DType>>;
using supported_types_list =
    arolla::meta::concat_t<arolla::meta::type_list<ObjectId>,
                           supported_primitives_list>;

// LINT.IfChange
using ScalarVariant = std::variant<  //
    MissingValue,                    //
    ObjectId,                        //
    // Note: int64_t should be specified right after int32_t, otherwise
    // DataItem::Less() comparison will be not transitive.
    int32_t,                         //
    int64_t,                         //
    // Note: double should be specified right after float, otherwise
    // DataItem::Less() comparison will be not transitive.
    float,                           //
    double,                          //
    bool,                            //
    arolla::Unit,                    //
    arolla::Text,                    //
    arolla::Bytes,                   //
    schema::DType,                   //
    arolla::expr::ExprQuote          //
  >;
// NOTE: Please update `koda_internal._to_data_slice`. ExprQuote and DType are
// intentionally omitted (for now).
// LINT.ThenChange(
//     //koladata/operators/BUILD,
//     //py/koladata/operators/arolla_bridge.py,
// )

using ArrayBuilderVariant = std::variant<                //
    std::monostate,                                      //
    arolla::DenseArrayBuilder<ObjectId>,                 //
    arolla::DenseArrayBuilder<int32_t>,                  //
    arolla::DenseArrayBuilder<int64_t>,                  //
    arolla::DenseArrayBuilder<float>,                    //
    arolla::DenseArrayBuilder<double>,                   //
    arolla::DenseArrayBuilder<bool>,                     //
    arolla::DenseArrayBuilder<arolla::Unit>,             //
    arolla::DenseArrayBuilder<arolla::Text>,             //
    arolla::DenseArrayBuilder<arolla::Bytes>,            //
    arolla::DenseArrayBuilder<arolla::expr::ExprQuote>,  //
    arolla::DenseArrayBuilder<schema::DType>>;

using KodaTypeId = int8_t;

// Index of the type in ScalarVariant.
// We rely that ScalarVariant(T()).index() == ScalarTypeId<T>()
template <typename T, KodaTypeId index = 0>
static constexpr KodaTypeId ScalarTypeId() {
  static_assert(std::variant_size_v<ScalarVariant> > index, "unsupported type");
  if constexpr (std::is_same_v<std::variant_alternative_t<index, ScalarVariant>,
                               T>) {
    return index;
  } else {
    return ScalarTypeId<T, index + 1>();
  }
}

// Returns QType corresponding to given Koda type. nullptr in case of
// MissingValue or invalid KodaTypeId.
const arolla::QType* ScalarTypeIdToQType(KodaTypeId id);

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_TYPES_H_
