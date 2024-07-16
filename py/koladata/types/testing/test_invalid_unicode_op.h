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
#ifndef THIRD_PARTY_PY_KOLADATA_TYPES_TESTING_TEST_INVALID_UNICODE_OP_H_
#define THIRD_PARTY_PY_KOLADATA_TYPES_TESTING_TEST_INVALID_UNICODE_OP_H_

#include <optional>

#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"

namespace koladata::ops::testing {

struct InvalidUnicodeOp {
  DataSlice operator()() const {
    auto values = arolla::CreateDenseArray<arolla::Text>({
      arolla::Text("valid"),
      arolla::Text("valid"),
      arolla::Text("\xaa\xff"),  // invalid utf-8
      arolla::Text("valid"),
      arolla::Text("valid"),
      std::nullopt,
      std::nullopt,
      arolla::Text("valid"),
      arolla::Text("valid"),
      arolla::Text("valid"),
      std::nullopt,
      std::nullopt
    });
    return *DataSlice::Create(
        internal::DataSliceImpl::Create(values),
        DataSlice::JaggedShape::FlatFromSize(12),
        internal::DataItem(schema::kText));
  }
};

}  // namespace koladata::ops::testing

#endif  // THIRD_PARTY_PY_KOLADATA_TYPES_TESTING_TEST_INVALID_UNICODE_OP_H_
