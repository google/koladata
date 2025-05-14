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
#include "koladata/schema_constants.h"

#include <tuple>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/types/span.h"
#include "arolla/util/meta.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"

namespace koladata {

absl::Span<const DataSlice> SupportedSchemas() {
  auto create_schemas = []() -> std::vector<DataSlice> {
    std::vector<DataSlice> schemas;
    schemas.reserve(std::tuple_size_v<schema::supported_dtype_values::tuple>);
    // Iterates through all dtypes T for which DType(GetQType<T>()) is defined
    // (int, int64_t, ..., ObjectDType, ...).
    arolla::meta::foreach_type(schema::supported_dtype_values(), [&](auto tpe) {
      using T = typename decltype(tpe)::type;
      schemas.push_back(
          *DataSlice::Create(
              internal::DataItem(schema::GetDType<T>()),
              internal::DataItem(schema::kSchema)));
    });
    return schemas;
  };
  static const absl::NoDestructor<std::vector<DataSlice>> supported_schemas(
      create_schemas());
  return *supported_schemas;
}

}  // namespace koladata
