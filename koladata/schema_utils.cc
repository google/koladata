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
#include "koladata/schema_utils.h"

#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_utils.h"

namespace koladata {

internal::DataItem GetNarrowedSchema(const DataSlice& slice) {
  const auto& schema = slice.GetSchemaImpl();
  if (schema == schema::kObject || schema == schema::kAny) {
    return slice.VisitImpl([&](const auto& impl) {
      if (auto data_schema = schema::GetDataSchema(impl);
          data_schema.has_value()) {
        return data_schema;
      } else {
        return schema;
      }
    });
  }
  return schema;
}

}  // namespace koladata
