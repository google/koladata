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
#ifndef KOLADATA_INTERNAL_ERROR_H_
#define KOLADATA_INTERNAL_ERROR_H_

#include "koladata/internal/data_item.h"

namespace koladata::internal {

// Error for not finding a common schema when creating DataSlice. This is used
// for carrying error information and rendering readable error message to user.
struct NoCommonSchemaError {
  // The most common schema fund in the input.
  DataItem common_schema;

  // The conflict schema that cannot be converted to the common schema.
  DataItem conflicting_schema;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_ERROR_H_
