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
#ifndef KOLADATA_INTERNAL_UUID_SCHEMAS_H_
#define KOLADATA_INTERNAL_UUID_SCHEMAS_H_

#include "absl/strings/string_view.h"
#include "koladata/internal/data_item.h"

namespace koladata::internal {

// Creates the UUID ObjectId for a list schema from its item schema.
DataItem CreateListSchemaId(const DataItem& item_schema);

// Creates the UUID ObjectId for a dict schema from its key and value schemas.
DataItem CreateDictSchemaId(const DataItem& key_schema,
                            const DataItem& value_schema);

// Creates the UUID ObjectId for a named schema from its name.
DataItem CreateNamedSchemaId(absl::string_view name);

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_UUID_SCHEMAS_H_
