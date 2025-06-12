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
#ifndef KOLADATA_INTERNAL_ERROR_H_
#define KOLADATA_INTERNAL_ERROR_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>

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

// Error for cases when objects with schema OBJECT are missing
// __schema__ attributes.
struct MissingObjectSchemaError {
  DataItem missing_schema_item;
};

// Conflicts when merging two DataBags.
struct DataBagMergeConflictError {
  // Conflict in the internal dict structure. It might be a schema conflict.
  struct DictConflict {
    DataItem object_id;
    DataItem key;
    DataItem expected_value;
    DataItem assigned_value;
  };
  struct EntityObjectConflict {
    DataItem object_id;
    std::string attr_name;
  };
  struct ListContentConflict {
    DataItem list_object_id;
    int64_t list_item_conflict_index;
    DataItem first_conflicting_item;
    DataItem second_conflicting_item;
  };
  struct ListSizeConflict {
    DataItem list_object_id;
    int64_t first_list_size;
    int64_t second_list_size;
  };
  std::variant<DictConflict, EntityObjectConflict, ListContentConflict,
               ListSizeConflict>
      conflict;
};

struct MissingCollectionItemSchemaError {
  enum class CollectionType {
    kList = 0,
    kDict = 1,
  };
  DataItem missing_schema_item;
  CollectionType collection_type;
  std::optional<int64_t> item_index;
};

// Error when the assigned value has a different schema than the expected one.
struct IncompatibleSchemaError {
  std::string attr;
  DataItem expected_schema;
  DataItem assigned_schema;
};

// Error when an attribute has a shape that is not broadcastable to the
// common shape.
struct ShapeAlignmentError {
  size_t common_shape_id;        // index of the common shape in the input.
  size_t incompatible_shape_id;  // index of the incompatible shape in the
                                 // input.
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_ERROR_H_
