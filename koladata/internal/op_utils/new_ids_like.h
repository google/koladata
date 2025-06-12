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
#ifndef KOLADATA_INTERNAL_OP_UTILS_ALLOCATE_IDS_LIKE_H_
#define KOLADATA_INTERNAL_OP_UTILS_ALLOCATE_IDS_LIKE_H_

#include "koladata/internal/data_slice.h"

namespace koladata::internal {

// Create a new ObjectId for each ObjectId in the ds. If some ObjectId is
// present in ds multiple times, new ObjectId will be created for each
// occurrence. For each ObjectId in ds, newly created ObjectId will have the
// same type (list/dict/object/schema).
DataSliceImpl NewIdsLike(const DataSliceImpl& ds);

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_ALLOCATE_IDS_LIKE_H_
