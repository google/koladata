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
#ifndef THIRD_PARTY_PY_KOLADATA_SERVING_EMBEDDED_SLICES_INTERNAL_H_
#define THIRD_PARTY_PY_KOLADATA_SERVING_EMBEDDED_SLICES_INTERNAL_H_

#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "koladata/data_slice.h"

namespace koladata::serving {

// Parses a single DataSlice from the serialized data, e.g. created by
// koladata_serialized_slices build rule.
absl::StatusOr<DataSlice> ParseSerializedSlice(absl::string_view data);

// TODO: b/432324816 - Make the functions below private.

using SliceMap = absl::flat_hash_map<std::string, koladata::DataSlice>;

// Parses slices from the serialized data.
absl::StatusOr<SliceMap> ParseSerializedSlices(absl::string_view data);

// Returns a slice by its name.
absl::StatusOr<koladata::DataSlice> GetSliceByName(const SliceMap& slices,
                                                   absl::string_view name);

}  // namespace koladata::serving

#endif  // THIRD_PARTY_PY_KOLADATA_SERVING_EMBEDDED_SLICES_INTERNAL_H_
