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
#ifndef KOLADATA_SERVING_SLICE_REGISTRY_H_
#define KOLADATA_SERVING_SLICE_REGISTRY_H_

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "koladata/data_slice.h"

namespace koladata::serving {

// Returns a slice with the given key from the global registry or nullptr if the
// key is not found.
const DataSlice* absl_nullable GetRegisteredSlice(absl::string_view key);

// Registers a slice with the given key in a global registry. Returns an error
// if the name is already taken.
absl::Status RegisterSlice(absl::string_view key, DataSlice slice);

}  // namespace koladata::serving

#endif  // KOLADATA_SERVING_SLICE_REGISTRY_H_
