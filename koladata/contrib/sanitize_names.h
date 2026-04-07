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
#ifndef KOLADATA_CONTRIB_SANITIZE_NAMES_H_
#define KOLADATA_CONTRIB_SANITIZE_NAMES_H_

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"

namespace koladata::contrib {

// Creates a slice where all reached objects have their attribute names
// sanitized so that they are valid python identifiers.
// - All inputs are ascii sequences (no unicode).
// - If sanitized names are not unique, we will add a numeric suffix "_{N}"
// - All changed names are prefixed with "san_".
//
// The returned DataSlice is created in a new DataBag (`new_databag`).
// The Object IDs of the items remain the same, only the attribute names are
// changed.
//
// kd_ext.contrib._sanitize_names
absl::StatusOr<DataSlice> SanitizeNames(const DataSlice& ds);

}  // namespace koladata::contrib


#endif  // KOLADATA_CONTRIB_SANITIZE_NAMES_H_
