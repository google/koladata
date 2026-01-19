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
// Utilities for converting Python proto messages to Koda objects.

#ifndef KOLADATA_BASE_PY_PROTO_UTILS_H_
#define KOLADATA_BASE_PY_PROTO_UTILS_H_

#include <optional>
#include <vector>

#include "Python.h"
#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"

namespace koladata::python {
// Treats Python objects (potentially sparse) as proto Messages and converts
// them to Koda objects (to missing elements if None).
// Basically, calls `FromProto` on the non-None objects, and sets the missing
// elements for None elements. If an element is neither a proto Message nor
// None, raises an error.
absl::StatusOr<DataSlice> FromProtoObjects(
    const absl_nonnull DataBagPtr& db, const std::vector<PyObject*>& py_objects,
    absl::Span<const absl::string_view> extensions,
    const std::optional<DataSlice>& itemid,
    const std::optional<DataSlice>& schema);
}  // namespace koladata::python

#endif  // KOLADATA_BASE_PY_PROTO_UTILS_H_
