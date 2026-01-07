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
#ifndef PY_KOLADATA_BASE_PY_CONVERSIONS_FROM_PY_H_
#define PY_KOLADATA_BASE_PY_CONVERSIONS_FROM_PY_H_

#include <Python.h>

#include <cstddef>
#include <optional>

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"

namespace koladata::python {

// New version of GenericFromPyObject; it will eventually replace the old one.
absl::StatusOr<DataSlice> FromPy_V2(PyObject* py_obj,
                                    const std::optional<DataSlice>& schema,
                                    size_t from_dim, bool dict_as_obj,
                                    const std::optional<DataSlice>& itemid);

}  // namespace koladata::python

#endif  // PY_KOLADATA_BASE_PY_CONVERSIONS_FROM_PY_H_
