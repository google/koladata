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
#ifndef PY_KOLADATA_PY_CONVERSIONS_DATACLASSES_UTIL_H_
#define PY_KOLADATA_PY_CONVERSIONS_DATACLASSES_UTIL_H_

#include <Python.h>

#include <deque>
#include <optional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/util/fingerprint.h"
#include "py/arolla/py_utils/py_utils.h"

namespace koladata::python {

// Util class for working with Python dataclasses (getting attributes, making
// instances, etc.) It also owns the dataclass object instances it creates.
class DataClassesUtil {
 public:
  struct AttrResult {
    std::vector<std::string> attr_names;
    std::vector<PyObject*> values;
  };

  DataClassesUtil() = default;

  absl::StatusOr<arolla::python::PyObjectPtr> MakeDataClassInstance(
      absl::Span<const absl::string_view> attr_names);

  // Returns attribute names and values on success and if `py_obj` represents a
  // Python object for which parsing attributes is supported (e.g. dataclasses).
  // Returned values are borrowed references owned by DataClassesUtil. If the
  // result is `std::nullopt`, the `py_obj` cannot provide attributes.
  absl::StatusOr<std::optional<AttrResult>> GetAttrNamesAndValues(
      PyObject* py_obj);

  // Get the values of object's attributes for corresponding attr_names.
  // Returned values are borrowed references owned by DataClassesUtil.
  absl::StatusOr<std::vector<PyObject*>> GetAttrValues(
      PyObject* py_obj, absl::Span<const absl::string_view> sorted_attr_names);

 private:
  absl::StatusOr<arolla::python::PyObjectPtr> MakeDataClass(
      absl::Span<const absl::string_view> attr_names);

  absl::flat_hash_map<arolla::Fingerprint, arolla::python::PyObjectPtr>
      dataclasses_cache_;

  arolla::python::PyObjectPtr fn_make_dataclass_;

  arolla::python::PyObjectPtr fn_fields_;

  // DataClasses returns borrowed references to the client, so it needs to own
  // them as it got new references from dataclass object.
  std::deque<arolla::python::PyObjectPtr> owned_values_;
};

}  // namespace koladata::python

#endif  // PY_KOLADATA_PY_CONVERSIONS_DATACLASSES_UTIL_H_
