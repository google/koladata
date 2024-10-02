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
#ifndef THIRD_PARTY_PY_KOLADATA_TYPES_PY_ATTR_PROVIDER_H_
#define THIRD_PARTY_PY_KOLADATA_TYPES_PY_ATTR_PROVIDER_H_

#include <Python.h>

#include <deque>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "py/arolla/py_utils/py_utils.h"

namespace koladata::python {

// AttrProvider provides an API to parse object-like Python structures for
// fetching atttribute names and values. These attribute names and values are
// used to create Koda Objects / Entities.
class AttrProvider {
 public:
  struct AttrResult {
    std::vector<absl::string_view> attr_names;
    std::vector<PyObject*> values;
  };

  AttrProvider();

  // Returns attribute names and values on success and if `py_obj` represents a
  // Python object for which parsing attributes is supported (e.g. dataclasses).
  // Returned values are borrowed references. If the result is `std::nullopt`
  // the `py_obj` cannot provide attributes.
  absl::StatusOr<std::optional<AttrResult>> GetAttrNamesAndValues(
      PyObject* py_obj);

 private:
  arolla::python::PyObjectPtr dataclasses_module_;
  // DataClasses returns borrowed references to the client, so it needs to own
  // them as it got new references from dataclass object.
  std::deque<arolla::python::PyObjectPtr> owned_values_;
};

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_TYPES_PY_ATTR_PROVIDER_H_
