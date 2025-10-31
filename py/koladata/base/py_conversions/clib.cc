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
// A module for exposing low-level C++ functions from base/ to Python as
// free-functions.
#include <Python.h>

#include <cstddef>
#include <optional>

#include "arolla/qtype/typed_value.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/py_conversions/from_py.h"
#include "py/koladata/base/wrap_utils.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

namespace koladata::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(clib, m) {
  m.def("_from_py_v2", [](py::handle py_obj, py::handle schema, size_t from_dim,
                          bool dict_as_obj, py::handle itemid) {
    arolla::python::PyCancellationScope cancellation_scope;
    std::optional<DataSlice> schema_ds;
    if (!UnwrapDataSliceOptionalArg(schema.ptr(), "schema", schema_ds)) {
      throw pybind11::error_already_set();
    }

    std::optional<DataSlice> itemid_ds;
    if (!UnwrapDataSliceOptionalArg(itemid.ptr(), "itemid", itemid_ds)) {
      throw pybind11::error_already_set();
    }

    return arolla::TypedValue::FromValue(arolla::python::pybind11_unstatus_or(
        FromPy_V2(py_obj.ptr(), schema_ds, from_dim, dict_as_obj, itemid_ds)));
  });
}

}  // namespace
}  // namespace koladata::python
