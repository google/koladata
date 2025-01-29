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
// Building blocks for "To Python".

#ifndef KOLADATA_BASE_TO_PY_OBJECT_H_
#define KOLADATA_BASE_TO_PY_OBJECT_H_

#include <Python.h>

#include <functional>

#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "py/arolla/py_utils/py_utils.h"

namespace koladata::python {

using ItemToPyConverter =
    std::function<arolla::python::PyObjectPtr(const internal::DataItem& item)>;

// Returns a new reference to a Python object, equivalent to the value stored in
// a `internal::DataItem`.
arolla::python::PyObjectPtr PyObjectFromDataItem(
    const internal::DataItem& item, const internal::DataItem& schema,
    const DataBagPtr& db);

// Converts a DataSlice `ds` to an equivalent Python value. In case of presence
// of multiple dimensions, a nested list of items is returned. Returns a new
// reference to a Python object.
arolla::python::PyObjectPtr PyObjectFromDataSlice(
    const DataSlice& ds, const ItemToPyConverter& optional_converter = nullptr);

}  // namespace koladata::python

#endif  // KOLADATA_BASE_TO_PY_OBJECT_H_
