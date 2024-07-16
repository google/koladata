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
#ifndef THIRD_PARTY_PY_KOLADATA_TYPES_BOXING_H_
#define THIRD_PARTY_PY_KOLADATA_TYPES_BOXING_H_

#include <Python.h>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"

namespace koladata::python {

// Returns a new reference to DataSlice / DataItem created from Python value
// which can be:
//   1) Python or Arolla scalar that is supported as an item of a DataSlice;
//   2) Arbitrarily nested Python list, whose leaf items are Python scalars that
//      satisfy (1) or DataItems;
//   3) Arolla DenseArray of primitives;
//   4) Arolla Array of primitives.
//
// In case of scalar inputs, the returned Python object is DataItem.
//
// In case of malformed inputs or unsupported types for nested list items,
// appropriate Status error is returned.
absl::StatusOr<DataSlice> DataSliceFromPyValue(
    PyObject* py_obj, AdoptionQueue& adoption_queue,
    const DataSlice* dtype = nullptr);

// If `py_obj` is a python list of DataItems, merges their DataBags and attaches
// to the resulted DataSlice. Otherwise returns a DataSlice without DataBag.
absl::StatusOr<DataSlice> DataSliceFromPyValueWithAdoption(
    PyObject* py_obj, const DataSlice* dtype = nullptr);

// If `py_obj` is a python list of DataItems, ignores their DataBags.
inline absl::StatusOr<DataSlice> DataSliceFromPyValueNoAdoption(
    PyObject* py_obj, const DataSlice* dtype = nullptr) {
  AdoptionQueue adoption_queue;
  return DataSliceFromPyValue(py_obj, adoption_queue, dtype);
}

// Converts a DataSlice `ds` to an equivalent Python value. In case of presence
// of multiple dimensions, a nested list of items is returned. Returns a new
// reference to a Python object.
absl::Nullable<PyObject*> DataSliceToPyValue(const DataSlice& ds);

// Converts Python objects into DataSlices and converts them into appropriate
// Koda Entities. The conversion is deep, such that all nested structures (e.g.
// dicts) are also Entities (including Koda Lists and Dicts) or primitive
// DataSlices.
//
// `db` and `adoption_queue` are side outputs. `db` is used to create Koda
// objects found under `py_obj`, while `adoption_queue` is used to collect
// DataBag(s) of DataSlice(s) from nested `py_obj`.
//
// Python lists are not further traversed recursively.
// TODO: Consider supporting list of items on which
// UniversalConverter is applied.
absl::StatusOr<DataSlice> EntitiesFromPyObject(
    PyObject* py_obj, const DataBagPtr& db, AdoptionQueue& adoption_queue);

// Same as above, but allows specifying the schemas of Lists / Dicts.
absl::StatusOr<DataSlice> EntitiesFromPyObject(
    PyObject* py_obj,
    const std::optional<DataSlice>& schema,
    const DataBagPtr& db, AdoptionQueue& adoption_queue);

// Converts Python objects into DataSlices and converts them into appropriate
// Koda Objects. The conversion is deep, such that all nested structures (e.g.
// dicts) are also Koda Objects (including Koda Lists and Dicts) or primitive
// DataSlices (which are casted using OBJECT schemas).
//
// `py_obj` is the true input argument that is being converted, while `db` is
// used to create all Koda objects into. `adoption_queue` is used to collect
// DataBag(s) of DataSlice(s) from nested `py_obj`.
//
// Python lists are not further traversed recursively.
// TODO: Consider supporting list of items on which
// UniversalConverter is applied.
absl::StatusOr<DataSlice> ObjectsFromPyObject(
    PyObject* py_obj, const DataBagPtr& db, AdoptionQueue& adoption_queue);

// Applies UniversalConverter (with EntityCreator) to each key and value of a
// dict `py_obj` and returns them as DataSlices.
//
// The only input argument is `py_obj`, while others are output arguments. `db`
// is not a real output argument, but is used to create Koda objects in it.
// `adoption_queue` is used to collect DataBag(s) of DataSlices found in nested
// `py_obj`.
//
// `keys` and `values` are true output arguments and are used to build a
// dictionary by the caller.
absl::Status ConvertDictKeysAndValues(PyObject* py_obj, const DataBagPtr& db,
                                      AdoptionQueue& adoption_queue,
                                      std::optional<DataSlice>& keys,
                                      std::optional<DataSlice>& values);

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_TYPES_BOXING_H_
