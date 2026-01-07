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
#ifndef PY_KOLADATA_BASE_BOXING_H_
#define PY_KOLADATA_BASE_BOXING_H_

#include <Python.h>

#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"

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
    const std::optional<DataSlice>& schema = std::nullopt);

// Creates a DataSlice from a flat vector of PyObject(s), shape and type of
// items. In case, edges is empty, we have a single value and treat it as 0-dim
// output.
// If the schema is provided, the resulting DataSlice will always be
// created with that schema. If `explicit_cast` is true, the `schema` must be
// provided and the resulting DataSlice will be explicitly casted to it.
// Otherwise, the aggregated schema of the elements must be implicitly castable
// to the provided schema (if any).
absl::StatusOr<DataSlice> DataSliceFromPyFlatList(
    const std::vector<PyObject*>& flat_list, DataSlice::JaggedShape shape,
    internal::DataItem schema, AdoptionQueue& adoption_queue,
    bool explicit_cast = false);

// Parses the Python list and returns flattened items (borrowed references)
// together with the appropriate JaggedShape. This is the same logic as used
// internally by `DataSliceFromPyValue`.
absl::StatusOr<std::pair<std::vector<PyObject*>, DataSlice::JaggedShape>>
FlattenPyList(PyObject* py_list, size_t max_depth = 0);

// If `py_obj` is a python list of DataItems, merges their DataBags and attaches
// to the resulted DataSlice. Otherwise returns a DataSlice without DataBag.
absl::StatusOr<DataSlice> DataSliceFromPyValueWithAdoption(
    PyObject* py_obj, const std::optional<DataSlice>& schema = std::nullopt);

// If `py_obj` is a python list of DataItems, ignores their DataBags.
inline absl::StatusOr<DataSlice> DataSliceFromPyValueNoAdoption(
    PyObject* py_obj, const std::optional<DataSlice>& schema = std::nullopt) {
  AdoptionQueue adoption_queue;
  return DataSliceFromPyValue(py_obj, adoption_queue, schema);
}

// Returns a new reference to DataItem created from Python value which can be a
// supported Python or Arolla scalar (int, float, bool, DataItem, etc.).
//
// In case of unsupported types , appropriate Status error is returned.
absl::StatusOr<DataSlice> DataItemFromPyValue(
    PyObject* py_obj, const std::optional<DataSlice>& schema = std::nullopt);

// Converts Python objects into DataSlices and converts them into appropriate
// Koda Entities. The conversion is deep, such that all nested structures (e.g.
// dicts) are also Entities (including Koda Lists and Dicts) or primitive
// DataSlices.
//
// `db` and `adoption_queue` are side outputs. `db` is used to create Koda
// objects found under `py_obj`, while `adoption_queue` is used to collect
// DataBag(s) of DataSlice(s) from nested `py_obj`.
absl::StatusOr<DataSlice> EntitiesFromPyObject(PyObject* py_obj,
                                               const DataBagPtr& db,
                                               AdoptionQueue& adoption_queue);

// Same as above, but allows specifying the schemas of Lists / Dicts. When
// schema == OBJECT, the behavior is the same as `ObjectFromPyObject`.
absl::StatusOr<DataSlice> EntitiesFromPyObject(
    PyObject* py_obj, const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& itemid, const DataBagPtr& db,
    AdoptionQueue& adoption_queue);

// Converts Python objects into DataSlices and converts them into appropriate
// Koda Objects. The conversion is deep, such that all nested structures (e.g.
// dicts) are also Koda Objects (including Koda Lists and Dicts) or primitive
// DataSlices (which are casted using OBJECT schemas).
//
// `py_obj` is the true input argument that is being converted, while `db` is
// used to create all Koda objects into. `adoption_queue` is used to collect
// DataBag(s) of DataSlice(s) from nested `py_obj`.
absl::StatusOr<DataSlice> ObjectsFromPyObject(
    PyObject* py_obj, const std::optional<DataSlice>& itemid,
    const DataBagPtr& db, AdoptionQueue& adoption_queue);

// Converts Python objects into DataSlices and converts them into appropriate
// Koda Objects / Entities depending on schema. The conversion is deep.
//
// This version of deep conversion supports a mode in which Python dictionaries
// are converted into Koda Objects / Entities (depending on which schema is
// provided).
absl::StatusOr<DataSlice> GenericFromPyObject(
    PyObject* py_obj, bool dict_as_obj, const std::optional<DataSlice>& schema,
    size_t from_dim, const std::optional<DataSlice>& itemid = std::nullopt);

// Returns an Error from provided status with incompatible schema information
// during narrow casting.
absl::Status CreateIncompatibleSchemaErrorFromStatus(
    absl::Status status, const DataSlice& item_schema,
    const DataSlice& input_schema);

// Returns true if the given object is a scalar Python object or a QValue.
bool IsPyScalarOrQValueObject(PyObject* py_obj);

// Parses a Python unicode or Koda DataItem into a absl::string_view if
// possible. Otherwise, returns an error (with dict key specific error message).
// TODO: move to a more appropriate file when launching
// `from_py_v2` (like `koladata/base`).
absl::StatusOr<absl::string_view> PyDictKeyAsStringView(PyObject* py_key);

}  // namespace koladata::python

#endif  // PY_KOLADATA_BASE_BOXING_H_
