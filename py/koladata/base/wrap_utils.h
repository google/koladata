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
// Utilities for wrapping / unwrapping QValue specializations defined by Koda.

#ifndef THIRD_PARTY_PY_KOLADATA_BASE_WRAP_UTILS_H_
#define THIRD_PARTY_PY_KOLADATA_BASE_WRAP_UTILS_H_

#include <Python.h>

#include <optional>

#include "absl/base/nullability.h"
#include "absl/strings/string_view.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"

namespace koladata::python {

// Returns a pointer to a DataSlice held by `py_obj`. This is a safe way to
// access the DataSlice from PyObject*. In case `py_obj` does not hold
// DataSlice, appropriate error is set and nullptr returned.
// `name_for_error` is used to format an informative error message.
const DataSlice* /*absl_nullable*/ UnwrapDataSlice(
    PyObject* py_obj, absl::string_view name_for_error);

// Returns a new PyQValue that wraps DataSlice `ds`. In case of errors in Python
// runtime during allocations, this function can return nullptr.
PyObject* /*absl_nullable*/ WrapPyDataSlice(DataSlice&& ds);
// Same as `WrapPyDataSlice`, but freeze DataBag and mark DataSlice as whole.
// It is a caller responsibility to ensure that `ds` is whole and that
// DataBag is not shared.
PyObject* /*absl_nullable*/ WrapPyDataSliceAsWholeWithFrozenDataBag(DataSlice&& ds);

// Unwraps a DataSlice from `py_obj` into `arg` if `py_obj` contains a
// DataSlice. Returns true on success and false on failure. On failure, this
// function sets Python exception. In case `py_obj` is None, `arg` will be
// assigned std::nullopt and true will be returned. `name_for_error` should
// accept the argument name of the argument that is being parsed.
bool UnwrapDataSliceOptionalArg(PyObject* py_obj,
                                absl::string_view name_for_error,
                                std::optional<DataSlice>& arg);

// Returns a const reference to underlying DataSlice object without any checks.
const DataSlice& UnsafeDataSliceRef(PyObject* py_obj);

// Returns a new PyQValue that wraps DataBagPtr `db`. In case of errors in
// Python runtime during allocations, this function can return nullptr.
PyObject* /*absl_nullable*/ WrapDataBagPtr(DataBagPtr db);

// Returns a copy of shared_ptr to DataBag held by `py_obj`. This is a safe way
// to access the DataBag from PyObject*. In case `py_obj` does not hold DataBag,
// appropriate error is set and std::nullopt returned.
//
// `name_for_error` is used to format an informative error message.
//
// NOTE: Python None is unwrapped as DataBagPtr(nullptr).
std::optional<DataBagPtr> UnwrapDataBagPtr(PyObject* py_obj,
                                           absl::string_view name_for_error);

// Returns a const reference to underlying DataBagPtr object without any checks.
const DataBagPtr& UnsafeDataBagPtr(PyObject* py_obj);

// Returns a pointer to the JaggedShape held by `py_obj`. This is a safe way to
// access the JaggedShape from PyObject*. In case `py_obj` does not hold
// JaggedShape, appropriate error is set and nullptr returned.
// `name_for_error` is used to format an informative error message.
const DataSlice::JaggedShape* /*absl_nullable*/ UnwrapJaggedShape(
    PyObject* py_obj, absl::string_view name_for_error);

// Returns a new PyQValue that wraps JaggedShape `shape`. In case of errors in
// Python runtime during allocations, this function can return nullptr.
PyObject* /*absl_nullable*/ WrapPyJaggedShape(DataSlice::JaggedShape shape);

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_BASE_WRAP_UTILS_H_
