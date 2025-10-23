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
#include "py/koladata/ext/view/clib.h"

#include <Python.h>

#include <cstdint>

#include "absl/container/inlined_vector.h"
#include "absl/types/span.h"
#include "py/arolla/py_utils/py_utils.h"

namespace koladata::python {
namespace {

// TODO: Implement non-recursive version.
//
// All PyObject* inputs are borrowed.
arolla::python::PyObjectPtr MapImpl(PyObject* fn,
                                    absl::Span<const int64_t> depths,
                                    int64_t current_depth, bool include_missing,
                                    absl::Span<PyObject*> py_args,
                                    Py_ssize_t nargs, PyObject* py_kwnames) {
  int64_t max_len = -1;
  for (int64_t i = 0; i < depths.size(); ++i) {
    if (depths[i] <= current_depth) {
      continue;  // Scalar.
    }
    if (!PyTuple_CheckExact(py_args[i])) {
      PyErr_Format(
          PyExc_TypeError,
          "expected the structure to be a tuple when depth > 0, got %s",
          Py_TYPE(py_args[i])->tp_name);
      return nullptr;
    }
    int64_t len = PyTuple_GET_SIZE(py_args[i]);
    if (max_len == -1) {
      max_len = len;
    } else if (len != max_len) {
      PyErr_Format(
          PyExc_TypeError,
          "expected all tuples to be the same length when depth > 0, got %d "
          "and %d",
          len, max_len);
      return nullptr;
    }
  }
  // All are scalars.
  if (max_len == -1) {
    if (!include_missing) {
      for (int64_t i = 0; i < depths.size(); ++i) {
        if (py_args[i] == Py_None) {
          return arolla::python::PyObjectPtr::NewRef(Py_None);
        }
      }
    }
    return arolla::python::PyObjectPtr::Own(
        PyObject_Vectorcall(fn, py_args.data(), nargs, py_kwnames));
  }
  // Set up common values for subsequent calls - i.e. scalars.
  absl::InlinedVector<PyObject*, 4> new_py_args(depths.size(), nullptr);
  for (int64_t i = 0; i < depths.size(); ++i) {
    if (depths[i] <= current_depth) {
      new_py_args[i] = py_args[i];
    }
  }
  auto py_result_tuple = arolla::python::PyObjectPtr::Own(PyTuple_New(max_len));
  if (py_result_tuple == nullptr) {
    return nullptr;
  }
  int64_t new_depth = current_depth + 1;
  for (int64_t i = 0; i < max_len; ++i) {
    for (int64_t j = 0; j < depths.size(); ++j) {
      if (depths[j] > current_depth) {
        new_py_args[j] = PyTuple_GET_ITEM(py_args[j], i);
      }
    }
    arolla::python::PyObjectPtr res =
        MapImpl(fn, depths, new_depth, include_missing,
                absl::MakeSpan(new_py_args), nargs, py_kwnames);
    if (res == nullptr) {
      return nullptr;
    }
    PyTuple_SET_ITEM(py_result_tuple.get(), i, res.release());
  }
  return py_result_tuple;
}

PyObject* PyMapStructures(PyObject* /*self*/, PyObject* const* py_args,
                          Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  // Note:
  //   py_args: [
  //    fn: Callable,
  //    depths: tuple[int, ...] | list[int],
  //    include_missing: bool,
  //    args: tuple[Any, ...] | list[Any],
  //    kwnames: tuple[str, ...] = (),  // Optional - can be missing.
  //  ]
  if (nargs < 4 || nargs > 5) {
    return PyErr_Format(
        PyExc_TypeError,
        "expected 4 or 5 positional arguments but %zu were given", nargs);
  }
  PyObject* fn = py_args[0];
  if (!PyCallable_Check(fn)) {
    return PyErr_Format(PyExc_TypeError,
                        "expected the first arg to be a callable, got %s",
                        Py_TYPE(fn)->tp_name);
  }
  absl::Span<PyObject*> py_depths;
  if (!arolla::python::PyTuple_AsSpan(py_args[1], &py_depths)) {
    return PyErr_Format(PyExc_TypeError,
                        "expected the second arg to be a tuple / list, got %s",
                        Py_TYPE(py_args[1])->tp_name);
  }
  int include_missing = PyObject_IsTrue(py_args[2]);
  if (include_missing < 0) {
    return nullptr;
  }
  absl::Span<PyObject*> fn_args;
  if (!arolla::python::PyTuple_AsSpan(py_args[3], &fn_args)) {
    return PyErr_Format(PyExc_TypeError,
                        "expected the third arg to be a tuple / list, got %s",
                        Py_TYPE(py_args[3])->tp_name);
  }
  // Parse the kwnames.
  PyObject* kwnames = nullptr;
  Py_ssize_t nposargs = fn_args.size();
  if (nargs == 5) {
    kwnames = py_args[4];
    if (!PyTuple_CheckExact(kwnames)) {
      return PyErr_Format(PyExc_TypeError,
                          "expected the fourth arg to be a tuple, got %s",
                          Py_TYPE(kwnames)->tp_name);
    }
    Py_ssize_t nkwnames = PyTuple_GET_SIZE(kwnames);
    if (nkwnames > fn_args.size()) {
      return PyErr_Format(
          PyExc_TypeError,
          "expected the number of kwnames to be at most the number of args, "
          "but got %zu kwnames and %zu args",
          nkwnames, fn_args.size());
    }
    nposargs -= nkwnames;
  }
  if (py_depths.size() != fn_args.size()) {
    return PyErr_Format(
        PyExc_TypeError,
        "expected the number of depths to be the same as the number of args, "
        "but got len(depths): %zu, number of args + kwargs: %zu",
        py_depths.size(), fn_args.size());
  }
  if (py_depths.empty()) {
    return PyErr_Format(PyExc_TypeError, "expected at least one arg");
  }
  // Parse the depths.
  absl::InlinedVector<int64_t, 4> depths(py_depths.size());
  for (int64_t i = 0; i < py_depths.size(); ++i) {
    PyObject* py_depth = py_depths[i];
    if (!PyLong_Check(py_depth)) {
      return PyErr_Format(PyExc_TypeError,
                          "expected all depths to be integers, got %s",
                          Py_TYPE(py_depth)->tp_name);
    }
    depths[i] = PyLong_AsLong(py_depth);
    if (PyErr_Occurred()) {
      return nullptr;
    }
    if (depths[i] < 0) {
      return PyErr_Format(PyExc_ValueError,
                          "expected all depths to be non-negative, got %d",
                          depths[i]);
    }
  }
  return MapImpl(fn, depths, /*current_depth=*/0, include_missing, fn_args,
                 nposargs, kwnames)
      .release();
}

}  // namespace

const PyMethodDef kDefMapStructures = {
    "map_structures", reinterpret_cast<PyCFunction>(&PyMapStructures),
    METH_FASTCALL,
    "map_structures(fn, depths, include_missing, args, kwnames=(), /)\n"
    "--\n\n"
    "Applies `fn` to each item in the given structures.\n"
    "\n"
    "Args:\n"
    "  fn: function that will be applied with args and kwargs taken from the\n"
    "    provided inputs.\n"
    "  depths: a tuple / list of integers specifying to which depth each arg\n"
    "    should be traversed to. Must have the same arity as args.\n"
    "  include_missing: whether to call fn when one of the args is None.\n"
    "  args: a tuple / list of arguments that will be traversed. Those levels\n"
    "    that should be recursed on are required to be tuples.\n"
    "  kwnames: optional tuple of names of keyword arguments."};

}  // namespace koladata::python
