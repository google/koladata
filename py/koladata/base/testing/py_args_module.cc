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
// Used to create py functions that test utilities from ../py_utils. Especially
// argument processing utilities.

#include <Python.h>

#include <cstddef>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/log/check.h"
#include "py/koladata/base/py_args.h"

namespace  koladata::python {
namespace {

constexpr const char* kThisModuleName = "koladata.types.testing.py_args";

int ToInt(PyObject* arg) {
  DCHECK(PyLong_Check(arg));
  return PyLong_AsLong(arg);
}

PyObject* ArgOrNone(PyObject* py_arg) {
  return Py_NewRef(py_arg ? py_arg : Py_None);
}

// All arguments must be present.
PyObject* PositionalKeyword_2_Args(PyObject* /*self*/, PyObject* const* py_args,
                                   Py_ssize_t nargs, PyObject* py_kwnames) {
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/false, "a"/*0*/, "b"/*1*/);
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  for (auto pos_kw_val : args.pos_kw_values) {
    if (pos_kw_val == nullptr) {
      PyErr_SetString(PyExc_TypeError, "both arguments should be present");
      return nullptr;
    }
  }
  return PyLong_FromLongLong(
      ToInt(args.pos_kw_values[0]) - ToInt(args.pos_kw_values[1]));
}

// Tests that proper arg names are mapped to proper positions.
PyObject* PositionalOnly_And_PositionalKeyword_3_Args(
    PyObject* /*self*/, PyObject* const* py_args, Py_ssize_t nargs,
    PyObject* py_kwnames) {
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/2, /*parse_kwargs=*/false, "a"/*0*/, "b"/*1*/, "c"/*2*/);
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  PyObject* tuple = PyTuple_New(args.pos_kw_values.size());
  for (size_t i = 0; i < args.pos_kw_values.size(); ++i) {
    PyTuple_SetItem(tuple, i, ArgOrNone(args.pos_kw_values[i]));
  }
  return tuple;
}

// Tests that all keyword arguments are collected with proper key-value
// matching.
PyObject* Kwargs(PyObject* /*self*/, PyObject* const* py_args,
                 Py_ssize_t nargs, PyObject* py_kwnames) {
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true);
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  DCHECK_EQ(args.pos_kw_values.size(), 0);
  DCHECK_EQ(args.kw_names.size(), args.kw_values.size());
  PyObject* tuple = PyTuple_New(2);  // keys and values.
  PyTuple_SetItem(tuple, 0, PyTuple_New(args.kw_names.size()));
  PyTuple_SetItem(tuple, 1, PyTuple_New(args.kw_values.size()));
  for (size_t i = 0; i < args.kw_names.size(); ++i) {
    PyTuple_SetItem(
        PyTuple_GetItem(tuple, 0), i,
        PyUnicode_DecodeUTF8(args.kw_names[i].data(),
                             args.kw_names[i].size(), nullptr));
    PyTuple_SetItem(PyTuple_GetItem(tuple, 1), i, Py_NewRef(args.kw_values[i]));
  }
  return tuple;
}

// Tests the combination of positional-keyword arguments and keyword arguments.
PyObject* PositionalKeyword_2_Args_And_Kwargs(
    PyObject* /*self*/, PyObject* const* py_args, Py_ssize_t nargs,
    PyObject* py_kwnames) {
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true, "a"/*0*/, "b"/*1*/);
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  DCHECK_EQ(args.pos_kw_values.size(), 2);
  DCHECK_EQ(args.kw_names.size(), args.kw_values.size());
  PyObject* tuple = PyTuple_New(3);  // pos_kw, keys and values.
  PyTuple_SetItem(tuple, 0, PyTuple_New(args.pos_kw_values.size()));
  PyTuple_SetItem(tuple, 1, PyTuple_New(args.kw_names.size()));
  PyTuple_SetItem(tuple, 2, PyTuple_New(args.kw_values.size()));
  for (size_t i = 0; i < args.pos_kw_values.size(); ++i) {
      PyTuple_SetItem(PyTuple_GetItem(tuple, 0), i,
                      ArgOrNone(args.pos_kw_values[i]));
  }
  for (size_t i = 0; i < args.kw_names.size(); ++i) {
    PyTuple_SetItem(
        PyTuple_GetItem(tuple, 1), i,
        PyUnicode_DecodeUTF8(args.kw_names[i].data(),
                             args.kw_names[i].size(), nullptr));
    PyTuple_SetItem(PyTuple_GetItem(tuple, 2), i, Py_NewRef(args.kw_values[i]));
  }
  return tuple;
}

// Tests keyword-only arguments without the support for positional and or
// variadic-keyword args.
PyObject* KeywordOnly(PyObject* /*self*/, PyObject* const* py_args,
                      Py_ssize_t nargs, PyObject* py_kwnames) {
  static const absl::NoDestructor<FastcallArgParser> parser(FastcallArgParser(
      /*pos_only_n=*/0, /*parse_kwargs=*/false, {"a", "b"}));
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  PyObject* tuple = PyTuple_New(2);
  PyTuple_SetItem(tuple, 0, ArgOrNone(args.kw_only_args["a"]));
  PyTuple_SetItem(tuple, 1, ArgOrNone(args.kw_only_args["b"]));
  return tuple;
}

PyObject* KeywordOnly_And_PositionalOnly(
    PyObject* /*self*/, PyObject* const* py_args, Py_ssize_t nargs,
    PyObject* py_kwnames) {
  static const absl::NoDestructor<FastcallArgParser> parser(FastcallArgParser(
      /*pos_only_n=*/2, /*parse_kwargs=*/false, {"a", "b"}));
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  PyObject* tuple = PyTuple_New(4);
  PyTuple_SetItem(tuple, 0, ArgOrNone(args.pos_only_args[0]));
  PyTuple_SetItem(tuple, 1, ArgOrNone(args.pos_only_args[1]));
  PyTuple_SetItem(tuple, 2, ArgOrNone(args.kw_only_args["a"]));
  PyTuple_SetItem(tuple, 3, ArgOrNone(args.kw_only_args["b"]));
  return tuple;
}

PyObject* KeywordOnly_And_VariadicKwargs(
    PyObject* /*self*/, PyObject* const* py_args, Py_ssize_t nargs,
    PyObject* py_kwnames) {
  static const absl::NoDestructor<FastcallArgParser> parser(FastcallArgParser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true, {"a", "b"}));
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  PyObject* tuple = PyTuple_New(4);
  PyTuple_SetItem(tuple, 0, ArgOrNone(args.kw_only_args["a"]));
  PyTuple_SetItem(tuple, 1, ArgOrNone(args.kw_only_args["b"]));
  PyTuple_SetItem(tuple, 2, PyTuple_New(args.kw_names.size()));
  PyTuple_SetItem(tuple, 3, PyTuple_New(args.kw_values.size()));
  for (size_t i = 0; i < args.kw_names.size(); ++i) {
    PyTuple_SetItem(
        PyTuple_GetItem(tuple, 2), i,
        PyUnicode_DecodeUTF8(args.kw_names[i].data(),
                             args.kw_names[i].size(), nullptr));
    PyTuple_SetItem(PyTuple_GetItem(tuple, 3), i, Py_NewRef(args.kw_values[i]));
  }
  return tuple;
}

PyObject* KeywordOnly_PositionalKeyword_And_VariadicKwargs(
    PyObject* /*self*/, PyObject* const* py_args, Py_ssize_t nargs,
    PyObject* py_kwnames) {
  static const absl::NoDestructor<FastcallArgParser> parser(FastcallArgParser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true, {"a", "b"}, "pos_1", "pos_2"));
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  PyObject* tuple = PyTuple_New(4 + args.pos_kw_values.size());
  PyTuple_SetItem(tuple, 0, ArgOrNone(args.kw_only_args["a"]));
  PyTuple_SetItem(tuple, 1, ArgOrNone(args.kw_only_args["b"]));
  PyTuple_SetItem(tuple, 2, PyTuple_New(args.kw_names.size()));
  PyTuple_SetItem(tuple, 3, PyTuple_New(args.kw_values.size()));
  for (size_t i = 0; i < args.kw_names.size(); ++i) {
    PyTuple_SetItem(
        PyTuple_GetItem(tuple, 2), i,
        PyUnicode_DecodeUTF8(args.kw_names[i].data(),
                             args.kw_names[i].size(), nullptr));
    PyTuple_SetItem(PyTuple_GetItem(tuple, 3), i, Py_NewRef(args.kw_values[i]));
  }
  for (size_t i = 0; i < args.pos_kw_values.size(); ++i) {
    PyTuple_SetItem(tuple, 4 + i, ArgOrNone(args.pos_kw_values[i]));
  }
  return tuple;
}

// Tests optional-positional-only.
PyObject* OptionalPositionalOnly(PyObject* /*self*/, PyObject* const* py_args,
                                 Py_ssize_t nargs, PyObject* py_kwnames) {
  static const absl::NoDestructor<FastcallArgParser> parser(FastcallArgParser(
      /*pos_only_n=*/2, /*optional_positional_only=*/true,
      /*parse_kwargs=*/false, {}, "pos_1", "pos_2"));
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  PyObject* tuple = PyTuple_New(args.pos_only_args.size() +
                                args.pos_kw_values.size());
  PyTuple_SetItem(tuple, 0, ArgOrNone(args.pos_only_args[0]));
  PyTuple_SetItem(tuple, 1, ArgOrNone(args.pos_only_args[1]));
  PyTuple_SetItem(tuple, 2, ArgOrNone(args.pos_kw_values[0]));
  PyTuple_SetItem(tuple, 3, ArgOrNone(args.pos_kw_values[1]));
  return tuple;
}

PyMethodDef kPyArgsModule_methods[] = {
    {"pos_kw_2_args", (PyCFunction)PositionalKeyword_2_Args,
     METH_FASTCALL | METH_KEYWORDS,
     "Test function."},
    {"pos_only_and_pos_kw_3_args",
     (PyCFunction)PositionalOnly_And_PositionalKeyword_3_Args,
     METH_FASTCALL | METH_KEYWORDS,
     "Test function."},
    {"kwargs", (PyCFunction)Kwargs, METH_FASTCALL | METH_KEYWORDS,
     "Test function."},
    {"pos_kw_2_and_kwargs", (PyCFunction)PositionalKeyword_2_Args_And_Kwargs,
     METH_FASTCALL | METH_KEYWORDS,
     "Test function."},
    {"kw_only", (PyCFunction)KeywordOnly, METH_FASTCALL | METH_KEYWORDS,
     "Test function."},
    {"kw_only_and_pos_only", (PyCFunction)KeywordOnly_And_PositionalOnly,
     METH_FASTCALL | METH_KEYWORDS,
     "Test function."},
    {"kw_only_and_var_kwargs", (PyCFunction)KeywordOnly_And_VariadicKwargs,
     METH_FASTCALL | METH_KEYWORDS,
     "Test function."},
    {"kw_only_pos_only_and_var_kwargs",
     (PyCFunction)KeywordOnly_PositionalKeyword_And_VariadicKwargs,
     METH_FASTCALL | METH_KEYWORDS,
     "Test function."},
    {"optional_positional_only", (PyCFunction)OptionalPositionalOnly,
     METH_FASTCALL | METH_KEYWORDS,
     "Test function."},
    {nullptr} /* sentinel */
};

struct PyModuleDef py_args_module = {
  PyModuleDef_HEAD_INIT,
  kThisModuleName,
  /*module docstring*/"A test module for Python utilities.",
  -1,
  /*methods=*/kPyArgsModule_methods,
};

// NOTE: This PyInit function must be named this way
// (PyInit_{py_extension.name}). Otherwise it does not get initialized.
PyMODINIT_FUNC PyInit_py_args_py_ext(void) {
  return PyModule_Create(&py_args_module);
}

}  // namespace
}  // namespace koladata::python
