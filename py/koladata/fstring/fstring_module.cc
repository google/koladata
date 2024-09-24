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
#include <Python.h>

#include "py/koladata/fstring/fstring.h"

namespace koladata::python {
namespace {

constexpr const char* kThisModuleName = "koladata.fstring";

PyMethodDef kPyDataSliceModule_methods[] = {
    {"fstr", (PyCFunction)PyEagerFStringEval, METH_O,
     R"DOC(Evaluates Koladata f-string into DataSlice.

  f-string must be created via Python f-string syntax. It must contain at least
  one formatted DataSlice.
  Each DataSlice must have custom format specification,
  e.g. `{ds:s}` or `{ds:.2f}`.
  Find more about format specification in kd.strings.format docs.

  NOTE: `{ds:s}` can be used for any type to achieve default string conversion.

  Examples:
    countries = kd.slice(['USA', 'Schweiz'])
    kd.fstr(f'Hello, {countries:s}!')
      # -> kd.slice(['Hello, USA!', 'Hello, Schweiz!'])

    greetings = kd.slice(['Hello', 'Gruezi'])
    kd.fstr(f'{greetings:s}, {countries:s}!')
      # -> kd.slice(['Hello, USA!', 'Gruezi, Schweiz!'])

    states = kd.slice([['California', 'Arizona', 'Nevada'], ['Zurich', 'Bern']])
    kd.fstr(f'{greetings:s}, {states:s} in {countries:s}!')
      # -> kd.slice([
               ['Hello, California in USA!',
                'Hello, Arizona in USA!',
                'Hello, Nevada in USA!'],
               ['Gruezi, Zurich in Schweiz!',
                'Gruezi, Bern in Schweiz!']]),

    prices = kd.slice([35.5, 49.2])
    currencies = kd.slice(['USD', 'CHF'])
    kd.fstr(f'Lunch price in {countries:s} is {prices:.2f} {currencies:s}.')
      # -> kd.slice(['Lunch price in USA is 35.50 USD.',
                     'Lunch price in Schweiz is 49.20 CHF.'])

  Args:
    s: f-string to evaluate.
  Returns:
    DataSlice with evaluated f-string.
)DOC"},
    {"fstr_expr", (PyCFunction)PyCreateFstrExpr, METH_O,
     R"DOC(Trasforms Koda f-string into an expression.

  See kde.fstr for more details.)DOC"},
    {"fstr_expr_placeholder", (PyCFunction)PyFStringExprPlaceholder,
     METH_FASTCALL,
     "Returns a placeholder for an Expr that occurs in an f-string."},
    {nullptr} /* sentinel */
};

struct PyModuleDef fstring = {
    PyModuleDef_HEAD_INIT,
    kThisModuleName,
    /*module docstring=*/"A f-string definition.",
    /*module_size=*/-1,
    /*methods=*/kPyDataSliceModule_methods,
};

// NOTE: This PyInit function must be named this way
// (PyInit_{py_extension.name}). Otherwise it does not get initialized.
PyMODINIT_FUNC PyInit_fstring_py_ext(void) {
  PyObject* m = PyModule_Create(&fstring);
  return m;
}

}  // namespace
}  // namespace koladata::python
