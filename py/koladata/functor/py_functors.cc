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
#include "py/koladata/functor/py_functors.h"

#include <Python.h>  // IWYU pragma: keep

#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_set.h"
#include "arolla/util/fingerprint.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/auto_variables.h"
#include "py/arolla/abc/py_fingerprint.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/py_args.h"
#include "py/koladata/base/wrap_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {

PyObject* absl_nullable PyAutoVariables(PyObject* /*self*/, PyObject** py_args,
                                        Py_ssize_t nargs) {
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/1, /*parse_kwargs=*/false, "fn", "extra_nodes_to_extract");
  arolla::python::DCheckPyGIL();
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, nullptr, args)) {
    return nullptr;
  }
  const auto* fn = UnwrapDataSlice(args.pos_only_args[0], "fn");
  if (fn == nullptr) {
    return nullptr;
  }
  absl::flat_hash_set<arolla::Fingerprint> extra_nodes_to_extract;
  PyObject* fingerprint_list = args.pos_kw_values[0];
  if (fingerprint_list != nullptr) {
    if (!PyList_Check(fingerprint_list)) {
      return PyErr_Format(PyExc_ValueError,
                          "second argument should be a list of fingerprints");
    }
    Py_ssize_t count = PyList_Size(fingerprint_list);
    extra_nodes_to_extract.reserve(count);
    for (Py_ssize_t i = 0; i < count; ++i) {
      const arolla::Fingerprint* fingerprint =
          arolla::python::UnwrapPyFingerprint(
              PyList_GetItem(fingerprint_list, i));
      if (fingerprint == nullptr) {
        return nullptr;
      }
      extra_nodes_to_extract.insert(*fingerprint);
    }
  }
  ASSIGN_OR_RETURN(
      DataSlice result,
      functor::AutoVariables(*fn, std::move(extra_nodes_to_extract)),
      arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(result));
}

}  // namespace koladata::python
