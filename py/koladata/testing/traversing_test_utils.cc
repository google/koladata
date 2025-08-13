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
#include "py/koladata/testing/traversing_test_utils.h"

#include <Python.h>
#include <string>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/testing/traversing_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/py_args.h"
#include "py/koladata/base/wrap_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {

PyObject* absl_nullable PyAssertDeepEquivalent(PyObject* /*module*/,
                                               PyObject* const* py_args,
                                               Py_ssize_t nargs,
                                               PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(FastcallArgParser(
      /*pos_only_n=*/2, /*parse_kwargs=*/false,
      {"partial", "schemas_equality"}));
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  const DataSlice* actual_ds =
      UnwrapDataSlice(args.pos_only_args[0], "actual_value");
  if (actual_ds == nullptr) {
    return nullptr;
  }
  const DataSlice* expected_ds =
      UnwrapDataSlice(args.pos_only_args[1], "expected_value");
  if (expected_ds == nullptr) {
    return nullptr;
  }
  bool partial = false;
  if (!ParseBoolArg(args, "partial", partial)) {
    return nullptr;
  }
  bool schemas_equality = false;
  if (!ParseBoolArg(args, "schemas_equality", schemas_equality)) {
    return nullptr;
  }
  // TODO: get max_count from kwargs.
  testing::DeepEquivalentParams comparison_params = {
      .partial = partial, .schemas_equality = schemas_equality};
  ASSIGN_OR_RETURN(
      auto mismatches,
      testing::DeepEquivalentMismatches(*actual_ds, *expected_ds,
                                        /*max_count=*/5, comparison_params),
      arolla::python::SetPyErrFromStatus(_));
  if (mismatches.empty()) {
    Py_RETURN_NONE;
  }
  ReprOption repr_option(
      {.depth = 1, .unbounded_type_max_len = 100, .show_databag_id = false});
  auto expected_repr = DataSliceRepr(*expected_ds, repr_option);
  auto actual_repr = DataSliceRepr(*actual_ds, repr_option);
  std::string msg = absl::StrFormat(
      "Expected: is equal to %s\nActual: %s, with difference:\n%s",
      expected_repr, actual_repr, absl::StrJoin(mismatches, "\n"));
  PyErr_SetString(PyExc_AssertionError, msg.c_str());
  return nullptr;
}

}  // namespace koladata::python
