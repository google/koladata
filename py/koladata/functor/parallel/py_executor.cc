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
#include "py/koladata/functor/parallel/py_executor.h"

#include <Python.h>

#include "absl/cleanup/cleanup.h"
#include "koladata/functor/parallel/executor.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {

using ::arolla::GetQType;
using ::arolla::python::AcquirePyGIL;
using ::arolla::python::CheckPyGIL;
using ::arolla::python::DCheckPyGIL;
using ::arolla::python::PyObjectGILSafePtr;
using ::arolla::python::PyObjectPtr;
using ::arolla::python::PyQValueType;
using ::arolla::python::RegisterPyQValueSpecializationByQType;
using ::arolla::python::SetPyErrFromStatus;
using ::arolla::python::UnsafeUnwrapPyQValue;
using ::koladata::functor::parallel::ExecutorPtr;

PyObject* PyExecutor_schedule(PyObject* self, PyObject* py_arg) {
  DCheckPyGIL();
  auto executor = UnsafeUnwrapPyQValue(self).UnsafeAs<ExecutorPtr>();
  if (executor == nullptr) {
    PyErr_SetString(PyExc_ValueError, "Executor is not initialized");
    return nullptr;
  }
  if (!PyCallable_Check(py_arg)) {
    return PyErr_Format(PyExc_TypeError, "expected a callable, got %s",
                        Py_TYPE(py_arg)->tp_name);
  }
  RETURN_IF_ERROR(executor->Schedule([py_callable =
                                          PyObjectGILSafePtr::NewRef(py_arg)] {
    AcquirePyGIL guard;
    auto py_result = PyObjectPtr::Own(PyObject_CallNoArgs(py_callable.get()));
    if (py_result == nullptr) {
      static PyObject* py_context =
          PyUnicode_InternFromString("koladata.functor.parallel.Executor._run");
      PyErr_WriteUnraisable(py_context);
    }
  })).With(SetPyErrFromStatus);
  Py_RETURN_NONE;
}

PyMethodDef kPyExecutor_methods[] = {
    {
        "schedule",
        PyExecutor_schedule,
        METH_O,
        "schedule(fn, /)\n"
        "--\n\n"
        "Schedules a task to be executed by the executor.",
    },
    {nullptr}, /* sentinel */
};

// Creates and initializes PyTypeObject for PyExecutor class.
PyTypeObject* InitPyExecutorType() {
  CheckPyGIL();
  PyTypeObject* py_qvalue_type = PyQValueType();
  if (py_qvalue_type == nullptr) {
    return nullptr;
  }
  absl::Cleanup py_qvalue_type_cleanup = [&] { Py_DECREF(py_qvalue_type); };
  PyType_Slot slots[] = {
      {Py_tp_base, py_qvalue_type},
      {Py_tp_methods, kPyExecutor_methods},
      {0, nullptr},
  };
  PyType_Spec spec = {
      .name = "koladata.functor.parallel.Executor",
      .flags = Py_TPFLAGS_DEFAULT,
      .slots = slots,
  };
  auto py_result = PyObjectPtr::Own(PyType_FromSpec(&spec));
  if (py_result == nullptr) {
    return nullptr;
  }
  if (!RegisterPyQValueSpecializationByQType(GetQType<ExecutorPtr>(),
                                             py_result.get())) {
    return nullptr;
  }
  return reinterpret_cast<PyTypeObject*>(py_result.release());
}

}  // namespace

PyTypeObject* PyExecutorType() {
  CheckPyGIL();
  static PyTypeObject* py_result = InitPyExecutorType();
  Py_XINCREF(py_result);
  return py_result;
}

}  // namespace koladata::python
