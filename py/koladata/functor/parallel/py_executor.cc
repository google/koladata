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

#include <utility>

#include "absl/strings/str_format.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/cancellation.h"
#include "koladata/functor/parallel/executor.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {

using ::arolla::CancellationContext;
using ::arolla::CheckCancellation;
using ::arolla::CurrentCancellationContext;
using ::arolla::GetQType;
using ::arolla::python::AcquirePyGIL;
using ::arolla::python::DCheckPyGIL;
using ::arolla::python::PyCancellationScope;
using ::arolla::python::PyObjectGILSafePtr;
using ::arolla::python::PyObjectPtr;
using ::arolla::python::PyQValueType;
using ::arolla::python::SetPyErrFromStatus;
using ::arolla::python::UnsafeUnwrapPyQValue;
using ::koladata::functor::parallel::ExecutorPtr;

PyObject* PyExecutor_schedule(PyObject* self, PyObject* py_arg) {
  DCheckPyGIL();
  PyCancellationScope cancellation_scope;
  auto& qvalue = UnsafeUnwrapPyQValue(self);
  if (qvalue.GetType() != GetQType<ExecutorPtr>()) {
    PyErr_SetString(
        PyExc_RuntimeError,
        absl::StrFormat("unexpected self.qtype: expected an executor, got %s",
                        qvalue.GetType()->name())
            .c_str());
    return nullptr;
  }
  auto executor = qvalue.UnsafeAs<ExecutorPtr>();
  if (executor == nullptr) {
    PyErr_SetString(PyExc_ValueError, "Executor is not initialized");
    return nullptr;
  }
  if (!PyCallable_Check(py_arg)) {
    return PyErr_Format(PyExc_TypeError, "expected a callable, got %s",
                        Py_TYPE(py_arg)->tp_name);
  }
  RETURN_IF_ERROR(CheckCancellation()).With(SetPyErrFromStatus);
  executor->Schedule([cancellation_context = CurrentCancellationContext(),
                      py_callable =
                          PyObjectGILSafePtr::NewRef(py_arg)]() mutable {
    CancellationContext::ScopeGuard cancellation_scope(
        std::move(cancellation_context));
    AcquirePyGIL guard;
    auto py_result = PyObjectPtr::Own(PyObject_CallNoArgs(py_callable.get()));
    if (py_result == nullptr) {
      static PyObject* py_context =
          PyUnicode_InternFromString("koladata.functor.parallel.Executor._run");
      PyErr_WriteUnraisable(py_context);
    }
  });
  Py_RETURN_NONE;
}

PyMethodDef kPyExecutor_methods[] = {
    {
        "schedule",
        PyExecutor_schedule,
        METH_O,
        ("schedule(task_fn, /)\n"
         "--\n\n"
         "Schedules a task to be executed by the executor.\n"
         "Note: The task inherits the current cancellation context."),
    },
    {nullptr}, /* sentinel */
};

PyTypeObject PyExecutor_Type = {
    .ob_base = {PyObject_HEAD_INIT(nullptr)},
    .tp_name = "koladata.functor.parallel.Executor",
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .tp_methods = kPyExecutor_methods,
};

}  // namespace

PyTypeObject* PyExecutorType() {
  DCheckPyGIL();
  if (!PyType_HasFeature(&PyExecutor_Type, Py_TPFLAGS_READY)) {
    PyExecutor_Type.tp_base = PyQValueType();
    if (PyExecutor_Type.tp_base == nullptr) {
      return nullptr;
    }
    if (PyType_Ready(&PyExecutor_Type) < 0) {
      return nullptr;
    }
  }
  Py_INCREF(&PyExecutor_Type);
  return &PyExecutor_Type;
}

}  // namespace koladata::python
