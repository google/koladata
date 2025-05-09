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
#include "py/koladata/functor/parallel/py_stream.h"

#include <Python.h>

#include <array>
#include <cstddef>
#include <limits>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_qtype.h"
#include "py/arolla/abc/py_qtype.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {

using ::arolla::CancellationContext;
using ::arolla::Cancelled;
using ::arolla::CheckCancellation;
using ::arolla::CurrentCancellationContext;
using ::arolla::GetQType;
using ::arolla::QTypePtr;
using ::arolla::TypedValue;
using ::arolla::python::AcquirePyGIL;
using ::arolla::python::DCheckPyGIL;
using ::arolla::python::IsPyQValueInstance;
using ::arolla::python::PyCancellationScope;
using ::arolla::python::PyErr_RestoreRaisedException;
using ::arolla::python::PyObjectGILSafePtr;
using ::arolla::python::PyObjectPtr;
using ::arolla::python::PyQValueType;
using ::arolla::python::SetPyErrFromStatus;
using ::arolla::python::StatusWithRawPyErr;
using ::arolla::python::UnsafeUnwrapPyQValue;
using ::arolla::python::UnwrapPyQType;
using ::arolla::python::WrapAsPyQValue;
using ::koladata::functor::parallel::ExecutorPtr;
using ::koladata::functor::parallel::IsStreamQType;
using ::koladata::functor::parallel::MakeStream;
using ::koladata::functor::parallel::MakeStreamQValue;
using ::koladata::functor::parallel::StreamPtr;
using ::koladata::functor::parallel::StreamReader;
using ::koladata::functor::parallel::StreamReaderPtr;
using ::koladata::functor::parallel::StreamWriterPtr;

// Forward declare.
extern PyTypeObject PyStreamReader_Type;
extern PyTypeObject PyStreamWriter_Type;

struct PyStreamWriterObject final {
  PyObject_HEAD;
  struct Fields {
    const QTypePtr value_qtype;
    StreamWriterPtr stream_writer;
    bool closed = false;
  };
  Fields fields;
  PyObject* weakrefs;
};

struct PyStreamReaderObject final {
  PyObject_HEAD;
  struct Fields {
    StreamReaderPtr stream_reader;
  };
  Fields fields;
  PyObject* weakrefs;
};

PyStreamWriterObject::Fields& PyStreamWriter_fields(PyObject* self) {
  DCHECK(Py_TYPE(self) == &PyStreamWriter_Type);
  return reinterpret_cast<PyStreamWriterObject*>(self)->fields;
}

PyStreamReaderObject::Fields& PyStreamReader_fields(PyObject* self) {
  DCHECK(Py_TYPE(self) == &PyStreamReader_Type);
  return reinterpret_cast<PyStreamReaderObject*>(self)->fields;
}

void PyStreamWriter_dealloc(PyObject* self) {
  PyObject_ClearWeakRefs(self);
  PyStreamWriter_fields(self).~Fields();
  Py_TYPE(self)->tp_free(self);
}

void PyStreamReader_dealloc(PyObject* self) {
  PyObject_ClearWeakRefs(self);
  PyStreamReader_fields(self).~Fields();
  Py_TYPE(self)->tp_free(self);
}

PyObject* PyStreamWriter_new(QTypePtr value_qtype,
                             StreamWriterPtr stream_writer) {
  DCheckPyGIL();
  DCHECK(value_qtype != nullptr);
  if (PyType_Ready(&PyStreamWriter_Type) < 0) {
    return nullptr;
  }
  PyObject* self = PyStreamWriter_Type.tp_alloc(&PyStreamWriter_Type, 0);
  if (self == nullptr) {
    return nullptr;
  }
  auto& fields = PyStreamWriter_fields(self);
  new (&fields) PyStreamWriterObject::Fields{std::move(value_qtype),
                                             std::move(stream_writer)};
  return self;
}

PyObject* PyStreamReader_new(StreamReaderPtr stream_reader) {
  DCheckPyGIL();
  if (PyType_Ready(&PyStreamReader_Type) < 0) {
    return nullptr;
  }
  PyObject* self = PyStreamReader_Type.tp_alloc(&PyStreamReader_Type, 0);
  if (self == nullptr) {
    return nullptr;
  }
  auto& fields = PyStreamReader_fields(self);
  new (&fields) PyStreamReaderObject::Fields{std::move(stream_reader)};
  return self;
}

PyObject* PyStream_make_reader(PyObject* self, PyObject* /*py_args*/) {
  DCheckPyGIL();
  auto& qvalue = UnsafeUnwrapPyQValue(self);
  if (!IsStreamQType(qvalue.GetType())) {
    PyErr_SetString(
        PyExc_RuntimeError,
        absl::StrFormat("unexpected self.qtype: expected a stream, got %s",
                        qvalue.GetType()->name())
            .c_str());
    return nullptr;
  }
  const auto& stream = qvalue.UnsafeAs<StreamPtr>();
  if (stream == nullptr) {
    PyErr_SetString(PyExc_RuntimeError, "Stream is not initialized");
    return nullptr;
  }
  return PyStreamReader_new(stream->MakeReader());
}

PyObject* PyStreamWriter_orphaned(PyObject* self, PyObject* /*py_arg*/) {
  DCheckPyGIL();
  return PyBool_FromLong(PyStreamWriter_fields(self).stream_writer->Orphaned());
}

PyObject* PyStreamWriter_write(PyObject* self, PyObject* py_arg) {
  DCheckPyGIL();
  auto& fields = PyStreamWriter_fields(self);
  if (!IsPyQValueInstance(py_arg)) {
    return PyErr_Format(PyExc_TypeError, "expected a qvalue, got %s",
                        Py_TYPE(py_arg)->tp_name);
  }
  const auto& qvalue = UnsafeUnwrapPyQValue(py_arg);
  if (qvalue.GetType() != fields.value_qtype) {
    PyErr_SetString(
        PyExc_ValueError,
        absl::StrFormat("expected a value of type %s, got %s",
                        fields.value_qtype->name(), qvalue.GetType()->name())
            .c_str());
    return nullptr;
  }
  if (fields.closed) {
    PyErr_SetString(PyExc_RuntimeError, "stream is already closed");
    return nullptr;
  }
  fields.stream_writer->Write(qvalue.AsRef());
  Py_RETURN_NONE;
}

PyObject* PyStreamWriter_close(PyObject* self, PyObject* py_tuple_args,
                               PyObject* py_dict_kwargs) {
  auto& fields = PyStreamWriter_fields(self);
  constexpr std::array<const char*, 2> kwlist = {"exception", nullptr};
  PyObject* py_exception = Py_None;
  if (!PyArg_ParseTupleAndKeywords(py_tuple_args, py_dict_kwargs,
                                   "|O:StreamWriter.close",
                                   (char**)kwlist.data(), &py_exception)) {
    return nullptr;
  }
  if (py_exception != Py_None &&
      !PyExceptionClass_Check(Py_TYPE(py_exception))) {
    return PyErr_Format(PyExc_TypeError, "expected an exception, got %s",
                        Py_TYPE(py_exception)->tp_name);
  }
  if (fields.closed) {
    PyErr_SetString(PyExc_RuntimeError, "stream is already closed");
    return nullptr;
  }
  absl::Status close_status;
  if (py_exception != Py_None) {
    PyErr_RestoreRaisedException(PyObjectPtr::NewRef(py_exception));
    close_status =
        StatusWithRawPyErr(absl::StatusCode::kInvalidArgument,
                           absl::StrFormat("Python exception: %s",
                                           Py_TYPE(py_exception)->tp_name));
  }
  std::move(*fields.stream_writer).Close(std::move(close_status));
  fields.closed = true;
  Py_RETURN_NONE;
}

PyObject* PyStreamReader_read_available(PyObject* self, PyObject* py_tuple_args,
                                        PyObject* py_dict_kwargs) {
  DCheckPyGIL();
  PyCancellationScope cancellation_scope;
  auto& fields = PyStreamReader_fields(self);
  constexpr std::array<const char*, 2> kwlist = {"limit", nullptr};
  PyObject* py_limit = Py_None;
  if (!PyArg_ParseTupleAndKeywords(py_tuple_args, py_dict_kwargs,
                                   "|O:StreamReader.read_available",
                                   (char**)kwlist.data(), &py_limit)) {
    return nullptr;
  }
  size_t limit = std::numeric_limits<size_t>::max();
  if (py_limit != Py_None) {
    limit = PyLong_AsSize_t(py_limit);
    if (PyErr_Occurred()) {
      return nullptr;
    }
    if (limit == 0) {
      PyErr_SetString(PyExc_ValueError, "`limit` needs to be positive");
      return nullptr;
    }
  }
  std::vector<PyObjectPtr> py_items;
  StreamReader::TryReadResult try_read_result;
  while (py_items.size() < limit) {
    RETURN_IF_ERROR(CheckCancellation()).With(SetPyErrFromStatus);
    try_read_result = fields.stream_reader->TryRead();
    if (auto* item = try_read_result.item()) {
      auto py_item = PyObjectPtr::Own(WrapAsPyQValue(TypedValue(*item)));
      if (py_item == nullptr) {
        return nullptr;
      }
      py_items.push_back(std::move(py_item));
    } else {
      break;
    }
  }
  if (py_items.empty()) {
    if (auto* status = try_read_result.close_status()) {
      if (status->ok()) {
        Py_RETURN_NONE;
      }
      return SetPyErrFromStatus(*status);
    }
  }
  RETURN_IF_ERROR(CheckCancellation()).With(SetPyErrFromStatus);
  auto py_result = PyObjectPtr::Own(PyList_New(py_items.size()));
  if (py_result == nullptr) {
    return nullptr;
  }
  for (size_t i = 0; i < py_items.size(); ++i) {
    PyList_SET_ITEM(py_result.get(), i, py_items[i].release());
  }
  return py_result.release();
}

PyObject* PyStreamReader_subscribe_once(PyObject* self, PyObject* py_tuple_args,
                                        PyObject* py_dict_kwargs) {
  DCheckPyGIL();
  PyCancellationScope cancellation_scope;
  auto& fields = PyStreamReader_fields(self);
  constexpr std::array<const char*, 3> kwlist = {"executor", "callback",
                                                 nullptr};
  PyObject* py_executor = nullptr;
  PyObject* py_callback = nullptr;
  if (!PyArg_ParseTupleAndKeywords(
          py_tuple_args, py_dict_kwargs, "OO:StreamReader.subscribe_once",
          (char**)kwlist.data(), &py_executor, &py_callback)) {
    return nullptr;
  }
  if (!IsPyQValueInstance(py_executor)) {
    return PyErr_Format(PyExc_TypeError, "expected an executor, got %s",
                        Py_TYPE(py_executor)->tp_name);
  }
  const auto& qvalue_executor = UnsafeUnwrapPyQValue(py_executor);
  if (qvalue_executor.GetType() != GetQType<ExecutorPtr>()) {
    return PyErr_Format(PyExc_TypeError, "expected an executor, got %s",
                        Py_TYPE(py_executor)->tp_name);
  }
  auto executor = qvalue_executor.UnsafeAs<ExecutorPtr>();
  if (executor == nullptr) {
    PyErr_SetString(PyExc_RuntimeError, "Executor is not initialized");
    return nullptr;
  }
  if (!PyCallable_Check(py_callback)) {
    return PyErr_Format(PyExc_TypeError, "expected a callable, got %s",
                        Py_TYPE(py_callback)->tp_name);
  }
  RETURN_IF_ERROR(CheckCancellation()).With(SetPyErrFromStatus);
  // As a potential optimization, we could try detecting if data is already
  // available and immediately executing the callback without using
  // the executor.
  auto callback = [executor = std::move(executor),
                   cancellation_context = CurrentCancellationContext(),
                   py_callable =
                       PyObjectGILSafePtr::NewRef(py_callback)]() mutable {
    if (cancellation_context != nullptr && cancellation_context->Cancelled()) {
      return;
    }
    executor->Schedule([cancellation_context = std::move(cancellation_context),
                        py_callable = std::move(py_callable)]() mutable {
      CancellationContext::ScopeGuard cancellation_scope(
          std::move(cancellation_context));
      if (Cancelled()) {
        return;
      }
      AcquirePyGIL guard;
      auto py_result = PyObjectPtr::Own(PyObject_CallNoArgs(py_callable.get()));
      if (py_result == nullptr) {
        static PyObject* py_context = PyUnicode_InternFromString(
            "StreamReader._run_callback_on_executor");
        PyErr_WriteUnraisable(py_context);
      }
    });
  };
  fields.stream_reader->SubscribeOnce(std::move(callback));
  Py_RETURN_NONE;
}

PyMethodDef kPyStream_methods[] = {
    {
        "make_reader",
        &PyStream_make_reader,
        METH_NOARGS,
        ("make_reader()\n"
         "--\n\n"
         "Returns a new reader for the stream."),
    },
    {nullptr} /* sentinel */
};

PyMethodDef kPyStreamWriter_methods[] = {
    {
        "orphaned",
        &PyStreamWriter_orphaned,
        METH_NOARGS,
        ("orphaned()\n"
         "--\n\n"
         "Returns true if the stream no longer has any readers."),
    },
    {
        "write",
        &PyStreamWriter_write,
        METH_O,
        ("write(item, /)\n"
         "--\n\n"
         "Write an item to the stream."),
    },
    {
        "close",
        reinterpret_cast<PyCFunction>(&PyStreamWriter_close),
        METH_VARARGS | METH_KEYWORDS,
        ("close(error=None)\n"
         "--\n\n"
         "Closes the stream."),
    },
    {nullptr} /* sentinel */
};

PyMethodDef kPyStreamReader_methods[] = {
    {
        "read_available",
        reinterpret_cast<PyCFunction>(&PyStreamReader_read_available),
        METH_VARARGS | METH_KEYWORDS,
        ("read_available(limit=None)\n"
         "--\n\n"
         "Returns the available items from the stream.\n\n"
         "Returns an empty list if no more data is currently available and\n"
         "the stream is still open.\n\n"
         "Returns `None` if the stream has been exhausted and closed without\n"
         "an error; otherwise, raises the error passed during closing.\n\n"
         "Args:\n"
         "  limit: The maximum number of items to return.\n"),
    },
    {
        "subscribe_once",
        reinterpret_cast<PyCFunction>(&PyStreamReader_subscribe_once),
        METH_VARARGS | METH_KEYWORDS,
        ("subscribe_once(executor, callback)\n"
         "--\n\n"
         "Subscribes for a notification when new items are available or when "
         "the stream is closed.\n\n"
         "Note: The `callback` will be invoked on the executor and is called\n"
         "without any arguments.\n\n"
         "When the `callback` is invoked, the subsequent `read_available()`\n"
         "call is guaranteed to return a non-trivial result:\n"
         " * a non-empty list if there are more items available,\n"
         " * `None` if the stream was closed without an error, or\n"
         " * raise an error if the stream was closed with an error."),
    },
    {nullptr} /* sentinel */
};

PyTypeObject PyStream_Type = {
    .ob_base = {PyObject_HEAD_INIT(nullptr)},
    .tp_name = "koladata.functor.parallel.Stream",
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .tp_doc = ("Stream of values.\n\nThe stream keeps all the values in "
               "memory, and can be read more than once."),
    .tp_methods = kPyStream_methods,
};

PyTypeObject PyStreamWriter_Type = {
    .ob_base = {PyObject_HEAD_INIT(nullptr)},
    .tp_name = "koladata.functor.parallel.StreamWriter",
    .tp_basicsize = sizeof(PyStreamWriterObject),
    .tp_dealloc = PyStreamWriter_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .tp_doc =
        ("Stream writer.\n\n"
         "Note: It is strongly advised that all streams be explicitly closed."),
    .tp_weaklistoffset = offsetof(PyStreamWriterObject, weakrefs),
    .tp_methods = kPyStreamWriter_methods,
};

PyTypeObject PyStreamReader_Type = {
    .ob_base = {PyObject_HEAD_INIT(nullptr)},
    .tp_name = "koladata.functor.parallel.StreamReader",
    .tp_basicsize = sizeof(PyStreamReaderObject),
    .tp_dealloc = PyStreamReader_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .tp_doc = ("Stream reader."),
    .tp_weaklistoffset = offsetof(PyStreamReaderObject, weakrefs),
    .tp_methods = kPyStreamReader_methods,
};

PyObject* PyMakeStream(PyObject* /*module*/, PyObject* py_arg) {
  auto* value_qtype = UnwrapPyQType(py_arg);
  if (value_qtype == nullptr) {
    return nullptr;
  }
  auto [stream, stream_writer] = MakeStream(value_qtype);
  auto py_stream =
      PyObjectPtr::Own(WrapAsPyQValue(MakeStreamQValue(std::move(stream))));
  if (py_stream == nullptr) {
    return nullptr;
  }
  auto py_stream_writer = PyObjectPtr::Own(
      PyStreamWriter_new(value_qtype, std::move(stream_writer)));
  if (py_stream_writer == nullptr) {
    return nullptr;
  }
  auto py_result = PyObjectPtr::Own(PyTuple_New(2));
  if (py_result == nullptr) {
    return nullptr;
  }
  PyTuple_SET_ITEM(py_result.get(), 0, py_stream.release());
  PyTuple_SET_ITEM(py_result.get(), 1, py_stream_writer.release());
  return py_result.release();
}

}  // namespace

PyTypeObject* PyStreamType() {
  DCheckPyGIL();
  if (!PyType_HasFeature(&PyStream_Type, Py_TPFLAGS_READY)) {
    PyStream_Type.tp_base = PyQValueType();
    if (PyStream_Type.tp_base == nullptr) {
      return nullptr;
    }
    if (PyType_Ready(&PyStream_Type) < 0) {
      return nullptr;
    }
  }
  Py_INCREF(&PyStream_Type);
  return &PyStream_Type;
}

PyTypeObject* PyStreamWriterType() {
  DCheckPyGIL();
  if (PyType_Ready(&PyStreamWriter_Type) < 0) {
    return nullptr;
  }
  Py_INCREF(&PyStreamWriter_Type);
  return &PyStreamWriter_Type;
}

PyTypeObject* PyStreamReaderType() {
  DCheckPyGIL();
  if (PyType_Ready(&PyStreamReader_Type) < 0) {
    return nullptr;
  }
  Py_XINCREF(&PyStreamReader_Type);
  return &PyStreamReader_Type;
}

const PyMethodDef kDefPyMakeStream = {
    "make_stream",
    &PyMakeStream,
    METH_O,
    ("make_stream(value_qtype, /)\n"
     "--\n\n"
     "Returns a tuple of Stream and StreamWriter objects."),
};

}  // namespace koladata::python
