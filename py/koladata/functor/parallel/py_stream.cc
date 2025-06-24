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
#include "py/koladata/functor/parallel/py_stream.h"

#include <Python.h>

#include <array>
#include <cstddef>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/permanent_event.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_qtype.h"
#include "py/arolla/abc/py_qtype.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/py_args.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {

using ::arolla::CancellationContext;
using ::arolla::Cancelled;
using ::arolla::CheckCancellation;
using ::arolla::CurrentCancellationContext;
using ::arolla::GetQType;
using ::arolla::PermanentEvent;
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
using ::arolla::python::ReleasePyGIL;
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
extern PyTypeObject PyStreamYieldAll_Type;
PyObject* PyStreamWriter_new(QTypePtr value_qtype,
                             StreamWriterPtr stream_writer);

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

struct PyStreamYieldAllObject final {
  PyObject_HEAD;
  struct Fields {
    StreamReaderPtr stream_reader;
    absl::Time deadline;
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

PyStreamYieldAllObject::Fields& PyStreamYieldAll_fields(PyObject* self) {
  DCHECK(Py_TYPE(self) == &PyStreamYieldAll_Type);
  return reinterpret_cast<PyStreamYieldAllObject*>(self)->fields;
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

void PyStreamYieldAll_dealloc(PyObject* self) {
  PyObject_ClearWeakRefs(self);
  PyStreamYieldAll_fields(self).~Fields();
  Py_TYPE(self)->tp_free(self);
}

PyObject* PyStreamNew(PyObject* /*py_cls*/, PyObject* py_arg) {
  auto* value_qtype = UnwrapPyQType(py_arg);
  if (value_qtype == nullptr) {
    PyErr_Clear();
    return PyErr_Format(PyExc_TypeError,
                        "Stream.new() expected a QType, got %s",
                        Py_TYPE(py_arg)->tp_name);
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

PyObject* PyStreamYieldAll_new(StreamReaderPtr stream_reader,
                               absl::Time deadline) {
  DCheckPyGIL();
  if (PyType_Ready(&PyStreamYieldAll_Type) < 0) {
    return nullptr;
  }
  PyObject* self = PyStreamYieldAll_Type.tp_alloc(&PyStreamYieldAll_Type, 0);
  if (self == nullptr) {
    return nullptr;
  }
  auto& fields = PyStreamYieldAll_fields(self);
  new (&fields) PyStreamYieldAllObject::Fields{std::move(stream_reader),
                                               std::move(deadline)};
  return self;
}

PyObject* PyStreamYieldAll_iternext(PyObject* self) {
  DCheckPyGIL();
  PyCancellationScope cancellation_scope;
  auto& fields = PyStreamYieldAll_fields(self);
  auto try_read_result = fields.stream_reader->TryRead();
  if (try_read_result.empty()) {
    ReleasePyGIL guard;
    auto cancellation_context = CurrentCancellationContext();
    while (try_read_result.empty() && !Cancelled()) {
      auto event = PermanentEvent::Make();
      CancellationContext::Subscription cancellation_subscription;
      if (cancellation_context != nullptr) {
        cancellation_subscription =
            cancellation_context->Subscribe([event] { event->Notify(); });
      }
      fields.stream_reader->SubscribeOnce([event] { event->Notify(); });
      if (!event->WaitWithDeadline(fields.deadline)) {
        break;
      }
      try_read_result = fields.stream_reader->TryRead();
    }
  }
  if (auto* item = try_read_result.item()) {
    return WrapAsPyQValue(TypedValue(*item));
  }
  if (auto* status = try_read_result.close_status()) {
    if (status->ok()) {
      return nullptr;
    }
    return SetPyErrFromStatus(*status);
  }
  RETURN_IF_ERROR(CheckCancellation()).With(SetPyErrFromStatus);
  PyErr_SetObject(PyExc_TimeoutError, nullptr);
  return nullptr;
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

PyObject* PyStream_read_all(PyObject* self, PyObject** py_args,
                            Py_ssize_t nargs, PyObject* py_tuple_kwnames) {
  DCheckPyGIL();
  PyCancellationScope cancellation_scope;
  auto cancellation_context = CurrentCancellationContext();
  // Check that self is a valid stream.
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
  // Parse arguments.
  static const absl::NoDestructor parser(FastcallArgParser(
      /*pos_only_n=*/0, /*parse_kwargs=*/false, /*kw_only_arg_names=*/
      {"timeout"}));
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_tuple_kwnames, args)) {
    return nullptr;
  }
  double timeout_seconds = std::numeric_limits<double>::infinity();
  PyObject* py_timeout_seconds = args.kw_only_args["timeout"];
  if (py_timeout_seconds == nullptr) {
    PyErr_SetString(PyExc_TypeError,
                    "Stream.read_all() missing 1 required keyword-only "
                    "argument: 'timeout'");
    return nullptr;
  }
  if (py_timeout_seconds != Py_None) {
    timeout_seconds = PyFloat_AsDouble(py_timeout_seconds);
    if (PyErr_Occurred()) {
      PyErr_Clear();
      return PyErr_Format(
          PyExc_TypeError,
          "Stream.read_all() 'timeout' must specify a non-negative "
          "number of seconds (or be None), got: %R",
          py_timeout_seconds);
    }
  }
  if (timeout_seconds < 0.0) {
    PyErr_SetString(PyExc_ValueError,
                    "Stream.read_all() 'timeout' cannot be negative");
    return nullptr;
  }
  // Scan through the stream to ensure it's finalized before the timeout and
  // determine its size. This should be relatively cheap, since the stream
  // owns all data and we don't create item copies during the scanning.
  size_t size = 0;
  {
    const absl::Time deadline = absl::Now() + absl::Seconds(timeout_seconds);
    bool timeout = false;
    absl::Status close_status;
    StreamReaderPtr reader = stream->MakeReader();
    for (ReleasePyGIL guard; !Cancelled();) {
      auto try_read_result = reader->TryRead();
      // Count available items.
      while (!Cancelled() && try_read_result.item() != nullptr) {
        size += 1;
        try_read_result = reader->TryRead();
      }
      if (auto* status = try_read_result.close_status()) {
        close_status = std::move(*status);
        break;
      }
      // Wait for more data.
      auto event = PermanentEvent::Make();
      CancellationContext::Subscription cancellation_subscription;
      if (cancellation_context != nullptr) {
        cancellation_subscription =
            cancellation_context->Subscribe([event] { event->Notify(); });
      }
      reader->SubscribeOnce([event] { event->Notify(); });
      if (!event->WaitWithDeadline(deadline)) {
        timeout = true;
        break;
      }
    }
    RETURN_IF_ERROR(CheckCancellation()).With(SetPyErrFromStatus);
    if (timeout) {
      PyErr_SetObject(PyExc_TimeoutError, nullptr);
      return nullptr;
    }
    if (!close_status.ok()) {
      return SetPyErrFromStatus(close_status);
    }
  }
  // Build the result.
  auto py_result = PyObjectPtr::Own(PyList_New(size));
  if (py_result == nullptr) {
    return nullptr;
  }
  auto reader = stream->MakeReader();
  for (size_t i = 0; i < size; ++i) {
    RETURN_IF_ERROR(CheckCancellation()).With(SetPyErrFromStatus);
    auto try_read_result = reader->TryRead();
    if (auto* item = try_read_result.item()) {
      if (auto* py_item = WrapAsPyQValue(TypedValue(*item))) {
        PyList_SET_ITEM(py_result.get(), i, py_item);
      } else {
        return nullptr;
      }
    } else {
      PyErr_SetString(PyExc_AssertionError, "stream returns inconsistent data");
      return nullptr;
    }
  }
  return py_result.release();
}

PyObject* PyStream_yield_all(PyObject* self, PyObject** py_args,
                             Py_ssize_t nargs, PyObject* py_tuple_kwnames) {
  DCheckPyGIL();
  // Check that self is a valid stream.
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
  // Parse arguments.
  static const absl::NoDestructor parser(FastcallArgParser(
      /*pos_only_n=*/0, /*parse_kwargs=*/false, /*kw_only_arg_names=*/
      {"timeout"}));
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_tuple_kwnames, args)) {
    return nullptr;
  }
  double timeout_seconds = std::numeric_limits<double>::infinity();
  PyObject* py_timeout_seconds = args.kw_only_args["timeout"];
  if (py_timeout_seconds == nullptr) {
    PyErr_SetString(PyExc_TypeError,
                    "Stream.yield_all() missing 1 required keyword-only "
                    "argument: 'timeout'");
    return nullptr;
  }
  if (py_timeout_seconds != Py_None) {
    timeout_seconds = PyFloat_AsDouble(py_timeout_seconds);
    if (PyErr_Occurred()) {
      PyErr_Clear();
      return PyErr_Format(
          PyExc_TypeError,
          "Stream.yield_all() 'timeout' must specify a non-negative "
          "number of seconds (or be None), got: %R",
          py_timeout_seconds);
    }
  }
  if (timeout_seconds < 0.0) {
    PyErr_SetString(PyExc_ValueError,
                    "Stream.yield_all() 'timeout' cannot be negative");
    return nullptr;
  }
  return PyStreamYieldAll_new(stream->MakeReader(),
                              absl::Now() + absl::Seconds(timeout_seconds));
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
    executor->Schedule([cancellation_context = std::move(cancellation_context),
                        py_callable = std::move(py_callable)]() mutable {
      CancellationContext::ScopeGuard cancellation_scope(
          std::move(cancellation_context));
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
        "new",
        &PyStreamNew,
        METH_O | METH_CLASS,
        ("new(value_qtype, /)\n"
         "--\n\n"
         "Returns a new `tuple[Stream, StreamWriter]`."),
    },
    {
        "make_reader",
        &PyStream_make_reader,
        METH_NOARGS,
        ("make_reader()\n"
         "--\n\n"
         "Returns a new reader for the stream."),
    },
    {
        "read_all",
        reinterpret_cast<PyCFunction>(&PyStream_read_all),
        METH_FASTCALL | METH_KEYWORDS,
        ("read_all(*, timeout)\n"
         "--\n\n"
         "Waits until the stream is closed and returns all its items.\n\n"
         "If `timeout` is not `None` and the stream is not closed within\n"
         "the given time, the method raises a `TimeoutError`.\n\n"
         "Args:\n"
         "  timeout: A timeout in seconds; None means wait indefinitely.\n\n"
         "Returns:\n"
         "  A list containing the stream items."),
    },
    {
        "yield_all",
        reinterpret_cast<PyCFunction>(&PyStream_yield_all),
        METH_FASTCALL | METH_KEYWORDS,
        ("yield_all(*, timeout)\n"
         "--\n\n"
         "Returns an iterator for the stream items.\n\n"
         "If `timeout` is not `None` and the stream doesn't close within\n"
         "the given time, the method raises a `TimeoutError`.\n\n"
         "Args:\n"
         "  timeout: A timeout in seconds. `None` means wait indefinitely."),
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
        ("close(exception=None)\n"
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

PyTypeObject PyStreamYieldAll_Type = {
    .ob_base = {PyObject_HEAD_INIT(nullptr)},
    .tp_name = "koladata.functor.parallel.StreamYieldAll",
    .tp_basicsize = sizeof(PyStreamYieldAllObject),
    .tp_dealloc = PyStreamYieldAll_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .tp_doc = ("Iterator over a Stream."),
    .tp_weaklistoffset = offsetof(PyStreamYieldAllObject, weakrefs),
    .tp_iter = Py_NewRef,
    .tp_iternext = PyStreamYieldAll_iternext,
};

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

}  // namespace koladata::python
