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
#include "py/koladata/types/pybind11_protobuf_wrapper.h"

#include <Python.h>

#include <any>
#include <memory>
#include <tuple>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "google/protobuf/message.h"
#include "py/arolla/py_utils/py_utils.h"
#include "pybind11/cast.h"
#include "pybind11/gil.h"
#include "pybind11/pybind11.h"
#include "pybind11/pytypes.h"
#include "pybind11_protobuf/native_proto_caster.h"

namespace koladata::python {

void ImportNativeProtoCasters() {
  arolla::python::DCheckPyGIL();
  ::pybind11_protobuf::ImportNativeProtoCasters();
}

absl::StatusOr<std::tuple<absl::Nonnull<const ::google::protobuf::Message*>, std::any>>
UnwrapPyProtoMessage(absl::Nonnull<PyObject*> py_object) {
  arolla::python::DCheckPyGIL();
  try {
    // Use a shared_ptr so that we have a copy constructor for std::any.
    auto caster =
        std::make_shared<pybind11::detail::type_caster<::google::protobuf::Message>>();
    if (!caster->load(py_object, false)) {
      return absl::InvalidArgumentError(
          absl::StrFormat("message cast from python to C++ failed, got type %s",
                          Py_TYPE(py_object)->tp_name));
    }
    // Calls type_caster<Message>::operator const Message *().
    const ::google::protobuf::Message* message_ptr =
        static_cast<const ::google::protobuf::Message*>(*caster);
    if (message_ptr == nullptr) {
      return absl::InvalidArgumentError(
          absl::StrFormat("message cast from python to C++ failed, got type %s",
                          Py_TYPE(py_object)->tp_name));
    }
    return std::make_tuple(message_ptr, std::move(caster));
  } catch (const pybind11::builtin_exception& e) {
    // These caster methods don't appear to throw exceptions, but we should
    // catch here for safety, since the caller is expected to have exceptions
    // disabled.
    e.set_error();
    return arolla::python::StatusWithRawPyErr(
        absl::StatusCode::kInvalidArgument,
        absl::StrFormat("message cast from python to C++ failed, got type %s",
                        Py_TYPE(py_object)->tp_name));
  }
}

bool IsFastCppPyProtoMessage(absl::Nonnull<PyObject*> py_object) {
  return pybind11_protobuf::PyProtoGetCppMessagePointer(py_object) != nullptr;
}

arolla::python::PyObjectPtr WrapProtoMessage(
    std::unique_ptr<::google::protobuf::Message> message, PyObject* py_message_class,
    bool using_fast_cpp_proto) {
  arolla::python::DCheckPyGIL();

  if (using_fast_cpp_proto) {
    try {
      pybind11::detail::type_caster<::google::protobuf::Message> caster;
      auto py_message = caster.cast(
          message.release(), pybind11::return_value_policy::take_ownership,
          pybind11::handle());
      return arolla::python::PyObjectPtr::Own(py_message.ptr());
    } catch (const pybind11::builtin_exception& e) {
      e.set_error();
      return arolla::python::PyObjectPtr::Own(nullptr);
    }
  }

  // Fall back to serializing and then deserializing via Python method calls.
  // The current pybind11_protobuf caster appears to have issues when returning
  // protos to the UPB backend. We should reexamine this later to simplify this
  // code (the backend must serialize/deserialize anyway, so this implementation
  // probably doesn't have additional overhead).

  auto py_message =
      arolla::python::PyObjectPtr::Own(PyObject_CallNoArgs(py_message_class));
  if (py_message == nullptr) {
    return nullptr;
  }

  auto bytes = message->SerializePartialAsString();
  auto py_bytes = arolla::python::PyObjectPtr::Own(
      PyBytes_FromStringAndSize(bytes.data(), bytes.size()));
  if (py_bytes == nullptr) {
    return nullptr;
  }
  bytes.clear();

  auto py_parse_result = arolla::python::PyObjectPtr::Own(PyObject_CallMethod(
      py_message.get(), "ParseFromString", "O", py_bytes.get()));
  if (py_parse_result == nullptr) {
    return nullptr;
  }

  return py_message;
}

}  // namespace koladata::python
