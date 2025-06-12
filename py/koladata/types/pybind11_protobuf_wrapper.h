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
// This module allows us to convert python proto messages to and from C++ proto
// messages using pybind11_protobuf without using pybind11 directly. This header
// does not refer to any pybind11 or pybind11_protobuf types, because pybind11
// uses C++ exceptions, so the cc file needs to be compiled with -fexceptions,
// so the header needs to be ODR-safe for both -fexceptions and -fno-exceptions.

#ifndef THIRD_PARTY_PY_KOLADATA_TYPES_PYBIND11_PROTOBUF_WRAPPER_H_
#define THIRD_PARTY_PY_KOLADATA_TYPES_PYBIND11_PROTOBUF_WRAPPER_H_

#include <Python.h>

#include <any>
#include <memory>
#include <tuple>

#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "google/protobuf/message.h"
#include "py/arolla/py_utils/py_utils.h"

namespace koladata::python {

// Calls ::pybind11_protobuf::ImportNativeProtoCasters().
//
// This must be called before any other functions in this header, and while
// holding the GIL. Normally it is called just before initializing a module
// that contains code using this header.
void ImportNativeProtoCasters();

// Converts a python proto message to a C++ proto message pointer and an opaque
// object that must outlive that message pointer. The input python proto must
// also outlive the message pointer.
//
// Requires the calling thread to be holding the GIL, and to continue holding
// the GIL until the opaque object (and all copies, if any have been made) is
// deleted.
absl::StatusOr<std::tuple<const ::google::protobuf::Message* absl_nonnull, std::any>>
UnwrapPyProtoMessage(PyObject* absl_nonnull py_object);

// Returns true if the given python object is a fast C++ proto.
bool IsFastCppPyProtoMessage(PyObject* absl_nonnull py_object);

// Converts a C++ proto message to a python proto message.
//
// `py_message_class` must be a reference to the python message class we are
// converting to. Depending on the python proto backend, this may be used to
// construct the returned message object. If `using_fast_cpp_proto` is true,
// the python proto backend must be the C++ proto backend; it is always safe
// to set it to false.
//
// Requires the calling thread to be holding the GIL. Returns nullptr and sets
// a python error on error.
arolla::python::PyObjectPtr WrapProtoMessage(
    std::unique_ptr<::google::protobuf::Message> message,
    PyObject* py_message_class,  // Borrowed.
    bool using_fast_cpp_proto = false);

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_TYPES_PYBIND11_PROTOBUF_WRAPPER_H_
