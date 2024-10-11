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
#include "pybind11_protobuf/native_proto_caster.h"

namespace koladata::python {

void ImportNativeProtoCasters() {
  arolla::python::DCheckPyGIL();
  ::pybind11_protobuf::ImportNativeProtoCasters();
}

absl::StatusOr<std::tuple<absl::Nonnull<const ::google::protobuf::Message*>, std::any>>
UnwrapPyProtoMessage(absl::Nonnull<PyObject*> py_object) {
  arolla::python::DCheckPyGIL();
  // Use a shared_ptr so that we have a copy constructor for std::any.
  auto caster =
      std::make_shared<pybind11::detail::type_caster<::google::protobuf::Message>>();
  if (!caster->load(py_object, false)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "message cast failed, got type %s", Py_TYPE(py_object)->tp_name));
  }
  // Calls type_caster<Message>::operator const Message *().
  const ::google::protobuf::Message* message_ptr =
      static_cast<const ::google::protobuf::Message*>(*caster);
  if (message_ptr == nullptr) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "message cast failed, got type %s", Py_TYPE(py_object)->tp_name));
  }
  return std::make_tuple(message_ptr, std::move(caster));
}

}  // namespace koladata::python
