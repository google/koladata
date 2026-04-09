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
#include "absl/strings/string_view.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/transform_config_registry.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/koladata/functor/parallel/py_executor.h"
#include "py/koladata/functor/parallel/py_stream.h"
#include "pybind11/attr.h"
#include "pybind11/cast.h"
#include "pybind11/pybind11.h"

namespace koladata::python {
namespace {

using ::arolla::GetQType;
using ::arolla::python::pybind11_steal_or_throw;
using ::arolla::python::RegisterPyQValueSpecializationByKey;
using ::arolla::python::RegisterPyQValueSpecializationByQType;
using ::koladata::functor::parallel::ExecutorPtr;

namespace py = pybind11;

PYBIND11_MODULE(clib, m) {
  const auto py_type_executor =
      pybind11_steal_or_throw<py::type>(PyExecutorType());
  const auto py_type_stream = pybind11_steal_or_throw<py::type>(PyStreamType());
  const auto py_type_stream_writer =
      pybind11_steal_or_throw<py::type>(PyStreamWriterType());
  const auto py_type_stream_reader =
      pybind11_steal_or_throw<py::type>(PyStreamReaderType());

  m.add_object("Executor", py_type_executor);
  m.add_object("Stream", py_type_stream);
  m.add_object("StreamWriter", py_type_stream_writer);
  m.add_object("StreamReader", py_type_stream_reader);

  if (!RegisterPyQValueSpecializationByQType(GetQType<ExecutorPtr>(),
                                             py_type_executor.ptr())) {
    throw py::error_already_set();
  }
  if (!RegisterPyQValueSpecializationByKey(
          "::koladata::functor::parallel::StreamQType", py_type_stream.ptr())) {
    throw py::error_already_set();
  }

  m.def(
      "get_default_parallel_transform_config",
      [](bool allow_runtime_transforms) {
        return arolla::TypedValue::FromValue(
            arolla::python::pybind11_unstatus_or(
                koladata::functor::parallel::GetDefaultParallelTransformConfig(
                    allow_runtime_transforms)));
      });
}

}  // namespace
}  // namespace koladata::python
