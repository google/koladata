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
#include <Python.h>

#include <string>

#include "arolla/serialization_base/base.pb.h"
#include "koladata/s11n/codec.pb.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/records/record_reader.h"
#include "riegeli/records/record_writer.h"

namespace koladata::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(serving_impl_clib, m) {
  m.def("cleanup_bag_id", [](const std::string& data) -> py::bytes {
    std::string result;
    riegeli::RecordWriter writer((riegeli::StringWriter(&result)));
    riegeli::RecordReader reader((riegeli::StringReader(data)));
    arolla::serialization_base::DecodingStepProto step;
    while (true) {
      if (!reader.ReadRecord(step)) {
        throw pybind11::value_error("DecodingStepProto expected");
      }
      if (step.has_value() &&
          step.value().HasExtension(s11n::KodaV1Proto::extension)) {
        s11n::KodaV1Proto& koda_proto = *step.mutable_value()->MutableExtension(
            s11n::KodaV1Proto::extension);
        if (koda_proto.has_data_bag_value()) {
          koda_proto.mutable_data_bag_value()->clear_bag_id_hi();
          koda_proto.mutable_data_bag_value()->clear_bag_id_lo();
        }
      }
      writer.WriteRecord(step);
      if (step.type_case() ==
          arolla::serialization_base::DecodingStepProto::TYPE_NOT_SET) {
        break;
      }
    }
    if (!writer.Close()) {
      throw pybind11::value_error("Riegeli write error");
    }
    return py::bytes(result);
  });
}

}  // namespace
}  // namespace koladata::python
