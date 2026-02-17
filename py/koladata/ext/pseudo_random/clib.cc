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
#include <random>
#include <utility>

#include "absl/strings/string_view.h"
#include "koladata/internal/pseudo_random.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "pybind11/pybind11.h"
#include "pybind11_abseil/absl_casters.h"

namespace koladata::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(clib, m) {
  m.def("epoch_id", &koladata::internal::PseudoRandomEpochId);
  m.def("next_uint64", &koladata::internal::PseudoRandomUint64);
  m.def("next_fingerprint", &koladata::internal::PseudoRandomFingerprint);
  m.def(
      "reseed",
      [](absl::string_view entropy) {
        std::seed_seq seed_seq(entropy.data(), entropy.end());
        arolla::python::pybind11_throw_if_error(
            koladata::internal::ReseedPseudoRandom(std::move(seed_seq)));
      },
      py::arg("entropy"));
}

}  // namespace
}  // namespace koladata::python
