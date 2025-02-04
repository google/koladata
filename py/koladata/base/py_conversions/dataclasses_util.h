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
#ifndef THIRD_PARTY_PY_KOLADATA_PY_CONVERSIONS_DATACLASSES_UTIL_H_
#define THIRD_PARTY_PY_KOLADATA_PY_CONVERSIONS_DATACLASSES_UTIL_H_

#include <Python.h>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/util/fingerprint.h"

namespace koladata::python {

// TODO(b/394023266) Consider moving dataclasses functions from
// koladata/types/py_attr_provider to this class.
class DataClassesUtil {
 public:
  DataClassesUtil() = default;

  arolla::python::PyObjectPtr MakeDataClassInstance(
      absl::Span<const absl::string_view> attr_names);

 private:
  arolla::python::PyObjectPtr MakeDataClass(
      absl::Span<const absl::string_view> attr_names);

  absl::flat_hash_map<arolla::Fingerprint, arolla::python::PyObjectPtr>
      dataclasses_cache_;

  arolla::python::PyObjectPtr fn_make_dataclass_;
};

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_PY_CONVERSIONS_DATACLASSES_UTIL_H_
