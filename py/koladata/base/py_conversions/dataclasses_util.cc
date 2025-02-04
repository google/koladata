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
#include "py/koladata/base/py_conversions/dataclasses_util.h"

#include <Python.h>

#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/util/fingerprint.h"

namespace koladata::python {
namespace {

constexpr const char* kDataClassesUtilModuleName =
    "koladata.base.py_conversions.dataclasses_util";

using arolla::python::PyObjectPtr;

std::vector<absl::string_view> GetSortedAttrNames(
    absl::Span<const absl::string_view> attr_names) {
  std::vector<absl::string_view> sorted_attr_names{attr_names.begin(),
                                                   attr_names.end()};
  std::sort(sorted_attr_names.begin(), sorted_attr_names.end());
  return sorted_attr_names;
}

arolla::Fingerprint GetAttrNamesFingerprint(
    absl::Span<const absl::string_view> sorted_attr_names) {
  arolla::FingerprintHasher hasher("dataclass");
  for (const absl::string_view& attr_name : sorted_attr_names) {
    hasher.Combine(attr_name);
  }
  return std::move(hasher).Finish();
}

PyObjectPtr InitializeMakeDataClassFn() {
  static PyObject* util_module_name =
      PyUnicode_InternFromString(kDataClassesUtilModuleName);
  PyObjectPtr dataclasses_util_module =
      PyObjectPtr::Own(PyImport_GetModule(util_module_name));

  if (dataclasses_util_module.get() == nullptr) {
    if (!PyErr_Occurred()) {
      PyErr_Format(PyExc_ImportError,
                   "failed to import '%s'; add the following to the "
                   "upper caller: 'import %s as _'",
                   kDataClassesUtilModuleName, kDataClassesUtilModuleName);
    }
    return PyObjectPtr();
  }

  PyObjectPtr fn_make_dataclass = PyObjectPtr::Own(
      PyObject_GetAttrString(dataclasses_util_module.get(), "make_dataclass"));

  if (fn_make_dataclass.get() == nullptr) {
    if (!PyErr_Occurred()) {
      PyErr_Format(PyExc_ImportError,
                   "failed to import make_dataclass from '%s'",
                   kDataClassesUtilModuleName);
    }
    return PyObjectPtr();
  }
  return fn_make_dataclass;
}

}  // namespace

PyObjectPtr DataClassesUtil::MakeDataClass(
    absl::Span<const absl::string_view> attr_names) {
  if (fn_make_dataclass_.get() == nullptr) {
    fn_make_dataclass_ = InitializeMakeDataClassFn();
    if (fn_make_dataclass_.get() == nullptr) {
      return PyObjectPtr();
    }
  }

  const std::vector<absl::string_view> sorted_attr_names =
      GetSortedAttrNames(attr_names);
  const size_t attr_names_size = sorted_attr_names.size();
  const arolla::Fingerprint fp = GetAttrNamesFingerprint(sorted_attr_names);
  if (auto it = dataclasses_cache_.find(fp); it != dataclasses_cache_.end()) {
    return it->second;
  }

  PyObjectPtr py_list = PyObjectPtr::Own(PyList_New(attr_names_size));
  if (py_list.get() == nullptr) {
    return PyObjectPtr();
  }

  for (size_t i = 0; i < attr_names_size; ++i) {
    const absl::string_view& attr_name = sorted_attr_names[i];
    const PyObject* attr_name_py =
        PyUnicode_FromStringAndSize(attr_name.data(), attr_name.size());
    if (attr_name_py == nullptr) {
      return PyObjectPtr();
    }
    PyList_SET_ITEM(py_list.get(), i, attr_name_py);
  }

  PyObjectPtr dataclass = PyObjectPtr::Own(
      PyObject_CallOneArg(fn_make_dataclass_.get(), /*args=*/py_list.get()));
  if (dataclass.get() == nullptr) {
    return PyObjectPtr();
  }

  dataclasses_cache_[fp] = dataclass;
  return dataclass;
}

PyObjectPtr DataClassesUtil::MakeDataClassInstance(
    absl::Span<const absl::string_view> attr_names) {
  PyObjectPtr py_dataclass = MakeDataClass(attr_names);
  if (py_dataclass.get() == nullptr) {
    return PyObjectPtr();
  }

  return PyObjectPtr::Own(PyObject_CallNoArgs(py_dataclass.get()));
}

}  // namespace koladata::python
