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
#include "py/koladata/base/py_args.h"

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/qtype_traits.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/operators/utils.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/wrap_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {

bool ParseStringOrDataItemArg(const FastcallArgParser::Args& args,
                              size_t arg_pos,
                              absl::string_view arg_name_for_error,
                              absl::string_view& arg) {
  if (args.pos_kw_values.size() <= arg_pos || !args.pos_kw_values[arg_pos] ||
      args.pos_kw_values[arg_pos] == Py_None) {
    // The argument was not specified and arg will retain its value from the
    // caller.
    return true;
  }
  return ParseStringOrDataItemArg(args.pos_kw_values[arg_pos],
                                  arg_name_for_error, arg);
}

bool ParseStringOrDataItemArg(PyObject* arg_py,
                              absl::string_view arg_name_for_error,
                              absl::string_view& arg) {
  if (arolla::python::IsPyQValueInstance(arg_py)) {
    const auto& typed_value = arolla::python::UnsafeUnwrapPyQValue(arg_py);
    if (typed_value.GetType() == arolla::GetQType<DataSlice>()) {
      ASSIGN_OR_RETURN(arg,
                       ops::GetStringArgument(typed_value.UnsafeAs<DataSlice>(),
                                              arg_name_for_error),
                       (arolla::python::SetPyErrFromStatus(_), false));
      return true;
    }
  }
  Py_ssize_t unicode_size;
  auto unicode_ptr = PyUnicode_AsUTF8AndSize(arg_py, &unicode_size);
  if (unicode_ptr == nullptr) {
    PyErr_Format(PyExc_TypeError, "argument `%s` must be a utf8 string, got %s",
                 std::string(arg_name_for_error).c_str(),
                 Py_TYPE(arg_py)->tp_name);
    return false;
  }
  // unicode_ptr is valid as long as arg_py is not garbage collected.
  arg = absl::string_view(unicode_ptr, unicode_size);
  return true;
}

bool ParseDataSliceArg(const FastcallArgParser::Args& args,
                       absl::string_view arg_name,
                       std::optional<DataSlice>& arg) {
  // args.pos_kw_values[arg_pos] is "arg_name" optional positional-keyword
  // argument.
  auto arg_it = args.kw_only_args.find(arg_name);
  if (arg_it == args.kw_only_args.end() || arg_it->second == Py_None) {
    arg = std::nullopt;
    return true;
  }
  const DataSlice* arg_ptr = UnwrapDataSlice(arg_it->second, arg_name);
  if (arg_ptr == nullptr) {
    return false;
  }
  arg = *arg_ptr;
  return true;
}

bool ParseBoolArg(const FastcallArgParser::Args& args, size_t arg_pos,
                  absl::string_view arg_name_for_error, bool& arg) {
  // args.pos_kw_values[arg_pos] is "arg_name" optional positional-keyword
  // argument.
  if (args.pos_kw_values.size() <= arg_pos ||
      args.pos_kw_values[arg_pos] == nullptr ||
      args.pos_kw_values[arg_pos] == Py_None) {
    arg = false;
    return true;
  }
  if (!PyBool_Check(args.pos_kw_values[arg_pos])) {
    PyErr_Format(PyExc_TypeError, "expected bool for %s, got %s",
                 std::string(arg_name_for_error).c_str(),
                 Py_TYPE(args.pos_kw_values[arg_pos])->tp_name);
    return false;
  }
  arg = PyObject_IsTrue(args.pos_kw_values[arg_pos]);
  return true;
}

bool ParseBoolArg(const FastcallArgParser::Args& args,
                  absl::string_view arg_name, bool& arg) {
  auto arg_it = args.kw_only_args.find(arg_name);
  if (arg_it == args.kw_only_args.end() || arg_it->second == Py_None) {
    arg = false;
    return true;
  }
  if (!PyBool_Check(arg_it->second)) {
    PyErr_Format(PyExc_TypeError, "expected bool for %s, got %s",
                 std::string(arg_name).c_str(),
                 Py_TYPE(arg_it->second)->tp_name);
    return false;
  }
  arg = PyObject_IsTrue(arg_it->second);
  return true;
}

namespace {

// Sets Python TypeError if inadequate number of positional arguments have been
// passed to a function / method.
bool InvalidPosArgCountError(Py_ssize_t nargs, size_t pos_only_n,
                             size_t pos_keyword_n, bool opt_pos_only) {
  if (pos_only_n > 0 || pos_keyword_n > 0) {
    if (pos_keyword_n == 0) {
      PyErr_Format(PyExc_TypeError,
                   "accepts %d positional-only argument%s but %d %s given",
                   pos_only_n, pos_only_n == 1 ? "" : "s", nargs,
                   nargs == 1 ? "was" : "were");
    } else {
      PyErr_Format(PyExc_TypeError,
                   "accepts %d to %d positional arguments but %d %s given",
                   opt_pos_only ? 0 : pos_only_n, pos_only_n + pos_keyword_n,
                   nargs, nargs == 1 ? "was" : "were");
    }
  } else {
    PyErr_Format(PyExc_TypeError,
                 "accepts 0 positional arguments but %d %s given", nargs,
                 nargs == 1 ? "was" : "were");
  }
  return false;
}

}  // namespace

bool FastcallArgParser::Parse(PyObject* const* py_args, Py_ssize_t nargs,
                              PyObject* py_kwnames, Args& args) const {
  if (parse_kwargs_) {
    args.kw_names.reserve(kKwargsVectorCapacity);
    args.kw_values.reserve(kKwargsVectorCapacity);
  }
  if (!kw_only_arg_names_.empty()) {
    args.kw_only_args.reserve(kw_only_arg_names_.size());
  }
  args.pos_only_args.assign(pos_only_n_, nullptr);
  args.pos_kw_values.assign(pos_kw_to_pos_.size(), nullptr);
  if ((!optional_positional_only_ && nargs < pos_only_n_) ||
      nargs > pos_kw_to_pos_.size() + pos_only_n_) {
    return InvalidPosArgCountError(nargs, pos_only_n_, pos_kw_to_pos_.size(),
                                   optional_positional_only_);
  }
  for (size_t i = 0; i < std::min(pos_only_n_, static_cast<size_t>(nargs));
       ++i) {
    args.pos_only_args[i] = py_args[i];
  }
  for (size_t i = pos_only_n_; i < nargs; ++i) {
    args.pos_kw_values[i - pos_only_n_] = py_args[i];
  }
  if (py_kwnames == nullptr) {
    return true;
  }
  Py_ssize_t kwargs_size = PyTuple_GET_SIZE(py_kwnames);
  for (Py_ssize_t i = 0; i < kwargs_size; ++i) {
    PyObject* const py_key = PyTuple_GET_ITEM(py_kwnames, i);
    Py_ssize_t key_size = 0;
    // NOTE: The underlying UTF8 is cached in Unicode object and the pointer
    // to the same buffer is returned on multiple calls. This means it is safe
    // to use these pointers as long as Unicode object is alive:
    // https://docs.python.org/3/c-api/unicode.html
    const char* key_data = PyUnicode_AsUTF8AndSize(py_key, &key_size);
    if (key_data == nullptr) {
      return false;
    }
    absl::string_view arg_name(key_data, key_size);
    // Positional-keyword argument.
    if (auto arg_pos_it = pos_kw_to_pos_.find(arg_name);
        arg_pos_it != pos_kw_to_pos_.end()) {
      if (args.pos_kw_values[arg_pos_it->second] != nullptr) {
        PyErr_Format(PyExc_TypeError, "got multiple values for argument %R",
                     py_key);
        return false;
      }
      args.pos_kw_values[arg_pos_it->second] = py_args[nargs + i];
      continue;
    }
    // Keyword-only argument.
    if (kw_only_arg_names_.contains(arg_name)) {
      auto [it, inserted] =
          args.kw_only_args.emplace(arg_name, py_args[nargs + i]);
      // NOTE: In Python 3.11 does not support duplicate kwargs.
      DCHECK(inserted);
      continue;
    }
    // Variadic-keyword argument.
    if (parse_kwargs_) {
      args.kw_names.push_back(arg_name);
      args.kw_values.push_back(py_args[nargs + i]);
      continue;
    }
    PyErr_Format(PyExc_TypeError, "got an unexpected keyword %R", py_key);
    return false;
  }
  return true;
}

}  // namespace koladata::python
