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
#include "py/koladata/base/py_utils.h"

#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype_traits.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/object_factories.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/koladata/base/boxing.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {

namespace {

// Returns true if there is at least one DataSlice that is not DataItem, i.e.
// its rank is >0.
bool HasNonItemDataSlice(const std::vector<PyObject*>& args) {
  for (PyObject* arg : args) {
    if (!arolla::python::IsPyQValueInstance(arg)) {
      continue;
    }
    if (const auto& tv = arolla::python::UnsafeUnwrapPyQValue(arg);
        tv.GetType() == arolla::GetQType<DataSlice>()) {
      const DataSlice& ds = tv.UnsafeAs<DataSlice>();
      if (ds.GetShape().rank() > 0) {
        return true;
      }
    }
  }
  return false;
}

}  // namespace

absl::StatusOr<DataSlice> AssignmentRhsFromPyValue(
    PyObject* rhs, bool prohibit_boxing_to_multi_dim_slice,
    const DataBagPtr& db, AdoptionQueue& adoption_queue) {
  // Short-circuit the most common case.
  if (arolla::python::IsPyQValueInstance(rhs)) {
    return DataSliceFromPyValue(rhs, adoption_queue);
  }
  if (PyDict_Check(rhs)) {
    if (prohibit_boxing_to_multi_dim_slice) {
      return absl::InvalidArgumentError(
          "assigning a Python dict to an attribute is only supported"
          " for Koda Dict DataItem, but not for 1+-dimensional slices."
          " use kd.dict() if you want to create the same"
          " dictionary instance to be assigned to all items in"
          " the slice, or kd.dict_like() to create multiple"
          " dictionary instances");
    }
    return EntitiesFromPyObject(rhs, db, adoption_queue);
  }
  ASSIGN_OR_RETURN(auto res, DataSliceFromPyValue(rhs, adoption_queue));
  if (res.GetShape().rank() > 0) {
    // NOTE: `rhs` is not a DataSlice and is a Python iterable / sequence that
    // `DataSliceFromPyValue` treats as multidimensional (e.g. lists and lists,
    // but not `str` or `bytes`).
    if (prohibit_boxing_to_multi_dim_slice) {
      return absl::InvalidArgumentError(
          "assigning a Python list/tuple to an attribute is only supported"
          " for Koda List DataItem, but not for 1+-dimensional slices."
          " use kd.list() if you want to create the same"
          " list instance to be assigned to all items in"
          " the slice, kd.list_like() to create multiple"
          " list instances, or kd.slice() to create a slice");
    }
    return CreateNestedList(db, res, /*schema=*/std::nullopt);
  }
  return std::move(res);
}

absl::StatusOr<DataSlice> AssignmentRhsFromPyValue(
    const DataSlice& lhs_ds, PyObject* rhs, AdoptionQueue& adoption_queue) {
  return AssignmentRhsFromPyValue(rhs, lhs_ds.GetShape().rank() != 0,
                                  lhs_ds.GetBag(), adoption_queue);
}

absl::StatusOr<std::vector<DataSlice>> UnwrapDataSlices(
    const std::vector<PyObject*>& args) {
  std::vector<DataSlice> values;
  values.reserve(args.size());
  for (PyObject* arg : args) {
    if (arolla::python::IsPyQValueInstance(arg)) {
      const auto& typed_value = arolla::python::UnsafeUnwrapPyQValue(arg);
      if (typed_value.GetType() != arolla::GetQType<DataSlice>()) {
        return absl::InvalidArgumentError(
            absl::StrFormat("expected DataSlice argument, got %s",
                            Py_TYPE(arg)->tp_name));
      }
      const DataSlice& ds_arg = typed_value.UnsafeAs<DataSlice>();
      values.push_back(ds_arg);
    } else {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected DataSlice argument, got %s",
                          Py_TYPE(arg)->tp_name));
    }
  }
  return values;
}

absl::StatusOr<std::vector<DataSlice>> ConvertArgsToDataSlices(
    const DataBagPtr& db, const std::vector<PyObject*>& args,
    AdoptionQueue& adoption_queue) {
  return ConvertArgsToDataSlices(db, HasNonItemDataSlice(args), args,
                                 adoption_queue);
}

absl::StatusOr<std::vector<DataSlice>> ConvertArgsToDataSlices(
    const DataBagPtr& db, bool prohibit_boxing_to_multi_dim_slice,
    const std::vector<PyObject*>& args, AdoptionQueue& adoption_queue) {
  std::vector<DataSlice> values;
  values.reserve(args.size());
  for (PyObject* arg : args) {
    ASSIGN_OR_RETURN(
        auto value,
        AssignmentRhsFromPyValue(arg, prohibit_boxing_to_multi_dim_slice, db,
                                 adoption_queue));
    values.push_back(std::move(value));
  }
  return std::move(values);
}

std::vector<DataSlice> ManyWithBag(absl::Span<const DataSlice> values,
                                   const DataBagPtr& bag) {
  std::vector<DataSlice> values_with_db;
  values_with_db.reserve(values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    values_with_db.push_back(values[i].WithBag(bag));
  }
  return values_with_db;
}

}  // namespace koladata::python
