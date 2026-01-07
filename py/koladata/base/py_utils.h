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
#ifndef PY_KOLADATA_BASE_PY_UTILS_H_
#define PY_KOLADATA_BASE_PY_UTILS_H_

#include <Python.h>

#include <optional>
#include <tuple>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"

namespace koladata::python {

// Verifies `rhs` and returns it converted to a DataSlice. If
// `prohibit_boxing_to_multi_dim_slice` is true (shape of a left-hand side will
// be non-0 ranked), `rhs` may only be a DataSlice (or its subclass) instance or
// a Python value convertible to a DataItem (not a Python list or a dict).
// In case `rhs` is a complex object, such as Python list or
// dict, create Koda data is stored in `db`. In case the assignment cannot
// happen or converting `rhs` to DataSlice is not successful, appropriate error
// is returned.
absl::StatusOr<DataSlice> AssignmentRhsFromPyValue(
    PyObject* rhs, bool prohibit_boxing_to_multi_dim_slice,
    const DataBagPtr& db, AdoptionQueue& adoption_queue);

// The same as above, but relies on JaggedShape and DataBag from `lhs_ds`, which
// has a meaning of assigning `rhs` to an `lhs_ds` object / entity.
absl::StatusOr<DataSlice> AssignmentRhsFromPyValue(
    const DataSlice& lhs_ds, PyObject* rhs, AdoptionQueue& adoption_queue);

// Converts PyObject* arguments to DataSlices with proper error reporting in the
// context of creating objects and entities. `db` is used to create lists /
// dicts from args.
absl::StatusOr<std::vector<DataSlice>> ConvertArgsToDataSlices(
    const DataBagPtr& db, const std::vector<PyObject*>& args,
    AdoptionQueue& adoption_queue);

// Unwraps DataSlices from PyObject* arguments or throws an error if something
// other than a DataSlice is encountered.
absl::StatusOr<std::vector<DataSlice>> UnwrapDataSlices(
    const std::vector<PyObject*>& args);

// Same as above, but if `prohibit_boxing_to_multi_dim_slice` is true (meaning
// the LHS to which `args` will be assigned is a Slice and not an Item), `args`
// must not contain Python dicts or lists / tuples, etc.
absl::StatusOr<std::vector<DataSlice>> ConvertArgsToDataSlices(
    const DataBagPtr& db, bool prohibit_boxing_to_multi_dim_slice,
    const std::vector<PyObject*>& args, AdoptionQueue& adoption_queue);

// Attach `bag` to each DataSlice in `values`.
std::vector<DataSlice> ManyWithBag(absl::Span<const DataSlice> values,
                                   const DataBagPtr& bag);

// Attach `bag` to each DataSlice in `values`. Returns a tuple of optional
// DataSlices.
template <class... OptDS>
auto FewWithBag(const DataBagPtr& bag, const OptDS&... values) {
  auto with_bag_fn = [&bag](const std::optional<DataSlice>& value) {
    return value.has_value() ?
        std::make_optional(value->WithBag(bag)) : std::nullopt;
  };
  return std::make_tuple(with_bag_fn(values)...);
}

}  // namespace koladata::python

#endif  // PY_KOLADATA_BASE_PY_UTILS_H_
