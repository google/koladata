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
#include "py/koladata/base/py_conversions/from_py.h"

#include <Python.h>

#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "koladata/adoption_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "py/koladata/base/boxing.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {
using internal::DataItem;

// Helper class for converting Python objects to DataSlices.
class FromPyConverter {
 public:
  explicit FromPyConverter(AdoptionQueue& adoption_queue)
      : adoption_queue_(adoption_queue) {}

  absl::StatusOr<DataSlice> Convert(PyObject* py_obj,
                                    const std::optional<DataSlice>& schema,
                                    size_t from_dim) {
    std::vector<PyObject*> py_objects{py_obj};
    DataSlice::JaggedShape cur_shape = DataSlice::JaggedShape::Empty();

    if (from_dim > 0) {
      ASSIGN_OR_RETURN((auto [py_objs, shape]),
                       FlattenPyList(py_obj, from_dim));
      py_objects = std::move(py_objs);
      cur_shape = std::move(shape);
    }
    return ConvertImpl(py_objects, cur_shape, schema);
  }

 private:
  absl::StatusOr<DataSlice> ConvertImpl(
      const std::vector<PyObject*>& py_objects,
      const DataSlice::JaggedShape& cur_shape,
      const std::optional<DataSlice>& schema) {
    DataItem schema_item;
    if (schema && schema->item() == schema::kObject) {
      schema_item = schema->item();
    }
    // We need implicit casting here, so we provide an empty schema if schema is
    // not OBJECT.
    ASSIGN_OR_RETURN(
        auto res_slice,
        DataSliceFromPyFlatList(py_objects, cur_shape, std::move(schema_item),
                                adoption_queue_));
    if (schema) {
      ASSIGN_OR_RETURN(res_slice, CastToImplicit(res_slice, schema->item()),
                       [&](absl::Status status) {
                         return CreateIncompatibleSchemaErrorFromStatus(
                             std::move(status),
                             res_slice.GetSchema().WithBag(
                                 adoption_queue_.GetBagWithFallbacks()),
                             *schema);
                       }(_));
    }
    return res_slice;
  }

  AdoptionQueue& adoption_queue_;
};

}  // namespace

absl::StatusOr<DataSlice> FromPy_V2(PyObject* py_obj,
                                    const std::optional<DataSlice>& schema,
                                    size_t from_dim) {
  AdoptionQueue adoption_queue;
  DataItem schema_item;
  if (schema) {
    RETURN_IF_ERROR(schema->VerifyIsSchema());
    adoption_queue.Add(*schema);
    schema_item = schema->item();
  }
  ASSIGN_OR_RETURN(
      DataSlice res_slice,
      FromPyConverter(adoption_queue).Convert(py_obj, schema, from_dim));

  DataBagPtr res_db = res_slice.GetBag();
  DCHECK(res_db == nullptr || res_db->IsMutable());
  if (res_slice.GetBag() == nullptr) {
    ASSIGN_OR_RETURN(res_db, adoption_queue.GetCommonOrMergedDb());
    // If the result has no associated DataBag but an OBJECT schema was
    // requested, attach an empty DataBag.
    if (res_db == nullptr && schema && schema->item() == schema::kObject) {
      res_db = DataBag::Empty();
      res_db->UnsafeMakeImmutable();
    }
    return res_slice.WithBag(std::move(res_db));
  }
  DCHECK(res_db != nullptr);
  return res_slice;
}

}  // namespace koladata::python
