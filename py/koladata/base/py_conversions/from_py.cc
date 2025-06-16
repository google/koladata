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

#include "absl/base/optimization.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/adoption_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/error_repr_utils.h"
#include "koladata/internal/casting.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/object_factories.h"
#include "koladata/shape_utils.h"
#include "py/koladata/base/boxing.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {
using internal::DataItem;

bool IsObjectSchema(const std::optional<DataSlice>& schema) {
  return schema && schema->item() == schema::kObject;
}

bool IsStructSchema(const std::optional<DataSlice>& schema) {
  return schema && schema->item() != schema::kObject;
}

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
  bool IsListOrTuple(const std::vector<PyObject*>& py_objects,
                     const std::optional<DataSlice>& schema) {
    if (IsStructSchema(schema)) {
      return schema->IsListSchema();
    }
    if (py_objects.empty()) {
      return false;
    }
    DCHECK_EQ(py_objects.size(), 1);
    return PyList_Check(py_objects[0]) || PyTuple_Check(py_objects[0]);
  }

  absl::StatusOr<DataSlice> CreateWithSchema(internal::DataSliceImpl impl,
                                             DataSlice::JaggedShape shape,
                                             DataItem schema) {
    // NOTE: CastDataTo does not do schema validation or schema embedding (in
    // case schema is OBJECT).
    ASSIGN_OR_RETURN(impl, schema::CastDataTo(impl, schema));
    return DataSlice::Create(std::move(impl), std::move(shape),
                             std::move(schema));
  }

  // Converts a list of Python objects to DataItems and creates a DataSlice.
  // This is used for list/dict/object attributes when their schema is not
  // specified. The schema argument is therefore `kObject` or not set. In the
  // latter case the common schema of the objects is inferred.
  // TODO(b/391097990) remove schema argument when schema inference is more
  // flexible.
  absl::StatusOr<DataSlice> ComputeObjectsSlice(
      const std::vector<PyObject*>& py_objects,
      const DataSlice::JaggedShape& cur_shape,
      const std::optional<DataSlice>& object_schema = std::nullopt) {
    DCHECK(!object_schema || object_schema->item() == schema::kObject);
    const size_t size = py_objects.size();
    internal::SliceBuilder bldr(size);
    schema::CommonSchemaAggregator schema_agg;
    for (size_t i = 0; i < size; ++i) {
      ASSIGN_OR_RETURN(
          DataSlice ds,
          ConvertImpl({py_objects[i]}, DataSlice::JaggedShape::Empty(),
                      object_schema));
      DCHECK(ds.is_item());
      bldr.InsertIfNotSetAndUpdateAllocIds(i, ds.item());
      if (!object_schema) {
        schema_agg.Add(ds.GetSchemaImpl());
      }
    }
    DataItem schema_item;
    if (object_schema) {
      schema_item = object_schema->item();
    } else {
      ASSIGN_OR_RETURN(schema_item, std::move(schema_agg).Get(),
                       KodaErrorCausedByNoCommonSchemaError(
                           _, adoption_queue_.GetBagWithFallbacks()));
    }
    return CreateWithSchema(std::move(bldr).Build(), cur_shape,
                            std::move(schema_item));
  }

  absl::StatusOr<DataSlice> ConvertImpl(
      const std::vector<PyObject*>& py_objects,
      const DataSlice::JaggedShape& cur_shape,
      const std::optional<DataSlice>& schema) {
    if (!IsStructSchema(schema)) {
      // Processing OBJECTs.
      if (py_objects.size() > 1) {
        // Each item will be processed separately as an OBJECT. If size <= 1, we
        // fallback to the rest of the functionality below that creates
        // Lists / Dicts / Entities or a Primitive and assign it an OBJECT
        // schema.
        // TODO: At the moment primitives get assigned their true
        // schema, when schema is std::nullopt, but in future in this branch
        // will always be OBJECT.
        return ComputeObjectsSlice(py_objects, cur_shape, schema);
      }
    }

    if (IsListOrTuple(py_objects, schema)) {
      return ConvertListOrTuple(py_objects, cur_shape, schema);
    }

    DataItem schema_item;
    if (IsObjectSchema(schema)) {
      schema_item = DataItem(schema::kObject);
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

  absl::StatusOr<DataSlice> ConvertListOrTuple(
      const std::vector<PyObject*>& py_objects,
      const DataSlice::JaggedShape& cur_shape,
      const std::optional<DataSlice>& schema) {
    std::vector<PyObject*> next_level_py_objs;
    shape::ShapeBuilder shape_builder(cur_shape);
    std::optional<DataSlice> item_schema;

    const bool is_struct_schema = IsStructSchema(schema);
    const bool is_object_schema = IsObjectSchema(schema);

    if (is_struct_schema) {
      ASSIGN_OR_RETURN(item_schema,
                       schema->GetAttr(schema::kListItemsSchemaAttr));
    } else if (is_object_schema) {
      item_schema = schema;
    } else {
      DCHECK(!schema.has_value());
      // We cannot call recursively ConvertImpl with `nullopt` schema, because
      // otherwise child items will not be casted to objects, p.ex.
      // `from_py([1, 2])` will return a list of INT32, not OBJECT.
      ASSIGN_OR_RETURN(item_schema,
                       DataSlice::Create(DataItem(schema::kObject),
                                         DataItem(schema::kSchema), nullptr));
    }
    for (PyObject* py_obj : py_objects) {
      if (!PyList_Check(py_obj) && !PyTuple_Check(py_obj)) {
        return absl::InvalidArgumentError(
            "cannot parse lists/tuples mixed with other types on the same "
            "level");
      }
      shape_builder.Add(PySequence_Fast_GET_SIZE(py_obj));
      absl::Span<PyObject*> py_items(PySequence_Fast_ITEMS(py_obj),
                                     PySequence_Fast_GET_SIZE(py_obj));
      for (PyObject* py_item : py_items) {
        next_level_py_objs.push_back(py_item);
      }
    }
    ASSIGN_OR_RETURN(DataSlice::JaggedShape next_level_shape,
                     std::move(shape_builder).Build());
    ASSIGN_OR_RETURN(std::optional<DataSlice> list_items,
                     ConvertImpl(next_level_py_objs, next_level_shape,
                                 item_schema));
    const std::optional<DataSlice>& schema_for_lists =
        is_struct_schema ? schema : std::nullopt;
    ASSIGN_OR_RETURN(
        DataSlice list,
        CreateListShaped(GetBag(), cur_shape, list_items, schema_for_lists));
    if (!is_struct_schema) {
      return ObjectCreator::ConvertWithoutAdopt(GetBag(), list);
    }
    return list;
  }

  const DataBagPtr& GetBag() {
    if (ABSL_PREDICT_FALSE(db_ == nullptr)) {
      db_ = DataBag::Empty();
      adoption_queue_.Add(db_);
    }
    return db_;
  }

  DataBagPtr db_;
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
    if (res_db == nullptr && IsObjectSchema(schema)) {
      res_db = DataBag::Empty();
      res_db->UnsafeMakeImmutable();
    }
    return res_slice.WithBag(std::move(res_db));
  }
  DCHECK(res_db != nullptr);
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*res_db));
  res_db->UnsafeMakeImmutable();
  return res_slice;
}

}  // namespace koladata::python
