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
#include "absl/strings/string_view.h"
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
#include "py/koladata/base/py_conversions/dataclasses_util.h"
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
  // Returns true if the given Python objects should be treated as a list or
  // tuple, either because of the schema or because of the Python types.
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

  // Returns true if the given Python objects should be treated as a dict,
  // either because of the schema or because of the Python types.
  bool IsDict(const std::vector<PyObject*>& py_objects,
              const std::optional<DataSlice>& schema) {
    if (IsStructSchema(schema)) {
      return schema->IsDictSchema();
    }
    if (py_objects.empty()) {
      return false;
    }

    DCHECK_EQ(py_objects.size(), 1);
    return PyDict_Check(py_objects[0]);
  }

  // If `schema` is a struct schema, returns the `attr_name` attribute of the
  // schema. Otherwise returns OBJECT schema. We cannot call recursively
  // ConvertImpl with `nullopt` schema, because otherwise child items will not
  // be casted to objects, p.ex. `from_py([1, 2])` will return a list of INT32,
  // not OBJECT.
  absl::StatusOr<DataSlice> GetSchemaAttrOrObjectSchema(
      const std::optional<DataSlice>& schema, absl::string_view attr_name) {
    if (IsStructSchema(schema)) {
      return schema->GetAttr(attr_name);
    }
    if (IsObjectSchema(schema)) {
      return *schema;
    }
    DCHECK(!schema.has_value());
    return DataSlice::Create(DataItem(schema::kObject),
                             DataItem(schema::kSchema), nullptr);
  }

  // Returns true if the given Python objects should be treated as an entity by
  // checking the schema. If all objects are None, returns false.
  bool IsEntity(const std::vector<PyObject*>& py_objects,
                const std::optional<DataSlice>& schema) {
    return IsStructSchema(schema) && schema->IsEntitySchema();
  }

  bool IsPrimitiveOrQValue(const std::vector<PyObject*>& py_objects,
                           const std::optional<DataSlice>& schema) {
    if (schema && schema->IsPrimitiveSchema()) {
      return true;
    }
    DCHECK_EQ(py_objects.size(), 1);
    PyObject* obj = py_objects[0];
    return IsPyScalarOrQValueObject(obj);
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

  // Recursively converts `py_objects` to a DataSlice.
  // When there is a struct schema, we do BFS traversal, i.e. we process the
  // whole list of objects at a certain depth together. Otherwise with OBJECT
  // schema, we do DFS traversal.
  //
  // `cur_shape` is used to support `from_dim` argument: if it is specified
  // (non-empty), it will be used as the shape of the resulting DataSlice.
  //
  // If `schema` is specified, it will be used for the resulting DataSlice.
  // Otherwise, the schema is inferred from the objects.
  //
  // If there will be new lists/dicts/objects created, a new DataBag will be
  // created and added to the adoption queue. Also if there will be DataItems in
  // `py_objects`, they will also be added to `adoption_queue_`.
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
    if (IsDict(py_objects, schema)) {
      return ConvertDict(py_objects, cur_shape, schema);
    }

    if (py_objects.empty() || IsPrimitiveOrQValue(py_objects, schema)) {
      DataItem schema_item;
      if (IsObjectSchema(schema)) {
        schema_item = DataItem(schema::kObject);
      }
      // We need implicit casting here, so we provide an empty schema if schema
      // is not OBJECT.
      // TODO: avoid precision loss when migrated to v2.
      ASSIGN_OR_RETURN(
          DataSlice res_slice,
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

    if (IsEntity(py_objects, schema)) {
      DCHECK(schema.has_value());
      return ConvertEntities(py_objects, cur_shape, *schema);
    }

    if (schema.has_value()) {
      if (!IsObjectSchema(schema)) {
        return absl::InvalidArgumentError(
            "schema mismatch: expected an object schema here.");
      }
    }

    DCHECK(!py_objects.empty());
    DCHECK_EQ(py_objects.size(), 1);

    return ConvertObject(py_objects[0], cur_shape);
  }

  // Converts a list of Python objects to a List DataSlice, assuming that
  // all of them are lists or tuples. Calls `ConvertImpl` with the child items
  // to convert the next level of objects and use them as list items; after that
  // creates a list DataSlice with the given shape and schema.
  absl::StatusOr<DataSlice> ConvertListOrTuple(
      const std::vector<PyObject*>& py_objects,
      const DataSlice::JaggedShape& cur_shape,
      const std::optional<DataSlice>& schema) {
    std::vector<PyObject*> next_level_py_objs;
    shape::ShapeBuilder shape_builder(cur_shape);
    ASSIGN_OR_RETURN(
        DataSlice item_schema,
        GetSchemaAttrOrObjectSchema(schema, schema::kListItemsSchemaAttr));
    const bool is_struct_schema = IsStructSchema(schema);
    for (PyObject* py_obj : py_objects) {
      if (!PyList_Check(py_obj) && !PyTuple_Check(py_obj)) {
        // This can only happen if there is a schema provided; otherwise we
        // always parse objects one by one.
        return absl::InvalidArgumentError(
            "schema mismatch: expected list/tuple");
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

  // Converts a list of Python objects to a Dict DataSlice, assuming that
  // all of them are dicts. Calls `ConvertImpl` both for the keys and values
  // and uses the result as dict keys and values; after that
  // creates a dict DataSlice with the given shape and schema.
  absl::StatusOr<DataSlice> ConvertDict(
      const std::vector<PyObject*>& py_objects,
      const DataSlice::JaggedShape& cur_shape,
      const std::optional<DataSlice>& schema) {
    std::vector<PyObject*> next_level_py_keys;
    std::vector<PyObject*> next_level_py_values;
    shape::ShapeBuilder shape_builder(cur_shape);
    const bool is_struct_schema = IsStructSchema(schema);

    ASSIGN_OR_RETURN(
        DataSlice key_schema,
        GetSchemaAttrOrObjectSchema(schema, schema::kDictKeysSchemaAttr));
    ASSIGN_OR_RETURN(
        DataSlice value_schema,
        GetSchemaAttrOrObjectSchema(schema, schema::kDictValuesSchemaAttr));
    for (PyObject* py_obj : py_objects) {
      if (!PyDict_Check(py_obj)) {
        // This can only happen if there is a schema provided; otherwise we
        // always parse objects one by one.
        return absl::InvalidArgumentError("schema mismatch: expected dict");
      }

      const size_t dict_size = PyDict_Size(py_obj);
      std::vector<PyObject*> cur_dict_keys;
      std::vector<PyObject*> cur_dict_values;
      cur_dict_keys.resize(dict_size);
      cur_dict_values.resize(dict_size);
      Py_ssize_t pos = 0;

      for (size_t i = dict_size; i > 0; --i) {
        CHECK(PyDict_Next(py_obj, &pos, &cur_dict_keys[i - 1],
                          &cur_dict_values[i - 1]));
      }
      DCHECK(!PyDict_Next(py_obj, &pos, nullptr, nullptr));

      shape_builder.Add(dict_size);
      next_level_py_keys.insert(next_level_py_keys.end(), cur_dict_keys.begin(),
                                cur_dict_keys.end());
      next_level_py_values.insert(next_level_py_values.end(),
                                  cur_dict_values.begin(),
                                  cur_dict_values.end());
    }
    ASSIGN_OR_RETURN(DataSlice::JaggedShape next_level_shape,
                     std::move(shape_builder).Build());
    ASSIGN_OR_RETURN(std::optional<DataSlice> keys,
                     ConvertImpl(next_level_py_keys, next_level_shape,
                                 std::move(key_schema)));
    ASSIGN_OR_RETURN(std::optional<DataSlice> values,
                     ConvertImpl(next_level_py_values, next_level_shape,
                                 std::move(value_schema)));

    const std::optional<DataSlice>& schema_for_dicts =
        is_struct_schema ? schema : std::nullopt;

    ASSIGN_OR_RETURN(
        DataSlice dict,
        CreateDictShaped(GetBag(), cur_shape, keys, values, schema_for_dicts));

    if (!is_struct_schema) {
      return ObjectCreator::ConvertWithoutAdopt(GetBag(), dict);
    }
    return dict;
  }

  // Converts a list of Python objects to an Entity DataSlice, assuming that
  // all of them have the same schema. First gets a list of attribute names from
  // the schema, then for each object in the list, gets the values of the
  // attributes and finally creates an Entity DataSlice.
  absl::StatusOr<DataSlice> ConvertEntities(
      const std::vector<PyObject*>& py_objects,
      const DataSlice::JaggedShape& cur_shape, const DataSlice& schema) {
    DCHECK(schema.IsEntitySchema());

    ASSIGN_OR_RETURN(DataSlice::AttrNamesSet attr_names, schema.GetAttrNames());

    std::vector<absl::string_view> attr_names_vec(attr_names.begin(),
                                                  attr_names.end());
    // Each element of the vector is a list of Python values for the
    // corresponding attribute. This is needed to process py_objects for a
    // single attribute in a single pass.
    std::vector<std::vector<PyObject*>> attr_python_values_vec(
        attr_names.size());
    for (PyObject* py_obj : py_objects) {
      if (py_obj == Py_None) {
        // TODO: find a smarter (and maybe faster) way to do this,
        // p.ex. by using `nullptr` instead of `None` in
        // `attr_python_values_vec` and skip recursive calls by handling them
        // specially in `ConvertImpl`.
        for (int i = 0; i < attr_names.size(); ++i) {
          attr_python_values_vec[i].push_back(Py_None);
        }
        continue;
      }

      ASSIGN_OR_RETURN(
          std::vector<PyObject*> attr_result,
          // TODO(b/379122942) consider creating one call to GetAttrValues for
          // all py_objects, if it makes the code faster.
          dataclasses_util_.GetAttrValues(py_obj, attr_names_vec));

      for (int i = 0; i < attr_result.size(); ++i) {
        attr_python_values_vec[i].push_back(attr_result[i]);
      }
    }

    std::vector<std::optional<DataSlice>> attr_schemas(attr_names_vec.size());
    for (int i = 0; i < attr_names_vec.size(); ++i) {
      ASSIGN_OR_RETURN(attr_schemas[i], schema.GetAttr(attr_names_vec[i]));
    }
    DCHECK_EQ(attr_schemas.size(), attr_python_values_vec.size());

    std::vector<DataSlice> values;
    values.reserve(attr_names.size());
    for (int i = 0; i < attr_names.size(); ++i) {
      ASSIGN_OR_RETURN(
          values.emplace_back(),
          ConvertImpl(attr_python_values_vec[i], cur_shape, attr_schemas[i]));
    }

    return EntityCreator::Shaped(GetBag(), cur_shape, attr_names_vec, values,
                                 schema);
  }

  // Converts a Python object (dataclass) to an Entity. Since we don't have a
  // schema, we get attribute names and values from the dataclass itself.
  absl::StatusOr<DataSlice> ConvertObject(
      PyObject* py_obj, const DataSlice::JaggedShape& cur_shape) {
    // This is a case when we try to parse a single PyObject as a dataclass
    // object. Checking that it is a dataclass is expensive, so we try to get
    // the attributes of a dataclass immediately.
    ASSIGN_OR_RETURN(
        std::optional<DataClassesUtil::AttrResult> attr_names_and_values,
        dataclasses_util_.GetAttrNamesAndValues(py_obj));

    if (!attr_names_and_values.has_value()) {
      // This should not normally happen, since all the types we support are
      // checked above.
      return absl::InvalidArgumentError(
          "could not parse object as a dataclass");
    }

    DCHECK(attr_names_and_values->attr_names.size() ==
           attr_names_and_values->values.size());
    const size_t size = attr_names_and_values->attr_names.size();
    std::vector<absl::string_view> attr_names(
        attr_names_and_values->attr_names.begin(),
        attr_names_and_values->attr_names.end());
    std::vector<DataSlice> values(size);
    for (int i = 0; i < size; ++i) {
      ASSIGN_OR_RETURN(
          values[i],
          ConvertImpl({attr_names_and_values->values[i]},
                      DataSlice::JaggedShape::Empty(), std::nullopt));
    }
    return ObjectCreator::Shaped(GetBag(), cur_shape, attr_names, values);
  }

  const DataBagPtr& GetBag() {
    if (ABSL_PREDICT_FALSE(db_ == nullptr)) {
      db_ = DataBag::EmptyMutable();
      adoption_queue_.Add(db_);
    }
    return db_;
  }

  DataBagPtr db_;
  AdoptionQueue& adoption_queue_;
  DataClassesUtil dataclasses_util_;
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
    }
    return res_slice.WithBag(std::move(res_db));
  }
  DCHECK(res_db != nullptr);
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*res_db));
  res_db->UnsafeMakeImmutable();
  return res_slice;
}

}  // namespace koladata::python
