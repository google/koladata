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
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/util/unit.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/error_repr_utils.h"
#include "koladata/internal/casting.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/trampoline_executor.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/object_factories.h"
#include "koladata/shape_utils.h"
#include "koladata/uuid_utils.h"
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

constexpr static absl::string_view kChildItemIdSeed = "__from_py_child__";
constexpr static absl::string_view kChildListItemAttributeName =
    "list_item_index";
constexpr static absl::string_view kChildDictKeyAttributeName =
    "dict_key_index";
constexpr static absl::string_view kChildDictValueAttributeName =
    "dict_value_index";

// Create a DataSlice of uuids that represent child objects of the given
// parent object.
//
// The result is a DataSlice with `items_shape`.
// The result will have `indexed_attribute_name` attribute that contains flat
// indices of the children in the parent object.
absl::StatusOr<std::optional<DataSlice>> MakeChildrenItemUuids(
    const std::optional<DataSlice>& parent_itemid,
    const DataSlice::JaggedShape& items_shape,
    std::string_view indexed_attribute_name) {
  size_t n_elements = items_shape.size();
  arolla::DenseArrayBuilder<int64_t> flat_index_builder(n_elements);
  for (int64_t i = 0; i < n_elements; ++i) {
    flat_index_builder.Add(i, i);
  }
  ASSIGN_OR_RETURN(
      DataSlice index,
      DataSlice::Create(internal::DataSliceImpl::Create(
                            std::move(flat_index_builder).Build()),
                        items_shape, internal::DataItem(schema::kInt64)));
  return CreateUuidFromFields(kChildItemIdSeed,
                              {"parent", indexed_attribute_name},
                              {*parent_itemid, std::move(index)});
}

// Helper class for converting Python objects to DataSlices.
class FromPyConverter {
 public:
  explicit FromPyConverter(AdoptionQueue& adoption_queue, bool dict_as_obj)
      : adoption_queue_(adoption_queue), dict_as_obj_(dict_as_obj) {}

  absl::StatusOr<DataSlice> Convert(PyObject* py_obj,
                                    const std::optional<DataSlice>& schema,
                                    size_t from_dim,
                                    const std::optional<DataSlice>& itemid) {
    std::vector<PyObject*> py_objects{py_obj};
    DataSlice::JaggedShape cur_shape = DataSlice::JaggedShape::Empty();

    if (from_dim > 0) {
      ASSIGN_OR_RETURN((auto [py_objs, shape]),
                       FlattenPyList(py_obj, from_dim));
      py_objects = std::move(py_objs);
      cur_shape = std::move(shape);

      if (itemid.has_value()) {
        if (itemid->is_item()) {
          return absl::InvalidArgumentError(
              "ItemId for DataSlice must be a DataSlice of non-zero rank if "
              "from_dim > 0");
        }
        if (itemid->size() != py_objects.size()) {
          return absl::InvalidArgumentError(
              absl::StrFormat("ItemId for DataSlice size=%d does not match the "
                              "input list size=%d when from_dim=%d",
                              itemid->size(), py_objects.size(), from_dim));
        }
      }
    }
    std::optional<DataSlice> result;
    RETURN_IF_ERROR(internal::TrampolineExecutor::Run([&](auto& executor) {
      return ConvertImpl(py_objects, std::move(cur_shape), schema, itemid, 0,
                         executor, result,
                         /*computing_object=*/false);
    }));
    DCHECK(result.has_value());
    return std::move(*result);
  }

  // Returns a DataBag used to create additional triples (e.g.
  // `kd.list([[1, 2], [3]], from_dim=1)` needs to create 2 lists and associate
  // them with list values and this requires a DataBag. Note that some Python
  // objects may also be DataItems with DataBags, but those are handled
  // separately (through adoption_queue). If there are no new triples created
  // during conversion, this method returns nullptr.
  absl_nullable DataBagPtr GetCreatedBag() && {
    return std::move(db_);
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

  // Returns true if the given Python objects should be parsed as a dict,
  // but converted to objects or entities.
  bool IsDictAsObj(const std::vector<PyObject*>& py_objects,
                   const std::optional<DataSlice>& schema) {
    if (py_objects.empty()) {
      return false;
    }

    if (!PyDict_Check(py_objects[0])) {
      return false;
    }
    if (schema && schema->IsDictSchema()) {
      return false;
    }
    return dict_as_obj_ || (schema && schema->IsEntitySchema());
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
  // checking the schema.
  bool IsEntity(const std::vector<PyObject*>& py_objects,
                const std::optional<DataSlice>& schema) {
    return IsStructSchema(schema) && schema->IsEntitySchema();
  }

  // Returns true if the given Python objects should be treated as a primitive
  // or a QValue, either because of the schema or because of the type of the
  // first object.
  bool IsPrimitiveOrQValue(const std::vector<PyObject*>& py_objects,
                           const std::optional<DataSlice>& schema) {
    if (schema && schema->IsPrimitiveSchema()) {
      return true;
    }
    DCHECK_GE(py_objects.size(), 1);
    PyObject* obj = py_objects[0];
    if (!IsStructSchema(schema) && Py_IsNone(obj)) {
      return true;
    }
    return IsPyScalarOrQValueObject(obj);
  }

  absl::StatusOr<DataSlice> CreateMaskSlice(
      arolla::bitmap::AlmostFullBuilder& bitmap_builder,
      const DataSlice::JaggedShape& shape) {
    arolla::DenseArray<arolla::Unit> mask = {arolla::VoidBuffer(shape.size()),
                                             std::move(bitmap_builder).Build()};

    internal::DataSliceImpl impl =
        internal::DataSliceImpl::Create(std::move(mask));
    return DataSlice::Create(std::move(impl), shape,
                             internal::DataItem(schema::kMask), GetBag());
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
  absl::Status ComputeObjectsSlice(
      const std::vector<PyObject*>& py_objects,
      DataSlice::JaggedShape cur_shape,
      const std::optional<DataSlice>& object_schema,
      const std::optional<DataSlice>& itemid, int cur_depth,
      internal::TrampolineExecutor& executor,
      std::optional<DataSlice>& result) {
    DCHECK(!object_schema || object_schema->item() == schema::kObject);
    bool is_struct_schema = IsStructSchema(object_schema);
    const size_t size = py_objects.size();

    if (itemid.has_value() && itemid->size() < size) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "ItemId size=%d is smaller than the input list size=%d",
          itemid->size(), size));
    }

    auto bldr = std::make_unique<internal::SliceBuilder>(size);
    auto schema_agg = std::make_unique<schema::CommonSchemaAggregator>();

    std::optional<DataItem> object_schema_item;
    if (object_schema) {
      // It can only be `kObject` here; it is only an optimization
      // in that case we are not using the schema aggregator.
      object_schema_item = object_schema->item();
    }

    auto process_element_fn =
        [this, is_struct_schema,
         schema_agg = !object_schema_item ? schema_agg.get() : nullptr,
         bldr = bldr.get()](size_t index,
                            std::optional<DataSlice>& ds) -> absl::Status {
      DCHECK(ds.has_value());
      if (!is_struct_schema && ds->GetSchemaImpl().is_struct_schema()) {
        // Converting only non-primitives to OBJECTs, in order to embed
        // schema.
        ASSIGN_OR_RETURN(ds, ObjectCreator::ConvertWithoutAdopt(GetBag(), *ds));
      }
      DCHECK((ds)->is_item());
      if (schema_agg != nullptr) {
        schema_agg->Add(ds->GetSchemaImpl());
      }
      bldr->InsertIfNotSetAndUpdateAllocIds(index, ds->item());
      return absl::OkStatus();
    };

    for (size_t i = 0; i < size; ++i) {
      std::optional<DataSlice> child_itemid_ds_item;
      if (itemid.has_value()) {
        DataItem itemid_item =
            itemid->is_item() ? itemid->item() : itemid->slice()[i];
        ASSIGN_OR_RETURN(
            child_itemid_ds_item,
            DataSlice::Create(itemid_item, DataSlice::JaggedShape::Empty(),
                              DataItem(schema::kItemId)));
      }

      auto ds = std::make_unique<std::optional<DataSlice>>();

      // For performance reasons, we do not call `ConvertImplBreakRecursion`
      // here, but rather call `ConvertImpl` directly to reduce over-queuing.
      // This is possible, because we are passing `computing_object=true`,
      // so there cannot be another recursive call inside.
      RETURN_IF_ERROR(ConvertImpl(
          {py_objects[i]}, DataSlice::JaggedShape::Empty(), object_schema,
          std::move(child_itemid_ds_item), cur_depth, executor, *ds,
          /*computing_object=*/true));

      // This is an optimization, if no queuing happened in `ConvertImpl`, we
      // can process the element immediately. This happens in case of
      // primitives, when adding another element to the queue has a significant
      // overhead.
      if (ds->has_value()) {
        RETURN_IF_ERROR(process_element_fn(i, *ds));
      } else {
        executor.Enqueue(
            [process_element_fn, i, ds = std::move(ds)]() -> absl::Status {
              RETURN_IF_ERROR(process_element_fn(i, *ds));
              return absl::OkStatus();
            });
      }
    }
    executor.Enqueue([this, object_schema_item = std::move(object_schema_item),
                      schema_agg = std::move(schema_agg),
                      bldr = std::move(bldr), cur_shape = std::move(cur_shape),
                      &result]() mutable -> absl::Status {
      DataItem schema_item;
      if (object_schema_item) {
        schema_item = *object_schema_item;
      } else {
        ASSIGN_OR_RETURN(schema_item, std::move(*schema_agg).Get(),
                         KodaErrorCausedByNoCommonSchemaError(
                             _, adoption_queue_.GetBagWithFallbacks()));
      }
      ASSIGN_OR_RETURN(result, CreateWithSchema(std::move(*bldr).Build(),
                                                std::move(cur_shape),
                                                std::move(schema_item)));
      return absl::OkStatus();
    });
    return absl::OkStatus();
  }

  // Conceptually the same as ConvertImpl, but enqueues the actual conversion
  // to the executor. The goal is to use TrampolineExecutor to limit the
  // recursion depth.
  absl::Status ConvertImplBreakRecursion(
      std::vector<PyObject*> py_objects, DataSlice::JaggedShape cur_shape,
      const std::optional<DataSlice>& schema, std::optional<DataSlice> itemid,
      int cur_depth, internal::TrampolineExecutor& executor,
      std::optional<DataSlice>& result, bool computing_object = false) {
    constexpr static int kMaxConversionDepth = 10000;
    if (cur_depth > kMaxConversionDepth) {
      return absl::InvalidArgumentError(
          absl::StrFormat("objects with depth > %d are not supported, "
                          "recursive Python object cannot be converted",
                          kMaxConversionDepth));
    }

    executor.Enqueue([this, py_objects = std::move(py_objects),
                      cur_shape = std::move(cur_shape), schema = schema,
                      itemid = std::move(itemid), cur_depth, &executor,
                      computing_object, &result]() mutable -> absl::Status {
      RETURN_IF_ERROR(ConvertImpl(py_objects, std::move(cur_shape), schema,
                                  std::move(itemid), cur_depth + 1, executor,
                                  result, computing_object));
      return absl::OkStatus();
    });
    return absl::OkStatus();
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
  // `computing_object` is used to indicate that the current control flow
  // originates from `ComputeObjectsSlice` and that each item is being processed
  // 1-by-1. A "recursive" `ComputeObjectsSlice` should NOT be invoked. It can
  // still happen deeper, when processing lists, dicts, etc.
  // `is_root` is used to indicate that the current control flow originates from
  // the root of the recursion and that the `itemid` argument should be used as
  // is.
  absl::Status ConvertImpl(const std::vector<PyObject*>& py_objects,
                           DataSlice::JaggedShape cur_shape,
                           const std::optional<DataSlice>& schema,
                           std::optional<DataSlice> itemid, int cur_depth,
                           internal::TrampolineExecutor& executor,
                           std::optional<DataSlice>& result,
                           bool computing_object = false) {
    if (py_objects.empty()) {
      ASSIGN_OR_RETURN(
          result, DataSlice::Create(
                      internal::DataSliceImpl::CreateEmptyAndUnknownType(0),
                      std::move(cur_shape),
                      schema ? schema->item() : DataItem(schema::kNone)));
      return absl::OkStatus();
    }

    if (!IsStructSchema(schema) && !computing_object) {
      // Processing elements one by one and creating a slice of them.
      // TODO: At the moment primitives get assigned their true
      // schema, when schema is std::nullopt, but in future in this branch
      // will always be OBJECT.
      return ComputeObjectsSlice(py_objects, std::move(cur_shape), schema,
                                 itemid, cur_depth, executor, result);
    }

    if (IsPrimitiveOrQValue(py_objects, schema)) {
      // If schema is OBJECT, we need to cast the result to object explicitly to
      // support Entity -> Object casting.
      // TODO(b/391097990) use implicit casting when schema inference is more
      // flexible.
      absl::StatusOr<DataSlice> ds_or = DataSliceFromPyFlatList(
          py_objects, std::move(cur_shape),
          schema ? schema->item() : DataItem(), adoption_queue_,
          /*explicit_cast=*/IsObjectSchema(schema));
      if (!ds_or.ok()) {
        // TODO (b/391097990) find a way to make the error message more user
        // friendly.
        return absl::InvalidArgumentError(absl::StrFormat(
            "could not parse list of primitives / data items: %s",
            ds_or.status().message()));
      }
      result = std::move(ds_or).value();
      return absl::OkStatus();
    }

    if (IsListOrTuple(py_objects, schema)) {
      return ConvertListOrTuple(py_objects, std::move(cur_shape), schema,
                                itemid, cur_depth, executor, result);
    }
    if (IsDictAsObj(py_objects, schema)) {
      return ConvertDictAsObj(py_objects, std::move(cur_shape), schema,
                              std::move(itemid), cur_depth, executor, result);
    }
    if (IsDict(py_objects, schema)) {
      return ConvertDict(py_objects, std::move(cur_shape), schema, itemid,
                         cur_depth, executor, result);
    }

    if (IsEntity(py_objects, schema)) {
      DCHECK(schema.has_value());
      return ConvertEntities(py_objects, std::move(cur_shape), *schema,
                             std::move(itemid), cur_depth, executor, result);
    }

    if (schema.has_value()) {
      if (!IsObjectSchema(schema)) {
        return absl::InvalidArgumentError(
            "schema mismatch: expected an object schema here.");
      }
    }

    DCHECK_EQ(py_objects.size(), 1);

    return ConvertObject(py_objects[0], std::move(itemid), cur_depth, executor,
                         result);
  }

  // Converts a list of Python objects to a List DataSlice, assuming that
  // all of them are lists or tuples. Calls `ConvertImpl` with the child items
  // to convert the next level of objects and use them as list items; after that
  // creates a list DataSlice with the given shape and schema.
  absl::Status ConvertListOrTuple(const std::vector<PyObject*>& py_objects,
                                  DataSlice::JaggedShape cur_shape,
                                  const std::optional<DataSlice>& schema,
                                  const std::optional<DataSlice>& itemid,
                                  int cur_depth,
                                  internal::TrampolineExecutor& executor,
                                  std::optional<DataSlice>& result) {
    std::vector<PyObject*> next_level_py_objs;
    shape::ShapeBuilder shape_builder(cur_shape);
    ASSIGN_OR_RETURN(
        DataSlice item_schema,
        GetSchemaAttrOrObjectSchema(schema, schema::kListItemsSchemaAttr));
    const bool is_struct_schema = IsStructSchema(schema);

    arolla::bitmap::AlmostFullBuilder bitmap_builder(py_objects.size());
    bool has_none = false;

    for (int i = 0; i < py_objects.size(); ++i) {
      PyObject* py_obj = py_objects[i];
      if (Py_IsNone(py_obj)) {
        bitmap_builder.AddMissed(i);
        shape_builder.Add(0);
        has_none = true;
        continue;
      }
      if (!PyList_Check(py_obj) && !PyTuple_Check(py_obj)) {
        // This can only happen if there is a schema provided; otherwise we
        // always parse objects one by one.
        return absl::InvalidArgumentError(
            "schema mismatch: expected list/tuple");
      }
      const size_t list_size = PySequence_Fast_GET_SIZE(py_obj);
      shape_builder.Add(list_size);
      absl::Span<PyObject*> py_items(PySequence_Fast_ITEMS(py_obj), list_size);
      for (PyObject* py_item : py_items) {
        next_level_py_objs.push_back(py_item);
      }
    }
    ASSIGN_OR_RETURN(DataSlice::JaggedShape next_level_shape,
                     std::move(shape_builder).Build());

    std::optional<DataSlice> list_itemid;
    std::optional<DataSlice> child_itemid_ds;
    if (itemid.has_value()) {
      DataSlice list_itemid_slice;
      if (cur_depth == 0) {  // root - itemid must be used as is.
        list_itemid = *itemid;
      } else {
        ASSIGN_OR_RETURN(list_itemid, CreateListUuidFromFields(
                                          "", {"base_itemid"}, {*itemid}));
      }
      ASSIGN_OR_RETURN(child_itemid_ds,
                       MakeChildrenItemUuids(list_itemid, next_level_shape,
                                             kChildListItemAttributeName));
    }
    auto list_items = std::make_unique<std::optional<DataSlice>>();
    RETURN_IF_ERROR(ConvertImplBreakRecursion(
        std::move(next_level_py_objs), std::move(next_level_shape),
        std::move(item_schema), std::move(child_itemid_ds), cur_depth, executor,
        *list_items));
    executor.Enqueue([&result, has_none, cur_shape = std::move(cur_shape),
                      bitmap_builder = std::move(bitmap_builder),
                      list_items = std::move(list_items),
                      schema_for_lists =
                          is_struct_schema ? schema : std::nullopt,
                      list_itemid = std::move(list_itemid),
                      this]() mutable -> absl::Status {
      if (has_none) {
        ASSIGN_OR_RETURN(DataSlice shape_and_mask,
                         CreateMaskSlice(bitmap_builder, cur_shape));

        ASSIGN_OR_RETURN(
            result, CreateListLike(GetBag(), shape_and_mask, *list_items,
                                   schema_for_lists,
                                   /*item_schema=*/std::nullopt, list_itemid));
      } else {
        ASSIGN_OR_RETURN(
            result,
            CreateListShaped(GetBag(), cur_shape, *list_items, schema_for_lists,
                             /*item_schema=*/std::nullopt, list_itemid));
      }
      return absl::OkStatus();
    });
    return absl::OkStatus();
  }

  // Fills a vector of pairs of (key, value) for each dict in `py_dicts`.
  // If there is a schema mismatch, returns an error with the given message.
  // Sets shape_and_mask of the dicts (i.e. Missing if
  // the dict is None, present otherwise) only if there are Nones in py_dicts.
  absl::Status ParseDict(
      std::vector<PyObject*> py_dicts, const DataSlice::JaggedShape& cur_shape,
      std::vector<std::vector<std::pair<PyObject*, PyObject*>>>& py_keys_values,
      std::optional<DataSlice>& shape_and_mask,
      absl::string_view error_message_for_schema_mismatch) {
    py_keys_values.resize(py_dicts.size());

    arolla::bitmap::AlmostFullBuilder bitmap_builder(py_dicts.size());
    bool has_none = false;
    for (int dict_index = 0; dict_index < py_dicts.size(); ++dict_index) {
      PyObject* py_obj = py_dicts[dict_index];
      if (Py_IsNone(py_obj)) {
        bitmap_builder.AddMissed(dict_index);
        py_keys_values[dict_index].resize(0);
        has_none = true;
        continue;
      }
      std::vector<std::pair<PyObject*, PyObject*>>& cur_dict_keys_values =
          py_keys_values[dict_index];
      if (!PyDict_CheckExact(py_obj)) {
        // This can only happen if there is a schema provided; otherwise we
        // always parse objects one by one.
        return absl::InvalidArgumentError(error_message_for_schema_mismatch);
      }

      const size_t dict_size = PyDict_Size(py_obj);

      cur_dict_keys_values.resize(dict_size);
      Py_ssize_t pos = 0;

      for (auto& [key, value] : cur_dict_keys_values) {
        if (!PyDict_Next(py_obj, &pos, &key, &value)) {
          return absl::InternalError(
              "failed to get the next key and value from the dictionary.");
        }
      }
      DCHECK(!PyDict_Next(py_obj, &pos, nullptr, nullptr));
    }

    if (has_none) {
      ASSIGN_OR_RETURN(shape_and_mask,
                       CreateMaskSlice(bitmap_builder, cur_shape));
    }
    return absl::OkStatus();
  }

  // Converts a list of Python objects to a Dict DataSlice, assuming that
  // all of them are dicts. Calls `ConvertImpl` both for the keys and values
  // and uses the result as dict keys and values; after that
  // creates a dict DataSlice with the given shape and schema.
  absl::Status ConvertDict(const std::vector<PyObject*>& py_objects,
                           DataSlice::JaggedShape cur_shape,
                           const std::optional<DataSlice>& schema,
                           const std::optional<DataSlice>& itemid,
                           int cur_depth,
                           internal::TrampolineExecutor& executor,
                           std::optional<DataSlice>& result) {
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

    std::vector<std::vector<std::pair<PyObject*, PyObject*>>> py_keys_values;
    std::optional<DataSlice> shape_and_mask;
    RETURN_IF_ERROR(
        ParseDict(py_objects, cur_shape, py_keys_values, shape_and_mask,
                  "schema mismatch: expected dict object for dict schema"));

    for (const std::vector<std::pair<PyObject*, PyObject*>>&
             cur_dict_keys_values : py_keys_values) {
      shape_builder.Add(cur_dict_keys_values.size());
      for (const auto& [key, value] : cur_dict_keys_values) {
        next_level_py_keys.push_back(key);
        next_level_py_values.push_back(value);
      }
    }
    ASSIGN_OR_RETURN(DataSlice::JaggedShape next_level_shape,
                     std::move(shape_builder).Build());

    std::optional<DataSlice> child_keys_itemid;
    std::optional<DataSlice> child_values_itemid;
    std::optional<DataSlice> dict_itemid;
    if (itemid.has_value()) {
      if (cur_depth == 0) {  // root - itemid must be used as is.
        dict_itemid = *itemid;
      } else {
        ASSIGN_OR_RETURN(dict_itemid, CreateDictUuidFromFields(
                                          "", {"base_itemid"}, {*itemid}));
      }

      ASSIGN_OR_RETURN(child_keys_itemid,
                       MakeChildrenItemUuids(dict_itemid, next_level_shape,
                                             kChildDictKeyAttributeName));
      ASSIGN_OR_RETURN(child_values_itemid,
                       MakeChildrenItemUuids(dict_itemid, next_level_shape,
                                             kChildDictValueAttributeName));
    }
    auto keys = std::make_unique<std::optional<DataSlice>>();
    auto values = std::make_unique<std::optional<DataSlice>>();
    RETURN_IF_ERROR(ConvertImplBreakRecursion(
        std::move(next_level_py_keys), next_level_shape, std::move(key_schema),
        std::move(child_keys_itemid), cur_depth, executor, *keys));
    RETURN_IF_ERROR(ConvertImplBreakRecursion(
        std::move(next_level_py_values), std::move(next_level_shape),
        std::move(value_schema), std::move(child_values_itemid), cur_depth,
        executor, *values));
    executor.Enqueue(
        [&result, keys = std::move(keys), values = std::move(values),
         shape_and_mask = std::move(shape_and_mask),
         dict_itemid = std::move(dict_itemid),
         schema_for_dicts = is_struct_schema ? schema : std::nullopt,
         cur_shape = std::move(cur_shape), this]() -> absl::Status {
          if (shape_and_mask.has_value()) {
            ASSIGN_OR_RETURN(
                result,
                CreateDictLike(GetBag(), *shape_and_mask, *keys, *values,
                               schema_for_dicts,
                               /*key_schema=*/std::nullopt,
                               /*value_schema=*/std::nullopt, dict_itemid));
          } else {
            ASSIGN_OR_RETURN(
                result,
                CreateDictShaped(GetBag(), cur_shape, *keys, *values,
                                 schema_for_dicts,
                                 /*key_schema=*/std::nullopt,
                                 /*value_schema=*/std::nullopt, dict_itemid));
          }
          return absl::OkStatus();
        });
    return absl::OkStatus();
  }

  // Converts a list of Python dicts to an Object or Entity DataSlice,
  // depending on the schema.
  absl::Status ConvertDictAsObj(const std::vector<PyObject*>& py_objects,
                                DataSlice::JaggedShape cur_shape,
                                const std::optional<DataSlice>& schema,
                                std::optional<DataSlice> itemid, int cur_depth,
                                internal::TrampolineExecutor& executor,
                                std::optional<DataSlice>& result) {
    const bool is_struct_schema = IsStructSchema(schema);

    absl::flat_hash_map<absl::string_view, std::vector<PyObject*>>
        attr_python_values_map;

    DataSlice::AttrNamesSet schema_attr_names;
    if (is_struct_schema) {
      ASSIGN_OR_RETURN(schema_attr_names, schema->GetAttrNames());
    }

    std::vector<std::vector<std::pair<PyObject*, PyObject*>>> py_keys_values;
    std::optional<DataSlice> shape_and_mask;
    RETURN_IF_ERROR(ParseDict(py_objects, cur_shape, py_keys_values,
                              shape_and_mask,
                              "schema mismatch: expected dict object when "
                              "parsing dict as object"));

    int i = 0;
    for (const std::vector<std::pair<PyObject*, PyObject*>>&
             cur_dict_keys_values : py_keys_values) {
      for (const std::pair<PyObject*, PyObject*>& key_value :
           cur_dict_keys_values) {
        ASSIGN_OR_RETURN(absl::string_view key,
                         PyDictKeyAsStringView(key_value.first));
        PyObject* py_value = key_value.second;
        // If there is a schema, we only parse the attributes that are
        // mentioned in the schema. Otherwise, we will fail when calling
        // `EntityCreator::Shaped` below.
        if (!is_struct_schema || schema_attr_names.contains(key)) {
          std::vector<PyObject*>& attr_values = attr_python_values_map[key];
          if (attr_values.size() < py_objects.size()) {
            attr_values.resize(py_objects.size(), Py_None);
          }
          attr_values[i] = py_value;
        }
      }
      ++i;
    }

    std::vector<std::string> attr_names;
    attr_names.reserve(attr_python_values_map.size());

    if (is_struct_schema) {
      // Used for entities: for each attribute, we have a list of values, one
      // for each object.
      std::vector<std::vector<PyObject*>> attributes_values(
          attr_python_values_map.size());

      i = 0;
      for (const auto& [key, values] : attr_python_values_map) {
        attr_names.push_back(std::string(key));
        attributes_values[i] = values;
        ++i;
      }
      return CreateEntitiesFromAttrNamesAndValues(
          std::move(attr_names), std::move(attributes_values),
          std::move(cur_shape), std::move(shape_and_mask), *schema,
          std::move(itemid), cur_depth, executor, result);
    }

    // Used for objects: we have a single list of values, one for each
    // attribute.
    std::vector<PyObject*> attr_values;

    for (const auto& [key, values] : attr_python_values_map) {
      attr_names.push_back(std::string(key));
      DCHECK_EQ(values.size(), 1);
      attr_values.push_back(values[0]);
    }

    return ConvertObjectFromAttrNamesAndValues(
        std::move(attr_names), std::move(attr_values), std::move(itemid),
        cur_depth, executor, result);
  }

  // Converts a list of Python objects to an Entity DataSlice, assuming that
  // all of them have the same schema. First gets a list of attribute names from
  // the schema, then for each object in the list, gets the values of the
  // attributes and finally creates an Entity DataSlice.
  absl::Status ConvertEntities(const std::vector<PyObject*>& py_objects,
                               DataSlice::JaggedShape cur_shape,
                               const DataSlice& schema,
                               std::optional<DataSlice> itemid, int cur_depth,
                               internal::TrampolineExecutor& executor,
                               std::optional<DataSlice>& result) {
    DCHECK(schema.IsEntitySchema());

    ASSIGN_OR_RETURN(DataSlice::AttrNamesSet attr_names, schema.GetAttrNames());

    std::vector<std::string> attr_names_vec(attr_names.begin(),
                                            attr_names.end());
    // Each element of the vector is a list of Python values for the
    // corresponding attribute. This is needed to process py_objects for a
    // single attribute in a single pass.

    arolla::bitmap::AlmostFullBuilder bitmap_builder(py_objects.size());

    std::vector<std::vector<PyObject*>> attr_python_values_vec(
        attr_names.size());
    bool has_none = false;
    for (size_t i = 0; i < py_objects.size(); ++i) {
      PyObject* py_obj = py_objects[i];
      if (py_obj == Py_None) {
        has_none = true;
        bitmap_builder.AddMissed(i);
        // TODO: find a smarter (and maybe faster) way to do
        // this,
        // p.ex. by using `nullptr` instead of `None` in
        // `attr_python_values_vec` and skip recursive calls by handling them
        // specially in `ConvertImpl`.
        for (int i = 0; i < attr_names.size(); ++i) {
          attr_python_values_vec[i].push_back(Py_None);
        }
        continue;
      }

      std::vector<absl::string_view> attr_names_views(attr_names_vec.begin(),
                                                      attr_names_vec.end());
      ASSIGN_OR_RETURN(
          std::vector<PyObject*> attr_result,
          // TODO(b/379122942) consider creating one call to GetAttrValues
          // for all py_objects, if it makes the code faster.

          dataclasses_util_.GetAttrValues(py_obj, attr_names_views));

      for (int i = 0; i < attr_result.size(); ++i) {
        attr_python_values_vec[i].push_back(attr_result[i]);
      }
    }

    if (has_none) {
      ASSIGN_OR_RETURN(DataSlice shape_and_mask,
                       CreateMaskSlice(bitmap_builder, cur_shape));

      return CreateEntitiesFromAttrNamesAndValues(
          std::move(attr_names_vec), std::move(attr_python_values_vec),
          std::move(cur_shape), std::move(shape_and_mask), schema,
          std::move(itemid), cur_depth, executor, result);
    } else {
      return CreateEntitiesFromAttrNamesAndValues(
          std::move(attr_names_vec), std::move(attr_python_values_vec),
          std::move(cur_shape), std::nullopt, schema, std::move(itemid),
          cur_depth, executor, result);
    }
  }

  // Converts a list of Python objects to an Entity DataSlice, assuming that
  // all of them have the same schema. First gets a list of attribute names from
  // the schema, then for each object in the list, gets the values of the
  // attributes and finally creates an Entity DataSlice.
  absl::Status CreateEntitiesFromAttrNamesAndValues(
      std::vector<std::string> attr_names,
      std::vector<std::vector<PyObject*>> attr_python_values_vec,
      DataSlice::JaggedShape cur_shape, std::optional<DataSlice> shape_and_mask,
      const DataSlice& schema, std::optional<DataSlice> itemid, int cur_depth,
      internal::TrampolineExecutor& executor,
      std::optional<DataSlice>& result) {
    DCHECK(schema.IsEntitySchema());

    std::vector<std::optional<DataSlice>> attr_schemas(attr_names.size());
    for (int i = 0; i < attr_names.size(); ++i) {
      ASSIGN_OR_RETURN(attr_schemas[i], schema.GetAttr(attr_names[i]));
    }
    DCHECK_EQ(attr_schemas.size(), attr_python_values_vec.size());

    auto values = std::make_unique<std::vector<std::optional<DataSlice>>>(
        attr_names.size());
    for (int attr_index = 0; attr_index < attr_names.size(); ++attr_index) {
      const absl::string_view attr_name = attr_names[attr_index];
      std::optional<DataSlice> attr_schema =
          std::move(attr_schemas[attr_index]);
      if (attr_schema && attr_schema->IsPrimitiveSchema()) {
        // This is a hack to get a nice error message with the attribute name.
        attr_schema = std::nullopt;
      }

      std::optional<DataSlice> child_itemid_ds;
      if (itemid.has_value()) {
        ASSIGN_OR_RETURN(child_itemid_ds,
                         MakeChildrenItemUuids(itemid, cur_shape, attr_name));
      }

      RETURN_IF_ERROR(ConvertImplBreakRecursion(
          std::move(attr_python_values_vec[attr_index]), cur_shape,
          std::move(attr_schema), std::move(child_itemid_ds), cur_depth,
          executor, (*values)[attr_index]));
    }
    executor.Enqueue(
        [this, &result, attr_names = std::move(attr_names),
         converted_values = std::move(values), schema = schema,
         itemid = std::move(itemid), cur_shape = std::move(cur_shape),
         shape_and_mask = std::move(shape_and_mask)]() -> absl::Status {
          std::vector<absl::string_view> attr_names_views(attr_names.begin(),
                                                          attr_names.end());
          std::vector<DataSlice> converted_values_vec;
          converted_values_vec.reserve(converted_values->size());
          for (const auto& converted_value : *converted_values) {
            DCHECK(converted_value.has_value());
            converted_values_vec.push_back(*converted_value);
          }
          if (shape_and_mask.has_value()) {
            ASSIGN_OR_RETURN(result,
                             EntityCreator::Like(
                                 GetBag(), *shape_and_mask, attr_names_views,
                                 converted_values_vec, schema, false, itemid));
          } else {
            ASSIGN_OR_RETURN(result, EntityCreator::Shaped(
                                         GetBag(), cur_shape, attr_names_views,
                                         converted_values_vec, schema,
                                         /*override_schema=*/false, itemid));
          }
          return absl::OkStatus();
        });
    return absl::OkStatus();
  }

  // Converts a Python object (dataclass) to an Entity. Since we don't have a
  // schema, we get attribute names and values from the dataclass itself.
  absl::Status ConvertObject(PyObject* py_obj, std::optional<DataSlice> itemid,
                             int cur_depth,
                             internal::TrampolineExecutor& executor,
                             std::optional<DataSlice>& result) {
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

    return ConvertObjectFromAttrNamesAndValues(
        std::move(attr_names_and_values->attr_names),
        std::move(attr_names_and_values->values), std::move(itemid), cur_depth,
        executor, result);
  }

  absl::Status ConvertObjectFromAttrNamesAndValues(
      std::vector<std::string> attr_names, const std::vector<PyObject*> values,
      std::optional<DataSlice> itemid, int cur_depth,
      internal::TrampolineExecutor& executor,
      std::optional<DataSlice>& result) {
    // Create an object DataSlice with the given shape and attributes
    // names/values.

    DCHECK(attr_names.size() == values.size());
    const size_t size = attr_names.size();

    auto converted_values =
        std::make_unique<std::vector<std::optional<DataSlice>>>(size);
    for (int attr_index = 0; attr_index < size; ++attr_index) {
      std::optional<DataSlice> child_itemid_ds;
      if (itemid.has_value()) {
        ASSIGN_OR_RETURN(child_itemid_ds,
                         MakeChildrenItemUuids(
                             itemid, DataSlice::JaggedShape::FlatFromSize(size),
                             attr_names[attr_index]));
      }

      RETURN_IF_ERROR(ConvertImplBreakRecursion(
          {values[attr_index]}, DataSlice::JaggedShape::Empty(), std::nullopt,
          std::move(child_itemid_ds), cur_depth, executor,
          (*converted_values)[attr_index]));
    }

    executor.Enqueue([this, &result, attr_names = std::move(attr_names),
                      converted_values = std::move(converted_values),
                      itemid = std::move(itemid)]() -> absl::Status {
      std::vector<absl::string_view> attr_names_views(attr_names.begin(),
                                                      attr_names.end());
      std::vector<DataSlice> converted_values_vec;
      converted_values_vec.reserve(converted_values->size());
      for (const auto& converted_value : *converted_values) {
        DCHECK(converted_value.has_value());
        converted_values_vec.push_back(*converted_value);
      }
      ASSIGN_OR_RETURN(result,
                       ObjectCreator::Shaped(
                           GetBag(), DataSlice::JaggedShape::Empty(),
                           attr_names_views, converted_values_vec, itemid));
      return absl::OkStatus();
    });
    return absl::OkStatus();
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
  bool dict_as_obj_;
};

}  // namespace

absl::StatusOr<DataSlice> FromPy_V2(PyObject* py_obj,
                                    const std::optional<DataSlice>& schema,
                                    size_t from_dim, bool dict_as_obj,
                                    const std::optional<DataSlice>& itemid) {
  AdoptionQueue adoption_queue;
  DataItem schema_item;
  if (schema) {
    RETURN_IF_ERROR(schema->VerifyIsSchema());
    adoption_queue.Add(*schema);
    schema_item = schema->item();
  }

  FromPyConverter from_py_converter(adoption_queue, dict_as_obj);
  ASSIGN_OR_RETURN(DataSlice res_slice,
                   from_py_converter.Convert(py_obj, schema, from_dim, itemid));

  DataBagPtr res_db = std::move(from_py_converter).GetCreatedBag();
  if (res_db == nullptr) {
    ASSIGN_OR_RETURN(res_db, adoption_queue.GetCommonOrMergedDb());
    // If the result has no associated DataBag but an OBJECT schema was
    // requested, attach an empty DataBag.
    if (res_db == nullptr && IsObjectSchema(schema)) {
      res_db = DataBag::Empty();
    }
  } else {
    RETURN_IF_ERROR(adoption_queue.AdoptInto(*res_db));
    res_db->UnsafeMakeImmutable();
  }
  return res_slice.WithBag(std::move(res_db));
}

}  // namespace koladata::python
