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
#include "py/koladata/base/py_conversions/to_py.h"

#include <Python.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/extract_utils.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/extract.h"
#include "koladata/internal/op_utils/traverser.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/schema_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/py_conversions/dataclasses_util.h"
#include "py/koladata/base/to_py_object.h"
#include "py/koladata/types/wrap_utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/text.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {

using arolla::python::PyObjectPtr;
using internal::DataItem;
using internal::DataSliceImpl;
using internal::ObjectId;

using OptionalText = arolla::OptionalValue<arolla::view_type_t<arolla::Text>>;

absl::StatusOr<PyObjectPtr> WrapDataSliceWithErrorCheck(DataSlice&& ds) {
  PyObjectPtr result = PyObjectPtr::Own(WrapPyDataSlice(std::move(ds)));
  if (result == nullptr) {
    return arolla::python::StatusWithRawPyErr(absl::StatusCode::kInternal,
                                              "could not wrap DataSlice");
  }
  return result;
}

absl::StatusOr<PyObjectPtr> AttrNamePyFromString(
    absl::string_view attr_name_str) {
  PyObjectPtr attr_name_py = PyObjectPtr::Own(
      PyUnicode_FromStringAndSize(attr_name_str.data(), attr_name_str.size()));
  if (attr_name_py == nullptr) {
    return arolla::python::StatusWithRawPyErr(
        absl::StatusCode::kInternal,
        absl::StrFormat(
            "could not create a Python string for an attribute name %s",
            attr_name_str));
  }
  return attr_name_py;
}

// Implementation of the "To Python" visitor.
// This visitor should be used to traverse DataSlice and convert it to a
// Python object. After traversing, one can call
// `PyObjectFromDataSlice(ds, visitor->GetItemToPyConverter())`
// to get the Python object corresponding to the root DataItem.
class ToPyVisitor : internal::AbstractVisitor {
 public:
  ToPyVisitor(bool obj_as_dict, bool include_missing_attrs,
              const absl::flat_hash_set<ObjectId>& objects_not_to_convert,
              DataBagPtr db, internal::DataBagImpl::FallbackSpan fallback_span)
      : obj_as_dict_(obj_as_dict),
        include_missing_attrs_(include_missing_attrs),
        objects_not_to_convert_(objects_not_to_convert),
        db_(std::move(db)),
        fallback_span_(fallback_span) {}

  ItemToPyConverter GetItemToPyConverter(const DataItem& schema) {
    return [&](const DataItem& item) -> absl::StatusOr<PyObjectPtr> {
      return GetCachedPyObjectOrCreatePrimitive(item);
    };
  }

  absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                    const DataItem& schema) final {
    // Visitor has to implement this method, but we don't need it, so leave it
    // as a no-op.
    return item;
  }

  // Pre-visiting Entities in order to decide whether we need to create a
  // `dataclass` or keep it as an Entity. The same for `dict` and `list`.
  // Traverser's visits are post-order, so there can be an issue that in case of
  // self references it first encounters an entity with no attributes and
  // then encounters it again with some attributes (cycle in
  // Koda structure). See `test_self_reference_...`.
  //
  // During pre-visits, these dataclasses are created and during visits, we have
  // a cache hit and just populate the attributes. Alternatively, when
  // populating object attributes in `VisitObject`, `VisitDict` and `VisitList`,
  // we could check that it is an entity and if so, query its attributes.
  absl::Status Previsit(const DataItem& item, const DataItem& schema) final {
    if (!item.holds_value<ObjectId>()) {
      return absl::OkStatus();
    }
    const ObjectId& object_id = item.value<ObjectId>();
    if (object_cache_.contains(object_id)) {
      return absl::OkStatus();
    }

    if (ShouldNotBeConverted(item)) {
      ASSIGN_OR_RETURN(DataSlice ds, DataSlice::Create(item, schema, db_));
      ASSIGN_OR_RETURN(object_cache_[object_id],
                       WrapDataSliceWithErrorCheck(std::move(ds)));
      return absl::OkStatus();
    }
    if (schema.is_itemid_schema()) {
      ASSIGN_OR_RETURN(object_cache_[object_id],
                       PyObjectFromDataItem(item, schema, db_));
      return absl::OkStatus();
    }

    if (!obj_as_dict_ && item.is_entity() && schema.is_struct_schema()) {
      ASSIGN_OR_RETURN(DataSliceImpl attr_names,
                       db_->GetImpl().GetSchemaAttrs(schema, fallback_span_));
      if (attr_names.size() == 0) {
        PyObjectPtr result = PyObjectPtr::Own(PyDict_New());
        if (result == nullptr) {
          return arolla::python::StatusWithRawPyErr(
              absl::StatusCode::kInternal, "could not create a new dict");
        }
        object_cache_[object_id] = std::move(result);
      } else {
        std::vector<absl::string_view> attr_names_vec;
        attr_names_vec.reserve(attr_names.size());
        if (attr_names.dtype() != arolla::GetQType<arolla::Text>()) {
          return absl::InternalError("dtype of attribute names must be STRING");
        }
        if (attr_names.present_count() != attr_names.size()) {
          return absl::InternalError("attributes must be non-empty");
        }
        attr_names.values<arolla::Text>().ForEachPresent(
            [&](int64_t id, absl::string_view attr) {
              if (attr != schema::kSchemaNameAttr) {
                attr_names_vec.push_back(attr);
              }
            });
        ASSIGN_OR_RETURN(
            object_cache_[object_id],
            dataclasses_util_.MakeDataClassInstance(attr_names_vec));
      }
      return absl::OkStatus();
    }
    if (item.is_dict() || (obj_as_dict_ && item.is_entity())) {
      PyObjectPtr result = PyObjectPtr::Own(PyDict_New());
      if (result == nullptr) {
        return arolla::python::StatusWithRawPyErr(
            absl::StatusCode::kInternal, "could not create a new dict");
      }
      object_cache_[object_id] = std::move(result);
      return absl::OkStatus();
    }
    if (item.is_list()) {
      PyObjectPtr result = PyObjectPtr::Own(PyList_New(0));
      if (result == nullptr) {
        return arolla::python::StatusWithRawPyErr(
            absl::StatusCode::kInternal, "could not create a new list");
      }
      object_cache_[object_id] = std::move(result);
      return absl::OkStatus();
    }

    return absl::OkStatus();
  }

  absl::Status VisitList(const DataItem& list, const DataItem& schema,
                         bool is_object_schema,
                         const internal::DataSliceImpl& items) final {
    if (ShouldNotBeConverted(list)) {
      return absl::OkStatus();
    }
    ASSIGN_OR_RETURN(PyObjectPtr result, GetCachedPyObject(list));
    for (const DataItem& item : items) {
      ASSIGN_OR_RETURN(PyObjectPtr py_item,
                       GetCachedPyObjectOrCreatePrimitive(item));
      if (PyList_Append(result.get(), py_item.get()) < 0) {
        return arolla::python::StatusWithRawPyErr(
            absl::StatusCode::kInternal, "could not append an item to a list");
      }
    }

    return absl::OkStatus();
  }

  absl::Status VisitDict(const DataItem& dict, const DataItem& schema,
                         bool is_object_schema, const DataSliceImpl& keys,
                         const DataSliceImpl& values) final {
    if (ShouldNotBeConverted(dict)) {
      return absl::OkStatus();
    }
    ASSIGN_OR_RETURN(PyObjectPtr result, GetCachedPyObject(dict));

    DCHECK_EQ(keys.size(), values.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      // If dict keys are objects, we keep them as data items, because
      // list/dict/dataclass object keys are not hashable in Python.
      const DataItem& key = keys[i];
      DataItem dict_schema = schema;
      if (is_object_schema) {
        ASSIGN_OR_RETURN(dict_schema,
                         db_->GetImpl().GetObjSchemaAttr(dict, fallback_span_));
      }
      ASSIGN_OR_RETURN(
          DataItem key_schema,
          db_->GetImpl().GetSchemaAttr(schema, schema::kDictKeysSchemaAttr,
                                       fallback_span_));
      ASSIGN_OR_RETURN(PyObjectPtr py_key,
                       PyObjectFromDataItem(keys[i], key_schema, nullptr));
      ASSIGN_OR_RETURN(PyObjectPtr py_value,
                       GetCachedPyObjectOrCreatePrimitive(values[i]));
      if (PyDict_SetItem(result.get(), py_key.get(), py_value.get()) < 0) {
        return arolla::python::StatusWithRawPyErr(
            absl::StatusCode::kInternal, "could not set an item in a dict");
      }
    }

    return absl::OkStatus();
  }

  absl::Status VisitObject(
      const DataItem& object, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_values) final {
    DCHECK_EQ(attr_names.size(), attr_values.size());
    if (ShouldNotBeConverted(object)) {
      return absl::OkStatus();
    }
    if (obj_as_dict_ || attr_names.empty()) {
      RETURN_IF_ERROR(CreateDictFromObject(object, schema, is_object_schema,
                                           attr_names, attr_values));
      return absl::OkStatus();
    }

    ASSIGN_OR_RETURN(PyObjectPtr result, GetCachedPyObject(object));
    for (size_t i = 0; i < attr_names.size(); ++i) {
      const arolla::OptionalValue<DataItem>& attr_value = attr_values[i];
      ASSIGN_OR_RETURN(PyObjectPtr py_value,
                       GetCachedPyObjectOrCreatePrimitive(attr_value.value));
      ASSIGN_OR_RETURN(PyObjectPtr attr_name_py,
                       AttrNamePyFromString(attr_names[i].value));
      if (PyObject_SetAttr(result.get(), attr_name_py.get(), py_value.get()) <
          0) {
        return arolla::python::StatusWithRawPyErr(
            absl::StatusCode::kInternal, "could not set object attrubute");
      }
    }

    return absl::OkStatus();
  }

  absl::Status VisitSchema(
      const DataItem& item, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_schema) final {
    // Visitor has to implement this method, but we don't need it, so leave it
    // as a no-op.
    return absl::OkStatus();
  }

 private:
  // Returns a cached Python object for the given `item`.
  // Returns an error if the `item` does not hold ObjectId, or if the object is
  // not found in the cache.
  absl::StatusOr<PyObjectPtr> GetCachedPyObject(const DataItem& item) {
    DCHECK(item.holds_value<ObjectId>());
    auto it = object_cache_.find(item.value<ObjectId>());
    if (it == object_cache_.end()) {
      return absl::InternalError(
          "item holds an object id, but it is not found in cache; probably, it "
          "was not pre-visited.");
    }
    DCHECK(it->second != nullptr);
    return it->second;
  }

  // If the given `item` holds ObjectId, returns a cached Python object for the
  // item (or an error if the object is not found in the cache). Otherwise,
  // returns a new Python primitive for it.
  absl::StatusOr<PyObjectPtr> GetCachedPyObjectOrCreatePrimitive(
      const DataItem& item) {
    if (!item.holds_value<ObjectId>()) {
      return PyObjectFromDataItem(item, DataItem(), db_);
    }
    return GetCachedPyObject(item);
  }

  absl::Status CreateDictFromObject(
      const DataItem& object, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_values) {
    ASSIGN_OR_RETURN(PyObjectPtr result, GetCachedPyObject(object));

    // Sort the attribute names to ensure that the order is deterministic.
    std::vector<absl::string_view> attr_names_vec;
    attr_names_vec.reserve(attr_names.size());
    absl::flat_hash_map<absl::string_view, size_t> attr_name_to_index;
    for (size_t i = 0; i < attr_names.size(); ++i) {
      const OptionalText& attr_name = attr_names[i];
      if (!attr_name.present) {
        continue;
      }
      attr_name_to_index[attr_name.value] = i;
      attr_names_vec.push_back(attr_name.value);
    }
    // Sort the attribute names to ensure that the keys order in the Python
    // dict is deterministic.
    std::sort(attr_names_vec.begin(), attr_names_vec.end());

    for (const absl::string_view& attr_name_str : attr_names_vec) {
      const size_t attr_index = attr_name_to_index[attr_name_str];

      const arolla::OptionalValue<DataItem>& attr_value =
          attr_values[attr_index];
      ASSIGN_OR_RETURN(PyObjectPtr attr_name_py,
                       AttrNamePyFromString(attr_name_str));
      ASSIGN_OR_RETURN(PyObjectPtr py_value,
                       GetCachedPyObjectOrCreatePrimitive(attr_value.value));
      if (py_value.get() != Py_None || include_missing_attrs_) {
        if (PyDict_SetItem(result.get(), attr_name_py.get(), py_value.get()) <
            0) {
          return arolla::python::StatusWithRawPyErr(
              absl::StatusCode::kInternal, "could not set an item in a dict");
        }
      }
    }
    return absl::OkStatus();
  }

  bool ShouldNotBeConverted(const DataItem& item) {
    return item.holds_value<ObjectId>() &&
           objects_not_to_convert_.contains(item.value<ObjectId>());
  }

  absl::flat_hash_map<ObjectId, arolla::python::PyObjectPtr> object_cache_;

  bool obj_as_dict_ = false;
  bool include_missing_attrs_ = false;
  // Ids of leaf DataItems, which will remain as DataItems and not be converted
  // to Python objects.
  const absl::flat_hash_set<ObjectId>& objects_not_to_convert_;

  DataClassesUtil dataclasses_util_;
  DataBagPtr db_;
  internal::DataBagImpl::FallbackSpan fallback_span_;
};

absl::Nullable<PyObject*> ToPyImplInternal(
    const DataSlice& ds, DataBagPtr bag, bool obj_as_dict,
    bool include_missing_attrs, const absl::flat_hash_set<ObjectId>& leaf_ids) {
  if (ds.IsEmpty() || bag == nullptr ||
      GetNarrowedSchema(ds).is_primitive_schema()) {
    ASSIGN_OR_RETURN(PyObjectPtr res, PyObjectFromDataSlice(ds),
                     arolla::python::SetPyErrFromStatus(_));
    return res.release();
  }
  const DataSlice& schema = ds.GetSchema();
  DCHECK(bag != nullptr);

  FlattenFallbackFinder fb_finder(*bag);
  const internal::DataBagImpl::FallbackSpan fallback_span =
      fb_finder.GetFlattenFallbacks();
  std::shared_ptr<ToPyVisitor> visitor = std::make_shared<ToPyVisitor>(
      obj_as_dict, include_missing_attrs, leaf_ids, bag, fallback_span);
  internal::Traverser<ToPyVisitor> traverse_op(bag->GetImpl(), fallback_span,
                                               visitor);

  RETURN_IF_ERROR(ds.VisitImpl([&]<class T>(const T& impl) -> absl::Status {
    if constexpr (std::is_same_v<T, DataItem>) {
      DataSliceImpl ds_impl = DataSliceImpl::Create(/*size=*/1, impl);
      RETURN_IF_ERROR(traverse_op.TraverseSlice(ds_impl, schema.item()));
    } else {
      RETURN_IF_ERROR(traverse_op.TraverseSlice(impl, schema.item()));
    }
    return absl::OkStatus();
  })).With(arolla::python::SetPyErrFromStatus);

  ASSIGN_OR_RETURN(
      PyObjectPtr res,
      PyObjectFromDataSlice(ds, visitor->GetItemToPyConverter(schema.item())),
      arolla::python::SetPyErrFromStatus(_));
  return res.release();
}

absl::Nullable<PyObject*> ToPyImpl(const DataSlice& ds, DataBagPtr bag,
                                   int max_depth, bool obj_as_dict,
                                   bool include_missing_attrs) {
  // When `max_depth != -1`, we want objects/dicts/lists at `max_depth`
  // to be kept as DataItems and not converted to Python objects.
  // To do that, we extract a DataSlice, and keep track of the leaf DataItems.
  absl::flat_hash_set<ObjectId> objects_not_to_convert;
  internal::LeafCallback leaf_callback = [&](const DataSliceImpl& slice,
                                             const DataItem& schema) {
    for (const DataItem& item : slice) {
      if (item.holds_value<ObjectId>())
        objects_not_to_convert.insert(item.value<ObjectId>());
    }
    return absl::OkStatus();
  };

  if (ds.GetBag() != nullptr) {
    ASSIGN_OR_RETURN(
        const DataSlice extracted_ds,
        koladata::extract_utils_internal::ExtractWithSchema(
            {}, ds, ds.GetSchema(), max_depth, std::move(leaf_callback)),
        arolla::python::SetPyErrFromStatus(_));
    return ToPyImplInternal(extracted_ds, ds.GetBag(), obj_as_dict,
                            include_missing_attrs, objects_not_to_convert);
  }
  return ToPyImplInternal(ds, nullptr, obj_as_dict, include_missing_attrs,
                          objects_not_to_convert);
}

}  // namespace

absl::Nullable<PyObject*> PyDataSlice_to_py(PyObject* self,
                                            PyObject* const* py_args,
                                            Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  if (nargs != 3) {
    PyErr_Format(PyExc_ValueError,
                 "DataSlice._to_py_impl accepts exactly 3 arguments, got %d",
                 nargs);
    return nullptr;
  }
  const auto& ds = UnsafeDataSliceRef(self);
  if (!PyLong_Check(py_args[0])) {
    PyErr_Format(PyExc_TypeError, "expecting max_depth to be an int, got %s",
                 Py_TYPE(py_args[0])->tp_name);
    return nullptr;
  }
  const int64_t max_depth = PyLong_AsLong(py_args[0]);
  if (PyErr_Occurred()) {
    return nullptr;
  }

  if (!PyBool_Check(py_args[1])) {
    PyErr_Format(PyExc_TypeError, "expecting obj_as_dict to be a bool, got %s",
                 Py_TYPE(py_args[1])->tp_name);
    return nullptr;
  }
  const bool obj_as_dict = py_args[1] == Py_True;

  if (!PyBool_Check(py_args[2])) {
    PyErr_Format(PyExc_TypeError,
                 "expecting include_missing_attrs to be a bool, got %s",
                 Py_TYPE(py_args[2])->tp_name);
    return nullptr;
  }
  const bool include_missing_attrs = py_args[2] == Py_True;

  return ToPyImpl(ds, ds.GetBag(), max_depth, obj_as_dict,
                  include_missing_attrs);
}

}  // namespace koladata::python
