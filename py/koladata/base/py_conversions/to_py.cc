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
#include "py/koladata/base/py_conversions/to_py.h"

#include <Python.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
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
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/text.h"
#include "arolla/util/view_types.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/extract_utils.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/extract.h"
#include "koladata/internal/op_utils/traverser.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/schema_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/py_conversions/dataclasses_util.h"
#include "py/koladata/base/to_py_object.h"
#include "py/koladata/base/wrap_utils.h"
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

std::string PyObjectTypeName(PyObject* py_obj) {
  if (!PyType_Check(py_obj)) {
    return "<unknown>";
  }
  return ((PyTypeObject*)py_obj)->tp_name;
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
              DataBagPtr db, internal::DataBagImpl::FallbackSpan fallback_span,
              PyObject* output_class)
      : obj_as_dict_(obj_as_dict),
        include_missing_attrs_(include_missing_attrs),
        objects_not_to_convert_(objects_not_to_convert),
        db_(std::move(db)),
        fallback_span_(fallback_span),
        output_class_(PyObjectPtr::NewRef(output_class)) {}

  ItemToPyConverter GetItemToPyConverter(const DataItem& schema) {
    return [&](const DataItem& item) -> absl::StatusOr<PyObjectPtr> {
      return GetConvertedPyObjectOrCreatePrimitive(item, output_class_.get());
    };
  }

  absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                    const DataItem& schema) final {
    // Visitor has to implement this method, but we don't need it, so leave it
    // as a no-op.
    return item;
  }

  // If output_class_ is provided, we need to compute the class for the `item`
  // and store it in the `class_cache_by_object_id` and `classes_by_path_`.
  absl::Status ComputeAndStoreClassForItem(
      const DataItem& from_item, absl::string_view from_item_attr_name,
      const DataItem& item) {
    const ObjectId& object_id = item.value<ObjectId>();

    if (from_item_attr_name == kSliceItemPath) {
      // Root case; take the output object as is.
      class_cache_by_object_id_[object_id] = output_class_;
      return absl::OkStatus();
    }

    ASSIGN_OR_RETURN(PyObject * parent_class, GetCachedClass(from_item));
    std::pair<PyObject*, std::string> path_key(parent_class,
                                               from_item_attr_name);
    PyObjectPtr& class_cached_by_path = classes_by_path_[std::move(path_key)];
    if (class_cached_by_path != nullptr) {
      class_cache_by_object_id_[object_id] = class_cached_by_path;
      return absl::OkStatus();
    }

    // In this case we are going to get class field type for an object, so
    // it is not a primitive here.
    ASSIGN_OR_RETURN(class_cached_by_path,
                     dataclasses_util_.GetClassFieldType(
                         PyObjectPtr::NewRef(parent_class), from_item_attr_name,
                         /*for_primitive=*/false));
    DCHECK(class_cached_by_path != nullptr);

    // We need to check that the same object is not reached with different
    // paths. We do this check only if this path was not visited before.
    auto& class_cached_by_object_id = class_cache_by_object_id_[object_id];
    if (class_cached_by_object_id == nullptr) {
      class_cached_by_object_id = class_cached_by_path;
      return absl::OkStatus();
    }

    // Koda object is the same, but Python class is different.
    if (class_cached_by_object_id.get() != class_cached_by_path.get()) {
      return absl::InternalError(absl::StrFormat(
          "same object is reached with different classes: %s and %s",
          PyObjectTypeName(class_cached_by_object_id.get()),
          PyObjectTypeName(class_cached_by_path.get())));
    }
    return absl::OkStatus();
  }

  // Pre-visiting Entities in order to decide whether we need to create a
  // `dataclass` or keep it as an Entity. The same for `dict` and `list`.
  // Traverser's visits are post-order, so there can be an issue that in case
  // of self references it first encounters an entity with no attributes and
  // then encounters it again with some attributes (cycle in
  // Koda structure). See `test_self_reference_...`.
  //
  // During pre-visits, these dataclasses are created and during visits, we
  // have a cache hit and just populate the attributes. Alternatively, when
  // populating object attributes in `VisitObject`, `VisitDict` and
  // `VisitList`, we could check that it is an entity and if so, query its
  // attributes.
  //
  // Returns false if the item should not be converted. Visit* methods will
  // not be called for this item. And the item would not be traversed further.
  absl::StatusOr<bool> Previsit(
      const DataItem& from_item, const DataItem& from_schema,
      const std::optional<absl::string_view>& from_item_attr_name,
      const DataItem& item, const DataItem& schema) final {
    if (!item.holds_value<ObjectId>()) {
      return true;
    }

    if (output_class_.get() != Py_None && schema.is_itemid_schema()) {
      return absl::InvalidArgumentError(
          "itemid is not supported together with output_class");
    }

    const ObjectId& object_id = item.value<ObjectId>();
    if (output_class_.get() != Py_None) {
      // `output_class` is provided, so we need to compute the class for
      // the `item`.
      if (!from_item_attr_name.has_value()) {
        return true;
      }
      RETURN_IF_ERROR(
          ComputeAndStoreClassForItem(from_item, *from_item_attr_name, item));
      return true;
    }

    if (schema.holds_value<ObjectId>()) {
      auto schema_object_id = schema.value<ObjectId>();
      auto [it, was_inserted] =
          item_schemas_.try_emplace(object_id, schema_object_id);
      if (!was_inserted && it->second != schema_object_id) {
        return absl::InternalError(
            absl::StrFormat("cannot convert object %v, that is reached with "
                            "different schemas: %v and %v",
                            object_id, it->second, schema_object_id));
      }
    }
    if (converted_object_cache_.contains(object_id)) {
      return !ShouldNotBeConverted(item);
    }

    if (ShouldNotBeConverted(item)) {
      ASSIGN_OR_RETURN(DataSlice ds, DataSlice::Create(item, schema, db_));
      ASSIGN_OR_RETURN(converted_object_cache_[object_id],
                       WrapDataSliceWithErrorCheck(std::move(ds)));
      // This ItemId will not be further traversed.
      return false;
    }
    if (schema.is_itemid_schema()) {
      ASSIGN_OR_RETURN(converted_object_cache_[object_id],
                       PyObjectFromDataItem(item, schema, db_));
      return true;
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
        converted_object_cache_[object_id] = std::move(result);
        return true;
      }
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
            if (attr != schema::kSchemaNameAttr &&
                attr != schema::kSchemaMetadataAttr) {
              // TODO: Add tests for metadata support.
              attr_names_vec.push_back(attr);
            }
          });

      // Do not store the class in the cache, just create an instance.
      ASSIGN_OR_RETURN(converted_object_cache_[object_id],
                       dataclasses_util_.MakeDataClassInstance(attr_names_vec));
      return true;
    }
    if (item.is_dict() || (obj_as_dict_ && item.is_entity())) {
      PyObjectPtr result = PyObjectPtr::Own(PyDict_New());
      if (result == nullptr) {
        return arolla::python::StatusWithRawPyErr(
            absl::StatusCode::kInternal, "could not create a new dict");
      }
      converted_object_cache_[object_id] = std::move(result);
      return true;
    }
    if (item.is_list()) {
      PyObjectPtr result = PyObjectPtr::Own(PyList_New(0));
      if (result == nullptr) {
        return arolla::python::StatusWithRawPyErr(
            absl::StatusCode::kInternal, "could not create a new list");
      }
      converted_object_cache_[object_id] = std::move(result);
      return true;
    }

    return true;
  }

  absl::Status AddItemsToPyList(const DataItem& list, PyObjectPtr py_list,
                                const DataSliceImpl& items) {
    DCHECK(PyList_CheckExact(py_list.get()));
    ASSIGN_OR_RETURN(PyObjectPtr attr_class,
                     GetAttrClass(list, schema::kListItemsSchemaAttr));
    for (const DataItem& item : items) {
      ASSIGN_OR_RETURN(
          PyObjectPtr py_item,
          GetConvertedPyObjectOrCreatePrimitive(item, attr_class.get()));
      if (PyList_Append(py_list.get(), py_item.get()) < 0) {
        return arolla::python::StatusWithRawPyErr(
            absl::StatusCode::kInternal, "could not append an item to a list");
      }
    }
    return absl::OkStatus();
  }

  absl::Status VisitList(const DataItem& list, const DataItem& schema,
                         bool is_object_schema,
                         const internal::DataSliceImpl& items) final {
    if (ShouldNotBeConverted(list)) {
      return absl::OkStatus();
    }

    // Since everything inside SimpleNamespace is a SimpleNamespace, we need
    // to apply the following logic:
    // 1. If the list was already created, we only need to add items to it.
    // 2. Otherwise, we get the class of the list (which can only be `tuple`
    // or `list`). If it is a simple namespace , set output_class to `list`.
    // Now we create an instance of `output_class(items=...)`. We do it
    // because we cannot add elements to a tuple, so we cannot create it
    // during Previsit.

    ObjectId list_id = list.value<ObjectId>();

    // This part can only happen if output_class_ is not provided; in that case
    // tuples are not supported, only lists, and they are created in Previsit.

    // We need to be careful not to invalidate the result reference.
    PyObjectPtr& result = converted_object_cache_[list_id];
    if (result != nullptr) {
      return AddItemsToPyList(list, result, items);
    }

    // Only list[T], tuple[T], or SimpleNamespace types are expected here.
    // SimpleNamespace is treated as list.
    ASSIGN_OR_RETURN(PyObject * result_class, GetCachedClass(list));
    ASSIGN_OR_RETURN(PyObjectPtr real_result_class,
                     DecayGenericAlias(result_class));
    ASSIGN_OR_RETURN(PyObjectPtr attr_class,
                     GetAttrClass(list, schema::kListItemsSchemaAttr));
    if (real_result_class.get() == (PyObject*)&PyTuple_Type) {
      result = PyObjectPtr::Own(PyTuple_New(items.size()));
    } else {
      // The type can be either list or SimpleNamespace here.
      result = PyObjectPtr::Own(PyList_New(items.size()));
    }
    if (result == nullptr) {
      return arolla::python::StatusWithRawPyErr(
          absl::StatusCode::kInternal, "could not create a new list/tuple");
    }
    absl::Span<PyObject*> span;
    arolla::python::PyTuple_AsSpan(result.get(), &span);
    for (size_t i = 0; i < items.size() && i < span.size(); ++i) {
      ASSIGN_OR_RETURN(
          PyObjectPtr py_item,
          GetConvertedPyObjectOrCreatePrimitive(items[i], attr_class.get()));
      span[i] = py_item.release();
    }
    return absl::OkStatus();
  }

  // If the type_or_alias is a generic alias, returns its origin.
  // Otherwise, returns the type_or_alias itself.
  absl::StatusOr<PyObjectPtr> DecayGenericAlias(PyObject* type_or_alias) {
    if (Py_TYPE(type_or_alias) != &Py_GenericAliasType) {
      return PyObjectPtr::NewRef(type_or_alias);
    }
    PyObjectPtr result =
        PyObjectPtr::Own(PyObject_GetAttrString(type_or_alias, "__origin__"));
    if (result == nullptr) {
      return arolla::python::StatusWithRawPyErr(
          absl::StatusCode::kInternal,
          "could not get __origin__ from a generic alias");
    }
    return result;
  }

  absl::Status VisitDict(const DataItem& dict, const DataItem& schema,
                         bool is_object_schema, const DataSliceImpl& keys,
                         const DataSliceImpl& values) final {
    if (ShouldNotBeConverted(dict)) {
      return absl::OkStatus();
    }
    ObjectId dict_id = dict.value<ObjectId>();
    // We need to be careful not to invalidate the result reference.
    PyObjectPtr& result = converted_object_cache_[dict_id];

    if (result == nullptr) {
      // This can only happen with output_class_ provided.
      result = PyObjectPtr::Own(PyDict_New());
      if (result == nullptr) {
        return arolla::python::StatusWithRawPyErr(
            absl::StatusCode::kInternal, "could not create a new dict");
      }
    }

    // In case of output_class_ is provided, these will fail if the class for
    // this dict is not a dict.
    PyObjectPtr dict_keys_class = PyObjectPtr::NewRef(Py_None);
    PyObjectPtr dict_values_class = PyObjectPtr::NewRef(Py_None);
    if (output_class_.get() != Py_None) {
      ASSIGN_OR_RETURN(dict_keys_class,
                       GetAttrClass(dict, schema::kDictKeysSchemaAttr));
      ASSIGN_OR_RETURN(dict_values_class,
                       GetAttrClass(dict, schema::kDictValuesSchemaAttr));
    }

    DCHECK_EQ(keys.size(), values.size());
    DataItem dict_schema = schema;
    if (is_object_schema) {
      ASSIGN_OR_RETURN(dict_schema,
                       db_->GetImpl().GetObjSchemaAttr(dict, fallback_span_));
    }
    ASSIGN_OR_RETURN(DataItem key_schema,
                     db_->GetImpl().GetSchemaAttr(
                         schema, schema::kDictKeysSchemaAttr, fallback_span_));

    for (size_t i = 0; i < keys.size(); ++i) {
      const DataItem& key = keys[i];

      PyObjectPtr py_key;

      if (output_class_.get() != Py_None) {
        // If output_class_ is provided, we convert the key to a Python object.
        // TODO: we can reconsider this behavior. and only
        // support primitive keys.
        ASSIGN_OR_RETURN(py_key, GetConvertedPyObjectOrCreatePrimitive(
                                     key, dict_keys_class.get()));
      } else {
        // In the case when output_class_ is not provided, If dict keys are
        // objects, we keep them as data items (without data bag being
        // assigned), because list/dict/dataclass object keys are not hashable
        // in Python.
        ASSIGN_OR_RETURN(py_key,
                         PyObjectFromDataItem(key, key_schema, /*db=*/nullptr));
      }

      ASSIGN_OR_RETURN(PyObjectPtr py_value,
                       GetConvertedPyObjectOrCreatePrimitive(
                           values[i], dict_values_class.get()));
      if (PyDict_SetItem(result.get(), py_key.get(), py_value.get()) < 0) {
        return arolla::python::StatusWithRawPyErr(
            absl::StatusCode::kInternal, "could not set an item in a dict");
      }
    }

    return absl::OkStatus();
  }

  // Sets the attributes of the given Python object.
  absl::Status SetObjectAttributes(
      PyObjectPtr object, const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_values) {
    for (size_t i = 0; i < attr_names.size(); ++i) {
      const OptionalText& attr_name = attr_names[i];
      const arolla::OptionalValue<DataItem>& attr_value = attr_values[i];
      if (!attr_value.present || !attr_name.present) {
        continue;
      }
      ASSIGN_OR_RETURN(
          PyObjectPtr py_value,
          GetConvertedPyObjectOrCreatePrimitive(attr_value.value, Py_None));
      ASSIGN_OR_RETURN(PyObjectPtr attr_name_py,
                       AttrNamePyFromString(attr_name.value));
      if (PyObject_SetAttr(object.get(), attr_name_py.get(), py_value.get()) <
          0) {
        return arolla::python::StatusWithRawPyErr(
            absl::StatusCode::kInternal, "could not set object attrubute");
      }
    }
    return absl::OkStatus();
  }

  // Returns the class of the given attribute of the given object.
  // If the output_class_ is not provided, returns Py_None.
  absl::StatusOr<PyObjectPtr> GetAttrClass(const DataItem& object,
                                           absl::string_view attr_name) {
    if (output_class_.get() == Py_None) {
      return output_class_;
    }
    ASSIGN_OR_RETURN(PyObject * result_class, GetCachedClass(object));
    DCHECK(result_class != nullptr);

    std::pair<PyObject*, std::string> key{result_class, attr_name};
    PyObjectPtr& attr_class = classes_by_path_[std::move(key)];
    if (attr_class == nullptr) {
      // If the attr would have been an object, it would have been already
      // pre-visited and cached; at this point it can only be a primitive.
      ASSIGN_OR_RETURN(attr_class, dataclasses_util_.GetClassFieldType(
                                       PyObjectPtr::NewRef(result_class),
                                       attr_name, /*for_primitive=*/true));
    }
    return attr_class;
  }

  absl::Status VisitObject(
      const DataItem& object, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_values) final {
    DCHECK_EQ(attr_names.size(), attr_values.size());

    if (ShouldNotBeConverted(object)) {
      return absl::OkStatus();
    }
    if (obj_as_dict_) {
      return CreateDictFromObject(object, schema, is_object_schema, attr_names,
                                  attr_values);
    }

    ObjectId object_id = object.value<ObjectId>();
    PyObjectPtr& result = converted_object_cache_[object_id];

    if (result != nullptr) {
      // Object was already converted, just set the attributes.
      return SetObjectAttributes(result, attr_names, attr_values);
    }
    ASSIGN_OR_RETURN(PyObject * result_class, GetCachedClass(object));
    DCHECK(result_class != nullptr);
    ASSIGN_OR_RETURN(PyObjectPtr simple_namespace_class,
                     dataclasses_util_.GetSimpleNamespaceClass());
    const bool result_class_is_simple_namespace =
        simple_namespace_class.get() == result_class;

    std::vector<std::string> attr_names_vec;
    attr_names_vec.reserve(attr_names.size());
    std::vector<PyObjectPtr> attr_values_vec;
    attr_values_vec.reserve(attr_names.size());
    for (size_t i = 0; i < attr_names.size(); ++i) {
      const OptionalText& attr_name = attr_names[i];
      if (!attr_name.present) {
        continue;
      }

      const arolla::OptionalValue<DataItem>& attr_value = attr_values[i];
      if (!attr_value.present || !attr_value.value.has_value()) {
        // If attribute exists but is not optional, an error will be returned.
        ASSIGN_OR_RETURN(
            bool attr_for_none_exists,
            dataclasses_util_.HasOptionalField(
                PyObjectPtr::NewRef(result_class), attr_name.value));

        if (attr_for_none_exists) {
          // These is an optional attribute, so we set it to None.
          attr_names_vec.emplace_back(attr_name.value);
          attr_values_vec.emplace_back(PyObjectPtr::NewRef(Py_None));
        }
        continue;
      }
      PyObjectPtr attr_class = PyObjectPtr::NewRef(Py_None);
      if (!result_class_is_simple_namespace) {
        ASSIGN_OR_RETURN(attr_class, GetAttrClass(object, attr_name.value));
        if (attr_class.get() == Py_None) {
          continue;  // no field with this name in the class
        }
      }
      ASSIGN_OR_RETURN(PyObjectPtr py_value,
                       GetConvertedPyObjectOrCreatePrimitive(attr_value.value,
                                                             attr_class.get()));
      attr_names_vec.push_back(std::string(attr_name.value));
      attr_values_vec.push_back(std::move(py_value));
    }

    ASSIGN_OR_RETURN(result, dataclasses_util_.CreateClassInstanceKwargs(
                                 PyObjectPtr::NewRef(result_class),
                                 attr_names_vec, attr_values_vec));

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
  // Returns a converted Python object for the given `item`.
  // Returns an error if the `item` does not hold ObjectId.
  // If the object is not found in the converted object cache, an error is
  // returned.
  // If the object is not found in the converted object cache, returns an error.
  absl::StatusOr<PyObjectPtr> GetConvertedPyObject(const DataItem& item) {
    DCHECK(item.holds_value<ObjectId>());
    ObjectId object_id = item.value<ObjectId>();
    const auto it = converted_object_cache_.find(object_id);
    if (it != converted_object_cache_.end() && it->second != nullptr) {
      return it->second;
    }
    if (item.is_schema()) {
      return absl::InvalidArgumentError(
          "schema is not supported in to_py conversion");
    }

    return absl::InvalidArgumentError(
        "object was not pre-visited; recursive structures with "
        "output_class are not supported");
  }

  // Returns a converted Python class for the given `item`.
  // Returns an error if the `item` does not hold ObjectId, or if the object
  // is not found in the converted object cache.
  absl::StatusOr<PyObject*> GetCachedClass(const DataItem& item) {
    if (!item.holds_value<ObjectId>()) {
      return absl::InternalError(
          "GetCachedClass is called, but item does not hold ObjectId");
    }
    if (output_class_.get() == Py_None) {
      return absl::InternalError(
          "GetCachedClass is called, but output_class is not provided");
    }
    ObjectId object_id = item.value<ObjectId>();
    auto it = class_cache_by_object_id_.find(object_id);
    if (it == class_cache_by_object_id_.end()) {
      if (item.is_schema()) {
        return absl::InvalidArgumentError(
            "schema is not supported in to_py conversion");
      }
      return absl::InternalError(
          "item holds an object id, but its class is not found in cache; "
          "probably, it was not pre-visited");
    }
    DCHECK(it->second != nullptr);
    return it->second.get();
  }

  // If the given `item` holds ObjectId, returns a cached Python object for
  // the item (or an error if the object is not found in the converted object
  // cache). Otherwise, returns a new Python primitive for it.
  absl::StatusOr<PyObjectPtr> GetConvertedPyObjectOrCreatePrimitive(
      const DataItem& item, PyObject* output_primitive_type) {
    if (!item.holds_value<ObjectId>()) {
      return PyObjectFromDataItem(item, DataItem(), db_, output_primitive_type);
    }
    return GetConvertedPyObject(item);
  }

  absl::Status CreateDictFromObject(
      const DataItem& object, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_values) {
    ASSIGN_OR_RETURN(PyObjectPtr result, GetConvertedPyObject(object));
    if (output_class_.get() != Py_None) {
      return absl::InvalidArgumentError(
          "obj_as_dict cannot be used with output_class");
    }

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
      ASSIGN_OR_RETURN(
          PyObjectPtr py_value,
          GetConvertedPyObjectOrCreatePrimitive(attr_value.value, Py_None));
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

  absl::flat_hash_map<ObjectId, arolla::python::PyObjectPtr>
      converted_object_cache_;

  // Cache for Python classes (e.g. dataclasses, SimpleNamespace).
  // Please note that the cache is used only if `output_class` is provided.
  // Otherwise `DataclassesUtil` is caching `dataclasses` on its own.
  // This cache is filled during the pre-visit stage (instead of
  // converted_object_cache_) and used during the visit stage to instantiate
  // the class for objects/entities/lists. Please note that it also used to
  // check if the same object is reached by different classes to fail early.
  absl::flat_hash_map<ObjectId, arolla::python::PyObjectPtr>
      class_cache_by_object_id_;

  // Cache for Python classes that are accessed by path.
  // It is used to avoid looking up the same path multiple times for the same
  // Python class.
  // P.ex. for
  // class Obj1:
  //  obj2: Obj2
  //  obj3: Obj3
  // classes_by_path_ will be:
  // { (Obj1, 'obj2'): Obj2, (Obj1, 'obj3'): Obj3 }
  absl::flat_hash_map<std::pair<PyObject*, std::string>, PyObjectPtr>
      classes_by_path_;

  absl::flat_hash_map<ObjectId, ObjectId> item_schemas_;

  bool obj_as_dict_ = false;
  bool include_missing_attrs_ = false;
  // Ids of leaf DataItems, which will remain as DataItems and not be
  // converted to Python objects.
  const absl::flat_hash_set<ObjectId>& objects_not_to_convert_;

  DataClassesUtil dataclasses_util_;
  DataBagPtr db_;
  internal::DataBagImpl::FallbackSpan fallback_span_;
  PyObjectPtr output_class_;
};

PyObject* absl_nullable ToPyImplInternal(
    const DataSlice& ds, DataBagPtr bag, bool obj_as_dict,
    bool include_missing_attrs, const absl::flat_hash_set<ObjectId>& leaf_ids,
    PyObject* output_class) {
  if (ds.IsEmpty() || bag == nullptr ||
      GetNarrowedSchema(ds).is_primitive_schema()) {
    ASSIGN_OR_RETURN(
        PyObjectPtr res,
        PyObjectFromDataSlice(ds, /*optional_converter=*/nullptr, output_class),
        arolla::python::SetPyErrFromStatus(_));
    return res.release();
  }
  if (obj_as_dict && output_class != Py_None) {
    arolla::python::SetPyErrFromStatus(absl::InvalidArgumentError(
        "obj_as_dict cannot be used with output_class"));
    return nullptr;
  }
  const DataSlice& schema = ds.GetSchema();
  DCHECK(bag != nullptr);

  FlattenFallbackFinder fb_finder(*bag);
  const internal::DataBagImpl::FallbackSpan fallback_span =
      fb_finder.GetFlattenFallbacks();
  // We use original DataBag in ToPyVisitor.
  std::shared_ptr<ToPyVisitor> visitor =
      std::make_shared<ToPyVisitor>(obj_as_dict, include_missing_attrs,
                                    leaf_ids, bag, fallback_span, output_class);
  // We use extracted DataBag for traversal.
  internal::Traverser<ToPyVisitor> traverse_op(bag->GetImpl(), fallback_span,
                                               visitor);

  if (output_class != Py_None && !leaf_ids.empty()) {
    return arolla::python::SetPyErrFromStatus(absl::InvalidArgumentError(
        "object depth exceeds the maximum depth, which is not supported "
        "together with output_class"));
  }

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

PyObject* absl_nullable ToPyImpl(const DataSlice& ds, DataBagPtr bag,
                                 int max_depth, bool obj_as_dict,
                                 bool include_missing_attrs,
                                 PyObject* output_class) {
  // When `max_depth != -1`, we want objects/dicts/lists at `max_depth`
  // to be kept as DataItems and not converted to Python objects.
  // To do that, we extract a DataSlice, and keep track of the leaf DataItems.
  absl::flat_hash_set<ObjectId> objects_not_to_convert;
  internal::LeafCallback leaf_callback = [&](const DataSliceImpl& slice,
                                             const DataItem& schema) {
    for (const DataItem& item : slice) {
      if (item.holds_value<ObjectId>()) {
        objects_not_to_convert.insert(item.value<ObjectId>());
      }
    }
    return absl::OkStatus();
  };

  if (ds.GetBag() != nullptr) {
    ASSIGN_OR_RETURN(
        const DataSlice extracted_ds,
        koladata::extract_utils_internal::ExtractWithSchema(
            ds, ds.GetSchema(), max_depth,
            /*casting_callback=*/std::nullopt, std::move(leaf_callback)),
        arolla::python::SetPyErrFromStatus(_));
    return ToPyImplInternal(extracted_ds, ds.GetBag(), obj_as_dict,
                            include_missing_attrs, objects_not_to_convert,
                            output_class);
  }
  return ToPyImplInternal(ds, nullptr, obj_as_dict, include_missing_attrs,
                          objects_not_to_convert, output_class);
}

}  // namespace

PyObject* absl_nullable PyDataSlice_to_py(PyObject* self,
                                          PyObject* const* py_args,
                                          Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  if (nargs != 4) {
    PyErr_Format(PyExc_ValueError,
                 "DataSlice._to_py_impl accepts exactly 4 arguments, got %d",
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
  PyObject* output_class = py_args[3];
  if (output_class == nullptr) {
    PyErr_Format(PyExc_TypeError,
                 "expecting output_class to be a class, got nullptr");
    return nullptr;
  }

  return ToPyImpl(ds, ds.GetBag(), max_depth, obj_as_dict,
                  include_missing_attrs, output_class);
}

}  // namespace koladata::python
