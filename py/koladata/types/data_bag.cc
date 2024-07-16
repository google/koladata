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
#include "py/koladata/types/data_bag.h"

#include <Python.h>

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_bag_comparison.h"
#include "koladata/data_bag_repr.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/object_factories.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/types/boxing.h"
#include "py/koladata/types/py_utils.h"
#include "py/koladata/types/wrap_utils.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {

using arolla::python::SetPyErrFromStatus;

// classmethod
absl::Nullable<PyObject*> PyDataBag_empty(PyTypeObject* cls) {
  arolla::python::DCheckPyGIL();
  return arolla::python::MakePyQValue(
      PyDataBag_Type(), arolla::TypedValue::FromValue(DataBag::Empty()));
}

const DataSlice& AnySchema() {
  static const absl::NoDestructor<DataSlice> any_schema{
      DataSlice::Create(internal::DataItem(schema::kAny),
                        internal::DataItem(schema::kSchema))
          .value()};
  return *any_schema;
}

// Returns an Arolla NamedTuple of DataSlices created from `**kwargs`.
absl::Nullable<PyObject*> PyDataBag_kwargs_to_namedtuple(
    PyObject* self, PyObject* const* py_args, Py_ssize_t nargs,
    PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true);
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  const DataBagPtr& self_db = UnsafeDataBagPtr(self);
  AdoptionQueue adoption_queue;
  ASSIGN_OR_RETURN(
      std::vector<DataSlice> values,
      ConvertArgsToDataSlices(self_db, args.kw_values, adoption_queue),
      SetPyErrFromStatus(_));
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_db)).With(SetPyErrFromStatus);
  std::vector<arolla::TypedRef> typed_refs;
  typed_refs.reserve(values.size());
  for (const auto& value : values) {
    typed_refs.push_back(arolla::TypedRef::FromValue(value));
  }
  arolla::QTypePtr tuple_qtype =
      arolla::MakeTupleQType(std::vector<arolla::QTypePtr>(
          values.size(), arolla::GetQType<DataSlice>()));
  ASSIGN_OR_RETURN(
      arolla::QTypePtr named_tuple_qtype,
      arolla::MakeNamedTupleQType(
          std::vector<std::string>(args.kw_names.begin(), args.kw_names.end()),
          tuple_qtype),
      SetPyErrFromStatus(_));
  ASSIGN_OR_RETURN(
      arolla::TypedValue named_tuple,
      arolla::TypedValue::FromFields(named_tuple_qtype, typed_refs),
      SetPyErrFromStatus(_));
  return arolla::python::WrapAsPyQValue(named_tuple);
}

// Helper factory functors to adapt interface of EntityCreator / ObjectCreator
// to ProcessObjectCreation:
// * EntityCreatorHelper
// * ObjectCreatorHelper
//
// They invoke UniversalConverter or create Entity / Object from **kwargs.
struct EntityCreatorHelper {
  static constexpr absl::string_view kKodaName = "entity";

  static absl::StatusOr<DataSlice> FromAttributes(
      const std::vector<absl::string_view>& attr_names,
      const std::vector<DataSlice>& values,
      const std::optional<DataSlice>& schema_arg, bool update_schema,
      const DataBagPtr& db, AdoptionQueue& adoption_queue) {
    if (schema_arg) {
      adoption_queue.Add(*schema_arg);
    }
    return EntityCreator::FromAttrs(db, attr_names, values, schema_arg,
                                    update_schema);
  }

  static absl::StatusOr<DataSlice> Shaped(
      DataSlice::JaggedShape shape,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& schema_arg, bool update_schema,
      const DataBagPtr& db, AdoptionQueue& adoption_queue) {
    if (schema_arg) {
      adoption_queue.Add(*schema_arg);
    }
    return EntityCreator::Shaped(db, std::move(shape), attr_names, values,
                                 schema_arg, update_schema);
  }

  static absl::StatusOr<DataSlice> Like(
      const DataSlice& shape_and_mask_from,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& schema_arg, bool update_schema,
      const DataBagPtr& db, AdoptionQueue& adoption_queue) {
    if (schema_arg) {
      adoption_queue.Add(*schema_arg);
    }
    return EntityCreator::Like(db, shape_and_mask_from, attr_names, values,
                               schema_arg, update_schema);
  }

  static absl::StatusOr<DataSlice> FromPyObject(
      PyObject* py_obj, const std::optional<DataSlice>& schema_arg,
      const DataBagPtr& db, AdoptionQueue& adoption_queue) {
    return EntitiesFromPyObject(py_obj, schema_arg, db, adoption_queue);
  }
};

struct ObjectCreatorHelper {
  static constexpr absl::string_view kKodaName = "object";

  static absl::StatusOr<DataSlice> FromAttributes(
      const std::vector<absl::string_view>& attr_names,
      const std::vector<DataSlice>& values,
      const std::optional<DataSlice>& schema_arg, bool update_schema,
      const DataBagPtr& db, [[maybe_unused]] AdoptionQueue& adoption_queue) {
    // Given that "schema" is not listed as a positional-keyword argument, it
    // will never be passed here. However, attr_names can contain "schema"
    // argument and will cause an Error to be returned.
    DCHECK(!schema_arg) << "guaranteed by FastcallArgParser set-up";
    DCHECK(!update_schema) << "unused and not filled";
    return ObjectCreator::FromAttrs(db, attr_names, values);
  }

  static absl::StatusOr<DataSlice> Shaped(
      DataSlice::JaggedShape shape,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& schema_arg, bool update_schema,
      const DataBagPtr& db, [[maybe_unused]] AdoptionQueue& adoption_queue) {
    // Given that "schema" is not listed as a positional-keyword argument, it
    // will never be passed here. However, attr_names can contain "schema"
    // argument and will cause an Error to be returned.
    DCHECK(!schema_arg) << "guaranteed by FastcallArgParser set-up";
    DCHECK(!update_schema) << "unused and not filled";
    return ObjectCreator::Shaped(db, std::move(shape), attr_names, values);
  }

  static absl::StatusOr<DataSlice> Like(
      const DataSlice& shape_and_mask_from,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& schema_arg, bool update_schema,
      const DataBagPtr& db, AdoptionQueue& adoption_queue) {
    // Given that "schema" is not listed as a positional-keyword argument, it
    // will never be passed here. However, attr_names can contain "schema"
    // argument and will cause an Error to be returned.
    DCHECK(!schema_arg) << "guaranteed by FastcallArgParser set-up";
    DCHECK(!update_schema) << "unused and not filled";
    return ObjectCreator::Like(db, shape_and_mask_from, attr_names, values);
  }

  static absl::StatusOr<DataSlice> FromPyObject(
      PyObject* py_obj, const std::optional<DataSlice>& schema_arg,
      const DataBagPtr& db, AdoptionQueue& adoption_queue) {
    // Given that "schema" is not listed as a positional-keyword argument, it
    // will never be passed here.
    DCHECK(!schema_arg) << "guaranteed by FastcallArgParser set-up";
    return ObjectsFromPyObject(py_obj, db, adoption_queue);
  }
};

// Populates `schema_arg` output argument if `args` contain a valid "schema"
// argument. Returns true on success, false on error, in which case it also sets
// Python Exception.
//
// TODO: Consider making "schema" and "itemid" keyword-only
// arguments in FastcallArgParser.
bool ParseSchemaArg(const FastcallArgParser::Args& args,
                    std::optional<DataSlice>& schema_arg) {
  // args.pos_kw_values[1] is "schema" optional positional-keyword argument.
  if (args.pos_kw_values.size() <= 1 || args.pos_kw_values[1] == nullptr ||
      args.pos_kw_values[1] == Py_None) {
    return true;
  }
  const DataSlice* schema = UnwrapDataSlice(args.pos_kw_values[1]);
  if (schema == nullptr) {
    return false;
  }
  auto status = schema->VerifyIsSchema();
  if (!status.ok()) {
    SetPyErrFromStatus(status);
    return false;
  }
  schema_arg = *schema;
  return true;
}

// Helper function that processes arguments for Entity / Object creators and
// dispatches to different implementation depending on the presence of those
// arguments.
template <class FactoryHelperT>
absl::Nullable<PyObject*> ProcessObjectCreation(
    const DataBagPtr& db, const FastcallArgParser::Args& args) {
  AdoptionQueue adoption_queue;
  std::optional<DataSlice> res;
  std::optional<DataSlice> schema_arg;
  if (!ParseSchemaArg(args, schema_arg)) {
    return nullptr;
  }
  bool update_schema = false;
  if (!ParseUpdateSchemaArg(args, /*arg_pos=*/2, update_schema)) {
    return nullptr;
  }
  // args.pos_kw_values[0] is "arg" positional-keyword argument.
  if (args.pos_kw_values[0] && args.pos_kw_values[0] != Py_None) {
    if (!args.kw_values.empty()) {
      PyErr_SetString(
          PyExc_TypeError,
          absl::StrCat("cannot set extra attributes when converting to ",
                       FactoryHelperT::kKodaName)
              .c_str());
      return nullptr;
    }
    ASSIGN_OR_RETURN(res,
                     FactoryHelperT::FromPyObject(
                         args.pos_kw_values[0], schema_arg, db, adoption_queue),
                     SetPyErrFromStatus(_));
  } else {
    ASSIGN_OR_RETURN(
        std::vector<DataSlice> values,
        ConvertArgsToDataSlices(db, args.kw_values, adoption_queue),
        SetPyErrFromStatus(_));
    ASSIGN_OR_RETURN(
        res,
        FactoryHelperT::FromAttributes(args.kw_names, values, schema_arg,
                                       update_schema, db, adoption_queue),
        SetPyErrFromStatus(_));
  }
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db)).With(SetPyErrFromStatus);
  return WrapPyDataSlice(*std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_schema_factory(
    PyObject* self, PyObject* const* py_args, Py_ssize_t nargs,
    PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true);
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  auto db = UnsafeDataBagPtr(self);
  AdoptionQueue adoption_queue;
  std::optional<DataSlice> res;
  ASSIGN_OR_RETURN(
      std::vector<DataSlice> values,
      UnwrapDataSlices(db, args.kw_values, adoption_queue),
      SetPyErrFromStatus(_));
  ASSIGN_OR_RETURN(
      res, SchemaCreator()(db, args.kw_names, values),
      SetPyErrFromStatus(_));
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db)).With(SetPyErrFromStatus);
  return WrapPyDataSlice(*std::move(res));
}

// Returns a DataSlice that represents an entity with the given DataBag
// associated with it.
//
// For single argument `arg`, a new Entity is created for it, e.g. converting a
// Python dictionary or list to Koda Entity.
//
// `kwargs` are traversed and key-value pairs are extracted and added as
// attributes of the newly created entity.
absl::Nullable<PyObject*> PyDataBag_new_factory(PyObject* self,
                                                PyObject* const* py_args,
                                                Py_ssize_t nargs,
                                                PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true, "arg", "schema",
      "update_schema");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  return ProcessObjectCreation<EntityCreatorHelper>(UnsafeDataBagPtr(self),
                                                    args);
}

// Returns a DataSlice that represents an object with the given DataBag
// associated with it.
//
// For single argument `arg`, a new object is created for it, e.g. converting a
// Python dictionary or list to Koda Object.
//
// `kwargs` are traversed and key-value pairs are extracted and added as
// attributes of the newly created object.
absl::Nullable<PyObject*> PyDataBag_obj_factory(PyObject* self,
                                                PyObject* const* py_args,
                                                Py_ssize_t nargs,
                                                PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true, "arg");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  return ProcessObjectCreation<ObjectCreatorHelper>(UnsafeDataBagPtr(self),
                                                    args);
}

// Helper function that processes arguments for Entity / Object creators and
// dispatches implementation to -Shaped behavior.
template <class FactoryHelperT>
absl::Nullable<PyObject*> ProcessObjectShapedCreation(
    const DataBagPtr& db, const FastcallArgParser::Args& args) {
  AdoptionQueue adoption_queue;
  std::optional<DataSlice> res;
  // args.pos_kw_values[0] is "shape" positional-keyword argument.
  if (args.pos_kw_values[0] == nullptr) {
    PyErr_SetString(PyExc_TypeError, "expected mandatory 'shape' argument");
    return nullptr;
  }
  const DataSlice::JaggedShape* shape = UnwrapJaggedShape(
      args.pos_kw_values[0]);
  if (shape == nullptr) {
    // Error message is set up in UnwrapJaggedShape.
    return nullptr;
  }
  std::optional<DataSlice> schema_arg;
  if (!ParseSchemaArg(args, schema_arg)) {
    return nullptr;
  }
  bool update_schema = false;
  if (!ParseUpdateSchemaArg(args, /*arg_pos=*/2, update_schema)) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(
      std::vector<DataSlice> values,
      ConvertArgsToDataSlices(
          db, /*prohibit_boxing_to_multi_dim_slice=*/shape->rank() != 0,
          args.kw_values, adoption_queue),
      SetPyErrFromStatus(_));
  ASSIGN_OR_RETURN(
      res,
      FactoryHelperT::Shaped(*std::move(shape), args.kw_names, values,
                             schema_arg, update_schema, db, adoption_queue),
      SetPyErrFromStatus(_));
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db)).With(SetPyErrFromStatus);
  return WrapPyDataSlice(*std::move(res));
}

// Returns a DataSlice that represents an Entity with the given DataBag
// associated with it.
//
// It accepts `shape` as an argument to indicate the shape of returned
// DataSlice. The returned DataSlice has a reference to a DataBag and thus can
// have attributes set.
absl::Nullable<PyObject*> PyDataBag_new_factory_shaped(PyObject* self,
                                                       PyObject* const* py_args,
                                                       Py_ssize_t nargs,
                                                       PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true, "shape", "schema",
      "update_schema");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  return ProcessObjectShapedCreation<EntityCreatorHelper>(
      UnsafeDataBagPtr(self), args);
}

// Returns a DataSlice that represents an Object with the given DataBag
// associated with it.
//
// It accepts `shape` as an argument to indicate the shape of returned
// DataSlice. The returned DataSlice has a reference to a DataBag and thus can
// have attributes set.
absl::Nullable<PyObject*> PyDataBag_obj_factory_shaped(PyObject* self,
                                                       PyObject* const* py_args,
                                                       Py_ssize_t nargs,
                                                       PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true, "shape");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  return ProcessObjectShapedCreation<ObjectCreatorHelper>(
      UnsafeDataBagPtr(self), args);
}

// Helper function that processes arguments for Entity / Object creators and
// dispatches implementation to -Like behavior.
template <class FactoryHelperT>
absl::Nullable<PyObject*> ProcessObjectLikeCreation(
    const DataBagPtr& db, const FastcallArgParser::Args& args) {
  AdoptionQueue adoption_queue;
  std::optional<DataSlice> res;
  // args.pos_kw_values[0] is "shape_and_mask_from" positional-keyword argument.
  if (args.pos_kw_values[0] == nullptr) {
    PyErr_SetString(PyExc_TypeError,
                    "expected mandatory 'shape_and_mask_from' argument");
    return nullptr;
  }
  const DataSlice* shape_and_mask_from = UnwrapDataSlice(args.pos_kw_values[0]);
  if (shape_and_mask_from == nullptr) {
    // Error message is set up in UnwrapJaggedShape.
    return nullptr;
  }
  std::optional<DataSlice> schema_arg;
  if (!ParseSchemaArg(args, schema_arg)) {
    return nullptr;
  }
  bool update_schema = false;
  if (!ParseUpdateSchemaArg(args, /*arg_pos=*/2, update_schema)) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(
      std::vector<DataSlice> values,
      ConvertArgsToDataSlices(
          db,
          /*prohibit_boxing_to_multi_dim_slice=*/shape_and_mask_from->GetShape()
          .rank() != 0,
          args.kw_values, adoption_queue),
      SetPyErrFromStatus(_));
  ASSIGN_OR_RETURN(
      res,
      FactoryHelperT::Like(*shape_and_mask_from, args.kw_names, values,
                           schema_arg, update_schema, db, adoption_queue),
      SetPyErrFromStatus(_));
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db)).With(SetPyErrFromStatus);
  return WrapPyDataSlice(*std::move(res));
}

// Returns a DataSlice that represents an Entity with the given DataBag
// associated with it.
//
// It accepts `shape_and_mask_from` as an argument from which shape and sparsity
// are used to create a DataSlice. The returned DataSlice has a reference to a
// DataBag and thus can have attributes set.
absl::Nullable<PyObject*> PyDataBag_new_factory_like(PyObject* self,
                                                     PyObject* const* py_args,
                                                     Py_ssize_t nargs,
                                                     PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true, "shape_and_mask_from", "schema",
      "update_schema");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  return ProcessObjectLikeCreation<EntityCreatorHelper>(
      UnsafeDataBagPtr(self), args);
}

// Returns a DataSlice that represents an Ontity with the given DataBag
// associated with it.
//
// It accepts `shape_and_mask_from` as an argument from which shape and sparsity
// are used to create a DataSlice. The returned DataSlice has a reference to a
// DataBag and thus can have attributes set.
absl::Nullable<PyObject*> PyDataBag_obj_factory_like(PyObject* self,
                                                     PyObject* const* py_args,
                                                     Py_ssize_t nargs,
                                                     PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true, "shape_and_mask_from");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  return ProcessObjectLikeCreation<ObjectCreatorHelper>(
      UnsafeDataBagPtr(self), args);
}

// Returns a DataSlice that represents a uu construct (depending on the factory
// helper) with the given DataBag associated with it. Handles `seed` argument.
//
// `kwargs` are traversed and key-value pairs are extracted and added as
// attributes of the newly created object.
template <typename FactoryHelperT>
absl::Nullable<PyObject*> PyDataBag_uu_factory(PyObject* self,
                                               PyObject* const* py_args,
                                               Py_ssize_t nargs,
                                               PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true, "seed");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  auto db = UnsafeDataBagPtr(self);
  AdoptionQueue adoption_queue;
  std::optional<DataSlice> res;
  ASSIGN_OR_RETURN(std::vector<DataSlice> values,
                   ConvertArgsToDataSlices(db, args.kw_values, adoption_queue),
                   SetPyErrFromStatus(_));
  if (args.pos_kw_values[0] && args.pos_kw_values[0] != Py_None) {
    auto seed_py_object = args.pos_kw_values[0];
    Py_ssize_t seed_size;
    auto seed_ptr = PyUnicode_AsUTF8AndSize(seed_py_object, &seed_size);
    if (seed_ptr == nullptr) {
      PyErr_Format(PyExc_TypeError, "seed must be a utf8 string, got %s",
                   Py_TYPE(seed_py_object)->tp_name);
      return nullptr;
    }
    auto seed_view = absl::string_view(seed_ptr, seed_size);
    ASSIGN_OR_RETURN(res,
                     FactoryHelperT()(db, seed_view, args.kw_names, values),
                     SetPyErrFromStatus(_));
  } else {
    ASSIGN_OR_RETURN(
        res, FactoryHelperT()(db, absl::string_view(""), args.kw_names, values),
        SetPyErrFromStatus(_));
  }
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db)).With(SetPyErrFromStatus);
  return WrapPyDataSlice(*std::move(res));
}

// Converts `py_items_or_keys` and `py_values` into DataSlices `keys` and
// `values` if present and if possible. On success, returns true. On failure,
// sets Python error and returns false.
bool NormalizeDictKeysAndValues(PyObject* py_items_or_keys, PyObject* py_values,
                                bool prohibit_boxing_to_multi_dim_slice,
                                const DataBagPtr& db,
                                AdoptionQueue& adoption_queue,
                                std::optional<DataSlice>& keys,
                                std::optional<DataSlice>& values) {
  if (py_values && py_values != Py_None) {
    if (PyDict_Check(py_items_or_keys) || PyList_Check(py_items_or_keys) ||
        PyTuple_Check(py_items_or_keys)) {
      PyErr_Format(
          PyExc_TypeError,
          "`items_or_keys` must be a DataSlice or DataItem (or convertible to "
          "DataItem) if `values` is provided, but got %s",
          Py_TYPE(py_items_or_keys)->tp_name);
      return false;
    }
    ASSIGN_OR_RETURN(
        keys,
        AssignmentRhsFromPyValue(py_items_or_keys,
                                 prohibit_boxing_to_multi_dim_slice, db,
                                 adoption_queue),
        (SetPyErrFromStatus(_), false));
    ASSIGN_OR_RETURN(
        values,
        AssignmentRhsFromPyValue(py_values,
                                 prohibit_boxing_to_multi_dim_slice, db,
                                 adoption_queue),
        (SetPyErrFromStatus(_), false));
    return true;
  }
  if (py_items_or_keys && py_items_or_keys != Py_None) {
    if (!PyDict_Check(py_items_or_keys)) {
      PyErr_Format(
          PyExc_TypeError,
          "`items_or_keys` must be a Python dict if `values` is not provided, "
          "but got %s",
          Py_TYPE(py_items_or_keys)->tp_name);
      return false;
    }
    if (prohibit_boxing_to_multi_dim_slice) {
      PyErr_SetString(
          PyExc_ValueError,
          "cannot create a DataSlice of dicts from a Python dictionary, only "
          "DataItem can be created directly from Python dictionary");
      return false;
    }
    if (auto status = ConvertDictKeysAndValues(py_items_or_keys, db,
                                               adoption_queue, keys, values);
        !status.ok()) {
      SetPyErrFromStatus(status);
      return false;
    }
  }
  return true;
}

absl::Nullable<PyObject*> PyDataBag_dict_shaped(PyObject* self,
                                                PyObject* const* args,
                                                Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  if (nargs != 6) {
    PyErr_SetString(PyExc_ValueError,
                    "DataBag.dict_shaped accepts exactly 6 arguments");
    return nullptr;
  }
  const auto& self_db = UnsafeDataBagPtr(self);
  PyObject* py_shape = args[0];
  PyObject* py_items_or_keys = args[1];
  PyObject* py_values = args[2];
  PyObject* py_key_schema = args[3];
  PyObject* py_value_schema = args[4];
  PyObject* py_schema = args[5];

  const DataSlice::JaggedShape* shape = UnwrapJaggedShape(py_shape);
  if (shape == nullptr) {
    // Error message is set up in UnwrapJaggedShape.
    return nullptr;
  }
  AdoptionQueue adoption_queue;
  std::optional<DataSlice> keys;
  std::optional<DataSlice> values;
  if (!NormalizeDictKeysAndValues(py_items_or_keys, py_values,
                                  shape->rank() > 0, self_db, adoption_queue,
                                  keys, values)) {
    // Error message is set in NormalizeDictKeysAndValues.
    return nullptr;
  }
  std::optional<DataSlice> key_schema;
  if (!Py_IsNone(py_key_schema)) {
    ASSIGN_OR_RETURN(key_schema,
                     DataSliceFromPyValue(py_key_schema, adoption_queue),
                     SetPyErrFromStatus(_));
  }
  std::optional<DataSlice> value_schema;
  if (!Py_IsNone(py_value_schema)) {
    ASSIGN_OR_RETURN(value_schema,
                     DataSliceFromPyValue(py_value_schema, adoption_queue),
                     SetPyErrFromStatus(_));
  }
  std::optional<DataSlice> schema;
  if (!Py_IsNone(py_schema)) {
    ASSIGN_OR_RETURN(schema, DataSliceFromPyValue(py_schema, adoption_queue),
                     SetPyErrFromStatus(_));
  }
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_db)).With(SetPyErrFromStatus);

  ASSIGN_OR_RETURN(auto res,
                   CreateDictShaped(self_db, *std::move(shape), keys, values,
                                    schema, key_schema, value_schema),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_dict_like(PyObject* self,
                                              PyObject* const* args,
                                              Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  if (nargs != 6) {
    PyErr_SetString(PyExc_ValueError,
                    "DataBag._dict_like accepts exactly 6 arguments");
    return nullptr;
  }
  const auto& self_db = UnsafeDataBagPtr(self);
  PyObject* py_shape_and_mask_from = args[0];
  PyObject* py_items_or_keys = args[1];
  PyObject* py_values = args[2];
  PyObject* py_key_schema = args[3];
  PyObject* py_value_schema = args[4];
  PyObject* py_schema = args[5];

  auto shape_and_mask_from = UnwrapDataSlice(py_shape_and_mask_from);
  if (shape_and_mask_from == nullptr) {
    return nullptr;
  }
  AdoptionQueue adoption_queue;
  std::optional<DataSlice> keys;
  std::optional<DataSlice> values;
  if (!NormalizeDictKeysAndValues(py_items_or_keys, py_values,
                                  shape_and_mask_from->GetShape().rank() > 0,
                                  self_db, adoption_queue, keys, values)) {
    // Error message is set in NormalizeDictKeysAndValues.
    return nullptr;
  }
  std::optional<DataSlice> key_schema;
  if (!Py_IsNone(py_key_schema)) {
    ASSIGN_OR_RETURN(key_schema,
                     DataSliceFromPyValue(py_key_schema, adoption_queue),
                     SetPyErrFromStatus(_));
  }
  std::optional<DataSlice> value_schema;
  if (!Py_IsNone(py_value_schema)) {
    ASSIGN_OR_RETURN(value_schema,
                     DataSliceFromPyValue(py_value_schema, adoption_queue),
                     SetPyErrFromStatus(_));
  }
  std::optional<DataSlice> schema;
  if (!Py_IsNone(py_schema)) {
    ASSIGN_OR_RETURN(schema, DataSliceFromPyValue(py_schema, adoption_queue),
                     SetPyErrFromStatus(_));
  }

  ASSIGN_OR_RETURN(
      auto res,
      CreateDictLike(UnsafeDataBagPtr(self), *shape_and_mask_from, keys, values,
                     schema, key_schema, value_schema),
      arolla::python::SetPyErrFromStatus(_));
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_db)).With(SetPyErrFromStatus);
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_list(PyObject* self, PyObject* const* args,
                                         Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  if (nargs != 3) {
    PyErr_Format(PyExc_ValueError,
                 "DataBag._list accepts exactly 3 arguments, got %d", nargs);
    return nullptr;
  }
  const auto& self_db = UnsafeDataBagPtr(self);
  PyObject* const py_values = args[0];
  PyObject* const py_item_schema = args[1];
  PyObject* const py_schema = args[2];

  AdoptionQueue adoption_queue;

  std::optional<DataSlice> item_schema;
  if (!Py_IsNone(py_item_schema)) {
    ASSIGN_OR_RETURN(item_schema,
                     DataSliceFromPyValue(py_item_schema, adoption_queue),
                     SetPyErrFromStatus(_));
  }
  std::optional<DataSlice> schema;
  if (!Py_IsNone(py_schema)) {
    ASSIGN_OR_RETURN(schema, DataSliceFromPyValue(py_schema, adoption_queue),
                     SetPyErrFromStatus(_));
  }

  std::optional<DataSlice> res;
  if (Py_IsNone(py_values)) {
    ASSIGN_OR_RETURN(res, CreateEmptyList(self_db, schema, item_schema),
                     SetPyErrFromStatus(_));
  } else {
    ASSIGN_OR_RETURN(auto values,
                     DataSliceFromPyValue(py_values, adoption_queue),
                     SetPyErrFromStatus(_));
    if (PyList_Check(py_values)) {
      ASSIGN_OR_RETURN(res,
                       CreateNestedList(self_db, values, schema, item_schema),
                       SetPyErrFromStatus(_));
    } else {
      ASSIGN_OR_RETURN(
          res,
          CreateListsFromLastDimension(self_db, values, schema, item_schema),
          SetPyErrFromStatus(_));
    }
  }

  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_db)).With(SetPyErrFromStatus);
  return WrapPyDataSlice(*std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_list_shaped(PyObject* self,
                                                PyObject* const* args,
                                                Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  if (nargs != 4) {
    PyErr_Format(PyExc_ValueError,
                 "DataBag._list_shaped accepts exactly 4 arguments, got %d",
                 nargs);
    return nullptr;
  }
  const auto& self_db = UnsafeDataBagPtr(self);
  PyObject* const py_shape = args[0];
  PyObject* const py_values = args[1];
  PyObject* const py_item_schema = args[2];
  PyObject* const py_schema = args[3];

  const DataSlice::JaggedShape* shape = UnwrapJaggedShape(py_shape);
  if (shape == nullptr) {
    return nullptr;  // Error message is set by UnwrapJaggedShape.
  }

  AdoptionQueue adoption_queue;
  std::optional<DataSlice> values;
  if (!Py_IsNone(py_values)) {
    ASSIGN_OR_RETURN(values, DataSliceFromPyValue(py_values, adoption_queue),
                     SetPyErrFromStatus(_));
  }

  std::optional<DataSlice> item_schema;
  if (!Py_IsNone(py_item_schema)) {
    ASSIGN_OR_RETURN(item_schema,
                     DataSliceFromPyValue(py_item_schema, adoption_queue),
                     SetPyErrFromStatus(_));
  }
  std::optional<DataSlice> schema;
  if (!Py_IsNone(py_schema)) {
    ASSIGN_OR_RETURN(schema, DataSliceFromPyValue(py_schema, adoption_queue),
                     SetPyErrFromStatus(_));
  }

  ASSIGN_OR_RETURN(
      auto res, CreateListShaped(self_db, *shape, values, schema, item_schema),
      SetPyErrFromStatus(_));

  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_db)).With(SetPyErrFromStatus);
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_list_like(PyObject* self,
                                              PyObject* const* args,
                                              Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  if (nargs != 4) {
    PyErr_Format(PyExc_ValueError,
                 "DataBag._list_like accepts exactly 4 arguments, got %d",
                 nargs);
    return nullptr;
  }
  const auto& self_db = UnsafeDataBagPtr(self);
  PyObject* const py_shape_and_mask_from = args[0];
  PyObject* const py_values = args[1];
  PyObject* const py_item_schema = args[2];
  PyObject* const py_schema = args[3];

  AdoptionQueue adoption_queue;

  auto shape_and_mask_from = UnwrapDataSlice(py_shape_and_mask_from);
  if (shape_and_mask_from == nullptr) {
    return nullptr;  // Error is set up by UnwrapDataSlice.
  }
  std::optional<DataSlice> values;
  if (!Py_IsNone(py_values)) {
    ASSIGN_OR_RETURN(values, DataSliceFromPyValue(py_values, adoption_queue),
                     SetPyErrFromStatus(_));
  }
  std::optional<DataSlice> item_schema;
  if (!Py_IsNone(py_item_schema)) {
    ASSIGN_OR_RETURN(item_schema,
                     DataSliceFromPyValue(py_item_schema, adoption_queue),
                     SetPyErrFromStatus(_));
  }
  std::optional<DataSlice> schema;
  if (!Py_IsNone(py_schema)) {
    ASSIGN_OR_RETURN(schema, DataSliceFromPyValue(py_schema, adoption_queue),
                     SetPyErrFromStatus(_));
  }

  ASSIGN_OR_RETURN(auto res,
                   CreateListLike(self_db, *shape_and_mask_from, values, schema,
                                  item_schema),
                   arolla::python::SetPyErrFromStatus(_));
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_db)).With(SetPyErrFromStatus);
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_exactly_equal(PyObject* self,
                                                  PyObject* const* args,
                                                  Py_ssize_t nargs) {
  if (nargs != 1) {
    PyErr_Format(PyExc_ValueError,
                 "DataBag._exactly_equal accepts exactly 1 argument, got %d",
                 nargs);
    return nullptr;
  }
  PyObject* const other = args[0];
  if (Py_TYPE(other) != PyDataBag_Type()) {
    PyErr_Format(PyExc_TypeError, "cannot compare DataBag with %s",
                 Py_TYPE(other)->tp_name);
    return nullptr;
  }
  return PyBool_FromLong(DataBagComparison::ExactlyEqual(
      UnsafeDataBagPtr(self), (UnsafeDataBagPtr(other))));
}

absl::Nullable<PyObject*> PyDataBag_merge_inplace(PyObject* self,
                                                  PyObject* const* args,
                                                  Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  if (nargs < 3) {
    PyErr_Format(PyExc_ValueError,
                 "DataBag._inplace_merge accepts at least 3 arguments, got %d",
                 nargs);
    return nullptr;
  }
  int overwrite = PyObject_IsTrue(args[0]);
  if (overwrite == -1) return nullptr;
  int allow_data_conflicts = PyObject_IsTrue(args[1]);
  if (allow_data_conflicts == -1) return nullptr;
  int allow_schema_conflicts = PyObject_IsTrue(args[2]);
  if (allow_schema_conflicts == -1) return nullptr;
  const auto& db = UnsafeDataBagPtr(self);
  for (int i = 3; i < nargs; ++i) {
    auto other = UnwrapDataBagPtr(args[i]);
    if (other == nullptr) return nullptr;
    RETURN_IF_ERROR(db->MergeInplace(other, overwrite, allow_data_conflicts,
                                     allow_schema_conflicts))
        .With(SetPyErrFromStatus);
  }
  Py_RETURN_NONE;
}

absl::Nullable<PyObject*> PyDataBag_merge_fallbacks(PyObject* self) {
  arolla::python::DCheckPyGIL();
  const auto& db = UnsafeDataBagPtr(self);
  ASSIGN_OR_RETURN(auto res, db->MergeFallbacks(),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapDataBagPtr(std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_fork(PyObject* self) {
  arolla::python::DCheckPyGIL();
  const auto& db = UnsafeDataBagPtr(self);
  ASSIGN_OR_RETURN(auto res, db->Fork(), arolla::python::SetPyErrFromStatus(_));
  return WrapDataBagPtr(std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_contents_repr(PyObject* self) {
  const DataBagPtr db = UnsafeDataBagPtr(self);

  ASSIGN_OR_RETURN(std::string str, DataBagToStr(db), SetPyErrFromStatus(_));
  return PyUnicode_FromStringAndSize(str.c_str(), str.size());
}

absl::Nullable<PyObject*> PyDataBag_get_fallbacks(PyObject* self) {
  const DataBagPtr& db = UnsafeDataBagPtr(self);

  const std::vector<DataBagPtr>& fallbacks = db->GetFallbacks();
  auto fallback_list =
      arolla::python::PyObjectPtr::Own(PyList_New(/*len=*/fallbacks.size()));
  int i = 0;
  for (const auto& bag : fallbacks) {
    PyList_SetItem(fallback_list.get(), i++, WrapDataBagPtr(bag));
  }
  return fallback_list.release();
}

PyMethodDef kPyDataBag_methods[] = {
    {"empty", (PyCFunction)PyDataBag_empty, METH_CLASS | METH_NOARGS,
     "Returns an empty DataBag."},
    {"new", (PyCFunction)PyDataBag_new_factory, METH_FASTCALL | METH_KEYWORDS,
     R"""(Creates Entities with given attrs.

Args:
  arg: optional Python object to be converted to an Entity.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
    Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
    schema instead.
  update_schema: if schema attribute is missing and the attribute is being set
    through `attrs`, schema is successfully updated.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.)"""},
    {"new_shaped", (PyCFunction)PyDataBag_new_factory_shaped,
     METH_FASTCALL | METH_KEYWORDS,
     R"""(Creates new Entities with the given shape.

Args:
  shape: mandatory JaggedShape that the returned DataSlice will have.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
    Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
    schema instead.
  update_schema: if schema attribute is missing and the attribute is being set
    through `attrs`, schema is successfully updated.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.)"""},
    {"new_like", (PyCFunction)PyDataBag_new_factory_like,
     METH_FASTCALL | METH_KEYWORDS,
     R"""(Creates new Entities with the shape and sparsity from shape_and_mask_from.

Args:
  shape_and_mask_from: mandatory DataSlice, whose shape and sparsity the
    returned DataSlice will have.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
    Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
    schema instead.
  update_schema: if schema attribute is missing and the attribute is being set
    through `attrs`, schema is successfully updated.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.)"""},
    {"obj", (PyCFunction)PyDataBag_obj_factory, METH_FASTCALL | METH_KEYWORDS,
     R"""(Creates new Objects with an implicit stored schema.

Returned DataSlice has OBJECT schema.

Args:
  arg: optional Python object to be converted to an Object.
  **attrs: attrs to set on the returned object.

Returns:
  data_slice.DataSlice with the given attrs and kd.OBJECT schema.)"""},
    {"obj_shaped", (PyCFunction)PyDataBag_obj_factory_shaped,
     METH_FASTCALL | METH_KEYWORDS,
     R"""(Creates Objects with the given shape.

Returned DataSlice has OBJECT schema.

Args:
  shape: mandatory JaggedShape that the returned DataSlice will have.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.)"""},
    {"obj_like", (PyCFunction)PyDataBag_obj_factory_like,
     METH_FASTCALL | METH_KEYWORDS,
     R"""(Creates Objects with shape and sparsity from shape_and_mask_from.

Returned DataSlice has OBJECT schema.

Args:
  shape_and_mask_from: mandatory DataSlice, whose shape and sparsity the
    returned DataSlice will have.
  db: optional DataBag where entities are created.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.)"""},
    {"uuobj", (PyCFunction)PyDataBag_uu_factory<UuObjectCreator>,
     METH_FASTCALL | METH_KEYWORDS, "DataBag.uuobj"},
    {"new_schema", (PyCFunction)PyDataBag_schema_factory,
     METH_FASTCALL | METH_KEYWORDS, "DataBag.new_schema"},
    {"uu_schema", (PyCFunction)PyDataBag_uu_factory<UuSchemaCreator>,
     METH_FASTCALL | METH_KEYWORDS, "DataBag.uu_schema"},
    {"_dict_shaped", (PyCFunction)PyDataBag_dict_shaped, METH_FASTCALL,
     "DataBag._dict_shaped"},
    {"_dict_like", (PyCFunction)PyDataBag_dict_like, METH_FASTCALL,
     "DataBag._dict_like"},
    {"_list", (PyCFunction)PyDataBag_list, METH_FASTCALL, "DataBag._list"},
    {"_list_shaped", (PyCFunction)PyDataBag_list_shaped, METH_FASTCALL,
     "DataBag._list_shaped"},
    {"_list_like", (PyCFunction)PyDataBag_list_like, METH_FASTCALL,
     "DataBag._list_like"},
    {"_exactly_equal", (PyCFunction)PyDataBag_exactly_equal, METH_FASTCALL,
     "DataBag._exactly_equal"},
    {"_kwargs_to_namedtuple", (PyCFunction)PyDataBag_kwargs_to_namedtuple,
     METH_FASTCALL | METH_KEYWORDS,
     "Converts **kwargs into an Arolla NamedTuple of DataSlices."},
    {"_merge_inplace", (PyCFunction)PyDataBag_merge_inplace, METH_FASTCALL,
     "DataBag._merge_inplace"},
    {"merge_fallbacks", (PyCFunction)PyDataBag_merge_fallbacks, METH_NOARGS,
     "Returns a new DataBag with all the fallbacks merged."},
    {"fork", (PyCFunction)PyDataBag_fork, METH_NOARGS,
     R"""(Returns a newly created mutable DataBag with the same content as self.

Changes to either DataBag will not be reflected in the other.
)"""},
    {"contents_repr", (PyCFunction)PyDataBag_contents_repr, METH_NOARGS,
     "Returns a string representation of the contents of this DataBag."},
    {"get_fallbacks", (PyCFunction)PyDataBag_get_fallbacks, METH_NOARGS,
     R"""(Returns the list of fallback DataBags in this DataBag.

The list will be empty if the DataBag does not have fallbacks. When
`DataSlice.with_fallback` is called, the original and provided DataBag will be
added to the fallback list of the newly created DataBag.)"""},
    {nullptr} /* sentinel */
};

PyTypeObject* InitPyDataBagType() {
  arolla::python::CheckPyGIL();
  PyTypeObject* py_qvalue_type = arolla::python::PyQValueType();
  if (py_qvalue_type == nullptr) {
    return nullptr;
  }
  PyType_Slot slots[] = {
      {Py_tp_base, py_qvalue_type},
      // NOTE: For now there is no need for alloc/dealloc, as everything is
      // handled by PyQValueObject's dealloc method.
      {Py_tp_methods, kPyDataBag_methods},
      {0, nullptr},
  };

  PyType_Spec spec = {
      .name = "data_bag.DataBag",
      .flags = Py_TPFLAGS_DEFAULT,
      .slots = slots,
  };

  PyObject* qvalue_subtype = PyType_FromSpec(&spec);
  if (!arolla::python::RegisterPyQValueSpecializationByQType(
          arolla::GetQType<DataBagPtr>(), qvalue_subtype)) {
    return nullptr;
  }
  return reinterpret_cast<PyTypeObject*>(qvalue_subtype);
}

}  // namespace

PyTypeObject* PyDataBag_Type() {
  arolla::python::CheckPyGIL();
  static PyTypeObject* type = InitPyDataBagType();
  return type;
}

}  // namespace koladata::python
