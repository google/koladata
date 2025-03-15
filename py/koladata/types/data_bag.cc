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

#include <any>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_bag_comparison.h"
#include "koladata/data_bag_repr.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/object_factories.h"
#include "koladata/operators/schema.h"
#include "koladata/operators/slices.h"
#include "koladata/proto/from_proto.h"
#include "koladata/repr_utils.h"
#include "google/protobuf/message.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/types/boxing.h"
#include "py/koladata/types/py_utils.h"
#include "py/koladata/types/pybind11_protobuf_wrapper.h"
#include "py/koladata/types/wrap_utils.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {

DataSlice AsMask(bool b) {
  return *DataSlice::Create(
      b ? internal::DataItem(arolla::kUnit) : internal::DataItem(),
      internal::DataItem(schema::kMask));
}

absl::Nullable<PyObject*> PyDataBag_is_mutable(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  const DataBagPtr& db = UnsafeDataBagPtr(self);
  return WrapPyDataSlice(AsMask(db->IsMutable()));
}

// classmethod
absl::Nullable<PyObject*> PyDataBag_empty(PyTypeObject* cls, PyObject*) {
  arolla::python::DCheckPyGIL();
  return arolla::python::MakePyQValue(
      PyDataBag_Type(), arolla::TypedValue::FromValue(DataBag::Empty()));
}

absl::StatusOr<std::optional<DataSlice>>
ParseSchemaArgWithStringToNamedSchemaConversion(
    const FastcallArgParser::Args& args, absl::string_view arg_name) {
  // args.pos_kw_values[arg_pos] is "arg_name" optional positional-keyword
  // argument.
  auto arg_it = args.kw_only_args.find(arg_name);
  if (arg_it == args.kw_only_args.end() || arg_it->second == Py_None) {
    return std::nullopt;
  }
  // We do no adoption here because we're getting a string or the input
  // is already a DataSlice which we receive unchanged.
  ASSIGN_OR_RETURN(auto schema_slice,
                   DataSliceFromPyValueNoAdoption(arg_it->second));
  return ops::InternalMaybeNamedSchema(schema_slice);
}

// Deduce List Item Schema to be passed to boxing.cc logic.
absl::StatusOr<std::optional<DataSlice>> ListItemSchemaForBoxing(
    const std::optional<DataSlice>& item_schema,
    const std::optional<DataSlice>& schema) {
  std::optional<DataSlice> item_schema_for_boxing;
  if (item_schema.has_value()) {
    RETURN_IF_ERROR(item_schema->VerifyIsSchema());
    item_schema_for_boxing = item_schema;
  } else if (schema.has_value()) {
    ASSIGN_OR_RETURN(item_schema_for_boxing, ops::GetItemSchema(*schema));
  }
  // NOTE: We only pass OBJECT schemas as the 3rd parameter, so that we do
  // not trigger the (explicit) casting logic in boxing.cc, and leave casting
  // to object_factories.cc which has more constrained (implicit) casting
  // logic and better error messages.
  if (item_schema_for_boxing.has_value() &&
      !item_schema_for_boxing->item().is_object_schema()) {
    item_schema_for_boxing = std::nullopt;
  }
  return item_schema_for_boxing;
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
      const std::optional<DataSlice>& schema_arg, bool overwrite_schema,
      const std::optional<DataSlice>& itemid, const DataBagPtr& db) {
    // Add `db` to each DataSlice value to avoid double adoption work.
    auto adopted_values = ManyWithBag(values, db);
    return EntityCreator::FromAttrs(db, attr_names, adopted_values, schema_arg,
                                    overwrite_schema, itemid);
  }

  static absl::StatusOr<DataSlice> Shaped(
      DataSlice::JaggedShape shape,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& schema_arg, bool overwrite_schema,
      const std::optional<DataSlice>& itemid, const DataBagPtr& db) {
    // Add `db` to each DataSlice value to avoid double adoption work.
    auto adopted_values = ManyWithBag(values, db);
    return EntityCreator::Shaped(db, std::move(shape), attr_names,
                                 adopted_values, schema_arg, overwrite_schema,
                                 itemid);
  }

  static absl::StatusOr<DataSlice> Like(
      const DataSlice& shape_and_mask_from,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& schema_arg, bool overwrite_schema,
      const std::optional<DataSlice>& itemid, const DataBagPtr& db) {
    // Add `db` to each DataSlice value to avoid double adoption work.
    auto adopted_values = ManyWithBag(values, db);
    return EntityCreator::Like(db, shape_and_mask_from, attr_names,
                               adopted_values, schema_arg, overwrite_schema,
                               itemid);
  }

  static absl::StatusOr<DataSlice> FromPyObject(
      PyObject* py_obj, const std::optional<DataSlice>& schema_arg,
      const std::optional<DataSlice>& itemid, const DataBagPtr& db,
      AdoptionQueue& adoption_queue) {
    ASSIGN_OR_RETURN(
        DataSlice res,
        EntitiesFromPyObject(py_obj, schema_arg, itemid, db, adoption_queue));
    return res.WithBag(db);
  }
};

struct ObjectCreatorHelper {
  static constexpr absl::string_view kKodaName = "object";

  static absl::StatusOr<DataSlice> FromAttributes(
      const std::vector<absl::string_view>& attr_names,
      const std::vector<DataSlice>& values,
      const std::optional<DataSlice>& schema_arg, bool overwrite_schema,
      const std::optional<DataSlice>& itemid, const DataBagPtr& db) {
    // Given that "schema" is not listed as a positional-keyword argument, it
    // will never be passed here. However, attr_names can contain "schema"
    // argument and will cause an Error to be returned.
    DCHECK(!schema_arg) << "guaranteed by FastcallArgParser set-up";
    DCHECK(!overwrite_schema) << "unused and not filled";
    // Add `db` to each DataSlice value to avoid double adoption work.
    auto adopted_values = ManyWithBag(values, db);
    return ObjectCreator::FromAttrs(db, attr_names, adopted_values, itemid);
  }

  static absl::StatusOr<DataSlice> Shaped(
      DataSlice::JaggedShape shape,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& schema_arg, bool overwrite_schema,
      const std::optional<DataSlice>& itemid, const DataBagPtr& db) {
    // Given that "schema" is not listed as a positional-keyword argument, it
    // will never be passed here. However, attr_names can contain "schema"
    // argument and will cause an Error to be returned.
    DCHECK(!schema_arg) << "guaranteed by FastcallArgParser set-up";
    DCHECK(!overwrite_schema) << "unused and not filled";
    // Add `db` to each DataSlice value to avoid double adoption work.
    auto adopted_values = ManyWithBag(values, db);
    return ObjectCreator::Shaped(db, std::move(shape), attr_names, values,
                                 itemid);
  }

  static absl::StatusOr<DataSlice> Like(
      const DataSlice& shape_and_mask_from,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& schema_arg, bool overwrite_schema,
      const std::optional<DataSlice>& itemid, const DataBagPtr& db) {
    // Given that "schema" is not listed as a positional-keyword argument, it
    // will never be passed here. However, attr_names can contain "schema"
    // argument and will cause an Error to be returned.
    DCHECK(!schema_arg) << "guaranteed by FastcallArgParser set-up";
    DCHECK(!overwrite_schema) << "unused and not filled";
    // Add `db` to each DataSlice value to avoid double adoption work.
    auto adopted_values = ManyWithBag(values, db);
    return ObjectCreator::Like(db, shape_and_mask_from, attr_names, values,
                               itemid);
  }

  static absl::StatusOr<DataSlice> FromPyObject(
      PyObject* py_obj, const std::optional<DataSlice>& schema_arg,
      const std::optional<DataSlice>& itemid, const DataBagPtr& db,
      AdoptionQueue& adoption_queue) {
    // Given that "schema" is not listed as a positional-keyword argument, it
    // will never be passed here.
    DCHECK(!schema_arg) << "guaranteed by FastcallArgParser set-up";
    ASSIGN_OR_RETURN(DataSlice res,
                     ObjectsFromPyObject(py_obj, itemid, db, adoption_queue));
    return res.WithBag(db).WithSchema(internal::DataItem(schema::kObject));
  }
};

// Returns true if `py_obj` is provided and not arolla.unspecified().
bool FirstArgProvided(PyObject* py_obj) {
  if (py_obj == nullptr) {
    return false;
  }
  if (!arolla::python::IsPyQValueInstance(py_obj)) {
    return true;
  }
  const auto& typed_value = arolla::python::UnsafeUnwrapPyQValue(py_obj);
  return typed_value.GetType() != arolla::GetUnspecifiedQType();
}

// Helper function that processes arguments for Entity / Object creators and
// dispatches to different implementation depending on the presence of those
// arguments.
template <class FactoryHelperT>
absl::Nullable<PyObject*> ProcessObjectCreation(
    const DataBagPtr& db, const FastcallArgParser::Args& args) {
  ASSIGN_OR_RETURN(
      std::optional<DataSlice> schema_arg,
      ParseSchemaArgWithStringToNamedSchemaConversion(args, "schema"),
      arolla::python::SetPyErrFromStatus(_));
  bool overwrite_schema = false;
  if (!ParseBoolArg(args, "overwrite_schema", overwrite_schema)) {
    return nullptr;
  }
  std::optional<DataSlice> itemid;
  if (!ParseDataSliceArg(args, "itemid", itemid)) {
    return nullptr;
  }
  std::optional<DataSlice> res;
  if (FirstArgProvided(args.pos_only_args[0])) {
    if (!args.kw_values.empty()) {
      PyErr_SetString(
          PyExc_TypeError,
          absl::StrCat("cannot set extra attributes when converting to ",
                       FactoryHelperT::kKodaName)
              .c_str());
      return nullptr;
    }
    AdoptionQueue adoption_queue;
    ASSIGN_OR_RETURN(
        res,
        FactoryHelperT::FromPyObject(args.pos_only_args[0], schema_arg, itemid,
                                     db, adoption_queue),
        arolla::python::SetPyErrFromStatus(_));
    RETURN_IF_ERROR(adoption_queue.AdoptInto(*db))
        .With([&](const absl::Status& status) {
          return arolla::python::SetPyErrFromStatus(
              CreateItemCreationError(status, schema_arg));
        });
  } else {
    AdoptionQueue adoption_queue;
    ASSIGN_OR_RETURN(
        std::vector<DataSlice> values,
        ConvertArgsToDataSlices(db, args.kw_values, adoption_queue),
        arolla::python::SetPyErrFromStatus(_));
    // Because `EntityCreator` relies on accurate databags of attrs for error
    // messages, and because `EntityCreatorHelper` strips attr databags to avoid
    // double adoption, we need to do adoption before calling the helper to have
    // accurate databags for error messages.
    RETURN_IF_ERROR(adoption_queue.AdoptInto(*db))
        .With([&](const absl::Status& status) {
          return arolla::python::SetPyErrFromStatus(
              CreateItemCreationError(status, schema_arg));
        });
    ASSIGN_OR_RETURN(
        res,
        FactoryHelperT::FromAttributes(args.kw_names, values, schema_arg,
                                       overwrite_schema, itemid, db),
        arolla::python::SetPyErrFromStatus(
            CreateItemCreationError(_, schema_arg)));
  }
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
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(FastcallArgParser(
      /*pos_only_n=*/1, /*optional_positional_only=*/true,
      /*parse_kwargs=*/true, {"schema", "overwrite_schema", "itemid"}));
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
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(FastcallArgParser(
      /*pos_only_n=*/1, /*optional_positional_only=*/true,
      /*parse_kwargs=*/true, {"itemid"}));
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
  std::optional<DataSlice> res;
  // args.pos_only_args[0] is "shape" positional-only argument.
  DCHECK_NE(args.pos_only_args[0], nullptr);
  const DataSlice::JaggedShape* shape =
      UnwrapJaggedShape(args.pos_only_args[0], "shape");
  if (shape == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(
      std::optional<DataSlice> schema_arg,
      ParseSchemaArgWithStringToNamedSchemaConversion(args, "schema"),
      arolla::python::SetPyErrFromStatus(_));
  bool overwrite_schema = false;
  if (!ParseBoolArg(args, "overwrite_schema", overwrite_schema)) {
    return nullptr;
  }
  std::optional<DataSlice> itemid;
  if (!ParseDataSliceArg(args, "itemid", itemid)) {
    return nullptr;
  }

  AdoptionQueue adoption_queue;
  ASSIGN_OR_RETURN(
      std::vector<DataSlice> values,
      ConvertArgsToDataSlices(
          db, /*prohibit_boxing_to_multi_dim_slice=*/shape->rank() != 0,
          args.kw_values, adoption_queue),
      arolla::python::SetPyErrFromStatus(_));
  // Because `EntityCreator` relies on accurate databags of attrs for error
  // messages, and because `EntityCreatorHelper` strips attr databags to avoid
  // double adoption, we need to do adoption before calling the helper to have
  // accurate databags for error messages.
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db))
      .With([&](const absl::Status& status) {
        return arolla::python::SetPyErrFromStatus(
            CreateItemCreationError(status, schema_arg));
      });
  ASSIGN_OR_RETURN(
      res,
      FactoryHelperT::Shaped(*std::move(shape), args.kw_names, values,
                             schema_arg, overwrite_schema, itemid, db),
      arolla::python::SetPyErrFromStatus(
          CreateItemCreationError(_, schema_arg)));
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
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(FastcallArgParser(
      /*pos_only_n=*/1, /*parse_kwargs=*/true,
      {"schema", "overwrite_schema", "itemid"}));
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
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(FastcallArgParser(
      /*pos_only_n=*/1, /*parse_kwargs=*/true, {"itemid"}));
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
  std::optional<DataSlice> res;
  // args.pos_only_args[0] is "shape_and_mask_from" positional-only argument.
  DCHECK_NE(args.pos_only_args[0], nullptr);
  const DataSlice* shape_and_mask_from =
      UnwrapDataSlice(args.pos_only_args[0], "shape_and_mask_from");
  if (shape_and_mask_from == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(
      std::optional<DataSlice> schema_arg,
      ParseSchemaArgWithStringToNamedSchemaConversion(args, "schema"),
      arolla::python::SetPyErrFromStatus(_));
  bool overwrite_schema = false;
  if (!ParseBoolArg(args, "overwrite_schema", overwrite_schema)) {
    return nullptr;
  }
  std::optional<DataSlice> itemid;
  if (!ParseDataSliceArg(args, "itemid", itemid)) {
    return nullptr;
  }
  AdoptionQueue adoption_queue;
  ASSIGN_OR_RETURN(
      std::vector<DataSlice> values,
      ConvertArgsToDataSlices(db,
                              /*prohibit_boxing_to_multi_dim_slice=*/
                              shape_and_mask_from->GetShape().rank() != 0,
                              args.kw_values, adoption_queue),
      arolla::python::SetPyErrFromStatus(_));
  // Because `EntityCreator` relies on accurate databags of attrs for error
  // messages, and because `EntityCreatorHelper` strips attr databags to avoid
  // double adoption, we need to do adoption before calling the helper to have
  // accurate databags for error messages.
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db))
      .With([&](const absl::Status& status) {
        return arolla::python::SetPyErrFromStatus(
            CreateItemCreationError(status, schema_arg));
      });
  ASSIGN_OR_RETURN(
      res,
      FactoryHelperT::Like(*shape_and_mask_from, args.kw_names, values,
                           schema_arg, overwrite_schema, itemid, db),
      arolla::python::SetPyErrFromStatus(
          CreateItemCreationError(_, schema_arg)));
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
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(FastcallArgParser(
      /*pos_only_n=*/1, /*parse_kwargs=*/true,
      {"schema", "overwrite_schema", "itemid"}));
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  return ProcessObjectLikeCreation<EntityCreatorHelper>(UnsafeDataBagPtr(self),
                                                        args);
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
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(FastcallArgParser(
      /*pos_only_n=*/1, /*parse_kwargs=*/true, {"itemid"}));
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  return ProcessObjectLikeCreation<ObjectCreatorHelper>(UnsafeDataBagPtr(self),
                                                        args);
}

// Returns a DataSlice that represents an allocated Entity Schema.
//
// `kwargs` are traversed and key-value pairs are extracted and added as Schema
// attributes of the newly created Schema.
absl::Nullable<PyObject*> PyDataBag_schema_factory(PyObject* self,
                                                   PyObject* const* py_args,
                                                   Py_ssize_t nargs,
                                                   PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true);
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  auto db = UnsafeDataBagPtr(self);
  DataSlice res;
  ASSIGN_OR_RETURN(std::vector<DataSlice> values,
                   UnwrapDataSlices(args.kw_values),
                   arolla::python::SetPyErrFromStatus(_));
  ASSIGN_OR_RETURN(res, CreateSchema(db, args.kw_names, values),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

// Returns a DataSlice that represents a UU Schema computed as a fingerprint of
// its arguments.
//
// Handles `seed` argument. `kwargs` are traversed and key-value pairs are
// extracted and added as Schema attributes of the newly created Schema.
absl::Nullable<PyObject*> PyDataBag_uu_schema_factory(PyObject* self,
                                                      PyObject* const* py_args,
                                                      Py_ssize_t nargs,
                                                      PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true, "seed");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  auto db = UnsafeDataBagPtr(self);
  DataSlice res;
  ASSIGN_OR_RETURN(std::vector<DataSlice> values,
                   UnwrapDataSlices(args.kw_values),
                   arolla::python::SetPyErrFromStatus(_));
  absl::string_view seed("");
  if (!ParseStringOrDataItemArg(args, /*arg_pos=*/0, "seed", seed)) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(res, CreateUuSchema(db, seed, args.kw_names, values),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

// Returns a DataSlice that represents a named schema with its item id derived
// only from its name.
absl::Nullable<PyObject*> PyDataBag_named_schema_factory(
    PyObject* self, PyObject* const* py_args, Py_ssize_t nargs,
    PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true, "name");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  auto db = UnsafeDataBagPtr(self);
  DataSlice res;
  ASSIGN_OR_RETURN(std::vector<DataSlice> values,
                   UnwrapDataSlices(args.kw_values),
                   arolla::python::SetPyErrFromStatus(_));
  absl::string_view name("");
  if (!ParseStringOrDataItemArg(args, /*arg_pos=*/0, "name", name)) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(res, CreateNamedSchema(db, name, args.kw_names, values),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

// Returns a DataSlice that represents an entity with the given DataBag
// associated with it.
//
// Entity IDs are UUIDs computed as fingerprints from the arguments (pointwise).
//
// For single argument `arg`, a new Entity is created for it, e.g. converting a
// Python dictionary or list to Koda Entity.
//
// `kwargs` are traversed and key-value pairs are extracted and added as
// attributes of the newly created entity.
absl::Nullable<PyObject*> PyDataBag_uu_entity_factory(PyObject* self,
                                                      PyObject* const* py_args,
                                                      Py_ssize_t nargs,
                                                      PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(FastcallArgParser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true, {"schema", "overwrite_schema"},
      /*pos_kw_args=*/"seed"));
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  auto db = UnsafeDataBagPtr(self);
  AdoptionQueue adoption_queue;
  DataSlice res;
  ASSIGN_OR_RETURN(std::vector<DataSlice> values,
                   ConvertArgsToDataSlices(db, args.kw_values, adoption_queue),
                   arolla::python::SetPyErrFromStatus(_));
  absl::string_view seed_arg("");
  if (!ParseStringOrDataItemArg(args, /*arg_pos=*/0, "seed", seed_arg)) {
    return nullptr;
  }
  std::optional<DataSlice> schema_arg;
  if (!ParseDataSliceArg(args, "schema", schema_arg)) {
    return nullptr;
  }
  bool overwrite_schema = false;
  if (!ParseBoolArg(args, "overwrite_schema", overwrite_schema)) {
    return nullptr;
  }
  // Add `db` to each DataSlice value to avoid double adoption work.
  auto adopted_values = ManyWithBag(values, db);
  // Because `EntityCreator` relies on accurate databags of attrs for error
  // messages, and because `EntityCreatorHelper` strips attr databags to avoid
  // double adoption, we need to do adoption before calling the helper to have
  // accurate databags for error messages.
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db))
      .With(arolla::python::SetPyErrFromStatus);
  ASSIGN_OR_RETURN(res,
                   CreateUu(db, seed_arg, args.kw_names, adopted_values,
                            schema_arg, overwrite_schema),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

// Returns a DataSlice that represents a uu construct (depending on the factory
// helper) with the given DataBag associated with it. Handles `seed` argument.
//
// `kwargs` are traversed and key-value pairs are extracted and added as
// attributes of the newly created object.
absl::Nullable<PyObject*> PyDataBag_uu_obj_factory(PyObject* self,
                                                   PyObject* const* py_args,
                                                   Py_ssize_t nargs,
                                                   PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true, "seed");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  auto db = UnsafeDataBagPtr(self);
  AdoptionQueue adoption_queue;
  DataSlice res;
  ASSIGN_OR_RETURN(std::vector<DataSlice> values,
                   ConvertArgsToDataSlices(db, args.kw_values, adoption_queue),
                   arolla::python::SetPyErrFromStatus(_));
  absl::string_view seed("");
  if (!ParseStringOrDataItemArg(args, /*arg_pos=*/0, "seed", seed)) {
    return nullptr;
  }
  // Add `db` to each DataSlice value to avoid double adoption work.
  auto adopted_values = ManyWithBag(values, db);
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db))
      .With(arolla::python::SetPyErrFromStatus);
  ASSIGN_OR_RETURN(res, CreateUuObject(db, seed, args.kw_names, adopted_values),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

namespace {

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
    ASSIGN_OR_RETURN(keys,
                     AssignmentRhsFromPyValue(
                         py_items_or_keys, prohibit_boxing_to_multi_dim_slice,
                         db, adoption_queue),
                     (arolla::python::SetPyErrFromStatus(_), false));
    ASSIGN_OR_RETURN(
        values,
        AssignmentRhsFromPyValue(py_values, prohibit_boxing_to_multi_dim_slice,
                                 db, adoption_queue),
        (arolla::python::SetPyErrFromStatus(_), false));
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
      arolla::python::SetPyErrFromStatus(status);
      return false;
    }
  }
  return true;
}

}  // namespace

absl::Nullable<PyObject*> PyDataBag_dict_shaped(PyObject* self,
                                                PyObject* const* args,
                                                Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  if (nargs != 7) {
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
  PyObject* py_itemid = args[6];

  const DataSlice::JaggedShape* shape = UnwrapJaggedShape(py_shape, "shape");
  if (shape == nullptr) {
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
  std::optional<DataSlice> value_schema;
  std::optional<DataSlice> schema;
  std::optional<DataSlice> itemid;
  if (!UnwrapDataSliceOptionalArg(py_key_schema, "key_schema", key_schema) ||
      !UnwrapDataSliceOptionalArg(py_value_schema, "value_schema",
                                  value_schema) ||
      !UnwrapDataSliceOptionalArg(py_schema, "schema", schema) ||
      !UnwrapDataSliceOptionalArg(py_itemid, "itemid", itemid)) {
    return nullptr;
  }
  // Replacing the DataBag to avoid double adoption.
  auto [adopted_keys, adopted_values] = FewWithBag(self_db, keys, values);
  ASSIGN_OR_RETURN(
      auto res,
      CreateDictShaped(self_db, *std::move(shape), adopted_keys, adopted_values,
                       schema, key_schema, value_schema, itemid),
      arolla::python::SetPyErrFromStatus(_));
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_db))
      .With(arolla::python::SetPyErrFromStatus);
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_dict_like(PyObject* self,
                                              PyObject* const* args,
                                              Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  if (nargs != 7) {
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
  PyObject* py_itemid = args[6];

  auto shape_and_mask_from =
      UnwrapDataSlice(py_shape_and_mask_from, "shape_and_mask_from");
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
  std::optional<DataSlice> value_schema;
  std::optional<DataSlice> schema;
  std::optional<DataSlice> itemid;
  if (!UnwrapDataSliceOptionalArg(py_key_schema, "key_schema", key_schema) ||
      !UnwrapDataSliceOptionalArg(py_value_schema, "value_schema",
                                  value_schema) ||
      !UnwrapDataSliceOptionalArg(py_schema, "schema", schema) ||
      !UnwrapDataSliceOptionalArg(py_itemid, "itemid", itemid)) {
    return nullptr;
  }
  // Replacing the DataBag to avoid double adoption.
  auto [adopted_keys, adopted_values] = FewWithBag(self_db, keys, values);
  ASSIGN_OR_RETURN(
      auto res,
      CreateDictLike(UnsafeDataBagPtr(self), *shape_and_mask_from, adopted_keys,
                     adopted_values, schema, key_schema, value_schema, itemid),
      arolla::python::SetPyErrFromStatus(_));
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_db))
      .With(arolla::python::SetPyErrFromStatus);
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_list(PyObject* self, PyObject* const* args,
                                         Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  if (nargs != 4) {
    PyErr_Format(PyExc_ValueError,
                 "DataBag._list accepts exactly 4 arguments, got %d", nargs);
    return nullptr;
  }
  const auto& self_db = UnsafeDataBagPtr(self);
  PyObject* const py_values = args[0];
  PyObject* const py_item_schema = args[1];
  PyObject* const py_schema = args[2];
  PyObject* const py_itemid = args[3];

  std::optional<DataSlice> item_schema;
  std::optional<DataSlice> schema;
  std::optional<DataSlice> itemid;
  if (!UnwrapDataSliceOptionalArg(py_item_schema, "item_schema", item_schema) ||
      !UnwrapDataSliceOptionalArg(py_schema, "schema", schema) ||
      !UnwrapDataSliceOptionalArg(py_itemid, "itemid", itemid)) {
    return nullptr;
  }

  std::optional<DataSlice> res;
  if (Py_IsNone(py_values)) {
    ASSIGN_OR_RETURN(res, CreateEmptyList(self_db, schema, item_schema, itemid),
                     arolla::python::SetPyErrFromStatus(_));
  } else {
    if (arolla::python::IsPyQValueInstance(py_values) &&
        arolla::python::UnsafeUnwrapPyQValue(py_values).GetType() ==
            arolla::GetQType<DataSlice>()) {
      PyErr_SetString(
          PyExc_TypeError,
          "kd.list does not accept DataSlice as an input, please use "
          "kd.implode");
      return nullptr;
    }
    AdoptionQueue adoption_queue;
    ASSIGN_OR_RETURN(auto item_schema_for_boxing,
                     ListItemSchemaForBoxing(item_schema, schema),
                     arolla::python::SetPyErrFromStatus(_));
    ASSIGN_OR_RETURN(auto values,
                     DataSliceFromPyValue(py_values, adoption_queue,
                                          /*schema=*/item_schema_for_boxing),
                     arolla::python::SetPyErrFromStatus(_));
    // Replacing the DataBag to avoid double adoption.
    values = values.WithBag(self_db);
    ASSIGN_OR_RETURN(
        res, CreateNestedList(self_db, values, schema, item_schema, itemid),
        arolla::python::SetPyErrFromStatus(_));
    RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_db))
        .With(arolla::python::SetPyErrFromStatus);
  }
  return WrapPyDataSlice(*std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_list_schema(PyObject* self,
                                                PyObject* const* py_args,
                                                Py_ssize_t nargs,
                                                PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/false, "item_schema");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  if (args.pos_kw_values[0] == nullptr) {
    PyErr_Format(
        PyExc_ValueError,
        "missing required argument to DataBag._list_schema: `item_schema`");
    return nullptr;
  }

  auto db = UnsafeDataBagPtr(self);
  const DataSlice* item_schema =
      UnwrapDataSlice(args.pos_kw_values[0], "item_schema");
  if (item_schema == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(DataSlice res, CreateListSchema(db, *item_schema),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_dict_schema(PyObject* self,
                                                PyObject* const* py_args,
                                                Py_ssize_t nargs,
                                                PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/false, "key_schema", "value_schema");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  if (args.pos_kw_values[0] == nullptr) {
    PyErr_Format(
        PyExc_ValueError,
        "missing required argument to DataBag._dict_schema: `key_schema`");
    return nullptr;
  }
  if (args.pos_kw_values[1] == nullptr) {
    PyErr_Format(
        PyExc_ValueError,
        "missing required argument to DataBag._dict_schema: `value_schema`");
    return nullptr;
  }

  auto db = UnsafeDataBagPtr(self);
  const DataSlice* key_schema =
      UnwrapDataSlice(args.pos_kw_values[0], "key_schema");
  if (key_schema == nullptr) {
    return nullptr;
  }
  const DataSlice* value_schema =
      UnwrapDataSlice(args.pos_kw_values[1], "value_schema");
  if (value_schema == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(DataSlice res,
                   CreateDictSchema(db, *key_schema, *value_schema),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_list_shaped(PyObject* self,
                                                PyObject* const* args,
                                                Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  if (nargs != 5) {
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
  PyObject* const py_itemid = args[4];

  const DataSlice::JaggedShape* shape = UnwrapJaggedShape(py_shape, "shape");
  if (shape == nullptr) {
    return nullptr;
  }

  AdoptionQueue adoption_queue;
  std::optional<DataSlice> item_schema;
  std::optional<DataSlice> schema;
  std::optional<DataSlice> itemid;
  if (!UnwrapDataSliceOptionalArg(py_item_schema, "item_schema", item_schema) ||
      !UnwrapDataSliceOptionalArg(py_schema, "schema", schema) ||
      !UnwrapDataSliceOptionalArg(py_itemid, "itemid", itemid)) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto item_schema_for_boxing,
                   ListItemSchemaForBoxing(item_schema, schema),
                   arolla::python::SetPyErrFromStatus(_));
  std::optional<DataSlice> values;
  if (!Py_IsNone(py_values)) {
    ASSIGN_OR_RETURN(values,
                     DataSliceFromPyValue(py_values, adoption_queue,
                                          /*schema=*/item_schema_for_boxing),
                     arolla::python::SetPyErrFromStatus(_));
    // Replacing the DataBag to avoid double adoption.
    values = values->WithBag(self_db);
  }
  ASSIGN_OR_RETURN(
      auto res,
      CreateListShaped(self_db, *shape, values, schema, item_schema, itemid),
      arolla::python::SetPyErrFromStatus(_));

  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_db))
      .With(arolla::python::SetPyErrFromStatus);
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_list_like(PyObject* self,
                                              PyObject* const* args,
                                              Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  if (nargs != 5) {
    PyErr_Format(PyExc_ValueError,
                 "DataBag._list_like accepts exactly 5 arguments, got %d",
                 nargs);
    return nullptr;
  }
  const auto& self_db = UnsafeDataBagPtr(self);
  PyObject* const py_shape_and_mask_from = args[0];
  PyObject* const py_values = args[1];
  PyObject* const py_item_schema = args[2];
  PyObject* const py_schema = args[3];
  PyObject* const py_itemid = args[4];

  auto shape_and_mask_from =
      UnwrapDataSlice(py_shape_and_mask_from, "shape_and_mask_from");
  if (shape_and_mask_from == nullptr) {
    return nullptr;
  }
  std::optional<DataSlice> item_schema;
  std::optional<DataSlice> schema;
  std::optional<DataSlice> itemid;
  if (!UnwrapDataSliceOptionalArg(py_item_schema, "item_schema", item_schema) ||
      !UnwrapDataSliceOptionalArg(py_schema, "schema", schema) ||
      !UnwrapDataSliceOptionalArg(py_itemid, "itemid", itemid)) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto item_schema_for_boxing,
                   ListItemSchemaForBoxing(item_schema, schema),
                   arolla::python::SetPyErrFromStatus(_));
  AdoptionQueue adoption_queue;
  std::optional<DataSlice> values;
  if (!Py_IsNone(py_values)) {
    ASSIGN_OR_RETURN(values,
                     DataSliceFromPyValue(py_values, adoption_queue,
                                          /*schema=*/item_schema_for_boxing),
                     arolla::python::SetPyErrFromStatus(_));
    // Replacing the DataBag to avoid double adoption.
    values = values->WithBag(self_db);
  }
  ASSIGN_OR_RETURN(auto res,
                   CreateListLike(self_db, *shape_and_mask_from, values, schema,
                                  item_schema, itemid),
                   arolla::python::SetPyErrFromStatus(_));
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_db))
      .With(arolla::python::SetPyErrFromStatus);
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_implode(PyObject* self,
                                            PyObject* const* args,
                                            Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  if (nargs != 3) {
    PyErr_Format(PyExc_ValueError,
                 "DataBag._implode accepts exactly 3 arguments, got %d", nargs);
    return nullptr;
  }

  const auto& self_db = UnsafeDataBagPtr(self);
  PyObject* const py_x = args[0];
  PyObject* const py_ndim = args[1];
  PyObject* const py_itemid = args[2];

  const DataSlice* x_ptr = UnwrapDataSlice(py_x, "x");
  if (x_ptr == nullptr) {
    return nullptr;
  }
  Py_ssize_t ndim = PyLong_AsSsize_t(py_ndim);
  if (ndim == -1 && PyErr_Occurred() != nullptr) {
    return nullptr;
  }
  std::optional<DataSlice> itemid;
  if (!UnwrapDataSliceOptionalArg(py_itemid, "itemid", itemid)) {
    return nullptr;
  }

  ASSIGN_OR_RETURN(DataSlice result, Implode(self_db, *x_ptr, ndim, itemid),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(result));
}

absl::Nullable<PyObject*> PyDataBag_concat_lists(PyObject* self,
                                                 PyObject* const* args,
                                                 Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& self_db = UnsafeDataBagPtr(self);

  std::vector<DataSlice> inputs;
  inputs.reserve(nargs);
  for (Py_ssize_t i = 0; i < nargs; ++i) {
    const DataSlice* input = UnwrapDataSlice(args[i], "*lists");
    if (input == nullptr) {
      return nullptr;
    }
    inputs.push_back(*input);
  }

  ASSIGN_OR_RETURN(DataSlice result, ConcatLists(self_db, std::move(inputs)),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(result));
}

absl::Nullable<PyObject*> PyDataBag_exactly_equal(PyObject* self,
                                                  PyObject* const* args,
                                                  Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
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
  arolla::python::PyCancellationScope cancellation_scope;
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
    auto other = UnwrapDataBagPtr(args[i], "each DataBag to be merged");
    if (other == std::nullopt) return nullptr;
    if (*other == nullptr) {
      PyErr_SetString(
          PyExc_TypeError,
          "expecting each DataBag to be merged to be a DataBag, got None");
      return nullptr;
    }
    RETURN_IF_ERROR(db->MergeInplace(*other, overwrite, allow_data_conflicts,
                                     allow_schema_conflicts))
        .With([&](const absl::Status& status) {
          return arolla::python::SetPyErrFromStatus(
              KodaErrorCausedByMergeConflictError(db, *other)(status));
        });
  }
  Py_RETURN_NONE;
}

absl::Nullable<PyObject*> PyDataBag_adopt(PyObject* self, PyObject* ds) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const DataBagPtr& db = UnsafeDataBagPtr(self);
  const DataSlice* slice = UnwrapDataSlice(ds, "slice");
  if (!slice) {
    return nullptr;
  }

  AdoptionQueue adoption_queue;
  adoption_queue.Add(*slice);
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db))
      .With(arolla::python::SetPyErrFromStatus);

  return WrapPyDataSlice(slice->WithBag(std::move(db)));
}

absl::Nullable<PyObject*> PyDataBag_adopt_stub(PyObject* self, PyObject* ds) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const DataBagPtr& db = UnsafeDataBagPtr(self);
  const DataSlice* slice = UnwrapDataSlice(ds, "slice");
  if (!slice) {
    return nullptr;
  }
  RETURN_IF_ERROR(AdoptStub(db, *slice))
      .With(arolla::python::SetPyErrFromStatus);
  return WrapPyDataSlice(slice->WithBag(std::move(db)));
}

absl::Nullable<PyObject*> PyDataBag_merge_fallbacks(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& db = UnsafeDataBagPtr(self);
  ASSIGN_OR_RETURN(auto res, db->MergeFallbacks(),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapDataBagPtr(std::move(res));
}

absl::Nullable<PyObject*> PyDataBag_fork(PyObject* self,
                                         PyObject* const* py_args,
                                         Py_ssize_t nargs,
                                         PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/false, "mutable");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  bool _mutable = true;
  if (args.pos_kw_values[0] != nullptr) {
    _mutable = PyObject_IsTrue(args.pos_kw_values[0]);
  }

  const auto& db = UnsafeDataBagPtr(self);
  ASSIGN_OR_RETURN(auto res, db->Fork(/*immutable=*/!_mutable),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapDataBagPtr(std::move(res));
}

template <absl::StatusOr<std::string> ToStrFunc(const DataBagPtr&, int64_t)>
absl::Nullable<PyObject*> PyDataBag_contents_repr(PyObject* self,
                                                  PyObject* const* py_args,
                                                  Py_ssize_t nargs,
                                                  PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/false,
      std::initializer_list<absl::string_view>{"triple_limit"});
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  const DataBagPtr db = UnsafeDataBagPtr(self);
  int64_t triple_limit = kDefaultTripleReprLimit;
  if (auto it = args.kw_only_args.find("triple_limit");
      it != args.kw_only_args.end()) {
    triple_limit = PyLong_AsLongLong(it->second);
    if (triple_limit == -1 && PyErr_Occurred()) {
      return nullptr;
    }
  }

  ASSIGN_OR_RETURN(std::string str, ToStrFunc(db, triple_limit),
                   arolla::python::SetPyErrFromStatus(_));
  return PyUnicode_FromStringAndSize(str.c_str(), str.size());
}

absl::Nullable<PyObject*> PyDataBag_get_fallbacks(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
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

absl::Nullable<PyObject*> PyDataBag_from_proto(PyObject* self,
                                               PyObject* const* args,
                                               Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const DataBagPtr db = UnsafeDataBagPtr(self);
  if (nargs != 4) {
    PyErr_Format(PyExc_ValueError,
                 "DataBag._from_proto accepts exactly 4 arguments, got %d",
                 nargs);
    return nullptr;
  }
  PyObject* py_messages_list = args[0];    // Borrowed.
  PyObject* py_extensions_list = args[1];  // Borrowed.
  PyObject* py_itemid = args[2];           // Borrowed.
  PyObject* py_schema = args[3];           // Borrowed.

  if (!PyList_CheckExact(py_messages_list)) {
    PyErr_Format(PyExc_ValueError,
                 "DataBag._from_proto expects messages to be a list, got %s",
                 Py_TYPE(py_messages_list)->tp_name);
    return nullptr;
  }
  const Py_ssize_t messages_list_len = PyList_Size(py_messages_list);

  internal::SliceBuilder message_mask_builder(messages_list_len);
  auto typed_message_mask_builder = message_mask_builder.typed<arolla::Unit>();
  std::vector<std::any> message_owners;
  message_owners.reserve(messages_list_len);
  std::vector<absl::Nonnull<const ::google::protobuf::Message*>> message_ptrs;
  message_ptrs.reserve(messages_list_len);
  for (Py_ssize_t i = 0; i < messages_list_len; ++i) {
    PyObject* py_message = PyList_GetItem(py_messages_list, i);  // Borrowed.
    if (!py_message) {
      return nullptr;
    }
    if (py_message != Py_None) {
      typed_message_mask_builder.InsertIfNotSet(i, arolla::kUnit);
      ASSIGN_OR_RETURN((auto [message_ptr, message_owner]),
                       UnwrapPyProtoMessage(py_message),
                       arolla::python::SetPyErrFromStatus(_));
      message_owners.push_back(std::move(message_owner));
      message_ptrs.push_back(message_ptr);
    }
  }
  ASSIGN_OR_RETURN(
      auto message_mask,
      DataSlice::Create(std::move(message_mask_builder).Build(),
                        DataSlice::JaggedShape::FlatFromSize(messages_list_len),
                        internal::DataItem(schema::kMask)),
      arolla::python::SetPyErrFromStatus(_));

  std::vector<absl::string_view> extensions;
  {
    // None is interpreted as empty list.
    if (!Py_IsNone(py_extensions_list)) {
      if (!PyList_CheckExact(py_extensions_list)) {
        PyErr_Format(PyExc_ValueError,
                     "DataBag._from_proto expects extensions to be a "
                     "list, got %s",
                     Py_TYPE(py_extensions_list)->tp_name);
        return nullptr;
      }
      const Py_ssize_t extensions_len = PyList_Size(py_extensions_list);
      extensions.reserve(extensions_len);
      for (Py_ssize_t i = 0; i < extensions_len; ++i) {
        PyObject* py_extension_str =
            PyList_GetItem(py_extensions_list, i);  // Borrowed.
        if (!py_extension_str) {
          return nullptr;
        }
        if (!PyUnicode_CheckExact(py_extension_str)) {
          PyErr_Format(PyExc_ValueError, "expected extension to be str, got %s",
                       Py_TYPE(py_extension_str)->tp_name);
          return nullptr;
        }
        Py_ssize_t extension_str_size = 0;
        const char* extension_str =
            PyUnicode_AsUTF8AndSize(py_extension_str, &extension_str_size);
        extensions.emplace_back(extension_str, extension_str_size);
      }
    }
  }

  std::optional<DataSlice> itemid;
  std::optional<DataSlice> schema;
  if (!UnwrapDataSliceOptionalArg(py_itemid, "itemid", itemid) ||
      !UnwrapDataSliceOptionalArg(py_schema, "schema", schema)) {
    return nullptr;
  }

  ASSIGN_OR_RETURN(DataSlice dense_result,
                   FromProto(db, message_ptrs, extensions, itemid, schema),
                   arolla::python::SetPyErrFromStatus(_));
  ASSIGN_OR_RETURN(DataSlice result,
                   ops::InverseSelect(dense_result, message_mask),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(result));
}

absl::Nullable<PyObject*> PyDataBag_schema_from_proto(PyObject* self,
                                                      PyObject* const* args,
                                                      Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const DataBagPtr db = UnsafeDataBagPtr(self);
  if (nargs != 2) {
    PyErr_Format(
        PyExc_ValueError,
        "DataBag._schema_from_proto accepts exactly 2 arguments, got %d",
        nargs);
    return nullptr;
  }
  PyObject* py_message = args[0];          // Borrowed.
  PyObject* py_extensions_list = args[1];  // Borrowed.

  ASSIGN_OR_RETURN((auto [message_ptr, message_owner]),
                   UnwrapPyProtoMessage(py_message),
                   arolla::python::SetPyErrFromStatus(_));
  const auto* message_descriptor = message_ptr->GetDescriptor();
  DCHECK(message_descriptor != nullptr);

  std::vector<absl::string_view> extensions;
  {
    // None is interpreted as empty list.
    if (!Py_IsNone(py_extensions_list)) {
      if (!PyList_CheckExact(py_extensions_list)) {
        PyErr_Format(PyExc_ValueError,
                     "DataBag._schema_from_proto expects extensions to be a "
                     "list, got %s",
                     Py_TYPE(py_extensions_list)->tp_name);
        return nullptr;
      }
      const Py_ssize_t extensions_len = PyList_Size(py_extensions_list);
      extensions.reserve(extensions_len);
      for (Py_ssize_t i = 0; i < extensions_len; ++i) {
        PyObject* py_extension_str =
            PyList_GetItem(py_extensions_list, i);  // Borrowed.
        if (!py_extension_str) {
          return nullptr;
        }
        if (!PyUnicode_CheckExact(py_extension_str)) {
          PyErr_Format(PyExc_ValueError, "expected extension to be str, got %s",
                       Py_TYPE(py_extension_str)->tp_name);
          return nullptr;
        }
        Py_ssize_t extension_str_size = 0;
        const char* extension_str =
            PyUnicode_AsUTF8AndSize(py_extension_str, &extension_str_size);
        extensions.emplace_back(extension_str, extension_str_size);
      }
    }
  }

  ASSIGN_OR_RETURN(
      DataSlice schema,
      SchemaFromProto(db, message_ptr->GetDescriptor(), extensions),
      arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(schema));
}

absl::Nullable<PyObject*> PyDataBag_get_approx_size(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const DataBagPtr& db = UnsafeDataBagPtr(self);
  FlattenFallbackFinder fallback_finder(*db);
  int64_t size = db->GetImpl().GetApproxTotalSize();
  for (const internal::DataBagImpl* fallback :
       fallback_finder.GetFlattenFallbacks()) {
    size += fallback->GetApproxTotalSize();
  }
  return PyLong_FromLongLong(size);
}

PyMethodDef kPyDataBag_methods[] = {
    {"is_mutable", (PyCFunction)PyDataBag_is_mutable, METH_NOARGS,
     "is_mutable()\n"
     "--\n\n"
     "Returns present iff this DataBag is mutable."},
    {"empty", (PyCFunction)PyDataBag_empty, METH_CLASS | METH_NOARGS,
     "empty()\n"
     "--\n\n"
     "Returns an empty DataBag."},
    {"new", (PyCFunction)PyDataBag_new_factory, METH_FASTCALL | METH_KEYWORDS,
     "new(arg, *, schema=None, overwrite_schema=False, itemid=None, **attrs)\n"
     "--\n\n"
     R"""(Creates Entities with given attrs.

Args:
  arg: optional Python object to be converted to an Entity.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
  overwrite_schema: if schema attribute is missing and the attribute is being
    set through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
    itemid will only be set when the args is not a primitive or primitive slice
    if args present.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.)"""},
    {"new_shaped", (PyCFunction)PyDataBag_new_factory_shaped,
     METH_FASTCALL | METH_KEYWORDS,
     "new_shaped(shape, *, schema=None, overwrite_schema=False, itemid=None, "
     "**attrs)\n"
     "--\n\n"
     R"""(Creates new Entities with the given shape.

Args:
  shape: JaggedShape that the returned DataSlice will have.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
  overwrite_schema: if schema attribute is missing and the attribute is being
    set through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.)"""},
    {"new_like", (PyCFunction)PyDataBag_new_factory_like,
     METH_FASTCALL | METH_KEYWORDS,
     "new_like(shape_and_mask_from, *, schema=None, overwrite_schema=False, "
     "itemid=None, **attrs)\n"
     "--\n\n"
     R"""(Creates new Entities with the shape and sparsity from shape_and_mask_from.

Args:
  shape_and_mask_from: DataSlice, whose shape and sparsity the returned
    DataSlice will have.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
  overwrite_schema: if schema attribute is missing and the attribute is being
    set through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.)"""},
    {"obj", (PyCFunction)PyDataBag_obj_factory, METH_FASTCALL | METH_KEYWORDS,
     "obj(arg, *, itemid=None, **attrs)\n"
     "--\n\n"
     R"""(Creates new Objects with an implicit stored schema.

Returned DataSlice has OBJECT schema.

Args:
  arg: optional Python object to be converted to an Object.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    itemid will only be set when the args is not a primitive or primitive slice
    if args presents.
  **attrs: attrs to set on the returned object.

Returns:
  data_slice.DataSlice with the given attrs and kd.OBJECT schema.)"""},
    {"obj_shaped", (PyCFunction)PyDataBag_obj_factory_shaped,
     METH_FASTCALL | METH_KEYWORDS,
     "obj_shaped(shape, *, itemid=None, **attrs)\n"
     "--\n\n"
     R"""(Creates Objects with the given shape.

Returned DataSlice has OBJECT schema.

Args:
  shape: JaggedShape that the returned DataSlice will have.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.)"""},
    {"obj_like", (PyCFunction)PyDataBag_obj_factory_like,
     METH_FASTCALL | METH_KEYWORDS,
     "obj_like(shape_and_mask_from, *, itemid=None, **attrs)\n"
     "--\n\n"
     R"""(Creates Objects with shape and sparsity from shape_and_mask_from.

Returned DataSlice has OBJECT schema.

Args:
  shape_and_mask_from: DataSlice, whose shape and sparsity the returned
    DataSlice will have.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
  db: optional DataBag where entities are created.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.)"""},
    {"uu", (PyCFunction)PyDataBag_uu_entity_factory,
     METH_FASTCALL | METH_KEYWORDS,
     "uu(seed, *, schema=None, overwrite_schema=False, **kwargs)\n"
     "--\n\n"
     R"""(Creates an item whose ids are uuid(s) with the set attributes.

In order to create a different "Type" from the same arguments, use
`seed` key with the desired value, e.g.

kd.uu(seed='type_1', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

and

kd.uu(seed='type_2', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

have different ids.

If 'schema' is provided, the resulting DataSlice has the provided schema.
Otherwise, uses the corresponding uuschema instead.

Args:
  seed: (str) Allows different item(s) to have different ids when created
    from the same inputs.
  schema: schema for the resulting DataSlice
  overwrite_schema: if true, will overwrite schema attributes in the schema's
    corresponding db from the argument values.
  **kwargs: key-value pairs of object attributes where values are DataSlices
    or can be converted to DataSlices using kd.new.

Returns:
  data_slice.DataSlice
    )"""},
    {"uuobj", (PyCFunction)PyDataBag_uu_obj_factory,
     METH_FASTCALL | METH_KEYWORDS,
     "uuobj(seed, **kwargs)\n"
     "--\n\n"
     R"""(Creates object(s) whose ids are uuid(s) with the provided attributes.

In order to create a different "Type" from the same arguments, use
`seed` key with the desired value, e.g.

kd.uuobj(seed='type_1', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

and

kd.uuobj(seed='type_2', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

have different ids.

Args:
  seed: (str) Allows different uuobj(s) to have different ids when created
    from the same inputs.
  **kwargs: key-value pairs of object attributes where values are DataSlices
    or can be converted to DataSlices using kd.new.

Returns:
  data_slice.DataSlice
    )"""},
    {"new_schema", (PyCFunction)PyDataBag_schema_factory,
     METH_FASTCALL | METH_KEYWORDS,
     "new_schema(**attrs)\n"
     "--\n\n"
     "Creates new schema object with given types of attrs."},
    {"uu_schema", (PyCFunction)PyDataBag_uu_schema_factory,
     METH_FASTCALL | METH_KEYWORDS,
     "uu_schema(seed, **attrs)\n"
     "--\n\n"
     "Creates new uuschema from given types of attrs."},
    {"named_schema", (PyCFunction)PyDataBag_named_schema_factory,
     METH_FASTCALL | METH_KEYWORDS,
     "named_schema(name, **attrs)\n"
     "--\n\n"
     "Creates a named schema with ItemId derived only from its name."},
    {"_dict_shaped", (PyCFunction)PyDataBag_dict_shaped, METH_FASTCALL,
     "_dict_shaped(shape, items_or_keys, values, key_schema, value_schema, "
     "schema, itemid, /)\n"
     "--\n\n"
     "DataBag._dict_shaped"},
    {"_dict_like", (PyCFunction)PyDataBag_dict_like, METH_FASTCALL,
     "_dict_like(shape_and_mask_from, items_or_keys, values, key_schema, "
     "value_schema, schema, itemid, /)\n"
     "--\n\n"
     "DataBag._dict_like"},
    {"_list", (PyCFunction)PyDataBag_list, METH_FASTCALL,
     "_list(values, item_schema, schema, itemid, /)\n"
     "DataBag._list"},
    {"list_schema", (PyCFunction)PyDataBag_list_schema,
     METH_FASTCALL | METH_KEYWORDS,
     "list_schema(item_schema)\n"
     "--\n\n"
     "Returns a list schema from the schema of the items"},
    {"dict_schema", (PyCFunction)PyDataBag_dict_schema,
     METH_FASTCALL | METH_KEYWORDS,
     "dict_schema(key_schema, value_schema)\n"
     "--\n\n"
     "Returns a dict schema from the schemas of the keys and values"},
    {"_list_shaped", (PyCFunction)PyDataBag_list_shaped, METH_FASTCALL,
     "_list_shaped(shape, values, item_schema, schema, itemid, /)\n"
     "--\n\n"
     "DataBag._list_shaped"},
    {"_list_like", (PyCFunction)PyDataBag_list_like, METH_FASTCALL,
     "_list_like(shape_and_mask_from, values, item_schema, schema, itemid, /)\n"
     "--\n\n"
     "DataBag._list_like"},
    {"_implode", (PyCFunction)PyDataBag_implode, METH_FASTCALL,
     "_implode(x, ndim, /)\n"
     "--\n\n"
     "DataBag._implode"},
    {"_concat_lists", (PyCFunction)PyDataBag_concat_lists, METH_FASTCALL,
     "_concat_lists(*lists)\n"
     "--\n\n"
     "DataBag._concat_lists"},
    {"_exactly_equal", (PyCFunction)PyDataBag_exactly_equal, METH_FASTCALL,
     "_exactly_equal(other, /)\n"
     "--\n\n"
     "DataBag._exactly_equal"},
    {"_merge_inplace", (PyCFunction)PyDataBag_merge_inplace, METH_FASTCALL,
     "_merge_inplace(overwrite, allow_data_conflicts, allow_schema_conflicts, "
     "*bags)\n"
     "--\n\n"
     "DataBag._merge_inplace"},
    {"adopt", (PyCFunction)PyDataBag_adopt, METH_O,
     "adopt(slice, /)\n"
     "--\n\n"
     R"""(Adopts all data reachable from the given slice into this DataBag.

Args:
  slice: DataSlice to adopt data from.

Returns:
  The DataSlice with this DataBag (including adopted data) attached.
)"""},
    {"adopt_stub", (PyCFunction)PyDataBag_adopt_stub, METH_O,
     "adopt_stub(slice, /)\n"
     "--\n\n"
     R"""(Copies the given DataSlice's schema stub into this DataBag.

The "schema stub" of a DataSlice is a subset of its schema (including embedded
schemas) that contains just enough information to support direct updates to
that DataSlice. See kd.stub() for more details.

Args:
  slice: DataSlice to extract the schema stub from.

Returns:
  The "stub" with this DataBag attached.
)"""},
    {"merge_fallbacks", PyDataBag_merge_fallbacks, METH_NOARGS,
     "merge_fallbacks()\n"
     "--\n\n"
     "Returns a new DataBag with all the fallbacks merged."},
    {"fork", (PyCFunction)PyDataBag_fork, METH_FASTCALL | METH_KEYWORDS,
     "fork(mutable=True)\n"
     "--\n\n"
     R"""(Returns a newly created DataBag with the same content as self.

Changes to either DataBag will not be reflected in the other.

Args:
  mutable: If true (default), returns a mutable DataBag. If false, the DataBag
    will be immutable.
Returns:
  data_bag.DataBag
)"""},
    {"_contents_repr", (PyCFunction)PyDataBag_contents_repr<DataBagToStr>,
     METH_FASTCALL | METH_KEYWORDS,
     "_contents_repr(triple_limit=1000)\n"
     "--\n\n"
     "Returns a string representation of the contents of this DataBag."},
    {"_data_triples_repr",
     (PyCFunction)PyDataBag_contents_repr<DataOnlyBagToStr>,
     METH_FASTCALL | METH_KEYWORDS,
     "_data_triples_repr(triple_limit=1000)\n"
     "--\n\n"
     "Returns a string representation of the contents of this DataBag, "
     "omitting schema contents"},
    {"_schema_triples_repr",
     (PyCFunction)PyDataBag_contents_repr<SchemaOnlyBagToStr>,
     METH_FASTCALL | METH_KEYWORDS,
     "_schema_triples_repr(triple_limit=1000)\n"
     "--\n\n"
     "Returns a string representation of the schema contents of this DataBag"},
    {"get_fallbacks", PyDataBag_get_fallbacks, METH_NOARGS,
     "get_fallbacks()\n"
     "--\n\n"
     R"""(Returns the list of fallback DataBags in this DataBag.

The list will be empty if the DataBag does not have fallbacks.)"""},
    {"_from_proto", (PyCFunction)PyDataBag_from_proto, METH_FASTCALL,
     "_from_proto(message_list, extensions_list, itemid, schema, /)\n"
     "--\n\n"
     "Returns a DataSlice converted from a list of proto messages."},
    {"_schema_from_proto", (PyCFunction)PyDataBag_schema_from_proto,
     METH_FASTCALL,
     "_schema_from_proto(message_class, extensions, /)\n"
     "--\n\n"
     "Returns a schema DataItem converted from a proto message class."},
    {"get_approx_size", PyDataBag_get_approx_size, METH_NOARGS,
     "get_approx_size()\n"
     "--\n\n"
     "Returns approximate size of the DataBag."},
    {nullptr} /* sentinel */
};

PyTypeObject* InitPyDataBagType() {
  arolla::python::CheckPyGIL();
  ImportNativeProtoCasters();
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
      .name = "koladata.types.data_bag.DataBag",
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
