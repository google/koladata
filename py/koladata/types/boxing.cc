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
#include "py/koladata/types/boxing.h"

#include <Python.h>

#include <climits>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stack>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/adoption_utils.h"
#include "koladata/arolla_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/types.h"
#include "koladata/object_factories.h"
#include "koladata/repr_utils.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/types/wrap_utils.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/quote.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {

namespace {

using ::arolla::Bytes;
using ::arolla::OptionalUnit;
using ::arolla::Text;
using ::arolla::Unit;

// Helper class for embedding schemas into an auxiliary DataBag.
class EmbeddingDataBag {
 public:
  absl::Status EmbedSchema(const DataSlice& ds) {
    if (!db_) {
      db_ = DataBag::Empty();
    }
    return ds.WithDb(db_).EmbedSchema(/*overwrite=*/false).status();
  }

  const absl::Nullable<DataBagPtr>& GetDb() { return db_; }

 private:
  absl::Nullable<DataBagPtr> db_ = nullptr;
};

/****************** Building blocks for "From Python" ******************/

// Creates a DataItem from PyObject. In case PyObject cannot be converted to
// DataItem, appropriate error is returned.
//
// `explicit_schema` is used to determine how to interpret `py_obj` in case of
// e.g. a float.
//
// `schemas` is updated by calling `schemas.Add` with the schema of the parsed
// `py_obj`.
//
// `embedding_db` is updated by calling `embedding_db.EmbedSchema` with the
// schema of the parsed `py_obj` iff the `py_obj` is an entity.
template <bool explicit_cast>
absl::StatusOr<internal::DataItem> DataItemFromPyObject(
    PyObject* py_obj, const internal::DataItem& explicit_schema,
    AdoptionQueue& adoption_queue, schema::CommonSchemaAggregator& schema_agg,
    EmbeddingDataBag& embedding_db) {
  if (py_obj == Py_None) {
    schema_agg.Add(schema::kNone);
    return internal::DataItem(internal::MissingValue());
  }
  // Checking the bool first, because PyLong_Check is also successful.
  if (PyBool_Check(py_obj)) {
    schema_agg.Add(schema::kBool);
    return internal::DataItem(static_cast<bool>(PyObject_IsTrue(py_obj)));
  }
  if (PyLong_Check(py_obj)) {
    int overflow = 0;
    // We check for overflow first to return INT64 if overflow happened.
    auto val = PyLong_AsLongLongAndOverflow(py_obj, &overflow);
    static_assert(sizeof(val) >= sizeof(int64_t));
    if (overflow) {
      // Note: casting from unsigned to signed causes the lhs to be equal to rhs
      // modulo 2^n, where n is the number of bits in the lhs. See
      // https://en.cppreference.com/w/cpp/language/implicit_conversion for more
      // details.
      val = static_cast<decltype(val)>(PyLong_AsUnsignedLongLongMask(py_obj));
    }
    if (overflow || val > INT_MAX || val < INT_MIN) {
      schema_agg.Add(schema::kInt64);
      return internal::DataItem(static_cast<int64_t>(val));
    } else {
      schema_agg.Add(schema::kInt32);
      return internal::DataItem(static_cast<int>(val));
    }
  }
  if (PyFloat_Check(py_obj)) {
    if constexpr (explicit_cast) {
      if (explicit_schema == schema::kFloat64) {
        schema_agg.Add(schema::kFloat64);
        return internal::DataItem(
            static_cast<double>(PyFloat_AsDouble(py_obj)));
      }
    }
    // NOTE: Parsing as float32 may lead to precision loss in case the final
    // schema is FLOAT64. However, if the final schema is e.g. OBJECT, we should
    // avoid FLOAT64. This is a compromise to avoid a double pass over the data.
    schema_agg.Add(schema::kFloat32);
    return internal::DataItem(static_cast<float>(PyFloat_AsDouble(py_obj)));
  }
  if (PyUnicode_Check(py_obj)) {
    Py_ssize_t size;
    const char* data = PyUnicode_AsUTF8AndSize(py_obj, &size);
    if (data == nullptr) {
      return arolla::python::StatusCausedByPyErr(
          absl::StatusCode::kInvalidArgument, "invalid unicode object");
    }
    schema_agg.Add(schema::kText);
    return internal::DataItem(Text(absl::string_view(data, size)));
  }
  if (PyBytes_Check(py_obj)) {
    Py_ssize_t size = PyBytes_Size(py_obj);
    const char* data = PyBytes_AsString(py_obj);
    if (data == nullptr) {
      return arolla::python::StatusCausedByPyErr(
          absl::StatusCode::kInvalidArgument, "invalid bytes object");
    }
    schema_agg.Add(schema::kBytes);
    return internal::DataItem(Bytes(absl::string_view(data, size)));
  }
  if (arolla::python::IsPyQValueInstance(py_obj)) {
    const auto& typed_value = arolla::python::UnsafeUnwrapPyQValue(py_obj);
    // Handling Primitive types. NOTE: This is the only way to handle MASK
    // instances as MASK is not available in pure Python.
    std::optional<internal::DataItem> res;
    arolla::QTypePtr qtype = typed_value.GetType();
    arolla::meta::foreach_type(
        internal::supported_primitives_list(), [&](auto tpe) {
          using PrimitiveT = typename decltype(tpe)::type;
          if (qtype == arolla::GetQType<PrimitiveT>()) {
            res = internal::DataItem(typed_value.UnsafeAs<PrimitiveT>());
          } else if (qtype ==
                     arolla::GetQType<arolla::OptionalValue<PrimitiveT>>()) {
            qtype = arolla::GetQType<PrimitiveT>();
            if (const auto& opt_value =
                    typed_value.UnsafeAs<arolla::OptionalValue<PrimitiveT>>();
                opt_value.present) {
              res = internal::DataItem(opt_value.value);
            } else {
              res = internal::DataItem();
            }
          }
        });
    if (res) {
      ASSIGN_OR_RETURN(auto dtype, schema::DType::FromQType(qtype));
      schema_agg.Add(dtype);
      return *std::move(res);
    }
    if (typed_value.GetType() == arolla::GetQTypeQType()) {
      auto qtype_val = typed_value.UnsafeAs<arolla::QTypePtr>();
      ASSIGN_OR_RETURN(schema::DType dtype,
                       schema::DType::FromQType(qtype_val));
      schema_agg.Add(schema::kSchema);
      return internal::DataItem(dtype);
    }
    // Handling DataItem(s).
    if (typed_value.GetType() == arolla::GetQType<DataSlice>()) {
      const auto& ds = typed_value.UnsafeAs<DataSlice>();
      if (ds.GetShape().rank() != 0) {
        return absl::InvalidArgumentError(
            "list containing multi-dim DataSlice(s) is not convertible to a "
            "DataSlice");
      }
      adoption_queue.Add(ds);
      schema_agg.Add(ds.GetSchemaImpl());
      // If casting to e.g. ANY, it does not need to be embedded.
      if (ds.GetSchemaImpl().is_entity_schema() &&
          explicit_schema == schema::kObject) {
        RETURN_IF_ERROR(embedding_db.EmbedSchema(ds));
      }
      return ds.item();
    }
  }
  return absl::InvalidArgumentError(
      absl::StrFormat("object with unsupported type: \"%s\" in nested list",
                      Py_TYPE(py_obj)->tp_name));
}

// Creates a DataSlice from a flat vector of PyObject(s), shape and type of
// items. In case, edges is empty, we have a single value and treat it as 0-dim
// output.
absl::StatusOr<DataSlice> DataSliceFromPyFlatList(
    const std::vector<PyObject*>& flat_list,
    DataSlice::JaggedShape::EdgeVec edges, internal::DataItem schema,
    AdoptionQueue& adoption_queue) {
  auto impl = [&]<bool explicit_cast>() -> absl::StatusOr<DataSlice> {
    internal::DataSliceImpl::Builder bldr(flat_list.size());
    schema::CommonSchemaAggregator schema_agg;
    EmbeddingDataBag embedding_db;
    for (int i = 0; i < flat_list.size(); ++i) {
      ASSIGN_OR_RETURN(auto item, DataItemFromPyObject<explicit_cast>(
                                      flat_list[i], schema, adoption_queue,
                                      schema_agg, embedding_db));
      bldr.Insert(i, std::move(item));
    }
    if constexpr (!explicit_cast) {
      ASSIGN_OR_RETURN(
          schema, std::move(schema_agg).Get(),
          AssembleErrorMessage(_, {.db = adoption_queue.GetDbWithFallbacks()}));
    }
    ASSIGN_OR_RETURN(auto shape,
                     DataSlice::JaggedShape::FromEdges(std::move(edges)));
    ASSIGN_OR_RETURN(
        auto ds, DataSlice::Create(std::move(bldr).Build(), std::move(shape),
                                   internal::DataItem(schema::kAny)));
    // The slice should be casted explicitly if the schema is provided by the
    // user. If this is gathered from data, it is validated to be implicitly
    // castable when finding the common schema. The schema attributes are not
    // validated, and are instead assumed to be part of the adoption queue.
    ASSIGN_OR_RETURN(auto res,
                     CastToExplicit(ds, schema, /*validate_schema=*/false));
    // Entity slices embedded to the aux db should be part of the final merged
    // db.
    if (const auto& db = embedding_db.GetDb()) {
      adoption_queue.Add(db);
    }
    return res;
  };
  if (schema.has_value()) {
    return impl.operator()<true>();
  } else {
    return impl.operator()<false>();
  }
}

// Parses the Python list and creates a DataSlice from its items with
// appropriate JaggedShape and type of items. The `py_list` can also have rank
// 0, in which case we treat it as a scalar Python value.
absl::StatusOr<DataSlice> DataSliceFromPyList(PyObject* py_list,
                                              internal::DataItem schema,
                                              AdoptionQueue& adoption_queue) {
  arolla::python::DCheckPyGIL();
  DataSlice::JaggedShape::EdgeVec edges;
  std::vector<PyObject*> lst_items;
  lst_items.push_back(py_list);
  int cur_len = 1;
  while (cur_len > 0) {
    std::vector<PyObject*> next_level_items;
    // There will be usually more than lst_items.size()/cur_len, but reserving
    // at least something. NOTE: This cannot be outside the loop, because of the
    // std::move at the bottom of the loop.
    next_level_items.reserve(cur_len);
    std::vector<int64_t> cur_split_points;
    cur_split_points.reserve(cur_len + 1);

    bool is_list = true;
    cur_split_points.push_back(0);
    for (auto lst : lst_items) {
      bool list_check = PyList_Check(lst);
      if (list_check || PyTuple_Check(lst)) {
        int64_t nested_len = list_check ? PyList_Size(lst) : PyTuple_Size(lst);
        auto getitem = list_check ? PyList_GetItem : PyTuple_GetItem;
        for (int j = 0; j < nested_len; ++j) {
          // Not using PySequence_GetItem as it increases refcount.
          next_level_items.push_back(getitem(lst, j));
        }
        cur_split_points.push_back(cur_split_points.back() + nested_len);
      } else {
        is_list = false;
      }
    }
    cur_len = next_level_items.size();
    if (!is_list && !next_level_items.empty()) {
      return absl::InvalidArgumentError(
          "input has to be a valid nested list. non-lists and lists cannot be"
          " mixed in a level");
    } else if (is_list) {
      // We do not add last edge, cur_split_points is not valid if all items
      // were non-lists.
      ASSIGN_OR_RETURN(
          edges.emplace_back(),
          arolla::DenseArrayEdge::FromSplitPoints(
              arolla::CreateFullDenseArray<int64_t>(cur_split_points)));
      lst_items = std::move(next_level_items);
    }
  }
  return DataSliceFromPyFlatList(lst_items, std::move(edges), schema,
                                 adoption_queue);
}

/****************** Building blocks for "To Python" ******************/

// The following functions Return a new reference to a Python object, equivalent
// to `value`.
PyObject* PyObjectFromValue(int value) { return PyLong_FromLongLong(value); }

PyObject* PyObjectFromValue(int64_t value) {
  return PyLong_FromLongLong(value);
}

PyObject* PyObjectFromValue(float value) { return PyFloat_FromDouble(value); }

PyObject* PyObjectFromValue(double value) { return PyFloat_FromDouble(value); }

PyObject* PyObjectFromValue(bool value) { return PyBool_FromLong(value); }

PyObject* PyObjectFromValue(Unit value) {
  return arolla::python::WrapAsPyQValue(arolla::TypedValue::FromValue(value));
}

PyObject* PyObjectFromValue(const Text& value) {
  absl::string_view text_view = value;
  return PyUnicode_DecodeUTF8(text_view.data(), text_view.size(), nullptr);
}

PyObject* PyObjectFromValue(const Bytes& value) {
  absl::string_view bytes_view = value;
  return PyBytes_FromStringAndSize(bytes_view.data(), bytes_view.size());
}

PyObject* PyObjectFromValue(const arolla::expr::ExprQuote& value) {
  return arolla::python::WrapAsPyQValue(arolla::TypedValue::FromValue(value));
}

// NOTE: Although DType is also a QValue, we don't want to expose it to user, as
// it is an internal type.
PyObject* PyObjectFromValue(schema::DType value) {
  auto ds_or = DataSlice::Create(internal::DataItem(value),
                                 internal::DataItem(schema::kSchema));
  // NOTE: `schema` is already consistent with `value` as otherwise DataSlice
  // would not even be created.
  DCHECK_OK(ds_or);
  return WrapPyDataSlice(*std::move(ds_or));
}
PyObject* PyObjectFromValue(internal::ObjectId value,
                            const internal::DataItem& schema,
                            const std::shared_ptr<DataBag>& db) {
  auto ds_or = DataSlice::Create(internal::DataItem(value), schema, db);
  // NOTE: `schema` is already consistent with `value` as otherwise DataSlice
  // would not even be created.
  DCHECK_OK(ds_or);
  return WrapPyDataSlice(*std::move(ds_or));
}

// Returns a new reference to a Python object, equivalent to the value stored in
// a `DataItem`.
absl::Nullable<PyObject*> PyObjectFromDataItem(
    const internal::DataItem& item, const internal::DataItem& schema,
    const std::shared_ptr<DataBag>& db) {
  PyObject* res = nullptr;
  item.VisitValue([&](const auto& value) {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, internal::MissingValue>) {
      res = Py_NewRef(Py_None);
    } else if constexpr (std::is_same_v<T, internal::ObjectId>) {
      res = PyObjectFromValue(value, schema, db);
    } else {
      res = PyObjectFromValue(value);
    }
  });
  return res;
}

}  // namespace

absl::StatusOr<DataSlice> DataSliceFromPyValue(PyObject* py_obj,
                                               AdoptionQueue& adoption_queue,
                                               const DataSlice* dtype) {
  arolla::python::DCheckPyGIL();
  if (dtype && dtype->GetShape().rank() != 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("schema can only be 0-rank schema slice, got: rank: ",
                     dtype->GetShape().rank()));
  }
  if (arolla::python::IsPyQValueInstance(py_obj)) {
    const auto& typed_value = arolla::python::UnsafeUnwrapPyQValue(py_obj);
    if (typed_value.GetType() == arolla::GetQType<DataSlice>()) {
      const auto& res = typed_value.UnsafeAs<DataSlice>();
      adoption_queue.Add(res);
      // Cast the input.
      if (dtype && res.GetSchemaImpl() != dtype->item()) {
        DataBagPtr db = nullptr;
        // We need to embed the schema.
        if (res.GetSchemaImpl().is_entity_schema() &&
            dtype->item() == schema::kObject) {
          db = DataBag::Empty();
          adoption_queue.Add(db);
        }
        // Schema attr validation is handled during adoption queue merging.
        return CastToExplicit(res.WithDb(db), dtype->item(),
                              /*validate_schema=*/false);
      } else {
        // Makes a copy of DataSlice object. Keeps the reference to DataBag.
        return res;
      }
    } else if (arolla::IsDenseArrayQType(typed_value.GetType())) {
      if (dtype) {
        return absl::InvalidArgumentError(
            "`dtype` should not be passed when creating a DataSlice from Arolla"
            " DenseArray");
      }
      return DataSliceFromPrimitivesDenseArray(typed_value.AsRef());
    } else if (arolla::IsArrayQType(typed_value.GetType())) {
      if (dtype) {
        return absl::InvalidArgumentError(
            "`dtype` should not be passed when creating a DataSlice from Arolla"
            " Array");
      }
      return DataSliceFromPrimitivesArray(typed_value.AsRef());
    }
  }
  return DataSliceFromPyList(
      py_obj, dtype ? dtype->item() : internal::DataItem(), adoption_queue);
}

absl::Nullable<PyObject*> DataSliceToPyValue(const DataSlice& ds) {
  arolla::python::DCheckPyGIL();
  if (ds.GetShape().rank() == 0) {
    DCHECK_EQ(ds.size(), 1);  // Invariant ensured by DataSlice creation.
    return PyObjectFromDataItem(ds.item(), ds.GetSchemaImpl(), ds.GetDb());
  }
  // Starting from a flat list of PyObject* equivalent to DataItems.
  auto py_list =
      arolla::python::PyObjectPtr::Own(PyList_New(/*len=*/ds.size()));
  const auto& ds_impl = ds.slice();
  for (int i = 0; i < ds.size(); ++i) {
    PyObject* val =
        PyObjectFromDataItem(ds_impl[i], ds.GetSchemaImpl(), ds.GetDb());
    if (val == nullptr) {
      return nullptr;
    }
    PyList_SetItem(py_list.get(), i, val);
  }
  // Nesting the flat list by iterating through JaggedShape in reverse. The last
  // Edge in shape is ignored, because the result is already produced.
  const auto& edges = ds.GetShape().edges();
  for (int edge_i = edges.size() - 1; edge_i > 0; --edge_i) {
    const auto& edge = edges[edge_i];
    DCHECK_EQ(edge.edge_type(), arolla::DenseArrayEdge::SPLIT_POINTS);
    auto new_list = arolla::python::PyObjectPtr::Own(
        PyList_New(/*len=*/edge.parent_size()));
    int offset = 0;
    edge.edge_values()
        .Slice(1, edge.parent_size())
        .ForEach([&](int64_t id, bool present, int64_t pt) {
          DCHECK(present);
          auto sub_list = PyList_New(/*len=*/pt - offset);
          for (int i = 0; i < pt - offset; ++i) {
            PyList_SetItem(
                sub_list, i,
                Py_NewRef(PyList_GetItem(py_list.get(), offset + i)));
          }
          PyList_SetItem(new_list.get(), id, sub_list);
          offset = pt;
        });
    py_list = std::move(new_list);
  }
  return py_list.release();
}

absl::StatusOr<DataSlice> DataSliceFromPyValueWithAdoption(
    PyObject* py_obj, const DataSlice* dtype) {
  AdoptionQueue adoption_queue;
  ASSIGN_OR_RETURN(DataSlice res_no_db,
                   DataSliceFromPyValue(py_obj, adoption_queue, dtype));
  ASSIGN_OR_RETURN(auto db, adoption_queue.GetCommonOrMergedDb());
  return res_no_db.WithDb(std::move(db));
}

namespace {

// Converts Python objects into DataSlices and converts them into appropriate
// Koda abstractions using `Factory`.
// `Factory` can be:
//   * EntityCreator, where each created Koda abstraction will behave as Entity;
//   * ObjectCreator, where each created Koda abstraction is converted to Object
//     and its schema gets embedded;
//   * some other, defined as an extension (e.g. DictAsObjFactory to convert
//     each dictionary into Koda Object where keys become attribute names, will
//     be used in `kd.from_py(..., dict_as_obj=True)`).
//
// UniversalConverter traverses Python dictionary recursively (without using
// recursion), such that we apply UniversalConverter on each key and each value.
// Thus, it can convert arbitrarily complex nested Python dict, even with its
// keys or values as Python lists.
//
// Python lists are not further traversed recursively.
// TODO: Support dicts nested inside lists. This is best done in
// DataSliceFromPyValue to invoke UniversalConverter when PyDict is reached.
template <typename Factory>
class UniversalConverterImpl {
 public:
  UniversalConverterImpl(const DataBagPtr& db, AdoptionQueue& adoption_queue,
                         const std::optional<DataSlice>& schema = std::nullopt)
      : db_(db), adoption_queue_(adoption_queue), schema_(schema) {
    computed_.reserve(kInitialCapacity);
  }

  absl::StatusOr<DataSlice> Convert(PyObject* py_obj) && {
    std::stack<DataSlice> return_stack;
    std::stack<CallStackFrame> call_stack;
    call_stack.push(CallStackFrame{.py_obj = py_obj,
                                   .action = CallStackFrame::kParsePyObject});
    RETURN_IF_ERROR(Process(call_stack, return_stack));
    return std::move(return_stack.top());
  }

  absl::Status ConvertDictKeysAndValues(PyObject* py_obj,
                                        std::optional<DataSlice>& keys,
                                        std::optional<DataSlice>& values) && {
    std::stack<DataSlice> return_stack;
    std::stack<CallStackFrame> call_stack;
    CollectDictInputs(py_obj, call_stack);
    RETURN_IF_ERROR(Process(call_stack, return_stack));
    keys = std::move(return_stack.top());
    return_stack.pop();
    values = std::move(return_stack.top());
    return absl::OkStatus();
  }

 private:
  static constexpr size_t kInitialCapacity = 1024;

  // Record that is put on stack to be processed, instead of using recursion.
  struct CallStackFrame {
    PyObject* py_obj;
    size_t num_inputs;
    enum {
      // Processes py_obj, and depending on its type, either immediately
      // converts it to a Koda value (and pushes the result to `return_stack`),
      // or schedules additional actions for the conversion (by pushing them to
      // `call_stack`).
      kParsePyObject,
      // Takes preconstructed keys and values slices from `result_stack`,
      // assembles them into a dictionary and pushes it back to `return_stack`.
      kComputeDict,
      // Takes `num_inputs` preconstructed Koda values from `result_stack` and
      // computes a DataSlice from them and pushes it back to `return_stack`.
      kComputeDictKeysOrValues,
    } action;
  };

  // Main method of UniversalConverterImpl, which actually traverses the Python
  // structure `py_obj`, by simulating recursion using a stack.
  //
  // The method is used in 2 ways:
  // * When we need the Koda abstraction result from PyObject, regardless what
  //   it is. In that case the caller should push PyObject to the stack. In this
  //   case the caller should expect the top of `return_stack` to represent the
  //   final converted PyObject into Koda object;
  // * when we want to convert keys and values of a dict, but not dict itself
  //   (we do NOT want garbage triples in a DataBag that won't be used), the
  //   caller should call `CollectDictInputs`, which will push "keys" and
  //   "values" arguments to the stack for processing. The caller should expect
  //   2 top values on the `return_stack` to represent converted `keys` and
  //   `values` (keys are on the top, while values are accessible after popping
  //   the keys).
  //
  // The algorithm works with 2 stacks:
  // * Input stack: `call_stack`;
  // * Output stack: `return_stack`.
  //
  // See `CallStackFrame::action` for details on different actions performed by
  // the algorithm.
  absl::Status Process(std::stack<CallStackFrame>& call_stack,
                       std::stack<DataSlice>& return_stack) {
    while (!call_stack.empty()) {
      CallStackFrame cur_arg = call_stack.top();
      call_stack.pop();
      switch (cur_arg.action) {
        case CallStackFrame::kParsePyObject:
          RETURN_IF_ERROR(
              ParsePyObject(cur_arg.py_obj, call_stack, return_stack));
          break;
        case CallStackFrame::kComputeDict:
          RETURN_IF_ERROR(ComputeDict(
              cur_arg.py_obj, /*is_root=*/call_stack.empty(), return_stack));
          break;
        case CallStackFrame::kComputeDictKeysOrValues:
          RETURN_IF_ERROR(
              ComputeDictKeysOrValues(cur_arg.num_inputs, return_stack));
          break;
      }
    }
    return absl::OkStatus();
  }

  // Collects keys and values of `py_obj` Python dict. It pushes appropriate
  // arguments to stack in appropriate state (first / second time on stack).
  //
  // Utility is very useful to initialze a stack state for processing only keys
  // and values, without the main PyDict root object `py_obj`.
  void CollectDictInputs(PyObject* py_obj,
                         std::stack<CallStackFrame>& call_stack) {
    size_t dict_size = PyDict_Size(py_obj);

    std::vector<PyObject*> keys;
    keys.reserve(dict_size);
    std::vector<PyObject*> values;
    values.reserve(dict_size);
    PyObject* key;
    PyObject* value;
    Py_ssize_t pos = 0;
    // All Py references are borrowed.
    while (PyDict_Next(py_obj, &pos, &key, &value)) {
      keys.push_back(key);
      values.push_back(value);
    }

    call_stack.push(
        CallStackFrame{.py_obj = nullptr,
                       .num_inputs = dict_size,
                       .action = CallStackFrame::kComputeDictKeysOrValues});
    for (auto it = keys.rbegin(); it != keys.rend(); ++it) {
      call_stack.push(CallStackFrame{.py_obj = *it,
                                     .action = CallStackFrame::kParsePyObject});
    }

    call_stack.push(
        CallStackFrame{.py_obj = nullptr,
                       .num_inputs = dict_size,
                       .action = CallStackFrame::kComputeDictKeysOrValues});
    for (auto it = values.rbegin(); it != values.rend(); ++it) {
      call_stack.push(CallStackFrame{.py_obj = *it,
                                     .action = CallStackFrame::kParsePyObject});
    }
  }

  // Implements `kParsePyObject` action (see the comment on the enum).
  absl::Status ParsePyObject(PyObject* py_obj,
                             std::stack<CallStackFrame>& call_stack,
                             std::stack<DataSlice>& return_stack) {
    // Push `std::nullopt` to detect recrusive Python structures.
    auto [computed_iter, emplaced] = computed_.emplace(py_obj, std::nullopt);
    if (!emplaced) {
      if (!computed_iter->second.has_value()) {
        return absl::InvalidArgumentError(
            "recursive Python structures cannot be converted to Koda "
            "object");
      }
      return_stack.push(*computed_iter->second);
      return absl::OkStatus();
    }

    if (PyDict_CheckExact(py_obj)) {
      call_stack.push(CallStackFrame{.py_obj = py_obj,
                                     .num_inputs = 2,
                                     .action = CallStackFrame::kComputeDict});
      CollectDictInputs(py_obj, call_stack);
      // The object will be put to `return_stack` later by the kComputeDict
      // action.
      return absl::OkStatus();
    } else {
      ASSIGN_OR_RETURN(auto res, DataSliceFromPyValue(py_obj, adoption_queue_));
      if (res.GetShape().rank() > 0) {
        if (arolla::python::IsPyQValueInstance(py_obj)) {
          return absl::InvalidArgumentError(
              "dict containing multi-dim DataSlice(s) is not convertible to"
              "a DataSlice");
        }
        ASSIGN_OR_RETURN(
            res, CreateNestedList(db_, res,
                                  call_stack.empty() ? schema_ : std::nullopt));
      }
      // Only Entities are converted using Factory, while primitives are kept as
      // is, unless they are the ones being converted explicitly (e.g.
      // kd.obj(42), in which case `call_stack` is empty).
      if (res.GetSchemaImpl().is_entity_schema() || call_stack.empty()) {
        ASSIGN_OR_RETURN(return_stack.emplace(), Factory::Convert(db_, res));
      } else {
        return_stack.emplace(std::move(res));
      }
      computed_iter->second = return_stack.top();
      return absl::OkStatus();
    }
  }

  // Implements `kComputeDict` action (see the comment on the enum).
  absl::Status ComputeDict(PyObject* py_obj, bool is_root,
                           std::stack<DataSlice>& return_stack) {
    DataSlice keys = std::move(return_stack.top());
    return_stack.pop();
    DataSlice values = std::move(return_stack.top());
    return_stack.pop();

    ASSIGN_OR_RETURN(
        auto res, CreateDictShaped(db_, DataSlice::JaggedShape::Empty(), keys,
                                   values, is_root ? schema_ : std::nullopt));
    ASSIGN_OR_RETURN(return_stack.emplace(),
                     Factory::Convert(db_, std::move(res)));
    computed_.insert_or_assign(py_obj, return_stack.top());
    return absl::OkStatus();
  }

  // Implements `kComputeDictKeysOrValues` action (see the comment on the enum).
  absl::Status ComputeDictKeysOrValues(size_t dict_size,
                                       std::stack<DataSlice>& return_stack) {
    // Dict keys or values.
    internal::DataSliceImpl::Builder kv_bldr(dict_size);
    schema::CommonSchemaAggregator kv_schema_agg;
    for (int64_t index = 0; index < dict_size; ++index) {
      DataSlice input = std::move(return_stack.top());
      return_stack.pop();
      kv_bldr.Insert(index, input.item());
      kv_schema_agg.Add(input.GetSchemaImpl());
    }
    ASSIGN_OR_RETURN(auto kv_schema, std::move(kv_schema_agg).Get());
    // NOTE: Factory is not applied on keys and values DataSlices (just on their
    // elements and dict created from those keys and values).
    ASSIGN_OR_RETURN(
        return_stack.emplace(),
        DataSlice::Create(std::move(kv_bldr).Build(),
                          DataSlice::JaggedShape::FlatFromSize(dict_size),
                          kv_schema, db_));
    return absl::OkStatus();
  }

  const DataBagPtr& db_;
  AdoptionQueue& adoption_queue_;
  const std::optional<DataSlice>& schema_;

  // Maps Python object to a computed DataSlice that is returned from the
  // "simulated recursive call". It is also used to detect recursive Python
  // structures (e.g. a Python object is already visited, but not computed, the
  // stored value is `std::nullopt`).
  absl::flat_hash_map<PyObject*, std::optional<DataSlice>> computed_;
};

}  // namespace

absl::StatusOr<DataSlice> EntitiesFromPyObject(PyObject* py_obj,
                                               const DataBagPtr& db,
                                               AdoptionQueue& adoption_queue) {
  if (arolla::python::IsPyQValueInstance(py_obj)) {
    ASSIGN_OR_RETURN(auto res, DataSliceFromPyValue(py_obj, adoption_queue));
    return EntityCreator::Convert(db, res);
  }
  return UniversalConverterImpl<EntityCreator>(db, adoption_queue)
      .Convert(py_obj);
}

absl::StatusOr<DataSlice> EntitiesFromPyObject(
    PyObject* py_obj, const std::optional<DataSlice>& schema,
    const DataBagPtr& db, AdoptionQueue& adoption_queue) {
  if (arolla::python::IsPyQValueInstance(py_obj)) {
    ASSIGN_OR_RETURN(auto res, DataSliceFromPyValue(py_obj, adoption_queue));
    return EntityCreator::Convert(db, res);
  }
  return UniversalConverterImpl<EntityCreator>(db, adoption_queue, schema)
      .Convert(py_obj);
}

absl::StatusOr<DataSlice> ObjectsFromPyObject(PyObject* py_obj,
                                              const DataBagPtr& db,
                                              AdoptionQueue& adoption_queue) {
  if (arolla::python::IsPyQValueInstance(py_obj)) {
    ASSIGN_OR_RETURN(auto res, DataSliceFromPyValue(py_obj, adoption_queue));
    return ObjectCreator::Convert(db, res);
  }
  return UniversalConverterImpl<ObjectCreator>(db, adoption_queue)
      .Convert(py_obj);
}

absl::Status ConvertDictKeysAndValues(PyObject* py_obj, const DataBagPtr& db,
                                      AdoptionQueue& adoption_queue,
                                      std::optional<DataSlice>& keys,
                                      std::optional<DataSlice>& values) {
  return UniversalConverterImpl<EntityCreator>(db, adoption_queue)
      .ConvertDictKeysAndValues(py_obj, keys, values);
}

}  // namespace koladata::python
