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
#include "py/koladata/base/boxing.h"

#include <Python.h>

#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/adoption_utils.h"
#include "koladata/arolla_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/error_repr_utils.h"
#include "koladata/internal/casting.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/internal/types.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {

using ::arolla::Bytes;
using ::arolla::OptionalUnit;
using ::arolla::Text;
using ::arolla::Unit;
using ::koladata::internal::DataItem;

// Creates a DataSlice and optionally casts data according to `schema`
// argument.
absl::StatusOr<DataSlice> CreateWithSchema(DataItem impl, DataItem schema) {
  // NOTE: CastDataTo does not do schema validation or schema embedding (in case
  // schema is OBJECT).
  ASSIGN_OR_RETURN(impl, schema::CastDataTo(impl, schema));
  return DataSlice::Create(std::move(impl), std::move(schema));
}

absl::StatusOr<DataSlice> CreateWithSchema(internal::DataSliceImpl impl,
                                           DataSlice::JaggedShape shape,
                                           DataItem schema) {
  // NOTE: CastDataTo does not do schema validation or schema embedding (in case
  // schema is OBJECT).
  ASSIGN_OR_RETURN(impl, schema::CastDataTo(impl, schema));
  return DataSlice::Create(std::move(impl), std::move(shape),
                           std::move(schema));
}

absl::Status CreateIncompatibleSchemaError(const DataSlice& item_schema,
                                           const DataSlice& input_schema) {
  std::string item_schema_str = SchemaToStr(item_schema);
  std::string input_schema_str = SchemaToStr(input_schema);
  return absl::InvalidArgumentError(
      absl::StrFormat("the schema is incompatible:\n"
                      "expected schema: %s\n"
                      "assigned schema: %s",
                      item_schema_str, input_schema_str));
}

// Helper class for embedding schemas into an auxiliary DataBag.
class EmbeddingDataBag {
 public:
  absl::Status EmbedSchema(const DataSlice& ds) {
    if (db_ == nullptr) {
      db_ = DataBag::EmptyMutable();
    }
    return ds.WithBag(db_).EmbedSchema(/*overwrite=*/false).status();
  }

  const absl_nullable DataBagPtr& GetBag() { return db_; }

 private:
  absl_nullable DataBagPtr db_ = nullptr;
};

/****************** Building blocks for "From Python" ******************/

// Calls `callback` with a value from PyObject. In case PyObject cannot be
// parsed, appropriate error is returned.
//
// `explicit_schema` is used to determine how to interpret `py_obj` in case of
// e.g. a float.
//
// `schemas` is updated by calling `schemas.Add` with the schema of the parsed
// `py_obj`.
//
// `embedding_db` is updated by calling `embedding_db.EmbedSchema` with the
// schema of the parsed `py_obj` iff the `py_obj` is an entity.
template <class Callback>
absl::Status ParsePyObject(PyObject* py_obj, const DataItem& explicit_schema,
                           AdoptionQueue& adoption_queue,
                           schema::CommonSchemaAggregator& schema_agg,
                           EmbeddingDataBag& embedding_db, Callback callback) {
  if (py_obj == Py_None) {
    schema_agg.Add(schema::kNone);
    callback(internal::MissingValue());
    return absl::OkStatus();
  }
  // Checking the bool first, because PyLong_Check is also successful.
  if (PyBool_Check(py_obj)) {
    schema_agg.Add(schema::kBool);
    callback(py_obj == Py_True);
    return absl::OkStatus();
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
      callback(static_cast<int64_t>(val));
    } else {
      schema_agg.Add(schema::kInt32);
      callback(static_cast<int>(val));
    }
    return absl::OkStatus();
  }
  if (PyFloat_Check(py_obj)) {
    if (explicit_schema == schema::kFloat64) {
      schema_agg.Add(schema::kFloat64);
      callback(static_cast<double>(PyFloat_AsDouble(py_obj)));
      return absl::OkStatus();
    }
    // NOTE: Parsing as float32 may lead to precision loss in case the final
    // schema is FLOAT64. However, if the final schema is e.g. OBJECT, we should
    // avoid FLOAT64. This is a compromise to avoid a double pass over the data.
    schema_agg.Add(schema::kFloat32);
    callback(static_cast<float>(PyFloat_AsDouble(py_obj)));
    return absl::OkStatus();
  }
  if (PyUnicode_Check(py_obj)) {
    Py_ssize_t size;
    const char* data = PyUnicode_AsUTF8AndSize(py_obj, &size);
    if (data == nullptr) {
      return arolla::python::StatusCausedByPyErr(
          absl::StatusCode::kInvalidArgument, "invalid unicode object");
    }
    schema_agg.Add(schema::kString);
    callback(DataItem::View<Text>(absl::string_view(data, size)));
    return absl::OkStatus();
  }
  if (PyBytes_Check(py_obj)) {
    Py_ssize_t size = PyBytes_Size(py_obj);
    const char* data = PyBytes_AsString(py_obj);
    if (data == nullptr) {
      return arolla::python::StatusCausedByPyErr(
          absl::StatusCode::kInvalidArgument, "invalid bytes object");
    }
    schema_agg.Add(schema::kBytes);
    callback(DataItem::View<Bytes>(absl::string_view(data, size)));
    return absl::OkStatus();
  }
  if (arolla::python::IsPyQValueInstance(py_obj)) {
    const auto& typed_value = arolla::python::UnsafeUnwrapPyQValue(py_obj);
    // Handling Primitive types. NOTE: This is the only way to handle MASK
    // instances as MASK is not available in pure Python.
    std::optional<DataItem> res;
    arolla::QTypePtr qtype = typed_value.GetType();
    arolla::meta::foreach_type(
        internal::supported_primitives_list(), [&](auto tpe) {
          using PrimitiveT = typename decltype(tpe)::type;
          if (qtype == arolla::GetQType<PrimitiveT>()) {
            res = DataItem(typed_value.UnsafeAs<PrimitiveT>());
          } else if (qtype ==
                     arolla::GetQType<arolla::OptionalValue<PrimitiveT>>()) {
            qtype = arolla::GetQType<PrimitiveT>();
            if (const auto& opt_value =
                    typed_value.UnsafeAs<arolla::OptionalValue<PrimitiveT>>();
                opt_value.present) {
              res = DataItem(opt_value.value);
            } else {
              res = DataItem();
            }
          }
        });
    if (res) {
      ASSIGN_OR_RETURN(auto dtype, schema::DType::FromQType(qtype));
      schema_agg.Add(dtype);
      callback(*std::move(res));
      return absl::OkStatus();
    }
    if (typed_value.GetType() == arolla::GetQTypeQType()) {
      auto qtype_val = typed_value.UnsafeAs<arolla::QTypePtr>();
      ASSIGN_OR_RETURN(schema::DType dtype,
                       schema::DType::FromQType(qtype_val));
      schema_agg.Add(schema::kSchema);
      callback(dtype);
      return absl::OkStatus();
    }
    // Handling DataItem(s).
    if (typed_value.GetType() == arolla::GetQType<DataSlice>()) {
      const auto& ds = typed_value.UnsafeAs<DataSlice>();
      if (!ds.is_item()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "python object can only contain DataItems, got DataSlice with shape"
            " %s",
            arolla::Repr(ds.GetShape())));
      }
      adoption_queue.Add(ds);
      schema_agg.Add(ds.GetSchemaImpl());
      // It only needs to be embedded in some cases.
      if (ds.GetSchemaImpl().is_struct_schema() &&
          explicit_schema == schema::kObject) {
        RETURN_IF_ERROR(embedding_db.EmbedSchema(ds));
      }
      callback(ds.item());
      return absl::OkStatus();
    }
    return absl::InvalidArgumentError(absl::StrFormat(
        "object with unsupported type: %s", Py_TYPE(py_obj)->tp_name));
  }
  DCHECK(!IsPyScalarOrQValueObject(py_obj));
  if (arolla::python::IsPyExprInstance(py_obj)) {
    const auto& expr = arolla::python::UnsafeUnwrapPyExpr(py_obj);
    return absl::InvalidArgumentError(absl::StrFormat(
        "object with unsupported type, %s, for value:"
        "\n\n  %s\n\n"
        "this can happen when calling kd.slice / kd.item, e.g."
        "\n\n  kd.slice(x)\n\n"
        "or when trying to concatenate objects during tracing, e.g."
        "\n\n  kd.slice([kd.obj(x=1), kd.obj(x=2)])\n\n"
        "please use kd.stack(kd.obj(x=1), ...), instead",
        Py_TYPE(py_obj)->tp_name, arolla::expr::ToDebugString(expr)));
  } else {
    return absl::InvalidArgumentError(absl::StrFormat(
        "object with unsupported type: %s", Py_TYPE(py_obj)->tp_name));
  }
}

// Parses the Python list and creates a DataSlice from its items with
// appropriate JaggedShape and type of items. The `py_list` can also have rank
// 0, in which case we treat it as a scalar Python value.
absl::StatusOr<DataSlice> DataSliceFromPyList(PyObject* py_list,
                                              DataItem schema,
                                              AdoptionQueue& adoption_queue) {
  arolla::python::DCheckPyGIL();
  ASSIGN_OR_RETURN((auto [py_objects, shape]),
                   FlattenPyList(py_list, /*max_depth=*/0));

  const bool explicit_cast = schema.has_value();
  return DataSliceFromPyFlatList(py_objects, std::move(shape),
                                 std::move(schema), adoption_queue,
                                 explicit_cast);
}

}  // namespace

bool IsPyScalarOrQValueObject(PyObject* py_obj) {
  // None is excluded from this check, since it can be both a complex structure
  // (dict/list/object) or a scalar.
  return PyBool_Check(py_obj) || PyLong_Check(py_obj) ||
         PyFloat_Check(py_obj) || PyUnicode_Check(py_obj) ||
         PyBytes_Check(py_obj) || arolla::python::IsPyQValueInstance(py_obj);
}

absl::Status CreateIncompatibleSchemaErrorFromStatus(
    absl::Status status, const DataSlice& item_schema,
    const DataSlice& input_schema) {
  std::string item_schema_str = SchemaToStr(item_schema);
  std::string input_schema_str = SchemaToStr(input_schema);
  return internal::KodaErrorFromCause(
      absl::StrFormat("the schema is incompatible:\n"
                      "expected schema: %s\n"
                      "assigned schema: %s",
                      item_schema_str, input_schema_str),
      std::move(status));
}

absl::StatusOr<DataSlice> DataSliceFromPyFlatList(
    const std::vector<PyObject*>& flat_list, DataSlice::JaggedShape shape,
    DataItem schema, AdoptionQueue& adoption_queue, bool explicit_cast) {
  if (explicit_cast && !schema.has_value()) {
    return absl::InvalidArgumentError(
        "schema must be provided for explicit_cast");
  }
  int64_t res_size = flat_list.size();

  // Helper lambdas for parsing text and bytes.

  // Creates a StringsBuffer from a list of (index, str) pairs.
  auto to_str_buffer =
      [res_size](absl::Span<const std::pair<int64_t, absl::string_view>> strs,
                 int64_t total_size) {
        auto str_bldr = arolla::StringsBuffer::Builder(
            res_size, /*initial_char_buffer_size=*/total_size);
        for (const auto& [idx, text] : strs) {
          str_bldr.Set(idx, text);
        }
        return std::move(str_bldr).Build();
      };

  // Creates a Bitmap from a list of (index, str) pairs.
  // Returns an empty bitmap if all elements are present.
  auto to_bitmask =
      [res_size](absl::Span<const std::pair<int64_t, absl::string_view>> strs) {
        if (strs.size() == res_size) {
          return arolla::bitmap::Bitmap();
        }
        auto bitmask_bldr = arolla::bitmap::Bitmap::Builder(
            arolla::bitmap::BitmapSize(res_size));
        auto bitmap = bitmask_bldr.GetMutableSpan();
        std::memset(bitmap.data(), 0,
                    bitmap.size() * sizeof(arolla::bitmap::Word));
        for (const auto& [idx, text] : strs) {
          arolla::bitmap::SetBit(bitmap.data(), idx);
        }
        return std::move(bitmask_bldr).Build();
      };

  // Adds a DenseArray of type T with the given strings.
  auto add_str_array =
      [&]<typename T>(
          std::type_identity<T>, internal::SliceBuilder& bldr,
          absl::Span<const std::pair<int64_t, absl::string_view>> strs,
          int64_t total_size) {
        auto str_buffer = to_str_buffer(strs, total_size);
        auto bitmask = to_bitmask(strs);
        bldr.InsertIfNotSet<T>(bitmask, {}, str_buffer);
      };

  internal::SliceBuilder bldr(res_size);
  schema::CommonSchemaAggregator schema_agg;
  schema_agg.Add(schema::kNone);
  EmbeddingDataBag embedding_db;
  int64_t text_total_size = 0;
  std::vector<std::pair<int64_t, absl::string_view>> texts;
  int64_t bytes_total_size = 0;
  std::vector<std::pair<int64_t, absl::string_view>> bytes;
  for (int i = 0; i < res_size; ++i) {
    auto parse_cb = [&]<class T>(T&& value) {
      if constexpr (std::is_same_v<T, DataItem::View<Text>>) {
        texts.reserve(res_size);
        text_total_size += value.view.size();
        texts.emplace_back(i, value.view);
      } else if constexpr (std::is_same_v<T, DataItem::View<Bytes>>) {
        bytes.reserve(res_size);
        bytes_total_size += value.view.size();
        bytes.emplace_back(i, value.view);
      } else if constexpr (std::is_same_v<std::decay_t<T>, DataItem>) {
        bldr.InsertIfNotSetAndUpdateAllocIds(i, std::forward<T>(value));
      } else {
        bldr.InsertIfNotSet(i, std::forward<T>(value));
      }
    };
    RETURN_IF_ERROR(ParsePyObject(flat_list[i], schema, adoption_queue,
                                  schema_agg, embedding_db, parse_cb));
  }

  if (!texts.empty()) {
    add_str_array(std::type_identity<arolla::Text>{}, bldr, texts,
                  text_total_size);
  }
  if (!bytes.empty()) {
    add_str_array(std::type_identity<arolla::Bytes>{}, bldr, bytes,
                  bytes_total_size);
  }
  if (!explicit_cast) {
    ASSIGN_OR_RETURN(DataItem agg_schema, std::move(schema_agg).Get(),
                     KodaErrorCausedByNoCommonSchemaError(
                         _, adoption_queue.GetBagWithFallbacks()));
    if (schema.has_value()) {
      if (!schema::IsImplicitlyCastableTo(agg_schema, schema)) {
        ASSIGN_OR_RETURN(
            DataSlice agg_schema_ds,
            DataSlice::Create(agg_schema, internal::DataItem(schema::kSchema),
                              adoption_queue.GetBagWithFallbacks()));
        ASSIGN_OR_RETURN(
            DataSlice schema_ds,
            DataSlice::Create(schema, internal::DataItem(schema::kSchema),
                              adoption_queue.GetBagWithFallbacks()));
        return CreateIncompatibleSchemaError(agg_schema_ds, schema_ds);
      }
    } else {
      schema = std::move(agg_schema);
    }
  }
  // The slice should be casted explicitly if the schema is provided by the
  // user. If this is gathered from data, it is validated to be implicitly
  // castable when finding the common schema. The schema attributes are not
  // validated, and are instead assumed to be part of the adoption queue.
  ASSIGN_OR_RETURN(auto res,
                   CreateWithSchema(std::move(bldr).Build(), std::move(shape),
                                    std::move(schema)));
  // Entity slices embedded to the aux db should be part of the final merged
  // db.
  adoption_queue.Add(embedding_db.GetBag());
  return res;
}

// Parses the Python list and returns flattened items together with appropriate
// JaggedShape.
absl::StatusOr<std::pair<std::vector<PyObject*>, DataSlice::JaggedShape>>
FlattenPyList(PyObject* py_list, size_t max_depth) {
  arolla::python::DCheckPyGIL();
  DataSlice::JaggedShape::EdgeVec edges;
  std::vector<PyObject*> lst_items;
  lst_items.push_back(py_list);
  int cur_len = 1;
  int cur_depth = 0;
  while (cur_len > 0 && (max_depth == 0 || cur_depth < max_depth)) {
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
    if (is_list) {
      ++cur_depth;
    }
  }
  if (max_depth > 0 && cur_depth < max_depth) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "could not traverse the nested list of depth %d up to the level %d",
        cur_depth, max_depth));
  }
  ASSIGN_OR_RETURN(DataSlice::JaggedShape shape,
                   DataSlice::JaggedShape::FromEdges(std::move(edges)));
  return std::make_pair(std::move(lst_items), std::move(shape));
}

absl::StatusOr<DataSlice> DataSliceFromPyValue(
    PyObject* py_obj, AdoptionQueue& adoption_queue,
    const std::optional<DataSlice>& schema) {
  arolla::python::DCheckPyGIL();
  if (schema) {
    RETURN_IF_ERROR(schema->VerifyIsSchema());
    adoption_queue.Add(*schema);
  }
  if (arolla::python::IsPyQValueInstance(py_obj)) {
    const auto& typed_value = arolla::python::UnsafeUnwrapPyQValue(py_obj);
    if (typed_value.GetType() == arolla::GetQType<DataSlice>()) {
      const auto& res = typed_value.UnsafeAs<DataSlice>();
      adoption_queue.Add(res);
      // Cast the input.
      if (schema && res.GetSchemaImpl() != schema->item()) {
        DataBagPtr db = nullptr;
        // We need to embed the schema.
        if (res.GetSchemaImpl().is_struct_schema() &&
            schema->item() == schema::kObject) {
          db = DataBag::EmptyMutable();
          adoption_queue.Add(db);
        }
        // Schema attr validation is handled during adoption queue merging.
        return CastToExplicit(res.WithBag(std::move(db)), schema->item(),
                              /*validate_schema=*/false);
      } else {
        // Makes a copy of DataSlice object. Keeps the reference to DataBag.
        return res;
      }
    } else if (arolla::IsDenseArrayQType(typed_value.GetType())) {
      if (schema) {
        return absl::InvalidArgumentError(
            "`schema` should not be passed when creating a DataSlice from "
            "Arolla DenseArray");
      }
      return DataSliceFromPrimitivesDenseArray(typed_value.AsRef());
    } else if (arolla::IsArrayQType(typed_value.GetType())) {
      if (schema) {
        return absl::InvalidArgumentError(
            "`schema` should not be passed when creating a DataSlice from "
            "Arolla Array");
      }
      return DataSliceFromPrimitivesArray(typed_value.AsRef());
    }
  }
  if (!PyList_CheckExact(py_obj) && !PyTuple_CheckExact(py_obj)) {
    return DataItemFromPyValue(py_obj, schema);
  }
  return DataSliceFromPyList(py_obj, schema ? schema->item() : DataItem(),
                             adoption_queue);
}

absl::StatusOr<DataSlice> DataItemFromPyValue(
    PyObject* py_obj, const std::optional<DataSlice>& schema) {
  AdoptionQueue adoption_queue;
  internal::DataItem res_item;
  internal::DataItem schema_item;
  auto to_data_item_fn = [&res_item]<class T>(T&& value) {
    res_item = internal::DataItem(std::forward<T>(value));
  };
  // NOTE: If there is a DataBag `py_obj`, it is added to the adoption_queue.
  if (schema.has_value()) {
    RETURN_IF_ERROR(schema->VerifyIsSchema());
    adoption_queue.Add(*schema);
    schema_item = schema->item();
    EmbeddingDataBag embedding_db;
    schema::CommonSchemaAggregator unused_schema_agg;
    RETURN_IF_ERROR(ParsePyObject(py_obj, /*explicit_schema=*/schema_item,
                                  adoption_queue, unused_schema_agg,
                                  embedding_db, to_data_item_fn));
    // Entity slices embedded to the aux db should be part of the final merged
    // db.
    adoption_queue.Add(embedding_db.GetBag());
  } else {
    schema::CommonSchemaAggregator schema_agg;
    EmbeddingDataBag unused_embedding_db;
    RETURN_IF_ERROR(ParsePyObject(
        py_obj, /*explicit_schema=*/internal::DataItem(), adoption_queue,
        schema_agg, unused_embedding_db, to_data_item_fn));
    DCHECK_EQ(unused_embedding_db.GetBag(), nullptr);
    ASSIGN_OR_RETURN(schema_item, std::move(schema_agg).Get());
  }
  ASSIGN_OR_RETURN(DataSlice res, CreateWithSchema(std::move(res_item),
                                                   std::move(schema_item)));
  if (adoption_queue.empty()) {
    return res;
  }
  ASSIGN_OR_RETURN(DataBagPtr res_bag, adoption_queue.GetCommonOrMergedDb());
  return res.WithBag(std::move(res_bag));
}

absl::StatusOr<DataSlice> DataSliceFromPyValueWithAdoption(
    PyObject* py_obj, const std::optional<DataSlice>& schema) {
  AdoptionQueue adoption_queue;
  ASSIGN_OR_RETURN(DataSlice res_no_db,
                   DataSliceFromPyValue(py_obj, adoption_queue, schema));
  ASSIGN_OR_RETURN(auto db, adoption_queue.GetCommonOrMergedDb());
  return res_no_db.WithBag(std::move(db));
}

// Parses a Python unicode or Koda DataItem into a absl::string_view if
// possible. Otherwise, returns an error (with dict key specific error message).
absl::StatusOr<absl::string_view> PyDictKeyAsStringView(PyObject* py_key) {
  if (PyUnicode_Check(py_key)) {
    Py_ssize_t size;
    const char* data = PyUnicode_AsUTF8AndSize(py_key, &size);
    if (data == nullptr) {
      PyErr_Clear();
    } else {
      return absl::string_view(data, size);
    }
  } else if (arolla::python::IsPyQValueInstance(py_key)) {
    if (auto typed_value = arolla::python::UnsafeUnwrapPyQValue(py_key);
        typed_value.GetType() == arolla::GetQType<DataSlice>()) {
      const auto& ds = typed_value.UnsafeAs<DataSlice>();
      // NOTE: This cannot happen, because multi-dim DataSlice is not hashable.
      if (!ds.is_item()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "python dict keys can only be DataItems, got DataSlice with shape "
            "%s",
            arolla::Repr(ds.GetShape())));
      }
      if (ds.item().holds_value<arolla::Text>()) {
        return ds.item().value<arolla::Text>().view();
      }
      return absl::InvalidArgumentError(absl::StrFormat(
          "dict keys cannot be non-STRING DataItems, got %v", ds.item()));
    }
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "dict_as_obj requires keys to be valid unicode objects, got %s",
      Py_TYPE(py_key)->tp_name));
}

}  // namespace koladata::python
