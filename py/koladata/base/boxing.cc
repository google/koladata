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
#include <functional>
#include <optional>
#include <stack>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
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
#include "arolla/util/fingerprint.h"
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
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/internal/types.h"
#include "koladata/object_factories.h"
#include "koladata/schema_utils.h"
#include "koladata/uuid_utils.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/py_conversions/dataclasses_util.h"
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
      db_ = DataBag::Empty();
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
template <bool explicit_cast, class Callback>
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
    if constexpr (explicit_cast) {
      if (explicit_schema == schema::kFloat64) {
        schema_agg.Add(schema::kFloat64);
        callback(static_cast<double>(PyFloat_AsDouble(py_obj)));
        return absl::OkStatus();
      }
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
  }
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

  return DataSliceFromPyFlatList(py_objects, std::move(shape),
                                 std::move(schema), adoption_queue);
}

}  // namespace

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
    DataItem schema, AdoptionQueue& adoption_queue) {
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

  auto impl = [&]<bool explicit_cast>() -> absl::StatusOr<DataSlice> {
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
      RETURN_IF_ERROR(ParsePyObject<explicit_cast>(flat_list[i], schema,
                                                   adoption_queue, schema_agg,
                                                   embedding_db, parse_cb));
    }

    if (!texts.empty()) {
      add_str_array(std::type_identity<arolla::Text>{}, bldr, texts,
                    text_total_size);
    }
    if (!bytes.empty()) {
      add_str_array(std::type_identity<arolla::Bytes>{}, bldr, bytes,
                    bytes_total_size);
    }
    if constexpr (!explicit_cast) {
      ASSIGN_OR_RETURN(schema, std::move(schema_agg).Get(),
                       KodaErrorCausedByNoCommonSchemaError(
                           _, adoption_queue.GetBagWithFallbacks()));
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
  };
  if (schema.has_value()) {
    return impl.operator()<true>();
  } else {
    return impl.operator()<false>();
  }
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
          db = DataBag::Empty();
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
    RETURN_IF_ERROR(ParsePyObject</*explicit_cast=*/true>(
        py_obj, /*explicit_schema=*/schema_item, adoption_queue,
        unused_schema_agg, embedding_db, to_data_item_fn));
    // Entity slices embedded to the aux db should be part of the final merged
    // db.
    adoption_queue.Add(embedding_db.GetBag());
  } else {
    schema::CommonSchemaAggregator schema_agg;
    EmbeddingDataBag unused_embedding_db;
    RETURN_IF_ERROR(ParsePyObject</*explicit_cast=*/false>(
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

namespace {

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

constexpr static absl::string_view kChildItemIdSeed = "__from_py_child__";
constexpr static absl::string_view kChildAttributeAttributeName = "attr_name";
constexpr static absl::string_view kChildListItemAttributeName =
    "list_item_index";
constexpr static absl::string_view kChildDictKeyAttributeName =
    "dict_key_index";
constexpr static absl::string_view kChildDictValueAttributeName =
    "dict_value_index";

absl::StatusOr<DataSlice> MakeIntItem(int64_t value) {
  return DataSlice::Create(internal::DataItem(value),
                           internal::DataItem(schema::kInt64));
}

absl::StatusOr<DataSlice> MakeTextItem(absl::string_view text) {
  return DataSlice::Create(internal::DataItem(arolla::Text(text)),
                           internal::DataItem(schema::kString));
}

struct ChildItemIdAttrsDescriptor {
  ChildItemIdAttrsDescriptor() = default;
  explicit ChildItemIdAttrsDescriptor(absl::string_view attr_name)
      : attr_name(attr_name) {}
  ChildItemIdAttrsDescriptor(absl::string_view index_attr_name, int64_t index)
      : attr_name_and_index(index_attr_name, index) {}

  absl::string_view attr_name;
  std::pair<absl::string_view, int64_t> attr_name_and_index;
};

// Creates a DataSlice of uuids that represent child objects of the given
// parent object.
//
// The result is a DataSlice with the same shape as `parent_itemid`.
// Conceptually the result contains
// [fingerprint(child_itemid_seed, parent_itemid, attr)]
// Where `attr` is either `kChildAttributeAttributeName` ->
// `attr_descriptor.attr_name` or `attr_descriptor.attr_name_and_index.first` ->
// `attr_descriptor.attr_name_and_index.second`.
//
// If `attr_descriptor` is empty, returns `parent_itemid`.
template <typename Fn>
absl::StatusOr<std::optional<DataSlice>> MaybeMakeChildAttrItemId(
    const std::optional<DataSlice>& parent_itemid,
    const ChildItemIdAttrsDescriptor& attr_descriptor, Fn create_uuid_fn) {
  if (!parent_itemid.has_value()) {
    return std::nullopt;
  }
  if (attr_descriptor.attr_name.empty() &&
      attr_descriptor.attr_name_and_index.first.empty()) {
    // This happens when we call Convert for the root object and parent_itemid
    // is actually the itemid for the root object.
    return parent_itemid;
  }

  if (!attr_descriptor.attr_name.empty()) {
    ASSIGN_OR_RETURN(auto attr_name_slice,
                     MakeTextItem(attr_descriptor.attr_name));
    return std::move(create_uuid_fn(
        kChildItemIdSeed, {"parent", kChildAttributeAttributeName},
        {*parent_itemid, std::move(attr_name_slice)}));
  }

  ASSIGN_OR_RETURN(auto element_index,
                   MakeIntItem(attr_descriptor.attr_name_and_index.second));
  ASSIGN_OR_RETURN(
      auto child_itemids,
      create_uuid_fn(kChildItemIdSeed,
                     {"parent", attr_descriptor.attr_name_and_index.first},
                     {*parent_itemid, std::move(element_index)}));
  return std::move(child_itemids);
}

absl::StatusOr<std::optional<DataSlice>> MaybeMakeChildObjectAttrItemId(
    const std::optional<DataSlice>& parent_itemid,
    const ChildItemIdAttrsDescriptor& attr_descriptor) {
  return MaybeMakeChildAttrItemId(parent_itemid, attr_descriptor,
                                  CreateUuidFromFields);
}

absl::StatusOr<std::optional<DataSlice>> MaybeMakeChildDictAttrItemId(
    const std::optional<DataSlice>& parent_itemid,
    const ChildItemIdAttrsDescriptor& attr_descriptor) {
  return MaybeMakeChildAttrItemId(parent_itemid, attr_descriptor,
                                  CreateDictUuidFromFields);
}

absl::StatusOr<std::optional<DataSlice>> MaybeMakeChildListAttrItemId(
    const std::optional<DataSlice>& parent_itemid,
    const ChildItemIdAttrsDescriptor& attr_descriptor) {
  return MaybeMakeChildAttrItemId(parent_itemid, attr_descriptor,
                                  CreateListUuidFromFields);
}

// Converts Python objects into DataSlices and converts them into appropriate
// Koda abstractions using `Factory`.
// `Factory` can be:
//   * EntityCreator, where each created Koda abstraction will behave as Entity;
//   * ObjectCreator, where each created Koda abstraction is converted to Object
//     and its schema gets embedded;
//
// UniversalConverter traverses Python dictionary recursively (without using
// recursion), such that we apply UniversalConverter on each key and each value.
// Thus, it can convert arbitrarily complex nested Python dict, even with its
// keys or values as Python lists.
template <typename Factory>
class UniversalConverter {
 public:
  UniversalConverter(const absl_nullable DataBagPtr& db,
                     AdoptionQueue& adoption_queue, bool dict_as_obj = false)
      : db_(db), adoption_queue_(adoption_queue), dict_as_obj_(dict_as_obj) {}

  absl::StatusOr<DataSlice> Convert(
      PyObject* py_obj, const std::optional<DataSlice>& schema = std::nullopt,
      size_t from_dim = 0,
      const std::optional<DataSlice>& parent_itemid = std::nullopt,
      const ChildItemIdAttrsDescriptor& attr_descriptor = {}) && {
    if (schema) {
      adoption_queue_.Add(*schema);
    }

    std::optional<DataSlice> parse_schema = std::nullopt;
    if constexpr (std::is_same_v<Factory, EntityCreator>) {
      parse_schema = schema;
    }

    if (from_dim == 0) {
      RETURN_IF_ERROR(CmdConvertPyObject(py_obj, parse_schema, parent_itemid,
                                         attr_descriptor));
      RETURN_IF_ERROR(Run());
      if (schema) {
        ASSIGN_OR_RETURN(DataSlice res,
                         CastToNarrow(value_stack_.top(), schema->item()),
                         [&](absl::Status status) {
                           return CreateIncompatibleSchemaErrorFromStatus(
                               std::move(status),
                               value_stack_.top().GetSchema().WithBag(
                                   adoption_queue_.GetBagWithFallbacks()),
                               *schema);
                         }(_));
        return res;
      }
      return std::move(value_stack_.top());
    }

    ASSIGN_OR_RETURN((auto [py_objects, shape]),
                     FlattenPyList(py_obj, from_dim));
    if (parent_itemid) {
      if (parent_itemid->is_item()) {
        return absl::InvalidArgumentError(
            "ItemId for DataSlice must be a DataSlice of non-zero rank if "
            "from_dim > 0");
      }
      if (parent_itemid->size() != py_objects.size()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "ItemId for DataSlice size=%d does not match the "
            "input list size=%d when from_dim=%d",
            parent_itemid->size(), py_objects.size(), from_dim));
      }
    }
    for (int i = 0; i < py_objects.size(); ++i) {
      std::optional<DataSlice> ith_itemid;
      if (parent_itemid) {
        ASSIGN_OR_RETURN(
            ith_itemid, DataSlice::Create(parent_itemid->slice()[i],
                                          internal::DataItem(schema::kItemId)));
      }

      cmd_stack_.push([this, py_obj = py_objects[i], &parse_schema,
                       ith_itemid_ds = std::move(ith_itemid)] {
        return this->CmdConvertPyObject(py_obj, parse_schema, ith_itemid_ds);
      });
    }
    RETURN_IF_ERROR(Run());
    ASSIGN_OR_RETURN(DataSlice res, ComputeDataSlice(py_objects.size(), schema,
                                                     /*is_root=*/true));
    return res.Reshape(std::move(shape));
  }

  // TODO: Consider passing `schema` here as well. This requires
  // refactoring dict_shaped in data_bag.cc.
  absl::Status ConvertDictKeysAndValues(PyObject* py_obj,
                                        std::optional<DataSlice>& keys,
                                        std::optional<DataSlice>& values) && {
    RETURN_IF_ERROR(ParsePyDict(py_obj, /*dict_schema=*/std::nullopt,
                                /*compute_dict=*/false));
    RETURN_IF_ERROR(Run());
    size_t dict_size = PyDict_Size(py_obj);
    ASSIGN_OR_RETURN(keys, ComputeDataSlice(dict_size));
    ASSIGN_OR_RETURN(values, ComputeDataSlice(dict_size));
    return absl::OkStatus();
  }

 private:
  // Main loop of UniversalConverter, which executes a stack of commands.
  //
  // This allows traversal of the Python structure of py_obj to be expressed
  // without using recursion, which could cause a stack overflow.
  //
  // Currently there are two workflows:
  //
  // * When we need the Koda abstraction result from PyObject, regardless what
  //   it is. In that case the caller should push CmdParsePyObject to the stack.
  //   In this case the caller should expect the top of `value_stack_` to
  //   represent the final converted PyObject into Koda object;
  //
  // * when we want to convert keys and values of a dict, but not dict itself
  //   (we do NOT want garbage triples in a DataBag that won't be used), the
  //   caller should call `ParsePyDict(..., /*compute_dict=*/false)`, which will
  //   push "keys" and "values" arguments to the stack for processing. The
  //   caller should expect the top values on the `value_stack_` to represent
  //   converted `keys` and `values` (keys are on the top, while values are
  //   accessible after popping the keys).
  absl::Status Run() {
    while (!cmd_stack_.empty()) {
      auto cmd = std::move(cmd_stack_.top());
      cmd_stack_.pop();
      RETURN_IF_ERROR(cmd());
    }
    return absl::OkStatus();
  }

  // Takes `size` preconstructed DataItems from `value_stack_` and constructs
  // a DataSlice. If `input_schema` is provided, the result is casted to it.
  // `is_root` is used by ObjectCreator: in that case leaves will be converted
  // to Objects.
  absl::StatusOr<DataSlice> ComputeDataSlice(
      size_t size, const std::optional<DataSlice>& input_schema = std::nullopt,
      bool is_root = false) {
    internal::SliceBuilder bldr(size);
    schema::CommonSchemaAggregator schema_agg;
    for (size_t i = 0; i < size; ++i) {
      bldr.InsertIfNotSetAndUpdateAllocIds(i, value_stack_.top().item());
      schema_agg.Add(value_stack_.top().GetSchemaImpl());
      value_stack_.pop();
    }
    ASSIGN_OR_RETURN(DataItem schema_item, std::move(schema_agg).Get(),
                     KodaErrorCausedByNoCommonSchemaError(
                         _, DataBag::ImmutableEmptyWithFallbacks(
                                {db_, adoption_queue_.GetBagWithFallbacks()})));
    if (input_schema) {
      if (schema_item.has_value() &&
          !schema::IsImplicitlyCastableTo(schema_item, input_schema->item())) {
        ASSIGN_OR_RETURN(
            DataSlice schema_item_ds,
            DataSlice::Create(schema_item, internal::DataItem(schema::kSchema),
                              adoption_queue_.GetBagWithFallbacks()));
        return CreateIncompatibleSchemaError(schema_item_ds, *input_schema);
      }
      schema_item = input_schema->item();
    } else if (!schema_item.has_value()) {
      schema_item = internal::DataItem(schema::kNone);
    }
    if constexpr (std::is_same_v<Factory, ObjectCreator>) {
      if (!is_root) {
        schema_item = internal::DataItem(schema::kObject);
      }
    }
    return CreateWithSchema(std::move(bldr).Build(),
                            DataSlice::JaggedShape::FlatFromSize(size),
                            std::move(schema_item));
  }

  // Collects the keys and values of the python dictionary `py_obj`, and
  // arranges the appropriate commands on the stack for their processing.
  // If `compute_dict` is false, only the keys and values DataSlices are
  // computed, while dict from those keys and values is not.
  absl::Status ParsePyDict(
      PyObject* py_obj, const std::optional<DataSlice>& dict_schema,
      bool compute_dict = true,
      const std::optional<DataSlice>& parent_itemid = std::nullopt,
      const ChildItemIdAttrsDescriptor& attr_descriptor = {}) {
    DCHECK(PyDict_CheckExact(py_obj));
    const size_t dict_size = PyDict_Size(py_obj);
    // Notes:
    //  * All Py references are borrowed.
    //  * We collect `keys` and `values` in reverse order, so it's easier
    //    to push them onto the stack later.
    std::vector<PyObject*> keys(dict_size);
    std::vector<PyObject*> values(dict_size);
    Py_ssize_t pos = 0;
    for (size_t i = dict_size; i > 0; --i) {
      CHECK(PyDict_Next(py_obj, &pos, &keys[i - 1], &values[i - 1]));
    }
    DCHECK(!PyDict_Next(py_obj, &pos, nullptr, nullptr));
    std::optional<DataSlice> key_schema;
    std::optional<DataSlice> value_schema;
    if (dict_schema) {
      DCHECK_NE(dict_schema->item(), schema::kObject);
      if (dict_schema->item().is_struct_schema()) {
        RETURN_IF_ERROR(dict_schema->VerifyIsDictSchema());
        ASSIGN_OR_RETURN(key_schema,
                         dict_schema->GetAttr(schema::kDictKeysSchemaAttr));
        ASSIGN_OR_RETURN(value_schema,
                         dict_schema->GetAttr(schema::kDictValuesSchemaAttr));
      }
      // Otherwise, the key_schema / value_schema should not be used
      // (i.e. they should remain std::nullopt).
    }
    ASSIGN_OR_RETURN(
        const std::optional<DataSlice> child_itemid,
        MaybeMakeChildDictAttrItemId(parent_itemid, attr_descriptor));
    if (compute_dict) {
      cmd_stack_.push([this, py_obj, dict_schema = dict_schema,
                       key_schema = key_schema, value_schema = value_schema,
                       itemid = child_itemid] {
        return this->CmdComputeDict(py_obj, dict_schema, key_schema,
                                    value_schema, itemid);
      });
    }
    int64_t child_key_index = keys.size() - 1;
    for (auto* py_key : keys) {
      cmd_stack_.push(
          [this, py_key, key_schema, itemid = child_itemid, child_key_index] {
            return this->CmdConvertPyObject(
                py_key, key_schema, itemid,
                ChildItemIdAttrsDescriptor(kChildDictKeyAttributeName,
                                           child_key_index));
          });
      --child_key_index;
    }
    int child_value_index = values.size() - 1;
    for (auto* py_value : values) {
      cmd_stack_.push([this, py_value, value_schema, itemid = child_itemid,
                       child_value_index] {
        return this->CmdConvertPyObject(
            py_value, value_schema, itemid,
            ChildItemIdAttrsDescriptor(kChildDictValueAttributeName,
                                       child_value_index));
      });
      --child_value_index;
    }
    return absl::OkStatus();
  }

  // Helper method to push commands for creating Object / Entity attributes and
  // commands for parsing individual attribute values.
  absl::Status PushObjAttrs(
      const arolla::DenseArray<arolla::Text>& attr_names,
      const std::vector<PyObject*>& py_values,
      const std::optional<DataSlice>& schema,
      const std::optional<DataSlice>& itemid = std::nullopt) {
    ASSIGN_OR_RETURN(
        auto attr_names_ds,
        DataSlice::Create(
            internal::DataSliceImpl::Create(attr_names),
            DataSlice::JaggedShape::FlatFromSize(attr_names.size()),
            DataItem(schema::kString)));
    cmd_stack_.push([this, attr_names_ds = std::move(attr_names_ds)] {
      this->value_stack_.push(attr_names_ds);
      return absl::OkStatus();
    });
    for (size_t i = 0, id = 0; i < py_values.size(); ++i) {
      std::optional<DataSlice> value_schema;
      if (schema) {
        ASSIGN_OR_RETURN(value_schema, schema->GetAttr(attr_names[id++].value));
      }
      cmd_stack_.push([this, py_val = py_values[i],
                       value_schema = std::move(value_schema), itemid = itemid,
                       attr_name = attr_names[i].value] {
        return this->CmdConvertPyObject(py_val, value_schema, itemid,
                                        ChildItemIdAttrsDescriptor(attr_name));
      });
    }
    return absl::OkStatus();
  }

  // Collects the keys and values of the python dictionary `py_obj`, and
  // arranges the appropriate commands on the stack to create Object / Entity.
  absl::Status ParsePyDictAsObj(
      PyObject* py_obj, const std::optional<DataSlice>& schema,
      const std::optional<DataSlice>& parent_itemid = std::nullopt,
      const ChildItemIdAttrsDescriptor& attr_descriptor = {}) {
    DCHECK(PyDict_CheckExact(py_obj));
    DataSlice::AttrNamesSet allowed_attr_names;
    ASSIGN_OR_RETURN(
        const std::optional<DataSlice> child_itemid,
        MaybeMakeChildObjectAttrItemId(parent_itemid, attr_descriptor));
    cmd_stack_.push([this, py_obj, schema = schema, itemid = child_itemid] {
      return this->CmdComputeObj(py_obj, schema, itemid);
    });
    if (schema) {
      ASSIGN_OR_RETURN(allowed_attr_names, schema->GetAttrNames());
    }
    size_t dict_size = PyDict_Size(py_obj);
    size_t new_attr_names_size = 0;
    arolla::DenseArrayBuilder<arolla::Text> attr_names_bldr(dict_size);
    std::vector<PyObject*> py_values;
    py_values.reserve(dict_size);
    Py_ssize_t pos = 0;
    PyObject* py_key;
    PyObject* py_value;
    while (PyDict_Next(py_obj, &pos, &py_key, &py_value)) {
      ASSIGN_OR_RETURN(auto key, PyDictKeyAsStringView(py_key));
      if (!schema || allowed_attr_names.contains(key)) {
        attr_names_bldr.Set(new_attr_names_size++, key);
        py_values.push_back(py_value);
      }
    }
    return PushObjAttrs(std::move(attr_names_bldr).Build(new_attr_names_size),
                        py_values, schema, child_itemid);
  }

  // Collects the keys and values of the python list/tuple `py_obj`, and
  // arranges the appropriate commands on the stack for their processing.
  absl::Status ParsePyList(
      PyObject* py_obj, const std::optional<DataSlice>& list_schema,
      const std::optional<DataSlice>& parent_itemid = std::nullopt,
      ChildItemIdAttrsDescriptor attr_descriptor = {}) {
    DCHECK(PyList_CheckExact(py_obj) || PyTuple_CheckExact(py_obj));
    std::optional<DataSlice> item_schema;
    if (list_schema) {
      DCHECK_NE(list_schema->item(), schema::kObject);
      if (list_schema->item().is_struct_schema()) {
        RETURN_IF_ERROR(list_schema->VerifyIsListSchema());
        ASSIGN_OR_RETURN(item_schema,
                         list_schema->GetAttr(schema::kListItemsSchemaAttr));
      }
      // Otherwise, the item_schema should not be used (i.e. they should remain
      // std::nullopt).
    }
    ASSIGN_OR_RETURN(
        const std::optional<DataSlice> child_itemid,
        MaybeMakeChildListAttrItemId(parent_itemid, attr_descriptor));
    cmd_stack_.push([this, py_obj, list_schema = list_schema,
                     item_schema = item_schema, itemid = child_itemid] {
      return this->CmdComputeList(py_obj, list_schema, item_schema, itemid);
    });
    int child_index = 0;
    for (auto* py_item : absl::Span<PyObject*>(
             PySequence_Fast_ITEMS(py_obj), PySequence_Fast_GET_SIZE(py_obj))) {
      cmd_stack_.push(
          [this, py_item, item_schema, itemid = child_itemid, child_index] {
            return this->CmdConvertPyObject(
                py_item, item_schema, itemid,
                ChildItemIdAttrsDescriptor(kChildListItemAttributeName,
                                           child_index));
          });
      ++child_index;
    }
    return absl::OkStatus();
  }

  // Collects the attribute names and values from the python object (e.g.
  // dataclass) `py_obj`, and arranges the appropriate commands on the stack to
  // create Object / Entity.
  absl::Status ParsePyAttrProvider(
      PyObject* py_obj, const DataClassesUtil::AttrResult& attr_result,
      const std::optional<DataSlice>& schema,
      const std::optional<DataSlice>& parent_itemid = std::nullopt,
      const ChildItemIdAttrsDescriptor& attr_descriptor = {}) {
    DataSlice::AttrNamesSet allowed_attr_names;
    ASSIGN_OR_RETURN(
        const std::optional<DataSlice> child_itemid,
        MaybeMakeChildObjectAttrItemId(parent_itemid, attr_descriptor));
    cmd_stack_.push([this, py_obj, schema = schema, itemid = child_itemid] {
      return this->CmdComputeObj(py_obj, schema, itemid);
    });
    if (schema) {
      ASSIGN_OR_RETURN(allowed_attr_names, schema->GetAttrNames());
    }
    const auto& attr_names = attr_result.attr_names;
    const auto& values = attr_result.values;
    size_t new_attr_names_size = 0;
    arolla::DenseArrayBuilder<arolla::Text> attr_names_bldr(attr_names.size());
    std::vector<PyObject*> py_values;
    py_values.reserve(values.size());
    for (size_t i = 0; i < attr_names.size(); ++i) {
      if (!schema || allowed_attr_names.contains(attr_names[i])) {
        attr_names_bldr.Set(new_attr_names_size++, attr_names[i]);
        py_values.push_back(values[i]);
      }
    }
    return PushObjAttrs(std::move(attr_names_bldr).Build(new_attr_names_size),
                        py_values, schema, child_itemid);
  }

  // Parses `py_obj` as if it was a supported Python scalar (bool, str, bytes,
  // int, DataItem, etc.). On failure, returns proper status error. The error
  // means that it is not a Python scalar supported by Koda and the caller
  // should evaluate other possibilities before returning this error.
  // On success, pushes the result to `value_stack_`.
  //
  // NOTE: This is not recommended as a general pattern, as returning error can
  // also be expensive. Here, it is justified as it is followed by more
  // expensive callbacks to Python (parsing dataclasses) and the point of this
  // `Try...` method is to skip doing expensive Python callbacks on each Python
  // scalar.
  absl::Status TryParsePythonScalar(PyObject* py_obj) {
    EmbeddingDataBag unused_embedding_db;
    schema::CommonSchemaAggregator schema_agg;
    internal::DataItem res_item;
    internal::DataItem schema_item;
    auto to_data_item_fn = [&res_item]<class T>(T&& value) {
      res_item = internal::DataItem(std::forward<T>(value));
    };
    // NOTE: If there is a DataBag `py_obj`, it is added to the adoption_queue.
    RETURN_IF_ERROR(ParsePyObject</*explicit_cast=*/false>(
        py_obj, /*explicit_schema=*/internal::DataItem(), adoption_queue_,
        schema_agg, unused_embedding_db, to_data_item_fn));
    DCHECK_EQ(unused_embedding_db.GetBag(), nullptr);
    ASSIGN_OR_RETURN(schema_item, std::move(schema_agg).Get());
    // The original bag was added to the adoption_queue, so returning an item
    // without a DataBag to respect the algorithm in UniversalConverter that
    // expects bag of the result only when it created a DataBag.
    ASSIGN_OR_RETURN(DataSlice res, DataSlice::Create(res_item, schema_item));
    // Only Entities are converted using Factory, while primitives are kept as
    // is, because building an OBJECT slice in ComputeDataSlice is not possible
    // if Entities are not already converted.
    if (schema_item.is_struct_schema()) {
      if constexpr (std::is_same_v<Factory, ObjectCreator>) {
        MaybeCreateEmptyBag();
        ASSIGN_OR_RETURN(res, Factory::ConvertWithoutAdopt(db_, res));
      }
    }
    value_stack_.push(res);
    return absl::OkStatus();
  }

  // Processes py_obj, and depending on its type, either immediately
  // converts it to a Koda value (and pushes the result to `value_stack_`),
  // or schedules additional actions for the conversion (by pushing them to
  // `cmd_stack_`).
  //
  // `schema` is used to create an appropriate Koda Abstraction with appropriate
  // schema. Schema is processed recursively.
  absl::Status CmdConvertPyObject(
      PyObject* py_obj, const std::optional<DataSlice>& schema,
      const std::optional<DataSlice>& parent_itemid = std::nullopt,
      const ChildItemIdAttrsDescriptor& attr_descriptor = {}) {
    CacheKey cache_key = MakeCacheKey(py_obj, schema);
    if (objects_in_progress_.find(cache_key) != objects_in_progress_.end()) {
      return absl::InvalidArgumentError(
          "recursive Python structures cannot be converted to Koda object");
    }
    objects_in_progress_.insert(cache_key);
    if (schema && schema->item() == schema::kObject) {
      // When processing Entities, if OBJECT schema is reached, the rest of
      // the Python tree is converted as Object.
      ASSIGN_OR_RETURN(
          value_stack_.emplace(),
          UniversalConverter<ObjectCreator>(db_, adoption_queue_, dict_as_obj_)
              .Convert(py_obj, /*schema=*/std::nullopt, /*from_dim=*/0,
                       parent_itemid, attr_descriptor));
      objects_in_progress_.erase(cache_key);
      return absl::OkStatus();
    }
    if (PyDict_CheckExact(py_obj)) {
      MaybeCreateEmptyBag();
      if (schema && schema->IsListSchema()) {
        return absl::InvalidArgumentError(
            absl::StrCat("Python Dict can be converted to either Entity or "
                         "Dict, got schema: ",
                         arolla::Repr(*schema)));
      }
      if (!dict_as_obj_ && (!schema || schema->IsDictSchema())) {
        return ParsePyDict(py_obj, schema, true, parent_itemid,
                           attr_descriptor);
      }
      return ParsePyDictAsObj(py_obj, schema, parent_itemid, attr_descriptor);
    }
    if (PyList_CheckExact(py_obj) || PyTuple_CheckExact(py_obj)) {
      MaybeCreateEmptyBag();
      return ParsePyList(py_obj, schema, parent_itemid, attr_descriptor);
    }
    // First trying the Python "scalar" conversion to avoid doing expensive
    // `AttrProvider` checks (e.g. dataclasses) on each leaf Python object.
    absl::Status status = TryParsePythonScalar(py_obj);
    if (status.ok()) {
      objects_in_progress_.erase(cache_key);
      return status;
    }
    ASSIGN_OR_RETURN(auto attr_result,
                     dataclasses_util_.GetAttrNamesAndValues(py_obj));
    if (attr_result) {
      MaybeCreateEmptyBag();
      return ParsePyAttrProvider(py_obj, *attr_result, schema, parent_itemid,
                                 attr_descriptor);
    }
    return status;
  }

  // Takes preconstructed keys and values from `value_stack_`, assembles them
  // into a dictionary and pushes it to `value_stack_`.
  absl::Status CmdComputeDict(
      PyObject* py_obj, const std::optional<DataSlice>& dict_schema,
      const std::optional<DataSlice>& key_schema,
      const std::optional<DataSlice>& value_schema,
      const std::optional<DataSlice>& itemid = std::nullopt) {
    size_t dict_size = PyDict_Size(py_obj);
    ASSIGN_OR_RETURN(auto keys, ComputeDataSlice(dict_size, key_schema));
    ASSIGN_OR_RETURN(auto values, ComputeDataSlice(dict_size, value_schema));
    ASSIGN_OR_RETURN(auto res,
                     CreateDictShaped(db_, DataSlice::JaggedShape::Empty(),
                                      std::move(keys), std::move(values),
                                      dict_schema, /*key_schema=*/std::nullopt,
                                      /*value_schema=*/std::nullopt, itemid));
    // NOTE: Factory is not applied on keys and values DataSlices (just on their
    // elements and dict created from those keys and values).
    ASSIGN_OR_RETURN(value_stack_.emplace(),
                     Factory::ConvertWithoutAdopt(db_, res));
    objects_in_progress_.erase(MakeCacheKey(py_obj, dict_schema));
    return absl::OkStatus();
  }

  // Takes preconstructed items from `value_stack_`, assembles them into a list
  // and pushes it to `value_stack_`.
  absl::Status CmdComputeList(
      PyObject* py_obj, const std::optional<DataSlice>& list_schema,
      const std::optional<DataSlice>& item_schema,
      const std::optional<DataSlice>& itemid = std::nullopt) {
    const size_t list_size = PySequence_Fast_GET_SIZE(py_obj);
    ASSIGN_OR_RETURN(auto items, ComputeDataSlice(list_size, item_schema));
    ASSIGN_OR_RETURN(
        auto res,
        CreateListShaped(db_, DataSlice::JaggedShape::Empty(), std::move(items),
                         list_schema, /*item_schema=*/std::nullopt, itemid));
    ASSIGN_OR_RETURN(value_stack_.emplace(),
                     Factory::ConvertWithoutAdopt(db_, res));
    objects_in_progress_.erase(MakeCacheKey(py_obj, list_schema));
    return absl::OkStatus();
  }

  // Takes preconstructed attribute names and their values from `value_stack_`,
  // assembles them into an Object / Entity and pushes it to `value_stack_`.
  //
  // NOTE: expects all attribute names to be stored as a single DataSlice of
  // STRING values on `value_stack_`.
  absl::Status CmdComputeObj(
      PyObject* py_obj, const std::optional<DataSlice>& entity_schema,
      const std::optional<DataSlice>& itemid = std::nullopt) {
    DataSlice attr_names_ds = std::move(value_stack_.top());
    value_stack_.pop();
    std::vector<absl::string_view> attr_names;
    attr_names.reserve(attr_names_ds.size());
    std::vector<DataSlice> values;
    values.reserve(attr_names_ds.size());
    if (attr_names_ds.size() > 0) {
      const arolla::DenseArray<arolla::Text> attr_names_array =
          attr_names_ds.slice().values<arolla::Text>();
      DCHECK(attr_names_array.IsFull());
      attr_names_array.ForEachPresent(
          [&](int64_t id, absl::string_view attr_name) {
            attr_names.push_back(attr_name);
            values.push_back(std::move(value_stack_.top()));
            value_stack_.pop();
          });
    }
    if constexpr (std::is_same_v<Factory, EntityCreator>) {
      ASSIGN_OR_RETURN(
          value_stack_.emplace(),
          Factory::FromAttrs(db_, attr_names, values, entity_schema,
                             /*update_schema=*/false, itemid));
    } else {
      DCHECK(!entity_schema) << "only EntityCreator should accept schema here";
      ASSIGN_OR_RETURN(value_stack_.emplace(),
                       Factory::FromAttrs(db_, attr_names, values, itemid));
    }
    objects_in_progress_.erase(MakeCacheKey(py_obj, entity_schema));
    return absl::OkStatus();
  }

  void MaybeCreateEmptyBag() {
    if (ABSL_PREDICT_FALSE(db_ == nullptr)) {
      db_ = DataBag::Empty();
      // TODO: Don't do this when we refactor out the nested
      // UniversalConverter call.
      adoption_queue_.Add(db_);
    }
  }

  absl_nullable DataBagPtr db_;

  AdoptionQueue& adoption_queue_;
  bool dict_as_obj_;
  DataClassesUtil dataclasses_util_;

  using Cmd = std::function<absl::Status()>;
  std::stack<Cmd> cmd_stack_;

  std::stack<DataSlice> value_stack_;

  // Maps Python object and schema (its fingerprint) to a computed DataSlice
  // that is returned from the "simulated recursive call". It is also used to
  // detect recursive Python structures (e.g. a Python object is already
  // visited, but not computed, the stored value is `std::nullopt`).
  using CacheKey = std::pair<PyObject*, arolla::Fingerprint>;

  static CacheKey MakeCacheKey(PyObject* py_obj,
                               const std::optional<DataSlice>& schema) {
    return CacheKey(py_obj,
                    (schema ? schema->item() : DataItem()).StableFingerprint());
  }

  // This set is used to detect recursive Python structures.
  absl::flat_hash_set<CacheKey> objects_in_progress_;
};

}  // namespace

absl::StatusOr<DataSlice> EntitiesFromPyObject(PyObject* py_obj,
                                               const DataBagPtr& db,
                                               AdoptionQueue& adoption_queue) {
  return EntitiesFromPyObject(py_obj, /*schema=*/std::nullopt,
                              /*itemid=*/std::nullopt, db, adoption_queue);
}

absl::StatusOr<DataSlice> EntitiesFromPyObject(
    PyObject* py_obj, const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& itemid, const DataBagPtr& db,
    AdoptionQueue& adoption_queue) {
  // NOTE: UniversalConverter does not allow converting multi-dimensional
  // DataSlices, so we are processing it before invoking the UniversalConverter.
  if (arolla::python::IsPyQValueInstance(py_obj) && !itemid.has_value()) {
    ASSIGN_OR_RETURN(auto res,
                     DataSliceFromPyValue(py_obj, adoption_queue, schema));
    return EntityCreator::ConvertWithoutAdopt(db, res);
  }
  return UniversalConverter<EntityCreator>(db, adoption_queue)
      .Convert(py_obj, schema, /*from_dim=*/0, itemid);
}

absl::StatusOr<DataSlice> ObjectsFromPyObject(
    PyObject* py_obj, const std::optional<DataSlice>& itemid,
    const DataBagPtr& db, AdoptionQueue& adoption_queue) {
  // NOTE: UniversalConverter does not allow converting multi-dimensional
  // DataSlices, so we are processing it before invoking the UniversalConverter.
  if (arolla::python::IsPyQValueInstance(py_obj) && !itemid.has_value()) {
    ASSIGN_OR_RETURN(auto res, DataSliceFromPyValue(py_obj, adoption_queue));
    return ObjectCreator::ConvertWithoutAdopt(db, res);
  }
  return UniversalConverter<ObjectCreator>(db, adoption_queue)
      .Convert(py_obj, /*schema=*/std::nullopt, /*from_dim=*/0, itemid);
}

absl::Status ConvertDictKeysAndValues(PyObject* py_obj, const DataBagPtr& db,
                                      AdoptionQueue& adoption_queue,
                                      std::optional<DataSlice>& keys,
                                      std::optional<DataSlice>& values) {
  return UniversalConverter<EntityCreator>(db, adoption_queue)
      .ConvertDictKeysAndValues(py_obj, keys, values);
}

absl::StatusOr<DataSlice> GenericFromPyObject(
    PyObject* py_obj, bool dict_as_obj, const std::optional<DataSlice>& schema,
    size_t from_dim, const std::optional<DataSlice>& itemid) {
  AdoptionQueue adoption_queue;
  DataSlice res_slice;
  if (schema) {
    RETURN_IF_ERROR(schema->VerifyIsSchema());
  }
  if (!schema || schema->item() == schema::kObject) {
    ASSIGN_OR_RETURN(res_slice, UniversalConverter<ObjectCreator>(
                                    nullptr, adoption_queue, dict_as_obj)
                                    .Convert(py_obj, schema, from_dim, itemid));
  } else {
    ASSIGN_OR_RETURN(res_slice, UniversalConverter<EntityCreator>(
                                    nullptr, adoption_queue, dict_as_obj)
                                    .Convert(py_obj, schema, from_dim, itemid));
  }
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
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*res_db));
  res_db->UnsafeMakeImmutable();
  return res_slice;
}

}  // namespace koladata::python
