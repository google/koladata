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
#include "koladata/schema_utils.h"

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/functional/overload.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "arolla/dense_array/dense_array.h"

namespace koladata {
namespace {

// A wrapper around schema::GetDType<T>().name() to handle a few special cases.
template <typename T>
constexpr absl::string_view DTypeName() {
  if constexpr (std::is_same_v<T, koladata::internal::ObjectId>) {
    // NOTE: internal::ObjectId can also mean OBJECT or SCHEMA, but for now we
    // decided to disambiguate it in the error messages.
    return "non-primitive";
  } else if constexpr (std::is_same_v<T, koladata::schema::DType>) {
    return "DTYPE";
  } else {
    return schema::GetDType<T>().name();
  }
}

// Returns OK if the DataSlice values are of the given dtype or NONE.
absl::Status ExpectNoneOr(schema::DType dtype, absl::string_view arg_name,
                          const DataSlice& arg) {
  internal::DataItem narrowed_schema = GetNarrowedSchema(arg);
  if (narrowed_schema != schema::kNone && narrowed_schema != dtype) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "argument `%s` must be a slice of %v, got a slice of %s", arg_name,
        dtype, DescribeSliceSchema(arg)));
  }
  return absl::OkStatus();
}

absl::Status NoCommonSchemaError(absl::string_view lhs_name,
                                 const DataSlice& lhs,
                                 absl::string_view rhs_name,
                                 const DataSlice& rhs) {
  std::string lhs_schema_str = DescribeSliceSchema(lhs);
  std::string rhs_schema_str = DescribeSliceSchema(rhs);
  if (lhs_schema_str == rhs_schema_str) {
    // Always true.
    if (lhs.GetSchemaImpl().holds_value<internal::ObjectId>()) {
      absl::StrAppend(&lhs_schema_str, " with id ",
                      lhs.GetSchemaImpl().value<internal::ObjectId>());
    }
    // Always true.
    if (rhs.GetSchemaImpl().holds_value<internal::ObjectId>()) {
      absl::StrAppend(&rhs_schema_str, " with id ",
                      rhs.GetSchemaImpl().value<internal::ObjectId>());
    }
  }
  return absl::InvalidArgumentError(
      absl::StrFormat("arguments `%s` and `%s` must contain values castable to "
                      "a common type, got %s and %s",
                      lhs_name, rhs_name, lhs_schema_str, rhs_schema_str));
}

}  // namespace

internal::DataItem GetNarrowedSchema(const DataSlice& slice) {
  const auto& schema = slice.GetSchemaImpl();
  if (schema == schema::kObject) {
    return slice.VisitImpl([&](const auto& impl) {
      if (auto data_schema = schema::GetDataSchema(impl);
          data_schema.has_value()) {
        return data_schema;
      } else {
        return schema;
      }
    });
  }
  return schema;
}

absl::Status ExpectNumeric(absl::string_view arg_name, const DataSlice& arg) {
  if (!schema::IsImplicitlyCastableTo(GetNarrowedSchema(arg),
                                      internal::DataItem(schema::kFloat64))) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "argument `%s` must be a slice of numeric values, got a slice of %s",
        arg_name, DescribeSliceSchema(arg)));
  }
  return absl::OkStatus();
}

absl::Status ExpectInteger(absl::string_view arg_name, const DataSlice& arg) {
  if (!schema::IsImplicitlyCastableTo(GetNarrowedSchema(arg),
                                      internal::DataItem(schema::kInt64))) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "argument `%s` must be a slice of integer values, got a slice of %s",
        arg_name, DescribeSliceSchema(arg)));
  }
  return absl::OkStatus();
}

absl::Status ExpectCanBeAdded(absl::string_view arg_name,
                              const DataSlice& arg) {
  auto narrowed_schema = GetNarrowedSchema(arg);
  bool can_be_added =
      schema::IsImplicitlyCastableTo(narrowed_schema,
                                     internal::DataItem(schema::kFloat64)) ||
      // NOTE: for performance reasons we are not using IsImplicitlyCastableTo
      // below, but kNone is already allowed via IsImplicitlyCastableTo above.
      narrowed_schema == schema::kBytes || narrowed_schema == schema::kString;
  if (!can_be_added) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "argument `%s` must be a slice of consistent numeric, bytes or "
        "string values, got a slice of %s",
        arg_name, DescribeSliceSchema(arg)));
  }
  return absl::OkStatus();
}

absl::Status ExpectCanBeOrdered(absl::string_view arg_name,
                                const DataSlice& arg) {
  auto narrowed_schema = GetNarrowedSchema(arg);
  bool can_be_ordered =
      schema::IsImplicitlyCastableTo(narrowed_schema,
                                     internal::DataItem(schema::kFloat64)) ||
      // NOTE: for performance reasons we are not using IsImplicitlyCastableTo
      // below, but kNone is already allowed via IsImplicitlyCastableTo above.
      narrowed_schema == schema::kBytes || narrowed_schema == schema::kString ||
      narrowed_schema == schema::kBool || narrowed_schema == schema::kMask;
  if (!can_be_ordered) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "argument `%s` must be a slice of orderable values, got a slice of %s",
        arg_name, DescribeSliceSchema(arg)));
  }
  return absl::OkStatus();
}

absl::Status ExpectString(absl::string_view arg_name, const DataSlice& arg) {
  return ExpectNoneOr(schema::kString, arg_name, arg);
}

absl::Status ExpectBytes(absl::string_view arg_name, const DataSlice& arg) {
  return ExpectNoneOr(schema::kBytes, arg_name, arg);
}

absl::Status ExpectMask(absl::string_view arg_name, const DataSlice& arg) {
  return ExpectNoneOr(schema::kMask, arg_name, arg);
}

absl::Status ExpectSchema(absl::string_view arg_name, const DataSlice& arg) {
  return ExpectNoneOr(schema::kSchema, arg_name, arg);
}

absl::Status ExpectPresentScalar(absl::string_view arg_name,
                                 const DataSlice& arg,
                                 const schema::DType expected_dtype) {
  if (arg.GetShape().rank() != 0) {
    return absl::InvalidArgumentError(
        absl::StrFormat("argument `%s` must be an item holding %v, got a "
                        "slice of rank %i > 0",
                        arg_name, expected_dtype, arg.GetShape().rank()));
  }
  if (GetNarrowedSchema(arg) != expected_dtype) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "argument `%s` must be an item holding %v, got an item of %s", arg_name,
        expected_dtype, DescribeSliceSchema(arg)));
  }
  if (arg.present_count() != 1) {
    return absl::InvalidArgumentError(
        absl::StrFormat("argument `%s` must be an item holding %v, got missing",
                        arg_name, expected_dtype));
  }
  return absl::OkStatus();
}

std::string DescribeSliceSchema(const DataSlice& slice) {
  if (slice.GetSchemaImpl() == schema::kObject) {
    std::string result = absl::StrCat(schema::kObject, " containing ");
    slice.VisitImpl(absl::Overload(
        [&](const internal::DataItem& impl) {
          impl.VisitValue([&]<typename T>(const T& value) {
            absl::StrAppend(&result, DTypeName<T>());
          });
        },
        [&](const internal::DataSliceImpl& impl) {
          std::vector<absl::string_view> type_names;
          // TODO: b/381785498 - We may add type_names.reserve() here.
          impl.VisitValues([&]<typename T>(const arolla::DenseArray<T>& array) {
            type_names.push_back(DTypeName<T>());
          });
          if (type_names.empty()) {
            absl::StrAppend(&result, schema::kNone.name());
          } else if (type_names.size() == 1) {
            absl::StrAppend(&result, type_names[0]);
          } else {
            std::sort(type_names.begin(), type_names.end());
            absl::StrAppend(
                &result,
                absl::StrJoin(absl::MakeConstSpan(type_names)
                                  .subspan(0, type_names.size() - 1),
                              ", "),
                " and ", type_names.back());
          }
        }));
    absl::StrAppend(&result, " values");
    return result;
  } else {
    return SchemaToStr(slice.GetSchema());
  }
}

namespace schema_utils_internal {

absl::Status ExpectConsistentStringOrBytesImpl(
    absl::Span<const absl::string_view> arg_names,
    absl::Span<const DataSlice* /*absl_nonnull*/ const> args) {
  if (args.size() != arg_names.size()) {
    return absl::InternalError("size mismatch between args and arg_names");
  }

  std::optional<size_t> string_arg_index;
  std::optional<size_t> bytes_arg_index;
  for (int i = 0; i < args.size(); ++i) {
    internal::DataItem narrowed_schema = GetNarrowedSchema(*args[i]);
    bool is_string = schema::IsImplicitlyCastableTo(
        narrowed_schema, internal::DataItem(schema::kString));
    bool is_bytes = schema::IsImplicitlyCastableTo(
        narrowed_schema, internal::DataItem(schema::kBytes));
    if (is_string && is_bytes) {
      continue;  // NONE schema.
    } else if (!is_string && !is_bytes) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "argument `%s` must be a slice of either %v or %v, got a slice of %s",
          arg_names[i], schema::kString, schema::kBytes,
          DescribeSliceSchema(*args[i])));
    } else if (is_string) {
      string_arg_index = string_arg_index.value_or(i);
    } else /* is_bytes */ {
      bytes_arg_index = bytes_arg_index.value_or(i);
    }
  }
  if (string_arg_index.has_value() && bytes_arg_index.has_value()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "mixing %v and %v arguments is not allowed, but "
        "`%s` contains %v and `%s` contains %v",
        schema::kString, schema::kBytes, arg_names[*string_arg_index],
        schema::kString, arg_names[*bytes_arg_index], schema::kBytes));
  }
  return absl::OkStatus();
}

}  // namespace schema_utils_internal

absl::Status ExpectHaveCommonSchema(
    absl::Span<const absl::string_view> arg_names, const DataSlice& lhs,
    const DataSlice& rhs) {
  if (arg_names.size() != 2) {
    return absl::InternalError("arg_names must have exactly 2 elements");
  }
  if (schema::CommonSchema(lhs.GetSchemaImpl(), rhs.GetSchemaImpl()).ok()) {
    return absl::OkStatus();
  }
  return NoCommonSchemaError(arg_names[0], lhs, arg_names[1], rhs);
}

absl::Status ExpectHaveCommonPrimitiveSchema(
    absl::Span<const absl::string_view> arg_names, const DataSlice& lhs,
    const DataSlice& rhs) {
  if (arg_names.size() != 2) {
    return absl::InternalError("arg_names must have exactly 2 elements");
  }

  auto lhs_narrowed = GetNarrowedSchema(lhs);
  auto rhs_narrowed = GetNarrowedSchema(rhs);
  auto common_schema = schema::CommonSchema(lhs_narrowed, rhs_narrowed);
  if (!common_schema.ok()) {
    return NoCommonSchemaError(arg_names[0], lhs, arg_names[1], rhs);
  }
  if (common_schema->holds_value<internal::ObjectId>() ||
      *common_schema == schema::kObject) {
    auto common_schema_slice = DataSlice::Create(
        *common_schema, internal::DataItem(schema::kSchema), lhs.GetBag());
    std::string common_schema_str = common_schema_slice.ok()
                                        ? SchemaToStr(*common_schema_slice)
                                        : absl::StrCat(*common_schema);
    return absl::InvalidArgumentError(absl::StrFormat(
        "arguments `%s` and `%s` must contain values castable to a common "
        "primitive type, got %s and %s with the common non-primitive schema %s",
        arg_names[0], arg_names[1], DescribeSliceSchema(lhs),
        DescribeSliceSchema(rhs), common_schema_str));
  }
  return absl::OkStatus();
}

}  // namespace koladata
