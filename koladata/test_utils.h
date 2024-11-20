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
// Utilities that allow easier creation of Koda abstractions for unit tests.

#ifndef KOLADATA_TEST_UTILS_H_
#define KOLADATA_TEST_UTILS_H_

#include <cstddef>
#include <initializer_list>
#include <optional>
#include <type_traits>
#include <vector>
#include <utility>

#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/object_factories.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace koladata {
namespace test {

// Utilities for creating DataItems.
template <typename T, typename SchemaT>
koladata::DataSlice DataItem(T data, SchemaT schema, DataBagPtr db = nullptr) {
  if constexpr (std::is_same_v<T, const char*>) {
    return *DataSlice::Create(internal::DataItem(arolla::Text(data)),
                              internal::DataItem(schema), db);
  } else {
    return *DataSlice::Create(
        internal::DataItem(data), internal::DataItem(schema), db);
  }
}

template <typename T>
schema::DType DeduceSchema() {
  if constexpr (!std::is_same_v<T, internal::ObjectId> &&
                !std::is_same_v<T, schema::DType> &&
                !std::is_same_v<T, internal::DataItem>) {
    return schema::GetDType<T>();
  }
  if constexpr (std::is_same_v<T, schema::DType>) {
    return schema::kSchema;
  }
  return schema::kObject;
}

template <typename T>
koladata::DataSlice DataItem(T data, DataBagPtr db = nullptr) {
  if constexpr (std::is_same_v<T, const char*>) {
    return DataItem(data, schema::kString, db);
  } else {
    return DataItem(data, DeduceSchema<T>(), db);
  }
}

template <typename T,
          typename = std::enable_if_t<std::is_same_v<T, schema::DType> ||
                                      std::is_same_v<T, internal::ObjectId> ||
                                      std::is_same_v<T, internal::DataItem>>>
koladata::DataSlice Schema(T schema, DataBagPtr db = nullptr) {
  if constexpr (std::is_same_v<T, internal::ObjectId>) {
    DCHECK(schema.IsSchema());
  }
  if constexpr (std::is_same_v<T, internal::DataItem>) {
    DCHECK(schema.template holds_value<schema::DType>() ||
           schema.template holds_value<internal::ObjectId>());
    DCHECK(!schema.template holds_value<internal::ObjectId>() ||
           schema.template value<internal::ObjectId>().IsSchema());
  }
  return DataItem(schema, schema::kSchema, db);
}

inline koladata::DataSlice EntitySchema(
    const std::vector<absl::string_view>& attr_names,
    const std::vector<koladata::DataSlice>& attr_schemas,
    DataBagPtr db = nullptr) {
  return koladata::CreateEntitySchema(db, attr_names, attr_schemas).value();
}

template <typename T>
struct test_val {
  using t = T;
};

template <>
struct test_val<arolla::Text> {
  using t = const char*;
};

template <>
struct test_val<arolla::Bytes> {
  using t = const char*;
};

template <typename T>
std::vector<arolla::OptionalValue<T>> ToVector(
    std::initializer_list<
        arolla::OptionalValue<typename test_val<T>::t>> values) {
  std::vector<arolla::OptionalValue<T>> vec_values;
  if constexpr (std::is_same_v<T, arolla::Text> ||
                std::is_same_v<T, arolla::Bytes>) {
    for (const auto& val : values) {
      if (val.present) {
        vec_values.push_back(T(val.value));
      } else {
        vec_values.push_back(std::nullopt);
      }
    }
  } else {
    vec_values = values;
  }
  return std::move(vec_values);
}

// Utilities for creating DataSlices (including Mixed, Allocated, Empty, etc.).
template <typename T,
          typename SchemaT,
          typename = std::enable_if_t<
              std::is_same_v<SchemaT, schema::DType> ||
              std::is_same_v<SchemaT, internal::ObjectId>>>
koladata::DataSlice DataSlice(
    std::initializer_list<
        arolla::OptionalValue<typename test_val<T>::t>> values,
    DataSlice::JaggedShape shape, SchemaT schema, DataBagPtr db = nullptr) {
  auto vec_values = ToVector<T>(values);
  return *koladata::DataSlice::Create(
      internal::DataSliceImpl::Create(
          arolla::CreateDenseArray<T>(absl::MakeSpan(vec_values))),
      shape, internal::DataItem(schema), db);
}

template <typename T,
          typename SchemaT,
          typename = std::enable_if_t<
              std::is_same_v<SchemaT, schema::DType> ||
              std::is_same_v<SchemaT, internal::ObjectId>>>
koladata::DataSlice DataSlice(
    std::initializer_list<
        arolla::OptionalValue<typename test_val<T>::t>> values,
    SchemaT schema, DataBagPtr db = nullptr) {
  return DataSlice<T, SchemaT>(
      values, DataSlice::JaggedShape::FlatFromSize(values.size()), schema, db);
}

template <typename T>
koladata::DataSlice DataSlice(
    std::initializer_list<
        arolla::OptionalValue<typename test_val<T>::t>> values,
    DataBagPtr db = nullptr) {
  return DataSlice<T, schema::DType>(
      values, DataSlice::JaggedShape::FlatFromSize(values.size()),
      DeduceSchema<T>(), db);
}

template <typename T>
koladata::DataSlice DataSlice(
    std::initializer_list<
        arolla::OptionalValue<typename test_val<T>::t>> values,
    DataSlice::JaggedShape shape, DataBagPtr db = nullptr) {
  return DataSlice<T, schema::DType>(values, shape, DeduceSchema<T>(), db);
}

template <typename T, typename U, typename V = void>
koladata::DataSlice MixedDataSlice(
    std::initializer_list<
        arolla::OptionalValue<typename test_val<T>::t>> items_1,
    std::initializer_list<
        arolla::OptionalValue<typename test_val<U>::t>> items_2,
    std::initializer_list<
        arolla::OptionalValue<typename test_val<V>::t>> items_3,
    DataSlice::JaggedShape shape, schema::DType schema = schema::kObject,
    DataBagPtr db = nullptr) {
  DCHECK_EQ(items_1.size(), items_2.size());
  if constexpr (!std::is_same_v<V, void>) {
    DCHECK(items_1.size() == items_3.size());
  }
  internal::DataSliceImpl ds_impl;
  auto values_1 = ToVector<T>(items_1);
  auto values_2 = ToVector<U>(items_2);
  auto array_1 = arolla::CreateDenseArray<T>(absl::MakeSpan(values_1));
  auto array_2 = arolla::CreateDenseArray<U>(absl::MakeSpan(values_2));
  if constexpr (std::is_same_v<V, void>) {
    ds_impl = internal::DataSliceImpl::Create(array_1, array_2);
  } else {
    auto values_3 = ToVector<V>(items_3);
    auto array_3 = arolla::CreateDenseArray<V>(absl::MakeSpan(values_3));
    ds_impl = internal::DataSliceImpl::Create(array_1, array_2, array_3);
  }
  return *DataSlice::Create(ds_impl, shape, internal::DataItem(schema), db);
}

template <typename T, typename U, typename V>
koladata::DataSlice MixedDataSlice(
    std::initializer_list<
        arolla::OptionalValue<typename test_val<T>::t>> items_1,
    std::initializer_list<
        arolla::OptionalValue<typename test_val<U>::t>> items_2,
    std::initializer_list<
        arolla::OptionalValue<typename test_val<V>::t>> items_3,
    schema::DType schema = schema::kObject, DataBagPtr db = nullptr) {
  return MixedDataSlice<T, U, V>(
      items_1, items_2, items_3,
      DataSlice::JaggedShape::FlatFromSize(items_1.size()), schema, db);
}

// Allows specifying only 2 types and optional schema and db.
template <typename T, typename U>
koladata::DataSlice MixedDataSlice(
    std::initializer_list<
        arolla::OptionalValue<typename test_val<T>::t>> items_1,
    std::initializer_list<
        arolla::OptionalValue<typename test_val<U>::t>> items_2,
    DataSlice::JaggedShape shape, schema::DType schema = schema::kObject,
    DataBagPtr db = nullptr) {
  return MixedDataSlice<T, U>(items_1, items_2, {}, shape, schema, db);
}

template <typename T, typename U>
koladata::DataSlice MixedDataSlice(
    std::initializer_list<
        arolla::OptionalValue<typename test_val<T>::t>> items_1,
    std::initializer_list<
        arolla::OptionalValue<typename test_val<U>::t>> items_2,
    schema::DType schema = schema::kObject,
    DataBagPtr db = nullptr) {
  return MixedDataSlice<T, U>(
      items_1, items_2, {},
      DataSlice::JaggedShape::FlatFromSize(items_1.size()), schema, db);
}

template <typename SchemaT>
koladata::DataSlice EmptyDataSlice(size_t size, SchemaT schema,
                                   DataBagPtr db = nullptr) {
  return *DataSlice::Create(
      internal::DataSliceImpl::CreateEmptyAndUnknownType(size),
      DataSlice::JaggedShape::FlatFromSize(size),
      internal::DataItem(schema), db);
}

template <typename SchemaT>
koladata::DataSlice EmptyDataSlice(DataSlice::JaggedShape shape,
                                   SchemaT schema, DataBagPtr db = nullptr) {
  return *DataSlice::Create(
      internal::DataSliceImpl::CreateEmptyAndUnknownType(shape.size()), shape,
      internal::DataItem(schema), db);
}

template <typename SchemaT>
koladata::DataSlice AllocateDataSlice(size_t size, SchemaT schema,
                                      DataBagPtr db = nullptr) {
  return *DataSlice::Create(
      internal::DataSliceImpl::AllocateEmptyObjects(size),
      DataSlice::JaggedShape::FlatFromSize(size),
      internal::DataItem(schema), db);
}

template <typename SchemaT>
koladata::DataSlice AllocateDataSlice(DataSlice::JaggedShape shape,
                                      SchemaT schema, DataBagPtr db = nullptr) {
  return *DataSlice::Create(
      internal::DataSliceImpl::AllocateEmptyObjects(shape.size()),
      shape, internal::DataItem(schema), db);
}

}  // namespace test
}  // namespace koladata

#endif  // KOLADATA_TEST_UTILS_H_
