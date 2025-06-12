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
#include "koladata/operators/predicates.h"

#include <cstdint>
#include <utility>

#include "absl/functional/overload.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/operators/masking.h"
#include "koladata/operators/utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace {

absl::StatusOr<internal::DataItem> HasPrimitiveImpl(
    const internal::DataItem& item) {
  return item.VisitValue([]<class T>(const T& value) {
    if constexpr (std::is_same_v<T, internal::ObjectId>) {
      return internal::DataItem();
    } else {
      return internal::DataItem(arolla::Unit());
    }
  });
}

absl::StatusOr<internal::DataSliceImpl> HasPrimitiveImpl(
    const internal::DataSliceImpl& slice) {
  internal::SliceBuilder builder(slice.size());
  auto typed_builder = builder.typed<arolla::Unit>();
  slice.VisitValues([&]<class T>(const arolla::DenseArray<T>& values) {
    if constexpr (!std::is_same_v<T, internal::ObjectId>) {
      values.ForEachPresent([&](int64_t id, arolla::view_type_t<T> value) {
        typed_builder.InsertIfNotSet(id, arolla::Unit());
      });
    }
  });
  return std::move(builder).Build();
}

absl::StatusOr<internal::DataItem> HasEntityImpl(
    const internal::DataItem& item) {
  if (item.is_entity()) {
    return internal::DataItem(arolla::Unit());
  } else {
    return internal::DataItem();
  }
}

absl::StatusOr<internal::DataSliceImpl> HasEntityImpl(
    const internal::DataSliceImpl& slice) {
  auto result = arolla::CreateEmptyDenseArray<arolla::Unit>(slice.size());
  slice.VisitValues([&]<class T>(const arolla::DenseArray<T>& values) {
    if constexpr (std::is_same_v<T, internal::ObjectId>) {
      result = arolla::CreateDenseOp(
          [](arolla::view_type_t<internal::ObjectId> value)
              -> arolla::OptionalValue<arolla::Unit> {
            return arolla::OptionalUnit(value.IsEntity());
          })(values);
    }
  });
  return internal::DataSliceImpl::Create(std::move(result));
}

absl::StatusOr<internal::DataItem> HasListImpl(const internal::DataItem& item) {
  if (item.is_list()) {
    return internal::DataItem(arolla::Unit());
  } else {
    return internal::DataItem();
  }
}

absl::StatusOr<internal::DataSliceImpl> HasListImpl(
    const internal::DataSliceImpl& slice) {
  auto result = arolla::CreateEmptyDenseArray<arolla::Unit>(slice.size());
  slice.VisitValues([&]<class T>(const arolla::DenseArray<T>& values) {
    if constexpr (std::is_same_v<T, internal::ObjectId>) {
      result = arolla::CreateDenseOp(
          [](arolla::view_type_t<internal::ObjectId> value)
              -> arolla::OptionalValue<arolla::Unit> {
            return arolla::OptionalUnit(value.IsList());
          })(values);
    }
  });
  return internal::DataSliceImpl::Create(std::move(result));
}

absl::StatusOr<internal::DataItem> HasDictImpl(const internal::DataItem& item) {
  if (item.is_dict()) {
    return internal::DataItem(arolla::Unit());
  } else {
    return internal::DataItem();
  }
}

absl::StatusOr<internal::DataSliceImpl> HasDictImpl(
    const internal::DataSliceImpl& slice) {
  auto result = arolla::CreateEmptyDenseArray<arolla::Unit>(slice.size());
  slice.VisitValues([&]<class T>(const arolla::DenseArray<T>& values) {
    if constexpr (std::is_same_v<T, internal::ObjectId>) {
      result = arolla::CreateDenseOp(
          [](arolla::view_type_t<internal::ObjectId> value)
              -> arolla::OptionalValue<arolla::Unit> {
            return arolla::OptionalUnit(value.IsDict());
          })(values);
    }
  });
  return internal::DataSliceImpl::Create(std::move(result));
}

}  // namespace

absl::StatusOr<DataSlice> HasPrimitive(const DataSlice& x) {
  auto schema = x.GetSchemaImpl();
  // Trust the schema if it is a primitive schema.
  if (schema.is_primitive_schema()) {
    return Has(x);
  }
  // Derive from the data for OBJECT and SCHEMA schemas. Note that primitive
  // schemas (e.g. INT32, SCHEMA) are stored as DTypes and considered as
  // primitives.
  if (schema.is_object_schema() || schema.is_schema_schema()) {
    return x.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
      ASSIGN_OR_RETURN(auto res, HasPrimitiveImpl(impl));
      return DataSlice::Create(std::move(res), x.GetShape(),
                               internal::DataItem(schema::kMask), nullptr);
    });
  }
  return DataSlice::Create(
      internal::DataSliceImpl::CreateEmptyAndUnknownType(x.size()),
      x.GetShape(), internal::DataItem(schema::kMask), nullptr);
}

absl::StatusOr<DataSlice> HasEntity(const DataSlice& x) {
  auto schema = x.GetSchemaImpl();
  // Trust the schema if it is a Entity schema.
  if (x.GetSchema().IsEntitySchema()) {
    return Has(x);
  }
  // Derive from the data for OBJECT schemas.
  if (schema.is_object_schema()) {
    return x.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
      ASSIGN_OR_RETURN(auto res, HasEntityImpl(impl));
      return DataSlice::Create(std::move(res), x.GetShape(),
                               internal::DataItem(schema::kMask), nullptr);
    });
  }
  return DataSlice::Create(
      internal::DataSliceImpl::CreateEmptyAndUnknownType(x.size()),
      x.GetShape(), internal::DataItem(schema::kMask), nullptr);
}

absl::StatusOr<DataSlice> HasList(const DataSlice& x) {
  auto schema = x.GetSchemaImpl();
  // Trust the schema if it is a List schema.
  if (x.GetSchema().IsListSchema()) {
    return Has(x);
  }
  // Derive from the data for OBJECT schemas.
  if (schema.is_object_schema()) {
    return x.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
      ASSIGN_OR_RETURN(auto res, HasListImpl(impl));
      return DataSlice::Create(std::move(res), x.GetShape(),
                               internal::DataItem(schema::kMask), nullptr);
    });
  }
  return DataSlice::Create(
      internal::DataSliceImpl::CreateEmptyAndUnknownType(x.size()),
      x.GetShape(), internal::DataItem(schema::kMask), nullptr);
}

absl::StatusOr<DataSlice> HasDict(const DataSlice& x) {
  auto schema = x.GetSchemaImpl();
  // Trust the schema if it is a Dict schema.
  if (x.GetSchema().IsDictSchema()) {
    return Has(x);
  }
  // Derive from the data for OBJECT schemas.
  if (schema.is_object_schema()) {
    return x.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
      ASSIGN_OR_RETURN(auto res, HasDictImpl(impl));
      return DataSlice::Create(std::move(res), x.GetShape(),
                               internal::DataItem(schema::kMask), nullptr);
    });
  }
  return DataSlice::Create(
      internal::DataSliceImpl::CreateEmptyAndUnknownType(x.size()),
      x.GetShape(), internal::DataItem(schema::kMask), nullptr);
}

absl::StatusOr<DataSlice> IsPrimitive(const DataSlice& x) {
  auto schema = x.GetSchemaImpl();
  // Trust the schema if it is a primitive schema.
  if (schema.is_primitive_schema()) {
    return AsMask(true);
  }
  // For non-primitive schemas which cannot contain primitives, return missing.
  if (!schema.is_object_schema() && !schema.is_schema_schema()) {
    return AsMask(false);
  }
  // Derive from the data for OBJECT and SCHEMA schemas. Note that primitive
  // schemas (e.g. INT32, SCHEMA) are stored as DTypes and considered as
  // primitives.
  bool contains_only_primitives = x.VisitImpl(absl::Overload(
      [](const internal::DataItem& item) {
        return item.VisitValue([]<class T>(const T& value) {
          return !std::is_same_v<T, internal::ObjectId>;
        });
      },
      [](const internal::DataSliceImpl& slice) {
        bool res = true;
        slice.VisitValues([&]<class T>(const arolla::DenseArray<T>& values) {
          res &= !std::is_same_v<T, internal::ObjectId>;
        });
        return res;
      }));
  return AsMask(contains_only_primitives);
}

absl::StatusOr<DataSlice> IsEntity(const DataSlice& x) {
  return AsMask(x.IsEntity());
}

absl::StatusOr<DataSlice> IsList(const DataSlice& x) {
  return AsMask(x.IsList());
}

absl::StatusOr<DataSlice> IsDict(const DataSlice& x) {
  return AsMask(x.IsDict());
}

}  // namespace koladata::ops
