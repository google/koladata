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
#include <pybind11/pybind11.h>

#include <cstddef>
#include <cstdint>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/lru_cache.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/slice_builder.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "pybind11_abseil/absl_casters.h"
#include "pybind11_abseil/status_casters.h"

namespace koladata::python {
namespace {

namespace py = ::pybind11;

using ::arolla::TypedValue;
using ::koladata::DataSlice;
using ::koladata::internal::DataItem;
using ::koladata::internal::DataSliceImpl;
using ::koladata::internal::ObjectId;

// It is a wrapper around `arolla::LruCache<ObjectId, DataSlice>` with batched
// Get and Set operations.
class LruCacheWrapper {
 public:
  explicit LruCacheWrapper(size_t capacity) : cache_(capacity) {}

  // `keys_tv` must be a DataSlice of ObjectIds (i.e. entities or objects).
  // For each ObjectId `Get` looks up the corresponding value in the cache (or
  // MISSING if not in the cache) and returns them as a single DataSlice.
  // The schemas are aggregated with koladata::schema::CommonSchemaAggregator.
  absl::StatusOr<arolla::TypedValue> Get(TypedValue keys_tv) {
    ASSIGN_OR_RETURN(const DataSlice& keys_slice, keys_tv.As<DataSlice>());

    if (keys_slice.is_item()) {
      const DataSlice* res = nullptr;
      if (keys_slice.item().holds_value<ObjectId>()) {
        ObjectId key = keys_slice.item().value<ObjectId>();
        res = cache_.LookupOrNull(key);
      } else if (keys_slice.item().has_value()) {
        return absl::InvalidArgumentError("ObjectId expected");
      }
      if (res == nullptr) {
        ASSIGN_OR_RETURN(
            auto none,
            DataSlice::Create(DataItem(), DataItem(koladata::schema::kNone)));
        return TypedValue::FromValue(none);
      }
      return TypedValue::FromValue(*res);
    }

    if (keys_slice.slice().is_empty_and_unknown()) {
      ASSIGN_OR_RETURN(auto none, DataSlice::Create(
                                      keys_slice.slice(), keys_slice.GetShape(),
                                      DataItem(koladata::schema::kNone)));
      return TypedValue::FromValue(std::move(none));
    }

    if (keys_slice.slice().dtype() != arolla::GetQType<ObjectId>()) {
      return absl::InvalidArgumentError("ObjectId expected");
    }

    const arolla::DenseArray<ObjectId>& keys =
        keys_slice.slice().values<ObjectId>();

    koladata::internal::SliceBuilder bldr(keys.size());
    koladata::schema::CommonSchemaAggregator schema_agg;
    schema_agg.Add(koladata::schema::kNone);
    koladata::AdoptionQueue bags;
    keys.ForEachPresent([&](int64_t offset, ObjectId key) {
      if (const DataSlice* v = cache_.LookupOrNull(key); v != nullptr) {
        DCHECK(v->is_item());
        bldr.InsertIfNotSetAndUpdateAllocIds(offset, v->item());
        bags.Add(v->GetBag());
        if (v->GetSchemaImpl() == koladata::schema::kNone) {
          return;
        }
        schema_agg.Add(v->GetSchemaImpl());
      }
    });

    ASSIGN_OR_RETURN(auto schema, std::move(schema_agg).Get());
    ASSIGN_OR_RETURN(
        DataSlice res,
        DataSlice::Create(std::move(bldr).Build(), keys_slice.GetShape(),
                          schema, bags.GetBagWithFallbacks()));
    return TypedValue::FromValue(std::move(res));
  }

  // Add or update values in the cache.
  // `keys_tv` must be a DataSlice of ObjectIds. `values_tv` must be a DataSlice
  // with shape broadcastable to keys.
  // Each value in the `values_tv` slice will be stored to the cache with
  // the corresponding ObjectId from `keys_tv` as a key.
  // If the cache reaches max capacity, then the values not accessed for the
  // longest time  are removed.
  absl::Status Set(arolla::TypedValue keys_tv, arolla::TypedValue values_tv) {
    ASSIGN_OR_RETURN(const DataSlice& keys_slice, keys_tv.As<DataSlice>());
    ASSIGN_OR_RETURN(DataSlice values_slice, values_tv.As<DataSlice>());
    ASSIGN_OR_RETURN(values_slice,
                     BroadcastToShape(values_slice, keys_slice.GetShape()));
    if (keys_slice.IsEmpty() || values_slice.IsEmpty()) {
      return absl::OkStatus();
    }
    if (keys_slice.dtype() != arolla::GetQType<ObjectId>()) {
      return absl::InvalidArgumentError("ObjectId expected");
    }
    if (keys_slice.is_item()) {
      (void)cache_.Put(keys_slice.item().value<ObjectId>(), values_slice);
      return absl::OkStatus();
    }

    const arolla::DenseArray<ObjectId>& keys =
        keys_slice.slice().values<ObjectId>();
    const auto& values = values_slice.slice();
    keys.ForEachPresent([&](int64_t offset, ObjectId key) {
      DataItem val = values[offset];
      if (!val.has_value()) {
        return;
      }
      DataSlice val_slice = DataSlice::UnsafeCreate(
          std::move(val), values_slice.GetSchemaImpl(), values_slice.GetBag());
      (void)cache_.Put(key, std::move(val_slice));
    });
    return absl::OkStatus();
  }

  void Clear() {
    cache_.Clear();
  }

 private:
  arolla::LruCache<ObjectId, DataSlice> cache_;
};

PYBIND11_MODULE(lru_cache, m) {
  pybind11::google::ImportStatusModule();
  py::class_<LruCacheWrapper>(m, "LruCache", R"doc(
LRU cache Object/Entity -> DataSlice
)doc")
      .def(py::init<size_t>(), py::arg("capacity") = 100000)
      .def("__getitem__", &LruCacheWrapper::Get)
      .def("__setitem__", &LruCacheWrapper::Set)
      .def("clear", &LruCacheWrapper::Clear);
}

}  // namespace
}  // namespace koladata::python
