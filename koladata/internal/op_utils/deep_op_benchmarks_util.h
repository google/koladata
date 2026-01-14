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
#ifndef KOLADATA_INTERNAL_OP_UTILS_DEEP_OP_BENCHMARKS_UTIL_H_
#define KOLADATA_INTERNAL_OP_UTILS_DEEP_OP_BENCHMARKS_UTIL_H_

#include <cmath>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "gmock/gmock.h"
#include "absl/log/check.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/unit.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/presence_and.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/uuid_object.h"

namespace koladata::internal {

namespace benchmarks_utils {

using ::arolla::CancellationContext;

constexpr auto kBenchmarkFn = [](auto* b) {
  // Number of objects per chain, number of chains.
  b->Args({1, 1})
      ->Args({1, 1000})
      ->Args({1, 1'000'000})
      ->Args({2, 1})
      ->Args({2, 1000})
      ->Args({2, 1'000'000})
      ->Args({1000, 1})
      ->Args({1000, 1000})
      ->Args({1'000'000, 1});
};

constexpr auto kLayersBenchmarkFn = [](auto* b) {
  // Number of layers, number of attributes per object, percent of present
  // attributes, number of objects per layer.
  b->Args({2, 10, 100, 1000})
      ->Args({2, 100, 100, 1000})
      ->Args({2, 100, 100, 10})
      ->Args({20, 10, 100, 10})
      ->Args({20, 100, 100, 10})
      ->Args({20, 100, 10, 10});
};

constexpr auto kTreesBenchmarkFn = [](auto* b) {
  // Number of layers, number of non-leaf childs per object, number of leaves
  // per object, size of the slice.
  b->Args({2, 10, 10, 1})
      ->Args({2, 10, 10, 10})
      ->Args({2, 10, 10, 100})
      ->Args({2, 1000, 1000, 1})
      ->Args({2, 1000, 1000, 10})
      ->Args({5, 10, 10, 1})
      ->Args({5, 10, 10, 10})
      ->Args({5, 10, 10, 100})
      ->Args({18, 2, 2, 1})
      ->Args({18, 2, 2, 10})
      ->Args({18, 2, 2, 100});
};

using RunBenchmarksFn =
    std::function<void(benchmark::State&, DataSliceImpl&, DataItem&,
                       DataBagImplPtr&, DataBagImpl::FallbackSpan)>;

using RunCastingBenchmarksFn = std::function<void(
    benchmark::State&, DataSliceImpl& ds_impl, DataItem& ds_schema,
    DataBagImplPtr& ds_databag, const DataBagImpl::FallbackSpan ds_fallbacks,
    DataItem& new_schema, DataBagImplPtr& new_schema_databag,
    const DataBagImpl::FallbackSpan new_schema_fallbacks)>;

inline DataSliceImpl ShuffleObjectsSlice(const DataSliceImpl& ds,
                                         absl::BitGen& gen) {
  // Generate random permutation.
  std::vector<int64_t> perm(ds.size());
  for (int64_t i = 0; i < ds.size(); ++i) {
    perm[i] = i;
  }
  for (int64_t i = 1; i < ds.size(); ++i) {
    std::swap(perm[i], perm[absl::Uniform(gen, 0, i)]);
  }
  // Shuffle data.
  auto obj_ids_bldr = arolla::DenseArrayBuilder<ObjectId>(ds.size());
  const auto& values = ds.values<ObjectId>();
  for (int64_t i = 0; i < ds.size(); ++i) {
    obj_ids_bldr.Set(perm[i], values[i]);
  }
  return DataSliceImpl::CreateObjectsDataSlice(std::move(obj_ids_bldr).Build(),
                                               ds.allocation_ids());
}

inline DataSliceImpl ApplyRandomMask(const DataSliceImpl& ds,
                                     int64_t presence_rate, absl::BitGen& gen) {
  auto filter = arolla::DenseArrayBuilder<arolla::Unit>(ds.size());
  for (int64_t i = 0; i < ds.size(); ++i) {
    if (absl::Uniform(gen, 0, 100) < presence_rate) {
      filter.Set(i, arolla::kPresent);
    }
  }
  auto ds_filter = DataSliceImpl::Create(std::move(filter).Build());
  auto result = PresenceAndOp()(ds, ds_filter).value();
  return result;
}

inline void BM_DisjointChains(benchmark::State& state,
                              const RunBenchmarksFn& run_fn) {
  int64_t schema_depth = state.range(0);
  int64_t ds_size = state.range(1);

  CancellationContext::ScopeGuard cancellation_scope;
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto root_ds = DataSliceImpl::AllocateEmptyObjects(ds_size);
  auto root_schema = DataItem(internal::AllocateExplicitSchema());
  auto ds = root_ds;
  auto schema = root_schema;
  const internal::DataItem kObjectSchema(schema::kObject);
  for (int64_t i = 0; i < schema_depth; ++i) {
    auto child_schema = DataItem(internal::AllocateExplicitSchema());
    auto child_ds = DataSliceImpl::AllocateEmptyObjects(ds_size);
    CHECK_OK(db->SetSchemaAttr(schema, "child", child_schema));
    CHECK_OK(db->SetAttr(ds, "child", child_ds));
    ds = std::move(child_ds);
    schema = std::move(child_schema);
  }
  run_fn(state, root_ds, root_schema, db, {});
}

inline void BM_DisjointChainsObjects(benchmark::State& state,
                                     const RunBenchmarksFn& run_fn) {
  int64_t schema_depth = state.range(0);
  int64_t ds_size = state.range(1);

  CancellationContext::ScopeGuard cancellation_scope;
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto root_ds = DataSliceImpl::AllocateEmptyObjects(ds_size);
  auto ds = root_ds;
  internal::DataItem kObjectSchema(schema::kObject);
  auto kObjectSchemaSlice =
      DataSliceImpl::Create(/*size=*/ds_size, kObjectSchema);
  auto kItemIdSchemaSlice =
      DataSliceImpl::Create(/*size=*/ds_size, DataItem(schema::kItemId));
  for (int64_t i = 0; i < schema_depth; ++i) {
    ASSERT_OK_AND_ASSIGN(
        auto schemas,
        CreateUuidWithMainObject<internal::ObjectId::kUuidImplicitSchemaFlag>(
            ds, schema::kImplicitSchemaSeed));
    CHECK_OK(db->SetAttr(ds, schema::kSchemaAttr, schemas));
    auto child_ds = DataSliceImpl::AllocateEmptyObjects(ds_size);
    if (i + 1 < schema_depth) {
      CHECK_OK(db->SetSchemaAttr(schemas, "child", kObjectSchemaSlice));
    } else {
      CHECK_OK(db->SetSchemaAttr(schemas, "child", kItemIdSchemaSlice));
    }
    CHECK_OK(db->SetAttr(ds, "child", child_ds));
    ds = std::move(child_ds);
  }
  run_fn(state, root_ds, kObjectSchema, db, {});
}

inline void BM_DAG(benchmark::State& state, const RunBenchmarksFn& run_fn) {
  int64_t schema_depth = state.range(0);
  int64_t attr_count = state.range(1);
  int64_t presence_rate = state.range(2);
  int64_t ds_size = state.range(3);
  absl::BitGen gen;

  CancellationContext::ScopeGuard cancellation_scope;
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto root_ds = DataSliceImpl::AllocateEmptyObjects(ds_size);
  auto root_schema = DataItem(internal::AllocateExplicitSchema());
  auto ds = root_ds;
  auto schema = root_schema;
  for (int64_t i = 0; i < schema_depth; ++i) {
    auto child_schema = DataItem(internal::AllocateExplicitSchema());
    auto child_ds = DataSliceImpl::AllocateEmptyObjects(ds_size);
    for (int64_t j = 0; j < attr_count; ++j) {
      std::string attr_name = absl::StrCat("layer_", i, "_child_", j);
      CHECK_OK(db->SetSchemaAttr(schema, attr_name, child_schema));
      CHECK_OK(db->SetAttr(ds, attr_name,
                           ApplyRandomMask(ShuffleObjectsSlice(child_ds, gen),
                                           presence_rate, gen)));
    }
    ds = std::move(child_ds);
    schema = std::move(child_schema);
  }
  run_fn(state, root_ds, root_schema, db, {});
}

inline void BM_DAGObjects(benchmark::State& state,
                          const RunBenchmarksFn& run_fn) {
  int64_t schema_depth = state.range(0);
  int64_t attr_count = state.range(1);
  int64_t presence_rate = state.range(2);
  int64_t ds_size = state.range(3);
  absl::BitGen gen;

  CancellationContext::ScopeGuard cancellation_scope;
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto root_ds = DataSliceImpl::AllocateEmptyObjects(ds_size);
  auto ds = root_ds;

  internal::DataItem kObjectSchema(schema::kObject);
  auto kObjectSchemaSlice =
      DataSliceImpl::Create(/*size=*/ds_size, kObjectSchema);
  auto kItemIdSchemaSlice =
      DataSliceImpl::Create(/*size=*/ds_size, DataItem(schema::kItemId));
  for (int64_t i = 0; i < schema_depth; ++i) {
    ASSERT_OK_AND_ASSIGN(
        auto schemas,
        CreateUuidWithMainObject<internal::ObjectId::kUuidImplicitSchemaFlag>(
            ds, schema::kImplicitSchemaSeed));
    CHECK_OK(db->SetAttr(ds, schema::kSchemaAttr, schemas));
    auto child_ds = DataSliceImpl::AllocateEmptyObjects(ds_size);
    for (int64_t j = 0; j < attr_count; ++j) {
      std::string attr_name = absl::StrCat("layer_", i, "_child_", j);
      if (i + 1 < schema_depth) {
        CHECK_OK(db->SetSchemaAttr(schemas, attr_name, kObjectSchemaSlice));
      } else {
        CHECK_OK(db->SetSchemaAttr(schemas, attr_name, kItemIdSchemaSlice));
      }
      CHECK_OK(db->SetAttr(ds, attr_name,
                           ApplyRandomMask(ShuffleObjectsSlice(child_ds, gen),
                                           presence_rate, gen)));
    }
    ds = std::move(child_ds);
  }
  run_fn(state, root_ds, kObjectSchema, db, {});
}

inline void BM_TreeShapedIntToFloat(benchmark::State& state,
                                    const RunCastingBenchmarksFn& run_fn) {
  int64_t schema_depth = state.range(0);
  int64_t schema_attr_count = state.range(1);
  int64_t primitive_attr_count = state.range(2);
  int64_t ds_size = state.range(3);
  int64_t num_leaves =
      static_cast<int64_t>(std::pow(schema_attr_count, schema_depth - 1)) *
      primitive_attr_count * ds_size;
  state.SetLabel(absl::StrFormat(
      "ds_size=%d; depth=%d; attrs_per_node=%d+%d; total_leaves=%d", ds_size,
      schema_depth, schema_attr_count, primitive_attr_count, num_leaves));

  CancellationContext::ScopeGuard cancellation_scope;
  auto db_a = DataBagImpl::CreateEmptyDatabag();
  auto db_b = DataBagImpl::CreateEmptyDatabag();
  auto root_schema_a = DataItem(internal::AllocateExplicitSchema());
  auto root_schema_b = DataItem(internal::AllocateExplicitSchema());
  auto root_ds = DataSliceImpl::AllocateEmptyObjects(ds_size);
  std::vector<DataItem> schemas_a({root_schema_a});
  std::vector<DataItem> schemas_b({root_schema_b});
  std::vector<DataSliceImpl> ds({root_ds});
  int value = 0;
  for (int64_t i = 0; i < schema_depth; ++i) {
    std::vector<DataItem> child_schemas_a;
    std::vector<DataItem> child_schemas_b;
    std::vector<DataSliceImpl> child_ds;
    for (uint64_t id = 0; id < schemas_a.size(); ++id) {
      auto schema_a = schemas_a[id];
      auto schema_b = schemas_b[id];
      auto cur_ds = ds[id];
      for (int64_t j = 0; j < schema_attr_count; ++j)
      {
        std::string attr_name = absl::StrCat("layer_", i, "_child_", j);
        auto child_schema_a = DataItem(internal::AllocateExplicitSchema());
        auto child_schema_b = DataItem(internal::AllocateExplicitSchema());
        auto cur_child_ds = DataSliceImpl::AllocateEmptyObjects(ds_size);
        CHECK_OK(db_a->SetSchemaAttr(schema_a, attr_name, child_schema_a));
        CHECK_OK(db_b->SetSchemaAttr(schema_b, attr_name, child_schema_b));
        CHECK_OK(db_a->SetAttr(cur_ds, attr_name, cur_child_ds));
        child_ds.push_back(std::move(cur_child_ds));
        child_schemas_a.push_back(std::move(child_schema_a));
        child_schemas_b.push_back(std::move(child_schema_b));
      }
      for (int64_t j = 0; j < primitive_attr_count; ++j)
      {
        std::string attr_name = absl::StrCat("layer_", i, "_primitive_", j);
        auto child_schema_a = DataItem(schema::kInt32);
        auto child_schema_b = DataItem(schema::kFloat32);
        std::vector<std::optional<int>> values(ds_size);
        for (int64_t h = 0; h < ds_size; ++h) {
          values[h] = ++value;
        }
        DataSliceImpl::Create(
            arolla::CreateDenseArray<int>(values.begin(), values.end()));
        CHECK_OK(db_a->SetSchemaAttr(schema_a, attr_name, child_schema_a));
        CHECK_OK(db_b->SetSchemaAttr(schema_b, attr_name, child_schema_b));
      }
    }
    std::swap(ds, child_ds);
    std::swap(schemas_a, child_schemas_a);
    std::swap(schemas_b, child_schemas_b);
  }
  run_fn(state, root_ds, root_schema_a, db_a, {}, root_schema_b, db_b,
                          {});
}

}  // namespace benchmarks_utils

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_DEEP_OP_BENCHMARKS_UTIL_H_
