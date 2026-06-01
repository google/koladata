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
#include <utility>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/qtype/base_types.h"
#include "koladata/internal/casting.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/deep_op_benchmarks_util.h"
#include "koladata/internal/op_utils/extract.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::internal {
namespace {

using benchmarks_utils::RunBenchmarksFn;

void RunBenchmarks(benchmark::State& state, DataSliceImpl& ds, DataItem& schema,
                   DataBagImplPtr& databag,
                   DataBagImpl::FallbackSpan fallbacks) {
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(ds);
    benchmark::DoNotOptimize(schema);
    benchmark::DoNotOptimize(databag);
    benchmark::DoNotOptimize(fallbacks);
    auto result_db = DataBagImpl::CreateEmptyDatabag();
    ExtractOp(result_db.get())(ds, schema, *databag, fallbacks, nullptr, {})
        .IgnoreError();
    benchmark::DoNotOptimize(result_db);
  }
}

void RunCastingBenchmarks(benchmark::State& state, DataSliceImpl& ds,
                          DataItem& schema, DataBagImplPtr& databag,
                          DataBagImpl::FallbackSpan fallbacks,
                          DataItem& new_schema, DataBagImplPtr& schema_databag,
                          DataBagImpl::FallbackSpan schema_fallbacks) {
  auto cast_data_callback =
      static_cast<absl::StatusOr<internal::DataSliceImpl> (*)(
          const internal::DataSliceImpl&, const internal::DataItem&)>(
          schema::CastDataTo);
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(ds);
    benchmark::DoNotOptimize(schema);
    benchmark::DoNotOptimize(databag);
    benchmark::DoNotOptimize(fallbacks);
    benchmark::DoNotOptimize(new_schema);
    benchmark::DoNotOptimize(schema_databag);
    benchmark::DoNotOptimize(schema_fallbacks);
    auto result_db = DataBagImpl::CreateEmptyDatabag();
    ExtractOp(result_db.get())(ds, new_schema, *databag, fallbacks,
                               &*schema_databag, schema_fallbacks,
                               /*max_depth=*/-1, cast_data_callback)
        .IgnoreError();
    benchmark::DoNotOptimize(result_db);
  }
}

void BM_DisjointChains(benchmark::State& state) {
  benchmarks_utils::BM_DisjointChains(state, RunBenchmarks);
}
BENCHMARK(BM_DisjointChains)->Apply(benchmarks_utils::kBenchmarkFn);

void BM_DisjointChainsObjects(benchmark::State& state) {
  benchmarks_utils::BM_DisjointChainsObjects(state, RunBenchmarks);
}
BENCHMARK(BM_DisjointChainsObjects)->Apply(benchmarks_utils::kBenchmarkFn);

void BM_DAG(benchmark::State& state) {
  benchmarks_utils::BM_DAG(state, RunBenchmarks);
}
BENCHMARK(BM_DAG)->Apply(benchmarks_utils::kLayersBenchmarkFn);

void BM_DAGObjects(benchmark::State& state) {
  benchmarks_utils::BM_DAGObjects(state, RunBenchmarks);
}
BENCHMARK(BM_DAGObjects)->Apply(benchmarks_utils::kLayersBenchmarkFn);

void BM_TreeShapedIntToFloat(benchmark::State& state) {
  benchmarks_utils::BM_TreeShapedIntToFloat(state, RunCastingBenchmarks);
}
BENCHMARK(BM_TreeShapedIntToFloat)->Apply(benchmarks_utils::kTreesBenchmarkFn);

inline void BM_ScalarPrimitive(benchmark::State& state) {
  auto input_db = DataBagImpl::CreateEmptyDatabag();
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ExtractOp op(result_db.get());
  DataItem item(1);
  DataItem schema(schema::kInt32);
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(item);
    benchmark::DoNotOptimize(schema);
    benchmark::DoNotOptimize(input_db);
    auto result_db = DataBagImpl::CreateEmptyDatabag();
    op(item, schema, *input_db, {}, nullptr, {}).IgnoreError();
    benchmark::DoNotOptimize(result_db);
  }
}
BENCHMARK(BM_ScalarPrimitive);

void BM_FullAllocs(benchmark::State& state) {
  int size = state.range(0);
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto schema = DataItem(AllocateExplicitSchema());
  CHECK_OK(db->SetSchemaAttr(schema, "a", DataItem(schema::kInt32)));
  CHECK_OK(db->SetSchemaAttr(schema, "b", DataItem(schema::kInt32)));
  CHECK_OK(db->SetSchemaAttr(schema, "c", DataItem(schema::kInt32)));

  SliceBuilder ba(size), bb(size), bc(size);
  for (int i = 0; i < size; ++i) {
    ba.InsertIfNotSet(i, i);
    bb.InsertIfNotSet(i, size + i);
    bc.InsertIfNotSet(i, size * 2 + i);
  }
  auto ds_x = std::move(ba).Build();
  auto ds_y = std::move(bb).Build();
  auto ds_z = std::move(bc).Build();
  auto ds1 =
      db->CreateObjectsFromFields({"a", "b", "c"}, {ds_x, ds_y, ds_z}).value();
  auto ds2 =
      db->CreateObjectsFromFields({"a", "b", "c"}, {ds_y, ds_z, ds_x}).value();

  while (state.KeepRunning()) {
    auto res_db = DataBagImpl::CreateEmptyDatabag();
    benchmark::DoNotOptimize(db);
    benchmark::DoNotOptimize(res_db);
    ExtractOp op(res_db.get());
    CHECK_OK(op(ds1, schema, *db, {}, nullptr, {}));
    benchmark::DoNotOptimize(res_db);
  }
}

BENCHMARK(BM_FullAllocs)->Arg(10)->Arg(100)->Arg(1000)->Arg(10000);

}  // namespace
}  // namespace koladata::internal
