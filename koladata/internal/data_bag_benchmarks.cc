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
#include <cstdint>
#include <functional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/benchmark_helpers.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/uuid_object.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/bytes.h"

namespace koladata::internal {
namespace {

struct BatchAccess {};
struct PointwiseAccess {};

template <typename Access>
void BM_ObjAttributeDataBagSingleSource(benchmark::State& state) {
  int64_t batch_size = state.range(0);
  int64_t skip_size = state.range(1);
  int64_t total_size = batch_size * (skip_size + 1);
  auto ds = DataSliceImpl::AllocateEmptyObjects(total_size);
  auto ds_a = DataSliceImpl::AllocateEmptyObjects(total_size);

  auto db = DataBagImpl::CreateEmptyDatabag();

  CHECK_OK(db->SetAttr(ds, "a", ds_a));

  auto ds_filtered = FilterSingleAllocDataSlice(ds, batch_size, skip_size);
  std::vector<DataItem> ds_items(ds_filtered.begin(), ds_filtered.end());

  while (state.KeepRunningBatch(batch_size)) {
    benchmark::DoNotOptimize(db);

    if constexpr (std::is_same_v<Access, BatchAccess>) {
      benchmark::DoNotOptimize(ds_filtered);
      DataSliceImpl ds_a_get = db->GetAttr(ds_filtered, "a").value();
      benchmark::DoNotOptimize(ds_a_get);
    } else {
      benchmark::DoNotOptimize(ds_items);
      for (int64_t i = 0; i != batch_size; ++i) {
        auto get_attr = db->GetAttr(ds_items[i], "a");
        benchmark::DoNotOptimize(get_attr);
      }
    }
  }
}

BENCHMARK(BM_ObjAttributeDataBagSingleSource<BatchAccess>)
    ->Apply(kBenchmarkObjectBatchPairsFn);
BENCHMARK(BM_ObjAttributeDataBagSingleSource<PointwiseAccess>)
    ->Apply(kPointwiseBenchmarkObjectBatchPairsFn);

struct FallbackEmpty {};
struct MainEmpty {};
struct BothFull {};
struct BothHalf {};

template <typename Access, typename FallbackMode>
void BM_ObjAttributeDataBagSingleSourceFallback(benchmark::State& state) {
  int64_t batch_size = state.range(0);
  auto ds = DataSliceImpl::AllocateEmptyObjects(batch_size);
  auto ds_full = DataSliceImpl::AllocateEmptyObjects(batch_size);

  auto db = DataBagImpl::CreateEmptyDatabag();
  auto db_fallback = DataBagImpl::CreateEmptyDatabag();

  if constexpr (std::is_same_v<FallbackMode, FallbackEmpty> ||
                std::is_same_v<FallbackMode, BothFull>) {
    CHECK_OK(db->SetAttr(ds, "a", ds_full));
  }
  if constexpr (std::is_same_v<FallbackMode, MainEmpty> ||
                std::is_same_v<FallbackMode, BothFull>) {
    CHECK_OK(db_fallback->SetAttr(ds, "a", ds_full));
  }

  if constexpr (std::is_same_v<FallbackMode, BothHalf>) {
    auto ds_half = RemoveItemsIf(ds_full, [](const DataItem& item) {
      return item.value<ObjectId>().Offset() % 2 == 0;
    });
    CHECK_OK(db->SetAttr(ds, "a", ds_half));
    ds_half = RemoveItemsIf(ds_full, [](const DataItem& item) {
      return item.value<ObjectId>().Offset() % 2 == 1;
    });
    CHECK_OK(db_fallback->SetAttr(ds, "a", ds_half));
  }

  std::vector<DataItem> ds_items(ds.begin(), ds.end());

  while (state.KeepRunningBatch(batch_size)) {
    benchmark::DoNotOptimize(db);

    if constexpr (std::is_same_v<Access, BatchAccess>) {
      benchmark::DoNotOptimize(ds);
      DataSliceImpl ds_a_get =
          db->GetAttr(ds, "a", {db_fallback.get()}).value();
      benchmark::DoNotOptimize(ds_a_get);
    } else {
      benchmark::DoNotOptimize(ds_items);
      for (int64_t i = 0; i != batch_size; ++i) {
        auto get_attr = db->GetAttr(ds_items[i], "a", {db_fallback.get()});
        benchmark::DoNotOptimize(get_attr);
      }
    }
  }
}

constexpr auto kFallbackSizeFn = [](auto* b) {
  b->Arg(2)->Arg(10)->Arg(10000);
};

BENCHMARK(
    BM_ObjAttributeDataBagSingleSourceFallback<BatchAccess, FallbackEmpty>)
    ->Apply(kFallbackSizeFn);
BENCHMARK(BM_ObjAttributeDataBagSingleSourceFallback<BatchAccess, MainEmpty>)
    ->Apply(kFallbackSizeFn);
BENCHMARK(BM_ObjAttributeDataBagSingleSourceFallback<BatchAccess, BothFull>)
    ->Apply(kFallbackSizeFn);
BENCHMARK(BM_ObjAttributeDataBagSingleSourceFallback<BatchAccess, BothHalf>)
    ->Apply(kFallbackSizeFn);
BENCHMARK(
    BM_ObjAttributeDataBagSingleSourceFallback<PointwiseAccess, FallbackEmpty>)
    ->Apply(kFallbackSizeFn);
BENCHMARK(
    BM_ObjAttributeDataBagSingleSourceFallback<PointwiseAccess, MainEmpty>)
    ->Apply(kFallbackSizeFn);
BENCHMARK(BM_ObjAttributeDataBagSingleSourceFallback<PointwiseAccess, BothFull>)
    ->Apply(kFallbackSizeFn);
BENCHMARK(BM_ObjAttributeDataBagSingleSourceFallback<PointwiseAccess, BothHalf>)
    ->Apply(kFallbackSizeFn);

template <typename Access>
void BM_ObjAttributeDataBagUUid(benchmark::State& state) {
  int64_t total_size = state.range(0);
  auto ds_obj = DataSliceImpl::AllocateEmptyObjects(total_size);
  auto ds = CreateUuidFromFields("", {"Q"}, {std::cref(ds_obj)}).value();
  std::vector<DataItem> ds_items(ds.begin(), ds.end());
  auto ds_a = DataSliceImpl::AllocateEmptyObjects(total_size);

  auto db = DataBagImpl::CreateEmptyDatabag();

  for (int64_t i = 0; i != total_size; ++i) {
    CHECK_OK(db->SetAttr(ds[i], "a", ds_a[i]));
  }

  while (state.KeepRunningBatch(total_size)) {
    benchmark::DoNotOptimize(db);

    if constexpr (std::is_same_v<Access, BatchAccess>) {
      benchmark::DoNotOptimize(ds);
      DataSliceImpl ds_a_get = db->GetAttr(ds, "a").value();
      benchmark::DoNotOptimize(ds_a_get);
    } else {
      benchmark::DoNotOptimize(ds_items);
      for (int64_t i = 0; i != total_size; ++i) {
        auto get_attr = db->GetAttr(ds_items[i], "a");
        benchmark::DoNotOptimize(get_attr);
      }
    }
  }
}

BENCHMARK(BM_ObjAttributeDataBagUUid<BatchAccess>)->Range(1, 100000);
BENCHMARK(BM_ObjAttributeDataBagUUid<PointwiseAccess>)->Range(1, 100000);

template <typename Access>
void BM_ObjAttributeDataBagManySources(benchmark::State& state) {
  int64_t batch_size = state.range(0);
  int64_t chain_size = state.range(1);

  auto db = DataBagImpl::CreateEmptyDatabag();
  arolla::DenseArrayBuilder<ObjectId> ds_bldr(batch_size);
  AllocationIdSet alloc_ids;

  for (int64_t i = 0; i != chain_size; ++i) {
    auto ds = DataSliceImpl::AllocateEmptyObjects(batch_size);
    auto ds_a = DataSliceImpl::AllocateEmptyObjects(batch_size);
    CHECK_OK(db->SetAttr(ds, "a", ds_a));
    for (int j = i; j < batch_size; j += chain_size) {
      ds_bldr.Set(j, ds_a.values<ObjectId>()[j]);
    }
    alloc_ids.Insert(ds_a.allocation_ids());
  }

  auto ds =
      DataSliceImpl::CreateWithAllocIds(alloc_ids, std::move(ds_bldr).Build());
  std::vector<DataItem> ds_items(ds.begin(), ds.end());

  while (state.KeepRunningBatch(batch_size)) {
    benchmark::DoNotOptimize(db);

    if constexpr (std::is_same_v<Access, BatchAccess>) {
      benchmark::DoNotOptimize(ds);
      DataSliceImpl ds_a_get = db->GetAttr(ds, "a").value();
      benchmark::DoNotOptimize(ds_a_get);
    } else {
      benchmark::DoNotOptimize(ds_items);
      for (int64_t i = 0; i != batch_size; ++i) {
        auto get_attr = db->GetAttr(ds_items[i], "a");
        benchmark::DoNotOptimize(get_attr);
      }
    }
  }
}

BENCHMARK(BM_ObjAttributeDataBagManySources<BatchAccess>)
    ->ArgPair(15, 3)
    ->ArgPair(10000, 3)
    ->ArgPair(10000, 8);
BENCHMARK(BM_ObjAttributeDataBagManySources<PointwiseAccess>)
    ->ArgPair(15, 3)
    ->ArgPair(10000, 3)
    ->ArgPair(10000, 8);

template <typename Access>
void BM_Int32AttributeDataBagSingleSource(benchmark::State& state) {
  int64_t batch_size = state.range(0);
  int64_t skip_size = state.range(1);
  int64_t total_size = batch_size * (skip_size + 1);
  auto ds = DataSliceImpl::AllocateEmptyObjects(total_size);
  auto values_a =
      arolla::CreateFullDenseArray(std::vector<int32_t>(total_size, 57));
  auto ds_a = DataSliceImpl::Create(values_a);

  auto db = DataBagImpl::CreateEmptyDatabag();

  CHECK_OK(db->SetAttr(ds, "a", ds_a));

  auto ds_filtered = FilterSingleAllocDataSlice(ds, batch_size, skip_size);
  std::vector<DataItem> ds_items(ds_filtered.begin(), ds_filtered.end());

  while (state.KeepRunningBatch(batch_size)) {
    benchmark::DoNotOptimize(db);

    if constexpr (std::is_same_v<Access, BatchAccess>) {
      benchmark::DoNotOptimize(ds_filtered);
      DataSliceImpl ds_a_get = db->GetAttr(ds_filtered, "a").value();
      benchmark::DoNotOptimize(ds_a_get);
    } else {
      benchmark::DoNotOptimize(ds_items);
      for (int64_t i = 0; i != batch_size; ++i) {
        auto get_attr = db->GetAttr(ds_items[i], "a");
        benchmark::DoNotOptimize(get_attr);
      }
    }
  }
}

BENCHMARK(BM_Int32AttributeDataBagSingleSource<BatchAccess>)
    ->Apply(kBenchmarkPrimitiveBatchPairsFn);
BENCHMARK(BM_Int32AttributeDataBagSingleSource<PointwiseAccess>)
    ->Apply(kPointwiseBenchmarkPrimitiveBatchPairsFn);

void BM_ObjAttributeDataBagSetToEmpty(benchmark::State& state) {
  int64_t batch_size = state.range(0);
  int64_t skip_size = state.range(1);
  int64_t total_size = batch_size * (skip_size + 1);
  auto ds = DataSliceImpl::AllocateEmptyObjects(total_size);
  auto ds_a = DataSliceImpl::AllocateEmptyObjects(total_size);

  auto ds_filtered = FilterSingleAllocDataSlice(ds, batch_size, skip_size);
  auto ds_a_filtered = FilterSingleAllocDataSlice(ds_a, batch_size, skip_size);

  while (state.KeepRunningBatch(batch_size)) {
    benchmark::DoNotOptimize(ds_filtered);
    benchmark::DoNotOptimize(ds_a_filtered);
    auto db = DataBagImpl::CreateEmptyDatabag();
    CHECK_OK(db->SetAttr(ds_filtered, "a", ds_a_filtered));
    benchmark::DoNotOptimize(db);
  }
}

BENCHMARK(BM_ObjAttributeDataBagSetToEmpty)
    ->Apply(kBenchmarkObjectBatchPairsFn);

void BM_Int32AttributeDataBagSetToEmpty(benchmark::State& state) {
  int64_t batch_size = state.range(0);
  int64_t skip_size = state.range(1);
  int64_t total_size = batch_size * (skip_size + 1);
  auto ds = DataSliceImpl::AllocateEmptyObjects(total_size);
  auto values_a = arolla::CreateConstDenseArray<int32_t>(batch_size, 57);
  auto ds_a_filtered = DataSliceImpl::Create(values_a);

  auto ds_filtered = FilterSingleAllocDataSlice(ds, batch_size, skip_size);

  while (state.KeepRunningBatch(batch_size)) {
    benchmark::DoNotOptimize(ds_filtered);
    benchmark::DoNotOptimize(ds_a_filtered);
    auto db = DataBagImpl::CreateEmptyDatabag();
    CHECK_OK(db->SetAttr(ds_filtered, "a", ds_a_filtered));
    benchmark::DoNotOptimize(db);
  }
}

BENCHMARK(BM_Int32AttributeDataBagSetToEmpty)
    ->Apply(kBenchmarkPrimitiveBatchPairsFn);

void BM_CreateObjectsFromFields(benchmark::State& state) {
  int64_t batch_size = state.range(0);
  auto values_a = arolla::CreateConstDenseArray<int32_t>(batch_size, 57);
  auto ds_a = DataSliceImpl::Create(values_a);
  auto ds_b = DataSliceImpl::AllocateEmptyObjects(batch_size);

  while (state.KeepRunningBatch(batch_size * 2)) {
    benchmark::DoNotOptimize(ds_b);
    benchmark::DoNotOptimize(ds_a);
    auto db = DataBagImpl::CreateEmptyDatabag();
    CHECK_OK(db->CreateObjectsFromFields({"a", "b"}, {ds_a, ds_b}));
    benchmark::DoNotOptimize(db);
  }
}

BENCHMARK(BM_CreateObjectsFromFields)
    ->Arg(1)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000);

void BM_ObjAttributeDataBagSetToMutableSingleSource(benchmark::State& state) {
  int64_t batch_size = state.range(0);
  int64_t skip_size = state.range(1);
  int64_t total_size = batch_size * (skip_size + 1);
  auto ds_a = DataSliceImpl::AllocateEmptyObjects(total_size);

  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds = db->CreateObjectsFromFields({"a"}, {ds_a}).value();

  auto ds_filtered = FilterSingleAllocDataSlice(ds, batch_size, skip_size);
  auto ds_a_filtered = FilterSingleAllocDataSlice(ds_a, batch_size, skip_size);

  // The first set would convert to mutable data source.
  CHECK_OK(db->SetAttr(ds_filtered, "a", ds_a_filtered));

  while (state.KeepRunningBatch(batch_size)) {
    benchmark::DoNotOptimize(ds_a_filtered);
    benchmark::DoNotOptimize(ds_filtered);
    benchmark::DoNotOptimize(db);
    CHECK_OK(db->SetAttr(ds_filtered, "a", ds_a_filtered));
    benchmark::DoNotOptimize(db);
  }
}

BENCHMARK(BM_ObjAttributeDataBagSetToMutableSingleSource)
    ->Apply(kBenchmarkObjectBatchPairsFn);

void BM_Int32AttributeDataBagSetToMutableSingleSource(benchmark::State& state) {
  int64_t batch_size = state.range(0);
  int64_t skip_size = state.range(1);
  int64_t total_size = batch_size * (skip_size + 1);
  auto ds_a = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<int32_t>(total_size, 57));

  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds = db->CreateObjectsFromFields({"a"}, {ds_a}).value();

  auto ds_filtered = FilterSingleAllocDataSlice(ds, batch_size, skip_size);
  auto ds_a_filtered = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<int32_t>(batch_size, 57));

  // The first set would convert to mutable data source.
  CHECK_OK(db->SetAttr(ds_filtered, "a", ds_a_filtered));

  while (state.KeepRunningBatch(batch_size)) {
    benchmark::DoNotOptimize(ds_a_filtered);
    benchmark::DoNotOptimize(ds_filtered);
    benchmark::DoNotOptimize(db);
    CHECK_OK(db->SetAttr(ds_filtered, "a", ds_a_filtered));
    benchmark::DoNotOptimize(db);
  }
}

BENCHMARK(BM_Int32AttributeDataBagSetToMutableSingleSource)
    ->Apply(kBenchmarkPrimitiveBatchPairsFn);

void BM_ForkAndSet(benchmark::State& state) {
  int64_t alloc_size = state.range(0);
  int64_t set_size = state.range(1);
  CHECK_LE(set_size, alloc_size);
  int64_t step = alloc_size / set_size;

  AllocationId alloc = Allocate(alloc_size);
  arolla::DenseArrayBuilder<ObjectId> objects_bldr(set_size);
  for (int64_t i = 0; i < set_size; ++i) {
    objects_bldr.Set(i, alloc.ObjectByOffset(i * step));
  }
  DataSliceImpl objects = DataSliceImpl::CreateWithAllocIds(
      AllocationIdSet(alloc), std::move(objects_bldr).Build());
  DataSliceImpl values = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<int32_t>(set_size, 57));

  auto db = DataBagImpl::CreateEmptyDatabag();
  CHECK_OK(db->SetAttr(objects, "a", values));

  for (auto _ : state) {
    benchmark::DoNotOptimize(objects);
    benchmark::DoNotOptimize(values);
    benchmark::DoNotOptimize(db);
    // TODO: DataBagImpl should automatically cut the
    //   link to the parent (with putting all parent dense sources to
    //   const_source and merging all sparse sources) if the chain exceeds some
    //   limit.
    db = db->PartiallyPersistentFork();
    CHECK_OK(db->SetAttr(objects, "a", values));
    benchmark::DoNotOptimize(db);
  }
  state.SetItemsProcessed(state.iterations() * set_size);
}

BENCHMARK(BM_ForkAndSet)
    ->ArgPair(1, 1)
    ->ArgPair(10, 1)
    ->ArgPair(100, 1)
    ->ArgPair(1000, 1)
    ->ArgPair(10, 10)
    ->ArgPair(100, 10)
    ->ArgPair(1000, 10)
    ->ArgPair(10000, 10)
    ->ArgPair(100, 100)
    ->ArgPair(1000, 100)
    ->ArgPair(10000, 100)
    ->ArgPair(1000, 1000)
    ->ArgPair(10000, 1000)
    ->ArgPair(10000, 10000);

void BM_AppendToListImpl(benchmark::State& state, bool mixed_type) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  DataItem list(AllocateSingleList());
  DataItem value(1.0f);

  CHECK_OK(db->AppendToList(
      list, mixed_type ? DataItem(arolla::Bytes("abc")) : value));

  for (auto _ : state) {
    benchmark::DoNotOptimize(list);
    benchmark::DoNotOptimize(value);
    benchmark::DoNotOptimize(db);

    CHECK_OK(db->AppendToList(list, value));

    // Remove (to prevent unlimited memory usage)
    CHECK_OK(db->RemoveInList(list, DataBagImpl::ListRange(1)));

    benchmark::DoNotOptimize(db);
  }
  state.SetItemsProcessed(state.iterations());
}

void BM_AppendToListSingleType(benchmark::State& state) {
  BM_AppendToListImpl(state, false);
}

void BM_AppendToListMixedType(benchmark::State& state) {
  BM_AppendToListImpl(state, true);
}

// Note: it is append+remove, so the size of the lists is always the same.
// mixed_type=true means that the lists contain values of different types
// (so internally lists are in the `std::vector<DataItem>` form), appended
// values are always of the same type.
void BM_AppendToListsImpl(benchmark::State& state, bool mixed_type) {
  int64_t alloc_size = state.range(0);

  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc = AllocateLists(alloc_size);
  DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc, alloc_size);
  DataSliceImpl values_float = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<float>(alloc_size, 1.0f));
  DataSliceImpl values_bytes = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<arolla::Bytes>(alloc_size, "abc"));

  // Initialize, so the list is not empty.
  CHECK_OK(db->AppendToList(lists, mixed_type ? values_bytes : values_float));

  for (auto _ : state) {
    benchmark::DoNotOptimize(lists);
    benchmark::DoNotOptimize(values_float);
    benchmark::DoNotOptimize(db);

    CHECK_OK(db->AppendToList(lists, values_float));

    // Remove (to prevent unlimited memory usage)
    CHECK_OK(db->RemoveInList(lists, DataBagImpl::ListRange(1)));

    benchmark::DoNotOptimize(db);
  }
  state.SetItemsProcessed(state.iterations() * alloc_size);
}

void BM_AppendToListsSingleType(benchmark::State& state) {
  BM_AppendToListsImpl(state, false);
}

void BM_AppendToListsMixedType(benchmark::State& state) {
  BM_AppendToListsImpl(state, true);
}

BENCHMARK(BM_AppendToListSingleType);
BENCHMARK(BM_AppendToListMixedType);

BENCHMARK(BM_AppendToListsSingleType)->Arg(1)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_AppendToListsMixedType)->Arg(1)->Arg(10)->Arg(100)->Arg(1000);

void BM_GetFromList(benchmark::State& state) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  DataItem list(AllocateSingleList());

  CHECK_OK(db->AppendToList(list, DataItem(1.0f)));

  for (auto _ : state) {
    benchmark::DoNotOptimize(list);
    benchmark::DoNotOptimize(db);
    auto res = db->GetFromList(list, 0);
    benchmark::DoNotOptimize(res);
  }
  state.SetItemsProcessed(state.iterations());
}

void BM_GetFromLists(benchmark::State& state) {
  int64_t alloc_size = state.range(0);

  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc = AllocateLists(alloc_size);
  DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc, alloc_size);
  auto indices = arolla::CreateConstDenseArray<int64_t>(alloc_size, 0);

  CHECK_OK(db->AppendToList(
      lists, DataSliceImpl::Create(
                 arolla::CreateConstDenseArray<float>(alloc_size, 1.0f))));

  for (auto _ : state) {
    benchmark::DoNotOptimize(lists);
    benchmark::DoNotOptimize(db);
    benchmark::DoNotOptimize(indices);

    auto res = db->GetFromLists(lists, indices);
    benchmark::DoNotOptimize(res);
  }
  state.SetItemsProcessed(state.iterations() * alloc_size);
}

BENCHMARK(BM_GetFromList);
BENCHMARK(BM_GetFromLists)->Arg(1)->Arg(10)->Arg(100)->Arg(1000);

void BM_ExplodeList(benchmark::State& state) {
  int64_t list_size = state.range(0);

  auto db = DataBagImpl::CreateEmptyDatabag();
  DataItem list(AllocateSingleList());
  auto values =
      DataSliceImpl::Create(arolla::CreateConstDenseArray<int>(list_size, 0));
  CHECK_OK(db->ExtendList(list, values));

  for (auto _ : state) {
    benchmark::DoNotOptimize(list);
    benchmark::DoNotOptimize(db);

    auto res = db->ExplodeList(list);
    benchmark::DoNotOptimize(res);
  }
  state.SetItemsProcessed(state.iterations() * list_size);
}

void BM_ExplodeLists(benchmark::State& state) {
  int64_t list_count = state.range(0);
  int64_t list_size = state.range(1);

  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc = AllocateLists(list_count);
  DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc, list_count);
  auto values =
      DataSliceImpl::Create(arolla::CreateConstDenseArray<int>(list_size, 0));
  for (int64_t i = 0; i < list_count; ++i) {
    CHECK_OK(db->ExtendList(lists[i], values));
  }

  for (auto _ : state) {
    benchmark::DoNotOptimize(lists);
    benchmark::DoNotOptimize(db);

    auto res = db->ExplodeLists(lists);
    benchmark::DoNotOptimize(res);
  }
  state.SetItemsProcessed(state.iterations() * list_count * list_size);
}

BENCHMARK(BM_ExplodeList)->Arg(1)->Arg(10)->Arg(100);

BENCHMARK(BM_ExplodeLists)
    ->ArgPair(1, 1)
    ->ArgPair(1, 10)
    ->ArgPair(1, 100)
    ->ArgPair(10, 1)
    ->ArgPair(10, 10)
    ->ArgPair(10, 100)
    ->ArgPair(100, 1)
    ->ArgPair(100, 10)
    ->ArgPair(100, 100);

void BM_ExtendList(benchmark::State& state) {
  int64_t extend_size = state.range(0);

  auto db = DataBagImpl::CreateEmptyDatabag();
  DataItem list(AllocateSingleList());
  auto values = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<int>(extend_size, 0));

  for (auto _ : state) {
    auto db2 = db->PartiallyPersistentFork();
    benchmark::DoNotOptimize(list);
    benchmark::DoNotOptimize(values);
    benchmark::DoNotOptimize(db2);

    CHECK_OK(db2->ExtendList(list, values));

    benchmark::DoNotOptimize(db2);
  }
  state.SetItemsProcessed(state.iterations() * extend_size);
}

void BM_ExtendLists(benchmark::State& state) {
  int64_t list_count = state.range(0);
  int64_t extend_size = state.range(1);

  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc = AllocateLists(list_count);
  DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc, list_count);
  auto values = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<int>(list_count * extend_size, 0));
  arolla::Buffer<int64_t>::Builder split_points_bldr(list_count + 1);
  for (int64_t i = 0; i < list_count + 1; ++i) {
    split_points_bldr.Set(i, i * extend_size);
  }
  auto edge = arolla::DenseArrayEdge::FromSplitPoints(
      {std::move(split_points_bldr).Build()}).value();

  for (auto _ : state) {
    auto db2 = db->PartiallyPersistentFork();
    benchmark::DoNotOptimize(lists);
    benchmark::DoNotOptimize(values);
    benchmark::DoNotOptimize(edge);
    benchmark::DoNotOptimize(db2);

    CHECK_OK(db2->ExtendLists(lists, values, edge));

    benchmark::DoNotOptimize(db2);
  }
  state.SetItemsProcessed(state.iterations() * list_count * extend_size);
}

BENCHMARK(BM_ExtendList)->Arg(1)->Arg(10)->Arg(100);

BENCHMARK(BM_ExtendLists)
    ->ArgPair(1, 1)
    ->ArgPair(1, 10)
    ->ArgPair(1, 100)
    ->ArgPair(10, 1)
    ->ArgPair(10, 10)
    ->ArgPair(10, 100)
    ->ArgPair(100, 1)
    ->ArgPair(100, 10)
    ->ArgPair(100, 100);

void BM_ReplaceInLists(benchmark::State& state) {
  int64_t list_count = state.range(0);
  int64_t list_size = state.range(1);

  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc = AllocateLists(list_count);
  DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc, list_count);
  auto values = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<int>(list_count * list_size, 0));
  arolla::Buffer<int64_t>::Builder split_points_bldr(list_count + 1);
  for (int64_t i = 0; i < list_count + 1; ++i) {
    split_points_bldr.Set(i, i * list_size);
  }
  auto edge = arolla::DenseArrayEdge::FromSplitPoints(
      {std::move(split_points_bldr).Build()}).value();

  for (auto _ : state) {
    auto db2 = db->PartiallyPersistentFork();
    benchmark::DoNotOptimize(lists);
    benchmark::DoNotOptimize(values);
    benchmark::DoNotOptimize(edge);
    benchmark::DoNotOptimize(db2);

    CHECK_OK(
        db2->ReplaceInLists(lists, DataBagImpl::ListRange(), values, edge));

    benchmark::DoNotOptimize(db2);
  }
  state.SetItemsProcessed(state.iterations() * list_count * list_size);
}

BENCHMARK(BM_ReplaceInLists)
    ->ArgPair(1, 1)
    ->ArgPair(1, 10)
    ->ArgPair(1, 100)
    ->ArgPair(10, 1)
    ->ArgPair(10, 10)
    ->ArgPair(10, 100)
    ->ArgPair(100, 1)
    ->ArgPair(100, 10)
    ->ArgPair(100, 100);

struct HitKey {};
struct MissKey {};

template <typename KeyMode>
void BM_GetFromDict(benchmark::State& state) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  DataItem dict(AllocateSingleDict());
  DataItem key(1);
  DataItem lookup_key(std::is_same_v<KeyMode, HitKey> ? key : DataItem(2));

  CHECK_OK(db->SetInDict(dict, key, DataItem(2.0f)));

  for (auto _ : state) {
    benchmark::DoNotOptimize(dict);
    benchmark::DoNotOptimize(db);
    auto res = db->GetFromDict(dict, lookup_key);
    benchmark::DoNotOptimize(res);
  }
  state.SetItemsProcessed(state.iterations());
}

void BM_GetFromDicts(benchmark::State& state) {
  int64_t alloc_size = state.range(0);

  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc = AllocateDicts(alloc_size);
  DataSliceImpl dicts = DataSliceImpl::ObjectsFromAllocation(alloc, alloc_size);
  auto keys = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<int>(alloc_size, 1));

  CHECK_OK(
      db->SetInDict(dicts, keys,
                    DataSliceImpl::Create(arolla::CreateConstDenseArray<float>(
                        alloc_size, 1.0f))));

  for (auto _ : state) {
    benchmark::DoNotOptimize(dicts);
    benchmark::DoNotOptimize(db);
    benchmark::DoNotOptimize(keys);

    auto res = db->GetFromDict(dicts, keys);
    benchmark::DoNotOptimize(res);
  }
  state.SetItemsProcessed(state.iterations() * alloc_size);
}

BENCHMARK(BM_GetFromDict<HitKey>);
BENCHMARK(BM_GetFromDict<MissKey>);
BENCHMARK(BM_GetFromDicts)->Arg(1)->Arg(10)->Arg(100)->Arg(1000);

void BM_SetInDict(benchmark::State& state) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  DataItem dict(AllocateSingleDict());
  DataItem key(1);
  DataItem value(2.0f);

  for (auto _ : state) {
    benchmark::DoNotOptimize(dict);
    benchmark::DoNotOptimize(db);
    CHECK_OK(db->SetInDict(dict, key, value));
    benchmark::DoNotOptimize(db);
  }
  state.SetItemsProcessed(state.iterations());
}

void BM_SetInDicts(benchmark::State& state) {
  int64_t alloc_size = state.range(0);

  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc = AllocateDicts(alloc_size);
  DataSliceImpl dicts = DataSliceImpl::ObjectsFromAllocation(alloc, alloc_size);
  auto keys = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<int>(alloc_size, 0));
  auto values = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<float>(alloc_size, 1.0f));

  for (auto _ : state) {
    benchmark::DoNotOptimize(dicts);
    benchmark::DoNotOptimize(db);
    benchmark::DoNotOptimize(keys);
    CHECK_OK(db->SetInDict(dicts, keys, values));
    benchmark::DoNotOptimize(db);
  }
  state.SetItemsProcessed(state.iterations() * alloc_size);
}

BENCHMARK(BM_SetInDict);
BENCHMARK(BM_SetInDicts)->Arg(1)->Arg(10)->Arg(100)->Arg(1000);

template <MergeOptions::ConflictHandlingOption kDataConflictPolicy>
void BM_MergeIntoEmpty(benchmark::State& state) {
  int64_t alloc_size = state.range(0);
  MergeOptions merge_options = {.data_conflict_policy = kDataConflictPolicy};

  auto db = DataBagImpl::CreateEmptyDatabag();

  {  // objects
    AllocationId alloc = Allocate(alloc_size);
    DataSliceImpl objects =
        DataSliceImpl::ObjectsFromAllocation(alloc, alloc_size);
    auto values = DataSliceImpl::Create(
        arolla::CreateConstDenseArray<float>(alloc_size, 1.0f));
    CHECK_OK(db->SetAttr(objects, "a", values));
  }
  {  // lists
    AllocationId alloc = AllocateLists(alloc_size);
    DataSliceImpl lists =
        DataSliceImpl::ObjectsFromAllocation(alloc, alloc_size);
    auto values = DataSliceImpl::Create(
        arolla::CreateConstDenseArray<float>(alloc_size, 1.0f));
    CHECK_OK(db->AppendToList(lists, values));
    CHECK_OK(db->AppendToList(lists, values));
  }
  {  // dicts
    AllocationId alloc = AllocateDicts(alloc_size);
    DataSliceImpl dicts =
        DataSliceImpl::ObjectsFromAllocation(alloc, alloc_size);
    auto zeroes = DataSliceImpl::Create(
        arolla::CreateConstDenseArray<int>(alloc_size, 0));
    auto ones = DataSliceImpl::Create(
        arolla::CreateConstDenseArray<int>(alloc_size, 1));
    CHECK_OK(db->SetInDict(dicts, zeroes, ones));
    CHECK_OK(db->SetInDict(dicts, ones, zeroes));
  }

  for (auto _ : state) {
    benchmark::DoNotOptimize(db);
    auto dbx = DataBagImpl::CreateEmptyDatabag();
    CHECK_OK(dbx->MergeInplace(*db, merge_options));
    benchmark::DoNotOptimize(dbx);
  }
  state.SetItemsProcessed(state.iterations() * alloc_size);
}

auto apply_merge_sizes = [](auto* b) {
  b->Arg(1)->Arg(10)->Arg(100)->Arg(1000);
};

BENCHMARK(BM_MergeIntoEmpty<MergeOptions::kRaiseOnConflict>)
    ->Apply(apply_merge_sizes);
BENCHMARK(BM_MergeIntoEmpty<MergeOptions::kOverwrite>)
    ->Apply(apply_merge_sizes);
BENCHMARK(BM_MergeIntoEmpty<MergeOptions::kKeepOriginal>)
    ->Apply(apply_merge_sizes);

template <typename OverwriteOrSet>
void BM_SetSchemaFields(benchmark::State& state) {
  int64_t alloc_size = state.range(0);
  int64_t attr_size = state.range(1);

  auto schema_slice =
      *CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          DataSliceImpl::AllocateEmptyObjects(alloc_size),
          schema::kImplicitSchemaSeed);

  std::vector<std::string> attr_names;
  attr_names.reserve(attr_size);
  for (int i = 0; i < attr_size; ++i) {
    attr_names.push_back(absl::StrCat("*********", i));
  }
  std::vector<absl::string_view> attr_name_views;
  attr_name_views.reserve(attr_size);
  for (int i = 0; i < attr_size; ++i) {
    attr_name_views.push_back(attr_names[i]);
  }
  std::vector<DataItem> attr_values;
  attr_values.reserve(attr_size);
  for (int i = 0; i < attr_size; ++i) {
    if (i & 1) {
      attr_values.push_back(DataItem(schema::kInt32));
    } else {
      attr_values.push_back(DataItem(schema::kObject));
    }
  }
  std::vector<std::reference_wrapper<const DataItem>> attr_refs;
  attr_refs.reserve(attr_size);
  for (int i = 0; i < attr_size; ++i) {
    attr_refs.push_back(std::cref(attr_values[i]));
  }

  auto db = DataBagImpl::CreateEmptyDatabag();
  for (auto _ : state) {
    benchmark::DoNotOptimize(db);
    benchmark::DoNotOptimize(schema_slice);
    benchmark::DoNotOptimize(attr_name_views);
    benchmark::DoNotOptimize(attr_refs);
    CHECK_OK(OverwriteOrSet()(db, schema_slice, attr_name_views, attr_refs));
  }
}

struct OverwriteFn {
  absl::Status operator()(
      DataBagImplPtr& db, const DataSliceImpl& schema_slice,
      const std::vector<absl::string_view>& attr_names,
      const std::vector<std::reference_wrapper<const DataItem>>& items) {
    return db->SetSchemaFields(schema_slice, attr_names, items);
  }
};

struct SetFn {
  absl::Status operator()(
      DataBagImplPtr& db, const DataSliceImpl& schema_slice,
      const std::vector<absl::string_view>& attr_names,
      const std::vector<std::reference_wrapper<const DataItem>>& items) {
    return db->SetSchemaFields(schema_slice, attr_names, items);
  }
};

BENCHMARK(BM_SetSchemaFields<SetFn>)
    ->ArgPair(10, 5)
    ->ArgPair(1000, 15)
    ->ArgPair(100000, 7)
    ->ArgPair(10000, 1000);

}  // namespace
}  // namespace koladata::internal
