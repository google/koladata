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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/object_factories.h"
#include "koladata/shape/shape_utils.h"
#include "koladata/test_utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"

namespace koladata {
namespace {

using internal::DataSliceImpl;

void BM_IsEquivalentToSameImpl(benchmark::State& state) {
  int64_t size = state.range(0);
  auto values = arolla::CreateFullDenseArray(std::vector<int>(size, 12));
  auto ds = *DataSlice::CreateWithSchemaFromData(
      DataSliceImpl::Create(values),
      DataSlice::JaggedShape::FlatFromSize(size));
  for (auto _ : state) {
    benchmark::DoNotOptimize(ds);
    auto equiv = ds.IsEquivalentTo(ds);
    benchmark::DoNotOptimize(equiv);
  }
}

BENCHMARK(BM_IsEquivalentToSameImpl)->Range(10, 100000);

void BM_IsEquivalentToSameJaggedShape(benchmark::State& state) {
  int64_t size = state.range(0);
  auto values = arolla::CreateFullDenseArray(std::vector<int>(size, 12));
  auto shape = DataSlice::JaggedShape::FlatFromSize(size);
  auto ds_a = *DataSlice::CreateWithSchemaFromData(
      DataSliceImpl::Create(values), shape);
  auto ds_b = *DataSlice::CreateWithSchemaFromData(
      DataSliceImpl::Create(values), shape);
  for (auto _ : state) {
    benchmark::DoNotOptimize(ds_a);
    benchmark::DoNotOptimize(ds_b);
    auto equiv = ds_a.IsEquivalentTo(ds_b);
    benchmark::DoNotOptimize(equiv);
  }
}

BENCHMARK(BM_IsEquivalentToSameJaggedShape)->Range(10, 100000);

void BM_Align(benchmark::State& state) {
  size_t size = state.range(0);
  auto ds = *DataSlice::Create(
      internal::DataSliceImpl::AllocateEmptyObjects(size),
      DataSlice::JaggedShape::FlatFromSize(size),
      internal::DataItem(schema::kAny));
  auto item = *DataSlice::Create(
      internal::DataItem(internal::AllocateSingleObject()),
      internal::DataItem(schema::kAny));
  std::vector<std::reference_wrapper<const DataSlice>> inputs({
      std::cref(ds), std::cref(item)});
  for (auto _ : state) {
    benchmark::DoNotOptimize(inputs);
    auto aligned_inputs = shape::Align<DataSlice>(inputs);
    benchmark::DoNotOptimize(aligned_inputs);
  }
}

BENCHMARK(BM_Align)->Arg(0)->Arg(1000)->Arg(100000);

template <typename ObjectFactory>
void BM_SetGetAttrItem(benchmark::State& state) {
  auto db = DataBag::Empty();
  auto o = *ObjectFactory()(db, DataSlice::JaggedShape::Empty());
  auto val = *DataSlice::Create(internal::DataItem(12),
                                internal::DataItem(schema::kInt32));
  if constexpr (std::is_same_v<ObjectFactory, EntityCreator>) {
    auto status = o.GetSchema().SetAttr("abc", val.GetSchema());
    CHECK_OK(status);
  }
  // If anything needs to be initialized.
  auto missing = o.GetAttr("missing");
  benchmark::DoNotOptimize(missing);
  for (auto _ : state) {
    benchmark::DoNotOptimize(o);
    benchmark::DoNotOptimize(val);
    auto status = o.SetAttr("abc", val);
    benchmark::DoNotOptimize(status);
    auto ds = o.GetAttr("abc");
    benchmark::DoNotOptimize(ds);
  }
}

template <typename ObjectFactory>
void BM_SetGetAttrOneDimSingle(benchmark::State& state) {
  auto db = DataBag::Empty();
  auto o = *ObjectFactory()(db, DataSlice::JaggedShape::FlatFromSize(1));

  internal::DataSliceImpl::Builder bldr_val(1);
  bldr_val.Insert(0, internal::DataItem(12));
  auto val = *DataSlice::Create(std::move(bldr_val).Build(),
                                DataSlice::JaggedShape::FlatFromSize(1),
                                internal::DataItem(schema::kInt32));
  if constexpr (std::is_same_v<ObjectFactory, EntityCreator>) {
    auto status = o.GetSchema().SetAttr("abc", val.GetSchema());
    CHECK_OK(status);
  }

  // If anything needs to be initialized.
  auto missing = o.GetAttr("missing");
  benchmark::DoNotOptimize(missing);
  for (auto _ : state) {
    benchmark::DoNotOptimize(o);
    benchmark::DoNotOptimize(val);
    auto status = o.SetAttr("abc", val);
    benchmark::DoNotOptimize(status);
    auto ds = o.GetAttr("abc");
    benchmark::DoNotOptimize(ds);
  }
}

template <typename ObjectFactory>
void BM_SetGetAttrMultiDim(benchmark::State& state) {
  auto db = DataBag::Empty();
  auto o = *ObjectFactory()(db, DataSlice::JaggedShape::FlatFromSize(10000));

  auto val = *DataSlice::Create(
      internal::DataSliceImpl::Create(
          arolla::CreateConstDenseArray<int>(10000, 12)),
      DataSlice::JaggedShape::FlatFromSize(10000),
      internal::DataItem(schema::kInt32));
  if constexpr (std::is_same_v<ObjectFactory, EntityCreator>) {
    auto status = o.GetSchema().SetAttr("abc", val.GetSchema());
    CHECK_OK(status);
  }

  // If anything needs to be initialized.
  auto missing = o.GetAttr("missing");
  benchmark::DoNotOptimize(missing);
  for (auto _ : state) {
    benchmark::DoNotOptimize(o);
    benchmark::DoNotOptimize(val);
    auto status = o.SetAttr("abc", val);
    benchmark::DoNotOptimize(status);
    auto ds = o.GetAttr("abc");
    benchmark::DoNotOptimize(ds);
  }
}

BENCHMARK(BM_SetGetAttrItem<EntityCreator>);
BENCHMARK(BM_SetGetAttrOneDimSingle<EntityCreator>);
BENCHMARK(BM_SetGetAttrMultiDim<EntityCreator>);

BENCHMARK(BM_SetGetAttrItem<ObjectCreator>);
BENCHMARK(BM_SetGetAttrOneDimSingle<ObjectCreator>);
BENCHMARK(BM_SetGetAttrMultiDim<ObjectCreator>);

void BM_ExplodeLists(benchmark::State& state) {
  int64_t first_dim = state.range(0);
  int64_t second_dim = state.range(1);
  auto db = DataBag::Empty();
  auto edge_1 = *DataSlice::JaggedShape::Edge::FromSplitPoints(
      arolla::CreateDenseArray<int64_t>({0, first_dim}));
  std::vector<arolla::OptionalValue<int64_t>> split_points_2;
  split_points_2.reserve(first_dim + 1);
  for (int64_t cur_sp = 0, i = 0; i <= first_dim; ++i, cur_sp += second_dim) {
    split_points_2.push_back(cur_sp);
  }
  auto edge_2 = *DataSlice::JaggedShape::Edge::FromSplitPoints(
      arolla::CreateDenseArray<int64_t>(split_points_2));
  auto o = *EntityCreator()(
      db, *DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));

  auto list = *CreateListsFromLastDimension(db, o, test::Schema(schema::kAny));

  for (auto _ : state) {
    benchmark::DoNotOptimize(list);
    auto items_or = list.ExplodeList(0, std::nullopt);
    benchmark::DoNotOptimize(items_or);
  }
}

BENCHMARK(BM_ExplodeLists)
  ->ArgPair(5, 5)->ArgPair(100, 100)->ArgPair(10, 10000);

void BM_CreateEntity(benchmark::State& state) {
  int64_t size = state.range(0);
  auto db = DataBag::Empty();
  auto a_values = arolla::CreateFullDenseArray(std::vector<int>(size, 12));
  auto a = *DataSlice::CreateWithSchemaFromData(
      DataSliceImpl::Create(a_values),
      DataSlice::JaggedShape::FlatFromSize(size));
  auto b_values = arolla::CreateFullDenseArray(std::vector<float>(size, 3.14));
  auto b = *DataSlice::CreateWithSchemaFromData(
      DataSliceImpl::Create(b_values),
      DataSlice::JaggedShape::FlatFromSize(size));
  auto c_values = arolla::CreateFullDenseArray(
      std::vector<int64_t>(size, 1l << 43));
  auto c = *DataSlice::CreateWithSchemaFromData(
      DataSliceImpl::Create(c_values),
      DataSlice::JaggedShape::FlatFromSize(size));

  for (auto _ : state) {
    benchmark::DoNotOptimize(db);
    benchmark::DoNotOptimize(a);
    benchmark::DoNotOptimize(b);
    benchmark::DoNotOptimize(c);
    auto entity_or = EntityCreator()(db, {"a", "b", "c"}, {a, b, c});
    CHECK_OK(entity_or);
    benchmark::DoNotOptimize(entity_or);
  }
}

BENCHMARK(BM_CreateEntity)->Arg(1)->Arg(10)->Arg(10000);

void BM_CreateEntityWithSchema(benchmark::State& state) {
  int64_t size = state.range(0);
  auto db = DataBag::Empty();
  auto a_values = arolla::CreateFullDenseArray(std::vector<int>(size, 12));
  auto a = *DataSlice::CreateWithSchemaFromData(
      DataSliceImpl::Create(a_values),
      DataSlice::JaggedShape::FlatFromSize(size));
  auto b_values = arolla::CreateFullDenseArray(std::vector<float>(size, 3.14));
  auto b = *DataSlice::CreateWithSchemaFromData(
      DataSliceImpl::Create(b_values),
      DataSlice::JaggedShape::FlatFromSize(size));
  auto c_values = arolla::CreateFullDenseArray(
      std::vector<int64_t>(size, 1l << 43));
  auto c = *DataSlice::CreateWithSchemaFromData(
      DataSliceImpl::Create(c_values),
      DataSlice::JaggedShape::FlatFromSize(size));

  auto schema_db = DataBag::Empty();
  std::optional<DataSlice> schema = *CreateEntitySchema(
      schema_db, {"a", "b", "c"},
      {test::Schema(schema::kInt32), test::Schema(schema::kFloat32),
       test::Schema(schema::kInt64)});

  for (auto _ : state) {
    benchmark::DoNotOptimize(db);
    benchmark::DoNotOptimize(a);
    benchmark::DoNotOptimize(b);
    benchmark::DoNotOptimize(c);
    auto entity_or = EntityCreator()(db, {"a", "b", "c"}, {a, b, c}, schema);
    CHECK_OK(entity_or);
    benchmark::DoNotOptimize(entity_or);
  }
}

BENCHMARK(BM_CreateEntityWithSchema)->Arg(1)->Arg(10)->Arg(10000);

void BM_CreateEntityWithSchemaAndCasting(benchmark::State& state) {
  int64_t size = state.range(0);
  auto db = DataBag::Empty();
  auto a_values = arolla::CreateFullDenseArray(std::vector<int>(size, 12));
  auto a = *DataSlice::CreateWithSchemaFromData(
      DataSliceImpl::Create(a_values),
      DataSlice::JaggedShape::FlatFromSize(size));
  auto b_values = arolla::CreateFullDenseArray(std::vector<float>(size, 3.14));
  auto b = *DataSlice::CreateWithSchemaFromData(
      DataSliceImpl::Create(b_values),
      DataSlice::JaggedShape::FlatFromSize(size));
  auto c_values = arolla::CreateFullDenseArray(
      std::vector<int64_t>(size, 1l << 43));
  auto c = *DataSlice::CreateWithSchemaFromData(
      DataSliceImpl::Create(c_values),
      DataSlice::JaggedShape::FlatFromSize(size));

  auto schema_db = DataBag::Empty();
  std::optional<DataSlice> schema = *CreateEntitySchema(
      schema_db, {"a", "b", "c"},
      {test::Schema(schema::kInt64), test::Schema(schema::kFloat64),
       test::Schema(schema::kFloat32)});

  for (auto _ : state) {
    benchmark::DoNotOptimize(db);
    benchmark::DoNotOptimize(a);
    benchmark::DoNotOptimize(b);
    benchmark::DoNotOptimize(c);
    auto entity_or = EntityCreator()(db, {"a", "b", "c"}, {a, b, c}, schema);
    CHECK_OK(entity_or);
    benchmark::DoNotOptimize(entity_or);
  }
}

BENCHMARK(BM_CreateEntityWithSchemaAndCasting)->Arg(1)->Arg(10)->Arg(10000);

}  // namespace
}  // namespace koladata
