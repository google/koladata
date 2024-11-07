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
#include <optional>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/object_factories.h"
#include "koladata/shape_utils.h"
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

void BM_IsEquivalentToCopy(benchmark::State& state) {
  int64_t size = state.range(0);
  auto values = arolla::CreateFullDenseArray(std::vector<int>(size, 12));
  auto ds = *DataSlice::CreateWithSchemaFromData(
      DataSliceImpl::Create(values),
      DataSlice::JaggedShape::FlatFromSize(size));
  auto ds2 = ds;
  for (auto _ : state) {
    benchmark::DoNotOptimize(ds);
    benchmark::DoNotOptimize(ds2);
    auto equiv = ds.IsEquivalentTo(ds2);
    benchmark::DoNotOptimize(equiv);
  }
}

BENCHMARK(BM_IsEquivalentToCopy)->Range(10, 100000);

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
  auto ds =
      *DataSlice::Create(internal::DataSliceImpl::AllocateEmptyObjects(size),
                         DataSlice::JaggedShape::FlatFromSize(size),
                         internal::DataItem(schema::kAny));
  auto item =
      *DataSlice::Create(internal::DataItem(internal::AllocateSingleObject()),
                         internal::DataItem(schema::kAny));
  std::vector<DataSlice> inputs({ds, item});
  for (auto _ : state) {
    benchmark::DoNotOptimize(inputs);
    auto aligned_inputs = shape::Align(inputs);
    benchmark::DoNotOptimize(aligned_inputs);
  }
}

BENCHMARK(BM_Align)->Arg(0)->Arg(1000)->Arg(100000);

template <typename ObjectFactory>
void BM_SetGetAttrItem(benchmark::State& state) {
  auto db = DataBag::Empty();
  auto o = *ObjectFactory::FromAttrs(db, {}, {});
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
  auto o = *ObjectFactory::Shaped(
      db, DataSlice::JaggedShape::FlatFromSize(1), {}, {});

  internal::SliceBuilder bldr_val(1);
  bldr_val.InsertIfNotSet(0, internal::DataItem(12));
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
  auto o = *ObjectFactory::Shaped(
      db, DataSlice::JaggedShape::FlatFromSize(10000), {}, {});

  auto val =
      *DataSlice::Create(internal::DataSliceImpl::Create(
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

void BM_SetGetAttrMultiDimObjectSchema(benchmark::State& state) {
  auto db = DataBag::Empty();
  auto val =
      *DataSlice::Create(internal::DataSliceImpl::Create(
                             arolla::CreateConstDenseArray<int>(10000, 12)),
                         DataSlice::JaggedShape::FlatFromSize(10000),
                         internal::DataItem(schema::kInt32));
  // Schema ids share the same Dict in the background.
  auto o = *ObjectCreator::FromAttrs(db, {"abc"}, {val});

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

BENCHMARK(BM_SetGetAttrMultiDimObjectSchema);

void BM_GetEmbeddedSchemaAttr(benchmark::State& state) {
  auto db = DataBag::Empty();
  auto val =
      *DataSlice::Create(internal::DataSliceImpl::Create(
                             arolla::CreateConstDenseArray<int>(10000, 12)),
                         DataSlice::JaggedShape::FlatFromSize(10000),
                         internal::DataItem(schema::kInt32));
  auto o = *EntityCreator::FromAttrs(db, {"abc"}, {val});
  o = *o.EmbedSchema();

  // If anything needs to be initialized.
  auto missing = o.GetAttr("missing");
  benchmark::DoNotOptimize(missing);
  for (auto _ : state) {
    benchmark::DoNotOptimize(o);
    benchmark::DoNotOptimize(val);
    auto ds = o.GetAttr("abc");
    benchmark::DoNotOptimize(ds);
  }
}

BENCHMARK(BM_GetEmbeddedSchemaAttr);

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
  auto o = *EntityCreator::Shaped(
      db, *DataSlice::JaggedShape::FromEdges({edge_1, edge_2}), {}, {});

  auto list = *CreateListsFromLastDimension(db, o, /*schema=*/std::nullopt,
                                            test::Schema(schema::kAny));

  for (auto _ : state) {
    benchmark::DoNotOptimize(list);
    auto items_or = list.ExplodeList(0, std::nullopt);
    benchmark::DoNotOptimize(items_or);
  }
}

BENCHMARK(BM_ExplodeLists)
    ->ArgPair(5, 5)
    ->ArgPair(100, 100)
    ->ArgPair(10, 10000);

void BM_GetFromList(benchmark::State& state) {
  bool should_broadcast_index = state.range(0);
  int64_t first_dim = state.range(1);
  int64_t second_dim = state.range(2);
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
  auto o = *EntityCreator::Shaped(
      db, *DataSlice::JaggedShape::FromEdges({edge_1, edge_2}), {}, {});

  auto list = *CreateListsFromLastDimension(db, o, test::Schema(schema::kAny));
  auto index = *DataSlice::Create(internal::DataItem(int64_t{0}),
                                  internal::DataItem(schema::kInt64));
  if (should_broadcast_index) {
    index = *BroadcastToShape(index, list.GetShape());
  }
  for (auto _ : state) {
    benchmark::DoNotOptimize(list);
    benchmark::DoNotOptimize(index);
    auto res = list.GetFromList(index);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_GetFromList)
    // should_broadcast_index, first_dim, second_dim
    ->Args({0, 1, 1})
    ->Args({0, 100, 100})
    ->Args({0, 10, 10000})
    ->Args({1, 1, 1})
    ->Args({1, 100, 100})
    ->Args({1, 10, 10000});

void BM_SetMultipleAttrs(benchmark::State& state) {
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

  auto entity = *EntityCreator::Shaped(db, a.GetShape(), {}, {});

  std::vector<absl::string_view> attr_names{"a", "b", "c"};
  std::vector<DataSlice> values{a, b, c};

  for (auto _ : state) {
    benchmark::DoNotOptimize(db);
    benchmark::DoNotOptimize(attr_names);
    benchmark::DoNotOptimize(values);
    CHECK_OK(entity.SetAttrs(attr_names, values, /*update_schema=*/true));
    benchmark::DoNotOptimize(entity);
  }
}

BENCHMARK(BM_SetMultipleAttrs)->Arg(1)->Arg(10)->Arg(10000);

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

  std::vector<absl::string_view> attr_names{"a", "b", "c"};
  std::vector<DataSlice> values{a, b, c};

  for (auto _ : state) {
    benchmark::DoNotOptimize(db);
    benchmark::DoNotOptimize(attr_names);
    benchmark::DoNotOptimize(values);
    auto entity_or = EntityCreator::FromAttrs(db, attr_names, values);
    CHECK_OK(entity_or);
    benchmark::DoNotOptimize(entity_or);
  }
}

BENCHMARK(BM_CreateEntity)->Arg(1)->Arg(10)->Arg(10000);

void BM_CreateUuEntity(benchmark::State& state) {
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

  std::vector<absl::string_view> attr_names{"a", "b", "c"};
  std::vector<DataSlice> values{a, b, c};

  for (auto _ : state) {
    benchmark::DoNotOptimize(db);
    benchmark::DoNotOptimize(attr_names);
    benchmark::DoNotOptimize(values);
    auto entity_or = CreateUu(db, "", attr_names, values, schema, false);
    CHECK_OK(entity_or);
    benchmark::DoNotOptimize(entity_or);
  }
}

BENCHMARK(BM_CreateUuEntity)->Arg(1)->Arg(10)->Arg(10000);

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

  std::vector<absl::string_view> attr_names{"a", "b", "c"};
  std::vector<DataSlice> values{a, b, c};

  for (auto _ : state) {
    benchmark::DoNotOptimize(db);
    benchmark::DoNotOptimize(attr_names);
    benchmark::DoNotOptimize(values);
    benchmark::DoNotOptimize(schema);
    auto entity_or = EntityCreator::FromAttrs(db, attr_names, values, schema);
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

  std::vector<absl::string_view> attr_names{"a", "b", "c"};
  std::vector<DataSlice> values{a, b, c};

  for (auto _ : state) {
    benchmark::DoNotOptimize(db);
    benchmark::DoNotOptimize(attr_names);
    benchmark::DoNotOptimize(values);
    benchmark::DoNotOptimize(schema);
    auto entity_or = EntityCreator::FromAttrs(db, attr_names, values, schema);
    CHECK_OK(entity_or);
    benchmark::DoNotOptimize(entity_or);
  }
}

BENCHMARK(BM_CreateEntityWithSchemaAndCasting)->Arg(1)->Arg(10)->Arg(10000);


void BM_ToInt32_Int32Data_AnySchema(benchmark::State& state) {
  int64_t size = state.range(0);
  auto values = arolla::CreateFullDenseArray(std::vector<int>(size, 12));
  auto ds = *DataSlice::Create(DataSliceImpl::Create(values),
                               DataSlice::JaggedShape::FlatFromSize(size),
                               internal::DataItem(schema::kAny));
  for (auto _ : state) {
    benchmark::DoNotOptimize(ds);
    auto res = *ToInt32(ds);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_ToInt32_Int32Data_AnySchema)->Arg(1)->Arg(10)->Arg(10000);

void BM_ToInt32_Float32Data_AnySchema(benchmark::State& state) {
  int64_t size = state.range(0);
  auto values = arolla::CreateFullDenseArray(std::vector<float>(size, 12.0));
  auto ds = *DataSlice::Create(DataSliceImpl::Create(values),
                               DataSlice::JaggedShape::FlatFromSize(size),
                               internal::DataItem(schema::kAny));
  for (auto _ : state) {
    benchmark::DoNotOptimize(ds);
    auto res = *ToInt32(ds);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_ToInt32_Float32Data_AnySchema)->Arg(1)->Arg(10)->Arg(10000);

}  // namespace
}  // namespace koladata
