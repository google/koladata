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
#include <vector>

#include "benchmark/benchmark.h"
#include "gmock/gmock.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/meta.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"

namespace koladata::schema {
namespace {

void BM_CommonSchema_SameSchema(benchmark::State& state) {
  const int n = state.range(0);
  auto schemas = internal::DataSliceImpl::Create(
      arolla::CreateConstDenseArray<schema::DType>(n, schema::kFloat32));
  for (auto _ : state) {
    benchmark::DoNotOptimize(schemas);
    auto res = CommonSchema(schemas);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_CommonSchema_SameSchema)->Range(1, 1 << 16);

void BM_CommonSchema_DifferentPrimitiveSchemas(benchmark::State& state) {
  const int n = state.range(0);
  std::vector<schema::DType> primitive_schemas;
  arolla::meta::foreach_type(
      schema::supported_primitive_dtypes(), [&](auto tpe) {
        using T = typename decltype(tpe)::type;
        primitive_schemas.push_back(schema::GetDType<T>());
      });
  std::vector<schema::DType> schemas;
  schemas.reserve(n);
  for (int i = 0; i < n; ++i) {
    schemas.push_back(primitive_schemas[i % primitive_schemas.size()]);
  }
  auto schema_slice = internal::DataSliceImpl::Create(
      arolla::CreateFullDenseArray<schema::DType>(schemas.begin(),
                                                  schemas.end()));
  ASSERT_OK(CommonSchema(schema_slice).status());
  for (auto _ : state) {
    benchmark::DoNotOptimize(schema_slice);
    auto res = CommonSchema(schema_slice);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_CommonSchema_DifferentPrimitiveSchemas)->Range(1, 1 << 16);

void BM_CommonSchema_Binary_DTypeDType_AsDTypes(benchmark::State& state) {
  DType lhs = kInt32;
  DType rhs = kFloat32;
  for (auto _ : state) {
    benchmark::DoNotOptimize(lhs);
    benchmark::DoNotOptimize(rhs);
    auto res = CommonSchema(lhs, rhs);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_CommonSchema_Binary_DTypeDType_AsDTypes);

void BM_CommonSchema_Binary_DTypeDType_AsSchemas(benchmark::State& state) {
  internal::DataItem lhs(kInt32);
  internal::DataItem rhs(kFloat32);
  for (auto _ : state) {
    benchmark::DoNotOptimize(lhs);
    benchmark::DoNotOptimize(rhs);
    auto res = CommonSchema(lhs, rhs);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_CommonSchema_Binary_DTypeDType_AsSchemas);

void BM_CommonSchema_Binary_SchemaDType(benchmark::State& state) {
  internal::DataItem lhs(internal::AllocateExplicitSchema());
  internal::DataItem rhs(kNone);
  for (auto _ : state) {
    benchmark::DoNotOptimize(lhs);
    benchmark::DoNotOptimize(rhs);
    auto res = CommonSchema(lhs, rhs);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_CommonSchema_Binary_SchemaDType);

void BM_CommonSchema_Binary_SchemaSchema(benchmark::State& state) {
  internal::DataItem schema(internal::AllocateExplicitSchema());
  for (auto _ : state) {
    benchmark::DoNotOptimize(schema);
    auto res = CommonSchema(schema, schema);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_CommonSchema_Binary_SchemaSchema);

void BM_IsImplicitlyCastableTo_DTypeDType_AsSchemas(benchmark::State& state) {
  internal::DataItem lhs(kInt32);
  internal::DataItem rhs(kFloat32);
  for (auto _ : state) {
    benchmark::DoNotOptimize(lhs);
    benchmark::DoNotOptimize(rhs);
    auto res = IsImplicitlyCastableTo(lhs, rhs);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_IsImplicitlyCastableTo_DTypeDType_AsSchemas);

void BM_IsImplicitlyCastableTo_SchemaDType(benchmark::State& state) {
  internal::DataItem lhs(internal::AllocateExplicitSchema());
  internal::DataItem rhs(kNone);
  for (auto _ : state) {
    benchmark::DoNotOptimize(lhs);
    benchmark::DoNotOptimize(rhs);
    auto res = IsImplicitlyCastableTo(lhs, rhs);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_IsImplicitlyCastableTo_SchemaDType);

void BM_IsImplicitlyCastableTo_SchemaSchema(benchmark::State& state) {
  internal::DataItem schema(internal::AllocateExplicitSchema());
  for (auto _ : state) {
    benchmark::DoNotOptimize(schema);
    auto res = IsImplicitlyCastableTo(schema, schema);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_IsImplicitlyCastableTo_SchemaSchema);

}  // namespace
}  // namespace koladata::schema
