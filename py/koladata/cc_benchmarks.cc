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
#include <tuple>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/strings/str_format.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/call.h"
#include "koladata/functor/functor.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/quote.h"
#include "arolla/io/tuple_input_loader.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"

namespace koladata {
namespace {

using ::arolla::CancellationContext;
using ::arolla::expr::CallOp;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;

void BM_Add(benchmark::State& state) {
  arolla::InitArolla();
  CancellationContext::ScopeGuard cancellation_scope;
  size_t slice_size = state.range(0);
  size_t num_operators = state.range(1);
  state.SetLabel(absl::StrFormat("slice_size=%d, num_operators=%d", slice_size,
                                 num_operators));

  std::vector<int> values(slice_size);
  for (int i = 0; i < slice_size; ++i) {
    values[i] = i;
  }

  auto db = DataBag::Empty();
  auto ds = DataSlice::CreateWithSchemaFromData(
                internal::DataSliceImpl::Create(
                    arolla::CreateFullDenseArray<int>(values)),
                DataSlice::JaggedShape::FlatFromSize(slice_size), db)
                .value();

  auto expr = Leaf("x");
  for (size_t i = 0; i < num_operators; ++i) {
    expr = CallOp("kd.add", {expr, Leaf("x")}).value();
  }

  using Inputs = std::tuple<DataSlice>;
  auto fn = arolla::ExprCompiler<Inputs, DataSlice>()
                .SetInputLoader(arolla::TupleInputLoader<Inputs>::Create({"x"}))
                .Compile(expr)
                .value();

  Inputs inputs = {ds};
  auto result = fn(inputs);  // Preheat caches;
  benchmark::DoNotOptimize(result);

  for (auto s : state) {
    benchmark::DoNotOptimize(inputs);
    auto result = fn(inputs);
    benchmark::DoNotOptimize(result);
  }

  state.SetItemsProcessed(slice_size * num_operators * state.iterations());
}

BENCHMARK(BM_Add)
    // {slice size, number of operators}
    ->Args({1, 1})
    ->Args({1, 100})
    ->Args({10000, 1})
    ->Args({10000, 100})
    ->ThreadRange(1, 16);

void BM_AggSum(benchmark::State& state) {
  arolla::InitArolla();
  CancellationContext::ScopeGuard cancellation_scope;
  const int nrows = state.range(0);
  const int ncols = state.range(1);
  const int ndim = state.range(2);
  state.SetLabel(
      absl::StrFormat("nrows=%d, ncols=%d, ndim=%d", nrows, ncols, ndim));

  auto shape = *DataSlice::JaggedShape::FromEdges(
      {*DataSlice::JaggedShape::Edge::FromUniformGroups(1, nrows),
       *DataSlice::JaggedShape::Edge::FromUniformGroups(nrows, ncols)});
  auto x_ds = *DataSlice::CreateWithSchemaFromData(
      internal::DataSliceImpl::Create(
          arolla::CreateConstDenseArray<int>(nrows * ncols, 1)),
      shape);
  auto ndim_ds = *DataSlice::Create(internal::DataItem(ndim),
                                    internal::DataItem(schema::kInt32));
  auto expr = *CallOp("kd.agg_sum", {Leaf("x"), Leaf("ndim")});

  using Inputs = std::tuple<DataSlice, DataSlice>;
  auto fn = arolla::ExprCompiler<Inputs, DataSlice>()
                .SetInputLoader(
                    arolla::TupleInputLoader<Inputs>::Create({"x", "ndim"}))
                .Compile(expr)
                .value();
  Inputs inputs = {x_ds, ndim_ds};
  auto result = fn(inputs);  // Preheat caches;

  for (auto s : state) {
    benchmark::DoNotOptimize(inputs);
    auto result = fn(inputs);
    benchmark::DoNotOptimize(result);
  }
}

BENCHMARK(BM_AggSum)
    // {nrows, ncols, ndim}
    ->Args({10, 10, 1})
    ->Args({10, 1000, 1})
    ->Args({1000, 10, 1})
    ->Args({1000, 1000, 1})
    ->Args({10, 10, 2})
    ->Args({10, 1000, 2})
    ->Args({1000, 10, 2})
    ->Args({1000, 1000, 2});

void BM_Equal_Int32_Int64(benchmark::State& state) {
  arolla::InitArolla();
  CancellationContext::ScopeGuard cancellation_scope;
  size_t slice_size = state.range(0);
  state.SetLabel(absl::StrFormat("slice_size=%d", slice_size));

  std::vector<int32_t> int32_values(slice_size);
  std::vector<int64_t> int64_values(slice_size);
  for (int i = 0; i < slice_size; ++i) {
    int32_values[i] = i;
    int64_values[i] = i;
  }

  auto db = DataBag::Empty();
  auto int32_ds = DataSlice::CreateWithSchemaFromData(
                      internal::DataSliceImpl::Create(
                          arolla::CreateFullDenseArray<int>(int32_values)),
                      DataSlice::JaggedShape::FlatFromSize(slice_size), db)
                      .value();
  auto int64_ds = DataSlice::CreateWithSchemaFromData(
                      internal::DataSliceImpl::Create(
                          arolla::CreateFullDenseArray<int64_t>(int64_values)),
                      DataSlice::JaggedShape::FlatFromSize(slice_size), db)
                      .value();

  auto expr = CallOp("kd.equal", {Leaf("x"), Leaf("y")}).value();

  using Inputs = std::tuple<DataSlice, DataSlice>;
  auto fn =
      arolla::ExprCompiler<Inputs, DataSlice>()
          .SetInputLoader(arolla::TupleInputLoader<Inputs>::Create({"x", "y"}))
          .Compile(expr)
          .value();

  Inputs inputs = {int32_ds, int64_ds};
  auto result = fn(inputs);  // Preheat caches;
  benchmark::DoNotOptimize(result);

  for (auto s : state) {
    benchmark::DoNotOptimize(inputs);
    auto result = fn(inputs);
    benchmark::DoNotOptimize(result);
  }

  state.SetItemsProcessed(slice_size * state.iterations());
}

// {slice size}
BENCHMARK(BM_Equal_Int32_Int64)->Range(1, 1 << 20);

template <typename X, typename Y>
void BM_Coalesce(benchmark::State& state) {
  arolla::InitArolla();
  CancellationContext::ScopeGuard cancellation_scope;
  size_t slice_size = state.range(0);
  state.SetLabel(absl::StrFormat("slice_size=%d", slice_size));

  std::vector<arolla::OptionalValue<X>> values_x(slice_size, std::nullopt);
  std::vector<arolla::OptionalValue<Y>> values_y(slice_size, std::nullopt);
  // Populate every other value in an alternating fashion.
  for (int i = 0; i < slice_size; ++i) {
    if (i % 2 == 0) {
      values_x[i] = i;
    } else {
      values_y[i] = i;
    }
  }
  auto db = DataBag::Empty();
  auto ds_x =
      DataSlice::Create(internal::DataSliceImpl::Create(
                            arolla::CreateDenseArray<X>(values_x)),
                        DataSlice::JaggedShape::FlatFromSize(slice_size),
                        internal::DataItem(schema::GetDType<X>()), db)
          .value();
  auto ds_y =
      DataSlice::Create(internal::DataSliceImpl::Create(
                            arolla::CreateDenseArray<Y>(values_y)),
                        DataSlice::JaggedShape::FlatFromSize(slice_size),
                        internal::DataItem(schema::GetDType<Y>()), db)
          .value();

  auto expr = CallOp("kd.coalesce", {Leaf("x"), Leaf("y")}).value();

  using Inputs = std::tuple<DataSlice, DataSlice>;
  auto fn =
      arolla::ExprCompiler<Inputs, DataSlice>()
          .SetInputLoader(arolla::TupleInputLoader<Inputs>::Create({"x", "y"}))
          .Compile(expr)
          .value();

  Inputs inputs = {ds_x, ds_y};
  auto result = fn(inputs);  // Preheat caches;
  benchmark::DoNotOptimize(result);

  for (auto s : state) {
    benchmark::DoNotOptimize(inputs);
    auto result = fn(inputs);
    benchmark::DoNotOptimize(result);
  }

  state.SetItemsProcessed(slice_size * state.iterations());
}

// {slice size}
// No casting.
BENCHMARK(BM_Coalesce<int32_t, int32_t>)->Range(1, 1 << 20);
// With casting.
BENCHMARK(BM_Coalesce<int32_t, float>)->Range(1, 1 << 20);

void BM_AddViaFunctor(benchmark::State& state) {
  arolla::InitArolla();
  CancellationContext::ScopeGuard cancellation_scope;
  size_t slice_size = state.range(0);
  size_t num_operators = state.range(1);
  state.SetLabel(absl::StrFormat("slice_size=%d, num_operators=%d", slice_size,
                                 num_operators));

  std::vector<int> values(slice_size);
  for (int i = 0; i < slice_size; ++i) {
    values[i] = i;
  }

  auto db = DataBag::Empty();
  auto ds = DataSlice::CreateWithSchemaFromData(
                internal::DataSliceImpl::Create(
                    arolla::CreateFullDenseArray<int>(values)),
                DataSlice::JaggedShape::FlatFromSize(slice_size), db)
                .value();

  auto input = CallOp("koda_internal.input", {Literal(arolla::Text("I")),
                                              Literal(arolla::Text("self"))})
                   .value();
  auto expr = input;
  for (size_t i = 0; i < num_operators; ++i) {
    expr = CallOp("kd.add", {expr, input}).value();
  }
  auto expr_slice =
      DataSlice::Create(internal::DataItem(arolla::expr::ExprQuote(expr)),
                        internal::DataItem(schema::kExpr))
          .value();
  auto functor = functor::CreateFunctor(expr_slice, std::nullopt, {}).value();

  auto fn = [&functor](const auto& ds) {
    return functor::CallFunctorWithCompilationCache(
        functor, {arolla::TypedRef::FromValue(ds)}, {});
  };

  {
    auto result = fn(ds);  // Preheat caches;
    benchmark::DoNotOptimize(result);
  }

  for (auto s : state) {
    benchmark::DoNotOptimize(ds);
    auto result = fn(ds);
    benchmark::DoNotOptimize(result);
  }

  state.SetItemsProcessed(slice_size * num_operators * state.iterations());
}

BENCHMARK(BM_AddViaFunctor)
    // {slice size, number of operators}
    ->Args({1, 1})
    ->Args({1, 100})
    ->Args({10000, 1})
    ->Args({10000, 100})
    ->ThreadRange(1, 16);

}  // namespace
}  // namespace koladata
