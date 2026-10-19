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
#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/quote.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/signature_utils.h"
#include "koladata/functor_storage.h"
#include "koladata/object_factories.h"
#include "koladata/signature.h"
#include "koladata/test_utils.h"

namespace koladata::functor {
namespace {

DataSlice MakeFunctor() {
  auto returns =
      test::DataItem(arolla::expr::ExprQuote(arolla::expr::Literal(57)));
  auto signature = *Signature::Create({});
  auto koda_signature = *CppSignatureToKodaSignature(signature);
  return *CreateFunctor(returns, koda_signature, {}, {});
}

void BM_IsFunctor_ImmutableFunctor(benchmark::State& state) {
  auto fn = MakeFunctor();
  CHECK(!fn.GetBag()->IsMutable());
  benchmark::DoNotOptimize(fn);
  for (auto _ : state) {
    auto is_functor = IsFunctor(fn);
    benchmark::DoNotOptimize(is_functor);
  }
}

BENCHMARK(BM_IsFunctor_ImmutableFunctor);

void BM_IsFunctor_MutableFunctor(benchmark::State& state) {
  auto fn = *MakeFunctor().ForkBag();
  CHECK(fn.GetBag()->IsMutable());
  benchmark::DoNotOptimize(fn);
  for (auto _ : state) {
    auto is_functor = IsFunctor(fn);
    benchmark::DoNotOptimize(is_functor);
  }
}

BENCHMARK(BM_IsFunctor_MutableFunctor);

void BM_IsFunctor_ImmutableNonFunctor(benchmark::State& state) {
  auto db = DataBag::EmptyMutable();
  auto obj = *ObjectCreator::FromAttrs(db, {"a"}, {test::DataItem(1)});
  db->UnsafeMakeImmutable();
  benchmark::DoNotOptimize(obj);
  for (auto _ : state) {
    auto is_functor = IsFunctor(obj);
    benchmark::DoNotOptimize(is_functor);
  }
}

BENCHMARK(BM_IsFunctor_ImmutableNonFunctor);

}  // namespace
}  // namespace koladata::functor
