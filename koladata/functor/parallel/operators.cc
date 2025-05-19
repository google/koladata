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
#include <memory>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/optools.h"
#include "arolla/qtype/qtype.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/asio_executor.h"
#include "koladata/functor/parallel/async_eval_operator.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/future_operators.h"
#include "koladata/functor/parallel/future_qtype.h"
#include "koladata/functor/parallel/stream_operators.h"
#include "koladata/functor/parallel/stream_qtype.h"
#include "koladata/internal/non_deterministic_token.h"

namespace koladata::functor::parallel {
namespace {

#define OPERATOR AROLLA_REGISTER_QEXPR_OPERATOR
#define OPERATOR_FAMILY AROLLA_REGISTER_QEXPR_OPERATOR_FAMILY

// go/keep-sorted start ignore_prefixes=OPERATOR,OPERATOR_FAMILY
OPERATOR_FAMILY("koda_internal.parallel.as_future",
                std::make_unique<AsFutureOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.async_eval",
                std::make_unique<AsyncEvalOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.async_unpack_tuple",
                std::make_unique<AsyncUnpackTupleOperatorFamily>());
OPERATOR("koda_internal.parallel.get_future_qtype",
         // Since there is a templated overload, we need to wrap in a lambda.
         [](arolla::QTypePtr value_qtype) -> arolla::QTypePtr {
           return GetFutureQType(value_qtype);
         });
OPERATOR_FAMILY("koda_internal.parallel.get_future_value_for_testing",
                std::make_unique<GetFutureValueForTestingOperatorFamily>());
OPERATOR("koda_internal.parallel.get_stream_qtype",
         // Since there is a templated overload, we need to wrap in a lambda.
         [](arolla::QTypePtr value_qtype) -> arolla::QTypePtr {
           return GetStreamQType(value_qtype);
         });
OPERATOR("koda_internal.parallel.is_future_qtype",
         [](arolla::QTypePtr qtype) -> arolla::OptionalUnit {
           return arolla::OptionalUnit(IsFutureQType(qtype));
         });
OPERATOR("koda_internal.parallel.is_stream_qtype",
         [](arolla::QTypePtr qtype) -> arolla::OptionalUnit {
           return arolla::OptionalUnit(IsStreamQType(qtype));
         });
OPERATOR("koda_internal.parallel.make_asio_executor",
         [](int64_t num_threads,
            internal::NonDeterministicToken) -> absl::StatusOr<ExecutorPtr> {
           if (num_threads < 0) {
             return absl::InvalidArgumentError(absl::StrCat(
                 "`num_threads` must be non-negative, but got: ", num_threads));
           }
           return MakeAsioExecutor(num_threads);
         });
OPERATOR_FAMILY("koda_internal.parallel.stream_chain",
                std::make_unique<StreamChainOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.stream_chain_from_stream",
                std::make_unique<StreamChainFromStreamOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.stream_from_iterable",
                std::make_unique<StreamFromIterableOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.stream_interleave",
                std::make_unique<StreamInterleaveOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.stream_interleave_from_stream",
                std::make_unique<StreamInterleaveFromStreamOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.stream_make",
                std::make_unique<StreamMakeOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.stream_map",
                std::make_unique<StreamMapOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.stream_map_unordered",
                std::make_unique<StreamMapUnorderedOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.stream_reduce",
                std::make_unique<StreamReduceOperatorFamily>());

// go/keep-sorted end

}  // namespace
}  // namespace koladata::functor::parallel
