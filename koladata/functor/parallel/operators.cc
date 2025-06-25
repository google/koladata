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
#include <cstdint>
#include <memory>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/optools.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/async_eval_operator.h"
#include "koladata/functor/parallel/create_execution_context.h"
#include "koladata/functor/parallel/execution_context.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/future_operators.h"
#include "koladata/functor/parallel/future_qtype.h"
#include "koladata/functor/parallel/get_default_executor.h"
#include "koladata/functor/parallel/make_executor.h"
#include "koladata/functor/parallel/stream_operators.h"
#include "koladata/functor/parallel/stream_qtype.h"
#include "koladata/functor/parallel/testing_stream_prime.h"
#include "koladata/functor/parallel/transform.h"
#include "koladata/internal/non_deterministic_token.h"

namespace koladata::functor::parallel {
namespace {

#define OPERATOR AROLLA_REGISTER_QEXPR_OPERATOR
#define OPERATOR_FAMILY AROLLA_REGISTER_QEXPR_OPERATOR_FAMILY

// go/keep-sorted start ignore_prefixes=OPERATOR,OPERATOR_FAMILY
OPERATOR_FAMILY("koda_internal.parallel._stream_for_returns",
                std::make_unique<StreamForReturnsOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel._stream_for_yields",
                std::make_unique<StreamForYieldsOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel._stream_while_returns",
                std::make_unique<StreamWhileReturnsOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel._stream_while_yields",
                std::make_unique<StreamWhileYieldsOperatorFamily>());
// TODO: move testing_*_prime operators to a separate file.
OPERATOR_FAMILY("koda_internal.parallel._testing_iterable_prime",
                std::make_unique<TestingIterableMakePrimeOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel._testing_stream_prime",
                std::make_unique<TestingStreamMakePrimeOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.as_future",
                std::make_unique<AsFutureOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.async_eval",
                std::make_unique<AsyncEvalOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.async_unpack_tuple",
                std::make_unique<AsyncUnpackTupleOperatorFamily>());
OPERATOR("koda_internal.parallel.create_execution_context",
         CreateExecutionContext);
OPERATOR_FAMILY("koda_internal.parallel.empty_stream_like",
                std::make_unique<EmptyStreamLikeOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.future_from_single_value_stream",
                std::make_unique<FutureFromSingleValueStreamOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.future_iterable_from_stream",
                std::make_unique<FutureIterableFromStreamOperatorFamily>());
OPERATOR("koda_internal.parallel.get_execution_context_qtype",
         []() { return arolla::GetQType<ExecutionContextPtr>(); });
OPERATOR("koda_internal.parallel.get_executor_from_context",
         [](const ExecutionContextPtr& context) {
           return context->executor();
         });
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
OPERATOR("koda_internal.parallel.make_executor",
         [](int64_t thread_limit,
            internal::NonDeterministicToken) -> absl::StatusOr<ExecutorPtr> {
           if (thread_limit < 0) {
             return absl::InvalidArgumentError(
                 absl::StrCat("`thread_limit` must be non-negative, but got: ",
                              thread_limit));
           }
           return MakeExecutor(thread_limit);
         });
OPERATOR_FAMILY("koda_internal.parallel.stream_chain",
                std::make_unique<StreamChainOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.stream_chain_from_stream",
                std::make_unique<StreamChainFromStreamOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.stream_from_future",
                std::make_unique<StreamFromFutureOperatorFamily>());
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
OPERATOR("koda_internal.parallel.transform", TransformToParallel);
OPERATOR_FAMILY("koda_internal.parallel.unwrap_future_to_future",
                std::make_unique<UnwrapFutureToFutureOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.unwrap_future_to_stream",
                std::make_unique<UnwrapFutureToStreamOperatorFamily>());

// go/keep-sorted end

}  // namespace
}  // namespace koladata::functor::parallel
