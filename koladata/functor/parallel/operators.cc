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
#include <memory>

#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/async_eval_operator.h"
#include "koladata/functor/parallel/future_operators.h"
#include "koladata/functor/parallel/future_qtype.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/optools.h"
#include "arolla/qtype/qtype.h"

namespace koladata::functor::parallel {
namespace {

#define OPERATOR AROLLA_REGISTER_QEXPR_OPERATOR
#define OPERATOR_FAMILY AROLLA_REGISTER_QEXPR_OPERATOR_FAMILY

// go/keep-sorted start ignore_prefixes=OPERATOR,OPERATOR_FAMILY
OPERATOR_FAMILY("koda_internal.parallel.as_future",
                std::make_unique<AsFutureOperatorFamily>());
OPERATOR_FAMILY("koda_internal.parallel.async_eval",
                std::make_unique<AsyncEvalOperatorFamily>());
OPERATOR("koda_internal.parallel.get_future_qtype",
         // Since there is a templated overload, we need to wrap in a lambda.
         [](arolla::QTypePtr value_qtype) -> arolla::QTypePtr {
           return GetFutureQType(value_qtype);
         });
OPERATOR_FAMILY("koda_internal.parallel.get_future_value_for_testing",
                std::make_unique<GetFutureValueForTestingOperatorFamily>());
OPERATOR("koda_internal.parallel.is_future_qtype",
         [](arolla::QTypePtr qtype) -> arolla::OptionalUnit {
           return arolla::OptionalUnit(IsFutureQType(qtype));
         });
// go/keep-sorted end

}  // namespace
}  // namespace koladata::functor::parallel
