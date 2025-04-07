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
#include "koladata/functor/aggregate_operator.h"
#include "koladata/functor/call_operator.h"
#include "koladata/functor/is_fn_operator.h"
#include "koladata/functor/expr_fn_operator.h"
#include "koladata/functor/map_operator.h"
#include "arolla/qexpr/optools.h"

namespace koladata::functor {
namespace {

#define OPERATOR AROLLA_REGISTER_QEXPR_OPERATOR
#define OPERATOR_FAMILY AROLLA_REGISTER_QEXPR_OPERATOR_FAMILY

// go/keep-sorted start ignore_prefixes=OPERATOR,OPERATOR_FAMILY
OPERATOR("kd.functor._maybe_call", MaybeCall);
OPERATOR_FAMILY("kd.functor.aggregate",
                std::make_unique<AggregateOperatorFamily>());
OPERATOR_FAMILY("kd.functor.call", std::make_unique<CallOperatorFamily>());
OPERATOR_FAMILY("kd.functor.call_and_update_namedtuple",
                std::make_unique<CallAndUpdateNamedTupleOperatorFamily>());
OPERATOR_FAMILY("kd.functor.expr_fn", std::make_unique<ExprFnOperatorFamily>());
OPERATOR("kd.functor.is_fn", IsFn);
OPERATOR_FAMILY("kd.functor.map", std::make_unique<MapOperatorFamily>());
// go/keep-sorted end

}  // namespace
}  // namespace koladata::functor
