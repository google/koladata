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
#include "koladata/iterables/iterable_qtype.h"
#include "koladata/iterables/sequence_operators.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/optools.h"
#include "arolla/qtype/qtype.h"

namespace koladata::iterables {
namespace {

#define OPERATOR AROLLA_REGISTER_QEXPR_OPERATOR
#define OPERATOR_FAMILY AROLLA_REGISTER_QEXPR_OPERATOR_FAMILY

// go/keep-sorted start ignore_prefixes=OPERATOR,OPERATOR_FAMILY

OPERATOR("koda_internal.iterables.get_iterable_qtype",
         // Since there is a templated overload, we need to wrap in a lambda.
         [](arolla::QTypePtr value_qtype) -> arolla::QTypePtr {
           return GetIterableQType(value_qtype);
         });
OPERATOR("koda_internal.iterables.is_iterable_qtype",
         [](arolla::QTypePtr qtype) -> arolla::OptionalUnit {
           return arolla::OptionalUnit(IsIterableQType(qtype));
         });
OPERATOR_FAMILY("koda_internal.iterables.sequence_chain",
                std::make_unique<SequenceChainOpFamily>());
OPERATOR_FAMILY("koda_internal.iterables.sequence_from_1d_slice",
                std::make_unique<SequenceFrom1DSliceOpFamily>());
// go/keep-sorted end

}  // namespace
}  // namespace koladata::iterables
