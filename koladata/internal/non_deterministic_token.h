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
#ifndef KOLADATA_OPERATORS_NON_DETERMINISTIC_TOKEN_H_
#define KOLADATA_OPERATORS_NON_DETERMINISTIC_TOKEN_H_

#include "absl/strings/string_view.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace koladata::internal {

// Type representing an argument in non-deterministic operators.
//
// It is a unique and distinguishable type for easy detection in custom Expr
// processing: e.g. eval, transformations, etc.
//
// It's value is not exposed to the users directly. Usual way of creating it is
// by "embedding" the following expression as an argument to non-deterministic
// operators:
//
// koda_internal.non_deterministic(
//     arolla.L._koladata_hidden_seed_leaf, <random int64>
// )
struct NonDeterministicToken {
  arolla::ReprToken ArollaReprToken() const {
    return {"NonDeterministicToken"};
  }
  void ArollaFingerprint(arolla::FingerprintHasher* hasher) const {
    // NOTE: If ever used as part of Expr for computing the cache key for some
    // Expr-Input-Value caching in the future, it should have non-deterministic
    // fingerprint (consistent for a particular instance).
    hasher->Combine(absl::string_view(
        "::koladata::internal::NonDeterministicToken"));
  }
};

// Returns a reference to statically instantiated NonDeterministicToken
// TypedValue.
const arolla::TypedValue& NonDeterministicTokenValue();

}  // namespace koladata::internal

namespace arolla {

AROLLA_DECLARE_SIMPLE_QTYPE(NON_DETERMINISTIC_TOKEN,
                            koladata::internal::NonDeterministicToken);

}  // namespace arolla

#endif  // KOLADATA_OPERATORS_NON_DETERMINISTIC_TOKEN_H_
