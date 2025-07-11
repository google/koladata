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
#include "koladata/internal/non_deterministic_token.h"

#include "absl/base/no_destructor.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_value.h"

namespace koladata::internal {

const arolla::TypedValue& NonDeterministicTokenValue() {
  static const absl::NoDestructor value(
      arolla::TypedValue::FromValue(NonDeterministicToken{}));
  return *value;
}

}  // namespace koladata::internal

namespace arolla {

AROLLA_DEFINE_SIMPLE_QTYPE(NON_DETERMINISTIC_TOKEN,
                           koladata::internal::NonDeterministicToken);

}  // namespace arolla
