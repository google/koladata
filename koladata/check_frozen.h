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
#ifndef THIRD_PARTY_KOLA_DATA_CHECK_FROZEN_H_
#define THIRD_PARTY_KOLA_DATA_CHECK_FROZEN_H_

#include "absl/status/status.h"
#include "arolla/qtype/typed_ref.h"

namespace koladata {

// If `value` is a DataBag or DataSlice, checks if it is frozen and returns
// an error if it is not.
//
// Note: If the value is compound (see arolla/qtype/qtype.h), this function will
// recursively traverse and check the fields. However, any data bags that are
// unreachable through the fields will be ignored.
//
absl::Status CheckFrozen(arolla::TypedRef value);

}  // namespace koladata

#endif  // THIRD_PARTY_KOLA_DATA_CHECK_FROZEN_H_
