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
// Tools to create the "default" signature for a given Expr.

#ifndef KOLADATA_FUNCTOR_DEFAULT_SIGNATURE_H_
#define KOLADATA_FUNCTOR_DEFAULT_SIGNATURE_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"

namespace koladata::functor {

// Returns the "default" signature for a given set of inputs: using this
// signature is equivalent to using kd.eval. More specifically:
// 1) It creates a positional-only parameter 'self', with UnspecifiedSelfInput()
// from expr/constants.h as the default value.
// 2) For each input name 'smth' where 'smth' is not 'self', it creates a
// keyword-only parameter 'smth'.
// 3) It creates a variadic-keyword parameter __extra_inputs__ that allows to
// pass unknown inputs without raising an error.
absl::StatusOr<DataSlice> DefaultKodaSignatureFromInputs(
    absl::Span<const std::string> inputs);

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_DEFAULT_SIGNATURE_H_
