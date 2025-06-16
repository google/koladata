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
#ifndef KOLADATA_FUNCTOR_FUNCTOR_H_
#define KOLADATA_FUNCTOR_FUNCTOR_H_

#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "koladata/data_slice.h"

namespace koladata::functor {

// The attribute name used to store the returns expression in a functor.
inline constexpr absl::string_view kReturnsAttrName = "returns";

// The attribute name used to store the signature in a functor.
inline constexpr absl::string_view kSignatureAttrName = "__signature__";

// Creates a functor with the given returns expression, signature,
// and variables.
// returns must contain a DataItem holding a quoted Expr.
// signature must contain a DataItem holding a signature as created by
// methods from signature_storage.h, or a missing DataItem. When signature is
// missing, we create the default signature (see default_signature.h for more
// details) based on the inputs found in the functor. Each DataSlice in
// variables must be a DataItem. When it holds a quoted Expr, it will also be
// evaluated when the functor is called, otherwise it will be treated as a
// literal value for the corresponding variable.
absl::StatusOr<DataSlice> CreateFunctor(
    const DataSlice& returns, const DataSlice& signature,
    std::vector<absl::string_view> variable_names,
    std::vector<DataSlice> variable_values);

// Checks if a given DataSlice represents a functor. This only does a basic
// check (that the slice is a data item and has the right attributes), so the
// functor may still fail on evaluation.
absl::StatusOr<bool> IsFunctor(const DataSlice& slice);

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_FUNCTOR_H_
