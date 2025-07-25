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
#ifndef KOLADATA_FUNCTOR_FUNCTOR_UTILS_H_
#define KOLADATA_FUNCTOR_FUNCTOR_UTILS_H_

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "koladata/data_slice.h"

namespace koladata::functor {

// The attribute name used to store the returns expression in a functor.
inline constexpr absl::string_view kReturnsAttrName = "returns";

// The attribute name used to store the signature in a functor.
inline constexpr absl::string_view kSignatureAttrName = "__signature__";

// The attribute name used to store the qualname of the functor.
inline constexpr absl::string_view kQualnameAttrName = "__qualname__";

// The attribute name used to store the module of the functor.
inline constexpr absl::string_view kModuleAttrName = "__module__";

// Checks if a given DataSlice represents a functor. This only does a basic
// check (that the slice is a data item and has the right attributes), so the
// functor may still fail on evaluation.
absl::StatusOr<bool> IsFunctor(const DataSlice& slice);

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_FUNCTOR_UTILS_H_
