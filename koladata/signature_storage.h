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
// This file contains functions related to converting between C++ Signature
// objects and the corresponding DataItems. Since the C++ Signature object
// is not serializable, converting it to DataItem/DataBag is the only way to
// store it for the long term, hence "storage" in the file name.

#ifndef KOLADATA_FUNCTOR_SIGNATURE_STORAGE_H_
#define KOLADATA_FUNCTOR_SIGNATURE_STORAGE_H_

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "koladata/data_slice.h"
#include "koladata/signature.h"

namespace koladata::functor {

inline constexpr absl::string_view kParameterKindField = "kind";
inline constexpr absl::string_view kNoDefaultValueParameterField =
    "no_default_value";
inline constexpr absl::string_view kPositionalOnlyParameterName =
    "positional_only";
inline constexpr absl::string_view kPositionalOrKeywordParameterName =
    "positional_or_keyword";
inline constexpr absl::string_view kVarPositionalParameterName =
    "var_positional";
inline constexpr absl::string_view kKeywordOnlyParameterName = "keyword_only";
inline constexpr absl::string_view kVarKeywordParameterName = "var_keyword";

// Converts a Koda DataItem storing a signature to a C++ Signature object.
// This method can also be used to verify the validity of a Koda signature.
// CppSignatureToKodaSignature can be found in
// koladata/functor/signature_utils.h.
absl::StatusOr<Signature> KodaSignatureToCppSignature(
    const DataSlice& signature, bool detach_default_values_db = false);

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_SIGNATURE_STORAGE_H_
