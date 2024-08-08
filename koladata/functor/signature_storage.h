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
// This file contains functions related to converting between C++ Signature
// objects and the corresponding DataItems. Since the C++ Signature object
// is not serializable, converting it to DataItem/DataBag is the only way to
// store it for the long term, hence "storage" in the file name.

#ifndef KOLADATA_FUNCTOR_SIGNATURE_STORAGE_H_
#define KOLADATA_FUNCTOR_SIGNATURE_STORAGE_H_

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/functor/signature.h"

namespace koladata::functor {

// Converts a C++ Signature object to a Koda DataItem storing the signature.
// The returned DataItem will have a new DataBag created to store the triples.
absl::StatusOr<DataSlice> CppSignatureToKodaSignature(
    const Signature& signature);

// Converts a Koda DataItem storing a signature to a C++ Signature object.
// This method can also be used to verify the validity of a Koda signature.
absl::StatusOr<Signature> KodaSignatureToCppSignature(
    const DataSlice& signature);

// Return the constants used to store the parameter kinds in the Koda signature.
const absl::StatusOr<DataSlice>& PositionalOnlyParameterKind();
const absl::StatusOr<DataSlice>& PositionalOrKeywordParameterKind();
const absl::StatusOr<DataSlice>& VarPositionalParameterKind();
const absl::StatusOr<DataSlice>& KeywordOnlyParameterKind();
const absl::StatusOr<DataSlice>& VarKeywordParameterKind();

// Returns the constant used to indicate that a parameter has no default value.
const absl::StatusOr<DataSlice>& NoDefaultValueMarker();

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_SIGNATURE_STORAGE_H_
