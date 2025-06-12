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
#ifndef KOLADATA_DATA_SLICE_QTYPE_H_
#define KOLADATA_DATA_SLICE_QTYPE_H_

// Define QTypeTraits related to DataSlice, allowing it to be used
// as an argument to and as a result of Expr(s).

// IWYU pragma: always_keep, the file defines QTypeTraits<T> specializations.

#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "koladata/data_slice.h"

namespace arolla {

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(::koladata::DataSlice);
AROLLA_DECLARE_REPR(::koladata::DataSlice);
AROLLA_DECLARE_QTYPE(::koladata::DataSlice);

}  // namespace arolla

#endif  // KOLADATA_DATA_SLICE_QTYPE_H_
