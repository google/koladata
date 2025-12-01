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
#ifndef KOLADATA_INTERNAL_RANDOM_H_
#define KOLADATA_INTERNAL_RANDOM_H_

#include <cstdint>

#include "arolla/util/fingerprint.h"

namespace koladata::internal {

// Returns a random uniformly distributed uint64.
uint64_t MaybeDeterministicRandomUint64();

// Returns a random fingerprint.
arolla::Fingerprint MaybeDeterministicRandomFingerprint();

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_RANDOM_H_
