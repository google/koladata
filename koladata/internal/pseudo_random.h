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
#ifndef KOLADATA_INTERNAL_PSEUDO_RANDOM_H_
#define KOLADATA_INTERNAL_PSEUDO_RANDOM_H_

#include <cstdint>

#include "arolla/util/fingerprint.h"

// Returns a uniformly distributed pseudo-random uint64.
//
// While not cryptographically secure, the default implementation generates
// unique sequences of random numbers for each process.
//
// Note: This function supports overriding via LD_PRELOAD. It is declared as "C"
// function because providing multiple definitions in C++ constitutes an ODR
// violation.
//
// WARNING: Koladata relies on high-entropy randomness to prevent collisions
// across processes. If you override this function to return a fixed sequence,
// you must ensure the process is isolated; it must not interact with any
// other process using the same sequence.
extern "C" uint64_t KoladataInternalPseudoRandomUint64();

namespace koladata::internal {

// Returns a pseudo-random uniformly distributed uint64.
inline uint64_t PseudoRandomUint64() {
  return KoladataInternalPseudoRandomUint64();
}

// Returns a pseudo-random uniformly distributed fingerprint.
arolla::Fingerprint PseudoRandomFingerprint();

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_PSEUDO_RANDOM_H_
