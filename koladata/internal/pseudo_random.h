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

#include <atomic>
#include <cstdint>
#include <random>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "arolla/util/fingerprint.h"

// Note: These symbols support overriding via LD_PRELOAD. They are declared
// using "C" ABI, because providing multiple definitions in C++ constitutes
// an ODR violation.
extern "C" {

// Returns a uniformly distributed pseudo-random uint64.
//
// While not cryptographically secure, the default implementation generates
// unique sequences of random numbers for each process.
//
// WARNING: Koladata relies on high-entropy randomness to prevent collisions
// across processes. If you override this function to return a fixed sequence,
// you must ensure the process is isolated; it must not interact with any
// other process using the same sequence.
uint64_t KoladataInternalPseudoRandomUint64();

// Reseeds `KoladataInternalPseudoRandomUint64()`. Returns -1 if the current
// implementation does not support reseeding.
//
// IMPORTANT: This function does not update the pseudo-random epoch ID
// and should not be used directly. Please instead use
// `koladata::internal::ReseedPseudoRandom()`.
//
// NOTE: The reseeding mechanism is primarily intended for cases where a process
// is snapshotted and respawned multiple times. In such scenarios, multiple
// instances sharing the same origin will also share the same pseudo-random
// sequence, which can lead to collisions.
//
// If this function is implemented, it must adhere to the following contract:
//  * It must be thread-safe; however, during the call, concurrent calls to
//    `KoladataInternalPseudoRandomUint64()` may return non-random values.
//  * Different `entropy` values are expected to produce different pseudo-random
//    sequences.
//  * It may use additional entropy sources; specifically, providing
//    the same `entropy` does not guarantee the same output sequence.
//
int KoladataInternalReseedPseudoRandom(std::seed_seq&& entropy);

}  // extern "C"

namespace koladata::internal {

// Returns a pseudo-random uniformly distributed uint64.
inline uint64_t PseudoRandomUint64() {
  return KoladataInternalPseudoRandomUint64();
}

// Returns a pseudo-random uniformly distributed fingerprint.
arolla::Fingerprint PseudoRandomFingerprint();

// Reseeds the pseudo-random sequence.
//
// A successful call affects all subsequent calls to `PseudoRandomUint64()`,
// `PseudoRandomFingerprint()`, and `PseudoRandomEpochId()`. During
// the execution of this function, concurrent calls to these functions may
// return inconsistent or non-random values.
//
// Providing the same `entropy` does not guarantee a reproducible output
// sequence. If you need reproducibility, consider implementing a deterministic
// pseudo-random generator.
//
// NOTE: The reseeding mechanism is primarily intended for cases where a process
// is snapshotted and respawned multiple times. In such scenarios, multiple
// instances sharing the same origin will also share the same pseudo-random
// sequence, which can lead to collisions.
absl::Status ReseedPseudoRandom(std::seed_seq&& entropy);

// Returns a pointer to the atomic identifier for the current pseudo-random
// generator epoch. The function always returns the same pointer.
//
// This ID remains constant until the underlying generator is reseeded
// (e.g., after process forking or checkpoint restoration), at which
// point a new ID can be generated to ensure uniqueness across the new state.
const std::atomic_uint64_t* absl_nonnull PseudoRandomEpochIdPtr();

// Returns the identifier for the current pseudo-random generator epoch.
//
// This ID remains constant until the underlying generator is reseeded
// (e.g., after process forking or checkpoint restoration), at which
// point a new ID can be generated to ensure uniqueness across the new state.
inline uint64_t PseudoRandomEpochId() {
  static const std::atomic_uint64_t* ptr;
  if (ptr == nullptr) [[unlikely]] {
    ptr = PseudoRandomEpochIdPtr();
  }
  return std::atomic_load_explicit(ptr, std::memory_order_relaxed);
}

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_PSEUDO_RANDOM_H_
