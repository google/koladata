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
#include "koladata/internal/random.h"

#include <cstdint>

#include "absl/numeric/int128.h"
#include "absl/random/random.h"
#include "absl/random/uniform_int_distribution.h"
#include "arolla/util/fingerprint.h"

namespace koladata::internal {

uint64_t MaybeDeterministicRandomUint64() {
  thread_local absl::BitGen gen;
  return absl::uniform_int_distribution<uint64_t>()(gen);
}

arolla::Fingerprint MaybeDeterministicRandomFingerprint() {
  return arolla::Fingerprint(absl::MakeUint128(
      MaybeDeterministicRandomUint64(), MaybeDeterministicRandomUint64()));
}

}  // namespace koladata::internal
