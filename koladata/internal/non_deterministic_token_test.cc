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
#include "koladata/internal/non_deterministic_token.h"

#include "gtest/gtest.h"
#include "arolla/util/fingerprint.h"

namespace koladata::internal {
namespace {

TEST(NonDeterministicToken, DeterministicFingerprint) {
  // NOTE: If ever used as part of Expr for computing the cache key for some
  // Expr-Input-Value caching in the future, it should have non-deterministic
  // fingerprint (consistent for a particular instance).
  EXPECT_EQ(
      arolla::FingerprintHasher("salt").Combine(NonDeterministicToken{})
          .Finish(),
      arolla::FingerprintHasher("salt").Combine(NonDeterministicToken{})
          .Finish());
}

}  // namespace
}  // namespace koladata::internal
