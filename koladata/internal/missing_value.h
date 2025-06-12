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
#ifndef KOLADATA_INTERNAL_MISSING_VALUE_H_
#define KOLADATA_INTERNAL_MISSING_VALUE_H_

namespace koladata::internal {

// Missing value of unspecified type.
struct MissingValue {
  friend bool operator==(const MissingValue& lhs, const MissingValue& rhs) {
    return true;
  }
  friend bool operator!=(const MissingValue& lhs, const MissingValue& rhs) {
    return false;
  }
  friend bool operator<(const MissingValue& lhs, const MissingValue& rhs) {
    return false;
  }
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_MISSING_VALUE_H_
