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
#ifndef KOLADATA_OPERATORS_STRINGS_H_
#define KOLADATA_OPERATORS_STRINGS_H_

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"

namespace koladata::ops {

// kde.strings.substr.
absl::StatusOr<DataSlice> Substr(const DataSlice& x, const DataSlice& start,
                                 const DataSlice& end);

// kde.strings.agg_join.
absl::StatusOr<DataSlice> AggJoin(const DataSlice& x, const DataSlice& sep);

// kde.strings.split.
absl::StatusOr<DataSlice> Split(const DataSlice& x, const DataSlice& sep);

// kde.strings.length.
absl::StatusOr<DataSlice> Length(const DataSlice& x);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_STRINGS_H_
