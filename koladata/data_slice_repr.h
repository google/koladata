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
#ifndef KOLADATA_DATA_SLICE_REPR_H_
#define KOLADATA_DATA_SLICE_REPR_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"

namespace koladata {

struct ReprOption {
  // The maximum depth when printing nested DataItem.
  int64_t depth = 5;
  // When it is a DataSlice, it means the maximum number of items to show across
  // all dimensions. When it is a DataItem, it means the maximum number of
  // entity/object attributes, list items, or dict key/value pairs to show.
  size_t item_limit = 20;
  // TODO: Add option to control DataSlice truncation.
};

// Returns the string for python __str__.
absl::StatusOr<std::string> DataSliceToStr(
    const DataSlice& ds, const ReprOption& option = ReprOption{});

}  // namespace koladata

#endif  // KOLADATA_DATA_SLICE_REPR_H_
