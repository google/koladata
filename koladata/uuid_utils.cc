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
#include "koladata/uuid_utils.h"

#include <functional>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/uuid_object.h"
#include "koladata/shape_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {

namespace {

absl::StatusOr<DataSlice> CreateUuidFromFieldsImpl(
    absl::string_view seed, absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values,
    internal::UuidType uuid_type) {
  DCHECK_EQ(attr_names.size(), values.size());
  if (values.empty()) {
    return DataSlice::Create(
        internal::CreateUuidFromFields(
            seed, {},
            std::vector<std::reference_wrapper<const internal::DataItem>>{}),
        internal::DataItem(schema::kItemId));
  }
  ASSIGN_OR_RETURN(auto aligned_values, shape::Align(values));
  return aligned_values.begin()->VisitImpl([&]<class T>(const T& impl) {
    std::vector<std::reference_wrapper<const T>> values_impl;
    values_impl.reserve(values.size());
    for (int i = 0; i < attr_names.size(); ++i) {
      values_impl.push_back(std::cref(aligned_values[i].impl<T>()));
    }
    return DataSlice::Create(internal::CreateUuidFromFields(
                                 seed, attr_names, values_impl, uuid_type),
                             aligned_values.begin()->GetShape(),
                             internal::DataItem(schema::kItemId), nullptr);
  });
}

}  // namespace

absl::StatusOr<DataSlice> CreateUuidFromFields(
    absl::string_view seed, absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values) {
  return CreateUuidFromFieldsImpl(seed, attr_names, values,
                                  internal::UuidType::kDefault);
}

absl::StatusOr<DataSlice> CreateListUuidFromFields(
    absl::string_view seed, absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values) {
  return CreateUuidFromFieldsImpl(seed, attr_names, values,
                                  internal::UuidType::kList);
}

absl::StatusOr<DataSlice> CreateDictUuidFromFields(
    absl::string_view seed, absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values) {
  return CreateUuidFromFieldsImpl(seed, attr_names, values,
                                  internal::UuidType::kDict);
}

}  // namespace koladata
