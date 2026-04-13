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
#ifndef KOLADATA_INTERNAL_OP_UTILS_APPLY_FILTER_H_
#define KOLADATA_INTERNAL_OP_UTILS_APPLY_FILTER_H_

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/uuid_object.h"

namespace koladata::internal {

namespace schema_filters {

inline const DataItem& AnyPrimitiveFilter() {
  static const absl::NoDestructor<DataItem> kSchema(
      CreateSchemaUuidFromFields("__named_schema____ANY_PRIMITIVE__", {}, {}));
  return *kSchema;
}

inline const DataItem& AnySchemaFilter() {
  static const absl::NoDestructor<DataItem> kSchema(
      CreateSchemaUuidFromFields("__named_schema____ANY_SCHEMA__", {}, {}));
  return *kSchema;
}

}  // namespace schema_filters

// Adds a new schema to new_databag with the same structure and ObjectIds as
// the given schema, but with the attributes and values filtered
// according to the given filter and with metadata added from the filter.
class ApplyFilterOp {
 public:
  explicit ApplyFilterOp(DataBagImpl& new_databag)
      : new_databag_(new_databag) {}

  absl::Status operator()(const DataSliceImpl& ds, const DataItem& schema,
                          const DataBagImpl& databag,
                          DataBagImpl::FallbackSpan fallbacks,
                          const DataItem& filter,
                          const DataBagImpl& filter_databag,
                          DataBagImpl::FallbackSpan filter_fallbacks) const;

  absl::Status operator()(const DataItem& item, const DataItem& schema,
                          const DataBagImpl& databag,
                          DataBagImpl::FallbackSpan fallbacks,
                          const DataItem& filter,
                          const DataBagImpl& filter_databag,
                          DataBagImpl::FallbackSpan filter_fallbacks) const;

 private:
  DataBagImpl& new_databag_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_APPLY_FILTER_H_
