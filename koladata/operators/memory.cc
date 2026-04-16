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
#include "koladata/operators/memory.h"

#include <cstdint>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/memory_stats.h"
#include "koladata/object_factories.h"

namespace koladata::ops {

namespace {

constexpr absl::string_view kAttrContainer = "container";
constexpr absl::string_view kAttrShallowSize = "shallow_size";
constexpr absl::string_view kAttrStringsSize = "strings_size";

absl::StatusOr<DataSlice> CreateMemoryStatsSchema() {
  auto db = DataBag::EmptyMutable();
  ASSIGN_OR_RETURN(
      auto schema,
      CreateEntitySchema(
          db, {kAttrContainer, kAttrShallowSize, kAttrStringsSize},
          {*DataSlice::Create(internal::DataItem(schema::kString),
                              internal::DataItem(schema::kSchema)),
           *DataSlice::Create(internal::DataItem(schema::kInt64),
                              internal::DataItem(schema::kSchema)),
           *DataSlice::Create(internal::DataItem(schema::kInt64),
                              internal::DataItem(schema::kSchema))}));
  db->UnsafeMakeImmutable();
  return schema;
}

absl::StatusOr<DataSlice> GetMemoryStatsSchema() {
  static absl::NoDestructor<absl::StatusOr<DataSlice>> res(
      CreateMemoryStatsSchema());
  return *res;
}

}  // namespace

absl::StatusOr<DataSlice> GetAttrMemoryStats(const DataSlice& ds,
                                             const DataSlice& attr_name) {
  if (ds.GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "get_attr_memory_stats expects data slice with a bag");
  }
  if (!attr_name.is_item() || !attr_name.item().holds_value<arolla::Text>()) {
    return absl::InvalidArgumentError(
        "get_attr_memory_stats expects second argument to be a string data "
        "item");
  }
  absl::string_view attr = attr_name.item().value<arolla::Text>().view();
  internal::DataSliceImpl objs;
  if (ds.is_item()) {
    objs = internal::DataSliceImpl::Create({ds.item()});
  } else {
    objs = ds.slice();
  }
  FlattenFallbackFinder fb(*ds.GetBag());
  koladata::internal::MemoryStats stats =
      ds.GetBag()->GetImpl().GetAttrMemoryStats(objs, attr,
                                                fb.GetFlattenFallbacks());
  internal::DataSliceImpl res_impl =
      internal::DataSliceImpl::AllocateEmptyObjects(stats.size());
  auto res_db = DataBag::EmptyMutable();
  ASSIGN_OR_RETURN(auto& db_impl, res_db->GetMutableImpl());
  for (int i = 0; i < stats.size(); ++i) {
    internal::DataItem c = res_impl[i];
    RETURN_IF_ERROR(db_impl.SetAttr(
        c, kAttrContainer,
        internal::DataItem(arolla::Text(stats[i].container_description))));
    RETURN_IF_ERROR(db_impl.SetAttr(
        c, kAttrShallowSize,
        internal::DataItem(static_cast<int64_t>(stats[i].shallow_size))));
    RETURN_IF_ERROR(db_impl.SetAttr(
        c, kAttrStringsSize,
        internal::DataItem(static_cast<int64_t>(stats[i].strings_size))));
  }
  ASSIGN_OR_RETURN(auto schema, GetMemoryStatsSchema());
  res_db->UnsafeMakeImmutable();
  ASSIGN_OR_RETURN(
      res_db, res_db->ImmutableEmptyWithFallbacks({res_db, schema.GetBag()}));
  return DataSlice::CreateWithFlatShape(res_impl, schema.item(), res_db,
                                        DataSlice::Wholeness::kWhole);
}

}  // namespace koladata::ops
