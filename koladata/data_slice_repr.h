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
#ifndef KOLADATA_DATA_SLICE_REPR_H_
#define KOLADATA_DATA_SLICE_REPR_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"

namespace koladata {

// LINT.IfChange
struct ReprOption {
  // The maximum depth when printing nested DataItem.
  int64_t depth = 5;
  // When it is a DataSlice, it means the maximum number of items to show across
  // all dimensions. When it is a DataItem, it means the maximum number of
  // entity/object attributes, list items, or dict key/value pairs to show.
  size_t item_limit = 20;
  // The maximum number of items to show per dimension in a DataSlice. It is
  // only enforced when the size of DataSlice is larger than `item_limit`.
  size_t item_limit_per_dimension = 5;
  // Don't add quotes around text values.
  bool strip_quotes = false;
  // When true, attributes and object ids are wrapped in HTML tags to make it
  // possible to style with CSS and interpret interactions with JS.
  bool format_html = false;
  // Maximum length of repr string to show for text and bytes if non negative.
  int32_t unbounded_type_max_len = -1;
  // Maximum length of an ExprQuote repr string to show.
  int32_t max_expr_quote_len = 10'000;
  // When true, show the attributes of the entity/object in non DataItem
  // DataSlice.
  bool show_attributes = false;
  // When true, the repr will show the databag id.
  bool show_databag_id = true;
  // When true, the repr will show the shape.
  bool show_shape = true;
  // When true, the repr will show the schema.
  bool show_schema = true;
  // When true, the repr will show the itemids for objects.
  bool show_item_id = false;
};
// LINT.ThenChange(//py/koladata/operators/slices.py)

// Returns the string for python __str__.
absl::StatusOr<std::string> DataSliceToStr(
    const DataSlice& ds, const ReprOption& option = ReprOption{});

// Returns a string representation of the provided `data_item`. `schema` is
// allowed to be empty, and `db` is allowed to be nullptr. If this is the case,
// a best effort representation is created.
//
// Prefer `DataSliceToStr` when possible. `DataItemToStr` is mainly intended for
// the case when the schema is unknown.
//
// Note that it's the responsibility of the caller to ensure that the `schema`
// aligns with the provided `data_item`.
absl::StatusOr<std::string> DataItemToStr(
    const internal::DataItem& data_item, const internal::DataItem& schema,
    const absl_nullable DataBagPtr& db,
    const ReprOption& option = ReprOption{});

// Returns the string for python __repr__ and arolla::Repr.
std::string DataSliceRepr(const DataSlice& ds,
                          const ReprOption& option = ReprOption{});

// Returns a human-readable description of the Schema DataSlice.
//
// NOTE: It is recommended that the caller either calls slice.VerifyIsSchema
// before this or to otherwise ensure that the `schema_slice` is a SchemaItem
// (e.g. calling .GetSchema() on a DataSlice).
std::string SchemaToStr(const DataSlice& schema_slice);

}  // namespace koladata

#endif  // KOLADATA_DATA_SLICE_REPR_H_
