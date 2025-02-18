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
#ifndef KOLADATA_OBJECT_FACTORIES_H_
#define KOLADATA_OBJECT_FACTORIES_H_

#include <memory>
#include <optional>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/uuid_object.h"

namespace koladata {

// Returns a DataSlice which contains a newly allocated Explicit Schema id whose
// attributes `attr_names` are set to `schemas` in DataBag `db`. In case
// `schemas` are not valid schemas, appropriate error is returned.
absl::StatusOr<DataSlice> CreateEntitySchema(
    const DataBagPtr& db,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> schemas);

// Functor that provides different factories for Entities. When created,
// Entities have DataSlice-level explicit schema (which is also stored in the
// referenced DataBag).
struct EntityCreator {
  // Implements kd.new function / operator.
  //
  // Returns an Entity (DataSlice with a reference to `db`) and attributes
  // `attr_names` set to `values`. The output DataSlice is a DataItem if all
  // `values` are DataItems or `attr_names` and `values` are empty. Otherwise,
  // the result has the shape of an input DataSlice with the highest rank. All
  // inputs have to be "broadcastable" to a DataSlice with the highest rank,
  // otherwise an error is returned.
  //
  // The returned Entity has an explicit schema whose attributes `attr_names`
  // are set to schemas of `values`. If `schema` is provided, attributes are
  // cast to `schema` attributes. In case some schema attribute is missing,
  // error is returned, unless `update_schema` is provided in which case, the
  // schema attribute is set from attribute's value.
  //
  // `itemid` can optionally accept ITEMID DataSlice used as ItemIds, instead of
  // allocating them.
  //
  // Adopts `values` and `schema`(if present) into `db`.
  static absl::StatusOr<DataSlice> FromAttrs(
      const DataBagPtr& db,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& schema = std::nullopt,
      bool update_schema = false,
      const std::optional<DataSlice>& itemid = std::nullopt);

  // Implements kd.new_shaped function / operator.
  //
  // Returns an Entity (DataSlice with a reference to `db`) with shape `shape`.
  //
  // If `attr_names` and `values` are non-empty, they are added as attributes.
  // All `values` must be broadcastable to `shape` or have the same `shape`.
  //
  // The returned Entity has an explicit schema whose attributes `attr_names`
  // are set to schemas of `values`. If `schema` is provided, attributes are
  // cast to `schema` attributes. In case some schema attribute is missing,
  // error is returned, unless `update_schema` is provided in which case, the
  // schema attribute is set from attribute's value.
  //
  // `itemid` can optionally accept ITEMID DataSlice used as ItemIds, instead of
  // allocating them.
  //
  // Adopts `values` and `schema`(if present) into `db`.
  static absl::StatusOr<DataSlice> Shaped(
      const DataBagPtr& db,
      DataSlice::JaggedShape shape,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& schema = std::nullopt,
      bool update_schema = false,
      const std::optional<DataSlice>& itemid = std::nullopt);

  // Implements kd.new_like function / operator.
  //
  // Returns an Entity (DataSlice with a reference to `db`) with shape and
  // sparsity from `shape_and_mask_from`.
  //
  // If `attr_names` and `values` are non-empty, they are added as attributes.
  // All `values` must be broadcastable to `shape_and_mask_from.GetShape()`.
  //
  // The returned Entity has an explicit schema whose attributes `attr_names`
  // are set to schemas of `values`. If `schema` is provided, attributes are
  // cast to `schema` attributes. In case some schema attribute is missing,
  // error is returned, unless `update_schema` is provided in which case, the
  // schema attribute is set from attribute's value.
  //
  // `itemid` can optionally accept ITEMID DataSlice used as ItemIds, instead of
  // allocating them.
  //
  // Adopts `values` and `schema`(if present) into `db`.
  static absl::StatusOr<DataSlice> Like(
      const DataBagPtr& db,
      const DataSlice& shape_and_mask_from,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& schema = std::nullopt,
      bool update_schema = false,
      const std::optional<DataSlice>& itemid = std::nullopt);

  // This method, together with ObjectCreator::Convert provides a uniform method
  // for adapting DataSlices for usage in converting complex Python structures
  // to Koda structure.
  //
  // In case of Entities, it does an Entity-compatible adaption (i.e. no-op) of
  // a DataSlice.
  //
  // NOTE: Does not adopt a DataBag from `value`.
  static absl::StatusOr<DataSlice> ConvertWithoutAdopt(const DataBagPtr& db,
                                                       const DataSlice& value) {
    return value;
  }
};

// Functor that provides different factories for Objects. When created, Objects
// have Object/Item-level implicit schemas.
struct ObjectCreator {
  // Implements kd.obj function / operator.
  //
  // Returns an Object (DataSlice with a reference to `db`) and attributes
  // `attr_names` set to `values`. The output DataSlice is a DataItem if all
  // `values` are DataItems or `attr_names` and `values` are empty. Otherwise,
  // the result has the shape of an input DataSlice with the highest rank. All
  // inputs have to be "broadcastable" to a DataSlice with the highest rank,
  // otherwise an error is returned.
  //
  // The returned Object's __schema__ attribute is implicit schema slice (each
  // schema item in this schema slice is a different allocated schema object).
  // Each of them has `attr_names` attributes set to schemas of `values`.
  //
  // `itemid` can optionally accept ITEMID DataSlice used as ItemIds, instead of
  // allocating them.
  //
  // Adopts `values` and `schema`(if present) into `db`.
  static absl::StatusOr<DataSlice> FromAttrs(
      const DataBagPtr& db,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& itemid = std::nullopt);

  // Implements kd.obj_shaped function / operator.
  //
  // Returns an Object (DataSlice with a reference to `db`) with shape `shape`.
  //
  // If `attr_names` and `values` are non-empty, they are added as attributes.
  // All `values` must be broadcastable to `shape` or have the same `shape`.
  //
  // The returned Object's __schema__ attribute is implicit schema slice (each
  // schema item in this schema slice is a different allocated schema object).
  // Each of them has `attr_names` attributes set to schemas of `values`.
  //
  // `itemid` can optionally accept ITEMID DataSlice used as ItemIds, instead of
  // allocating them.
  //
  // Adopts `values` and `schema`(if present) into `db`.
  static absl::StatusOr<DataSlice> Shaped(
      const DataBagPtr& db,
      DataSlice::JaggedShape shape,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& itemid = std::nullopt);

  // Implements kd.obj_like function / operator.
  //
  // Returns an Object (DataSlice with a reference to `db`) with shape and
  // sparsity from `shape_and_mask_from`.
  //
  // If `attr_names` and `values` are non-empty, they are added as attributes.
  // All `values` must be broadcastable to `shape_and_mask_from.GetShape()`.
  //
  // The returned Object's __schema__ attribute is implicit schema slice (each
  // schema item in this schema slice is a different allocated schema object).
  // Each of them has `attr_names` attributes set to schemas of `values`.
  //
  // `itemid` can optionally accept ITEMID DataSlice used as ItemIds, instead of
  // allocating them.
  //
  // Adopts `values` and `schema`(if present) into `db`.
  static absl::StatusOr<DataSlice> Like(
      const DataBagPtr& db,
      const DataSlice& shape_and_mask_from,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& itemid = std::nullopt);

  // This method, together with EntityCreator::Convert provides a uniform method
  // for adapting DataSlices for usage in converting complex Python structures
  // to Koda structure.
  //
  // Converts DataSlices with non-primitive schemas to `OBJECT`. Keeps `OBJECT`s
  // unchanged.
  //
  // NOTE: Does not adopt a DataBag from `value`.
  static absl::StatusOr<DataSlice> ConvertWithoutAdopt(const DataBagPtr& db,
                                                       const DataSlice& value);
};

// Returns a UuEntity ((DataSlice of UuIds generated as row-wise fingerprints
// from attribute names and values) with a reference to `db`) and attributes
// `attr_names` set to `values`. The output DataSlice is a DataItem if all
// `values` are DataItems or `attr_names` and `values` are empty. Otherwise, the
// result has the shape of an input DataSlice with the highest rank. All inputs
// have to be "broadcastable" to a DataSlice with the highest rank, otherwise an
// error is returned.
//
// Also supports the following arguments:
// - `seed` for uuid computation.
// - `schema`, the schema for the entity. If missing, it will be inferred from
// the argument values.
// - `update_schema`, if true, will overwrite schema attributes in the schema's
// corresponding db from the argument values.
//
// The schema of the entities is stored on the returned DataSlice.
//
// Adopts `values` and `schema`(if present) into `db`.
absl::StatusOr<DataSlice> CreateUu(
    const DataBagPtr& db,
    absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values,
    const std::optional<DataSlice>& schema = std::nullopt,
    bool update_schema = false);

// Returns a UuObject (DataSlice of UuIds generated as row-wise fingerprints
// from attribute names and values) with a reference to `db`) and attributes
// `attr_names` set to `values`. The output DataSlice is a DataItem if all
// `values` are DataItems or `attr_names` and `values` are empty. Otherwise, the
// result has the shape of an input DataSlice with the highest rank. All inputs
// have to be "broadcastable" to a DataSlice with the highest rank, otherwise an
// error is returned.
//
// The returned Object's __schema__ attribute is implicit schema slice (each
// schema item in this schema slice is a different allocated schema object).
// Each of them has `attr_names` attributes set to schemas of `values`.
absl::StatusOr<DataSlice> CreateUuObject(
    const DataBagPtr& db,
    absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values);

// Returns a UuSchema (DataItem generated as a row-wise fingerprint from
// attribute names and schemas) with a reference to `db`) and attributes
// `attr_names` set to `schemas`.

// Adopts `schemas` into `db`.
absl::StatusOr<DataSlice> CreateUuSchema(
    const DataBagPtr& db,
    absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> schemas);

// Returns a named entity schema with its ItemId derived only from its name.
absl::StatusOr<DataSlice> CreateNamedSchema(
    const DataBagPtr& db, absl::string_view name,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> schemas);

// Returns an allocated schema with attributes `attr_names` set to `schemas` in
// `db`.

// Adopts `schemas` into `db`.
absl::StatusOr<DataSlice> CreateSchema(
    const DataBagPtr& db,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> schemas);

// Returns a list schema from a given `item_schema`. Adopts `item_schema` into
// `db`.
absl::StatusOr<DataSlice> CreateListSchema(const DataBagPtr& db,
                                           const DataSlice& item_schema);

// Returns a dict schema given `key_schema` and `value_schema`. Adopts
// `key_schema` and `value_schema` into `db`.
absl::StatusOr<DataSlice> CreateDictSchema(const DataBagPtr& db,
                                           const DataSlice& key_schema,
                                           const DataSlice& value_schema);

// Creates dicts with the given shape. If `keys` and `values` are provided, they
// will be set to the dicts after creation (that implies potential type casting
// and broadcasting). If `key_schema` and `value_schema` are not provided, they
// will be taken from `keys` and `values` or defaulted to OBJECT.
//
// `itemid` can optionally accept ITEMID DataSlice used as ItemIds, instead of
// allocating them. If `itemid` already had dicts in `db`, those will be
// overwritten.
absl::StatusOr<DataSlice> CreateDictShaped(
    const DataBagPtr& db, DataSlice::JaggedShape shape,
    const std::optional<DataSlice>& keys,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema = std::nullopt,
    const std::optional<DataSlice>& key_schema = std::nullopt,
    const std::optional<DataSlice>& value_schema = std::nullopt,
    const std::optional<DataSlice>& itemid = std::nullopt);

// Creates dicts with the given shape_and_mask_from. If `keys` and `values` are
// provided, they will be set to the dicts after creation (that implies
// potential type casting and broadcasting). If `key_schema` and `value_schema`
// are not provided, they will be taken from `keys` and `values` or defaulted to
// OBJECT.
//
// `itemid` can optionally accept ITEMID DataSlice used as ItemIds, instead of
// allocating them. They will be filtered based on `shape_and_mask_from`. If
// `itemid` already had dicts in `db`, those will overwritten.
absl::StatusOr<DataSlice> CreateDictLike(
    const DataBagPtr& db, const DataSlice& shape_and_mask_from,
    const std::optional<DataSlice>& keys,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema = std::nullopt,
    const std::optional<DataSlice>& key_schema = std::nullopt,
    const std::optional<DataSlice>& value_schema = std::nullopt,
    const std::optional<DataSlice>& itemid = std::nullopt);

// Creates a single empty list. If `item_schema` is not provided, it will be
// taken from `values` or defaulted to OBJECT.
//
// `itemid` can optionally accept ITEMID DataSlice used as ItemIds, instead of
// allocating them. If `itemid` already had lists in `db`, those will be
// overwritten.
absl::StatusOr<DataSlice> CreateEmptyList(
    const DataBagPtr& db,
    const std::optional<DataSlice>& schema = std::nullopt,
    const std::optional<DataSlice>& item_schema = std::nullopt,
    const std::optional<DataSlice>& itemid = std::nullopt);

// Creates a DataSlice of lists with given values. The dimension of the resulted
// DataSlice will be one less than the dimension of the values. If `item_schema`
// is not provided, it will be taken from `values` or defaulted to OBJECT.
//
// `itemid` can optionally accept ITEMID DataSlice used as ItemIds, instead of
// allocating them. If `itemid` already had lists in `db`, those will be
// overwritten.
absl::StatusOr<DataSlice> CreateListsFromLastDimension(
    const DataBagPtr& db, const DataSlice& values,
    const std::optional<DataSlice>& schema = std::nullopt,
    const std::optional<DataSlice>& item_schema = std::nullopt,
    const std::optional<DataSlice>& itemid = std::nullopt);

// Creates a list from values. If `values` dimension is more than one, the list
// will contain other lists. If `item_schema` is not provided, it will be
// taken from `values`.
//
// `itemid` can optionally accept ITEMID DataSlice used as ItemIds, instead of
// allocating them. If `itemid` already had lists in `db`, those will be
// overwritten.
absl::StatusOr<DataSlice> CreateNestedList(
    const DataBagPtr& db, const DataSlice& values,
    const std::optional<DataSlice>& schema = std::nullopt,
    const std::optional<DataSlice>& item_schema = std::nullopt,
    const std::optional<DataSlice>& itemid = std::nullopt);

// Creates a DataSlice of nested lists from the last `ndim` dimensions of
// `values` if `ndim` >= 0, or from all dimensions of `values` if `ndim` < 0.
// The contents of `values` are adopted into `db`.
//
// `itemid` can optionally accept ITEMID DataSlice used as ItemIds, instead of
// allocating them. If `itemid` already had lists in `db`, those will be
// overwritten. itemid's shape must match first values.GetShape().rank() - ndim
// dimensions.
absl::StatusOr<DataSlice> Implode(
    const DataBagPtr& db, const DataSlice& values, int ndim,
    const std::optional<DataSlice>& itemid = std::nullopt);

// Creates a DataSlice of lists concatenated from the lists in `inputs`, All
// input DataSlices must contain lists with compatible item schemas, and are
// aligned before being concatenated. If `inputs` is empty, returns a DataSlice
// with a single empty list.
//
// The input DataSlices are adopted into `db`, except for the lists they
// directly contain, because those lists are not in the output.
absl::StatusOr<DataSlice> ConcatLists(const DataBagPtr& db,
                                      std::vector<DataSlice> inputs);

// Creates a DataSlice of lists with the provided shape. If `values` are
// provided, they will be appended to the lists after creation (that implies
// potential type casting and broadcasting). If `item_schema` is not provided,
// it will be taken from `values` or defaulted to OBJECT.
//
// `itemid` can optionally accept ITEMID DataSlice used as ItemIds, instead of
// allocating them. If `itemid` already had lists in `db`, those will be
// overwritten.
absl::StatusOr<DataSlice> CreateListShaped(
    const DataBagPtr& db, DataSlice::JaggedShape shape,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema = std::nullopt,
    const std::optional<DataSlice>& item_schema = std::nullopt,
    const std::optional<DataSlice>& itemid = std::nullopt);

// Creates empty lists of the given shape_and_mask_from. If `values` are
// provided, they will be appended to the lists after creation (that implies
// potential type casting and broadcasting). If `item_schema` is not provided,
// it will be taken from `values` or defaulted to OBJECT.
//
// `itemid` can optionally accept ITEMID DataSlice used as ItemIds, instead of
// allocating them. If `itemid` already had lists in `db`, those will be
// overwritten.
absl::StatusOr<DataSlice> CreateListLike(
    const DataBagPtr& db, const DataSlice& shape_and_mask_from,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema = std::nullopt,
    const std::optional<DataSlice>& item_schema = std::nullopt,
    const std::optional<DataSlice>& itemid = std::nullopt);

// Creates a DataSlice of missing items with the given shape and schema.
// If `schema` is an Entity schema and db is missing, an empty DataBag is
// created and attached to the resulting DataSlice.
absl::StatusOr<DataSlice> CreateEmptyShaped(const DataSlice::JaggedShape& shape,
                                            const DataSlice& schema,
                                            absl::Nullable<DataBagPtr> db);

// Creates a NoFollow schema from `target_schema`. `target_schema` must be a
// valid schema slice. If `target_schema` is NoFollow, primitive or ITEMID
// schema, the error is returned.
//
// CreateNoFollowSchema is reversible with `ds.GetActualSchema()`.
absl::StatusOr<DataSlice> CreateNoFollowSchema(const DataSlice& target_schema);

// Returns a new DataSlice with same contents as `target`, but with NoFollow
// schema created from `target.GetSchema()` slice. NoFollow, Primitive and
// ITEMID slices are not accepted and the appropriate error is returned.
absl::StatusOr<DataSlice> NoFollow(const DataSlice& target);


}  // namespace koladata

#endif  // KOLADATA_OBJECT_FACTORIES_H_
