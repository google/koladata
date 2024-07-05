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

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"

namespace koladata {

// Returns a DataSlice which contains a newly allocated Explicit Schema id whose
// attributes `attr_names` are set to `schemas` in DataBag `db`. In case
// `schemas` are not valid schemas, appropriate error is returned.
absl::StatusOr<DataSlice> CreateEntitySchema(
    const DataBagPtr& db,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& schemas);

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
  absl::StatusOr<DataSlice> operator()(
      const DataBagPtr& db,
      const std::vector<absl::string_view>& attr_names,
      const std::vector<DataSlice>& values,
      const std::optional<DataSlice>& schema = std::nullopt,
      bool update_schema = false) const;

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
  absl::StatusOr<DataSlice> operator()(
      const DataBagPtr& db,
      DataSlice::JaggedShape shape,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& schema = std::nullopt,
      bool update_schema = false) const;

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
  absl::StatusOr<DataSlice> operator()(
      const DataBagPtr& db,
      const DataSlice& shape_and_mask_from,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values,
      const std::optional<DataSlice>& schema = std::nullopt,
      bool update_schema = false) const;

  // Assigns DataBag `db` to `value`.
  absl::StatusOr<DataSlice> operator()(const DataBagPtr& db,
                                       const DataSlice& value) const {
    return value.WithDb(db);
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
  absl::StatusOr<DataSlice> operator()(
      const DataBagPtr& db,
      const std::vector<absl::string_view>& attr_names,
      const std::vector<DataSlice>& values) const;

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
  absl::StatusOr<DataSlice> operator()(
      const DataBagPtr& db,
      DataSlice::JaggedShape shape,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values) const;

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
  absl::StatusOr<DataSlice> operator()(
      const DataBagPtr& db,
      const DataSlice& shape_and_mask_from,
      absl::Span<const absl::string_view> attr_names,
      absl::Span<const DataSlice> values) const;

  // Convert a DataSlice into an Object. If DataSlice is primitive or an entity,
  // it converts it into an Object. If it is already an Object, it returns this
  // DataSlice. Otherwise, returns an appropriate error.
  // NOTE: Adoption of `value.GetDb()` is the caller's responsibility.
  absl::StatusOr<DataSlice> operator()(const DataBagPtr& db,
                                       const DataSlice& value) const;
};

// Creates a DataSlice whose items are Fingerprints identifying `args`.
//
// In order to create a different "Type" from the same arguments, use `seed` key
// with the desired value, e.g.
//
// db_ops.uuid(seed='type_1', x=[1, 2, 3], y=[4, 5, 6])
//
// and
//
// db_ops.uuid(seed='type_2', x=[1, 2, 3], y=[4, 5, 6])
//
// have different ids.
absl::StatusOr<DataSlice> CreateUuidFromFields(
    absl::string_view seed,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& values);

struct UuObjectCreator {
  // Returns a UuObject (DataSlice of UuIds generated as row-wise fingerprints
  // from attribute names and values) with a reference to `db`) and attributes
  // `attr_names` set to `values`. The output DataSlice is a DataItem if all
  // `values` are DataItems or `attr_names` and `values` are empty. Otherwise,
  // the result has the shape of an input DataSlice with the highest rank. All
  // inputs have to be "broadcastable" to a DataSlice with the highest rank,
  // otherwise an error is returned.
  //
  // The returned Object's __schema__ attribute is implicit schema slice (each
  // schema item in this schema slice is a different allocated schema object).
  // Each of them has `attr_names` attributes set to schemas of `values`.
  absl::StatusOr<DataSlice> operator()(
      const DataBagPtr& db,
      absl::string_view seed,
      const std::vector<absl::string_view>& attr_names,
      const std::vector<DataSlice>& values) const;
};

struct UuSchemaCreator {
  // Returns a UuSchema (DataItem generated as a row-wise fingerprint
  // from attribute names and schemas) with a reference to `db`) and attributes
  // `attr_names` set to `schemas`.
  absl::StatusOr<DataSlice> operator()(
      const DataBagPtr& db,
      absl::string_view seed,
      const std::vector<absl::string_view>& attr_names,
      const std::vector<DataSlice>& schemas) const;
};

struct SchemaCreator {
  // Returns an allocated schema with attributes
  // `attr_names` set to `schemas` in `db`.
  absl::StatusOr<DataSlice> operator()(
      const DataBagPtr& db,
      const std::vector<absl::string_view>& attr_names,
      const std::vector<DataSlice>& schemas) const;
};

// Creates dict schema with the given keys and values schemas.
absl::StatusOr<internal::DataItem> CreateDictSchema(
    const DataBagPtr& db, const DataSlice& key_schema,
    const DataSlice& value_schema);

// Creates dicts with the given shape. If `keys` and `values` are provided, they
// will be set to the dicts after creation (that implies potential type casting
// and broadcasting). If `key_schema` and `value_schema` are not provided, they
// will be taken from `keys` and `values` or defaulted to OBJECT.
absl::StatusOr<DataSlice> CreateDictShaped(
    const std::shared_ptr<DataBag>& db, DataSlice::JaggedShape shape,
    const std::optional<DataSlice>& keys,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema = std::nullopt,
    const std::optional<DataSlice>& key_schema = std::nullopt,
    const std::optional<DataSlice>& value_schema = std::nullopt);

// Creates dicts with the given shape_and_mask_from. If `keys` and `values` are
// provided, they will be set to the dicts after creation (that implies
// potential type casting and broadcasting). If `key_schema` and `value_schema`
// are not provided, they will be taken from `keys` and `values` or defaulted to
// OBJECT.
absl::StatusOr<DataSlice> CreateDictLike(
    const std::shared_ptr<DataBag>& db, const DataSlice& shape_and_mask_from,
    const std::optional<DataSlice>& keys,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema = std::nullopt,
    const std::optional<DataSlice>& key_schema = std::nullopt,
    const std::optional<DataSlice>& value_schema = std::nullopt);

// Creates list schema with the given item schema.
absl::StatusOr<internal::DataItem> CreateListSchema(
    const DataBagPtr& db, const DataSlice& item_schema);

// Creates a single empty list. If `item_schema` is not provided, it will be
// taken from `values` or defaulted to OBJECT.
absl::StatusOr<DataSlice> CreateEmptyList(
    const std::shared_ptr<DataBag>& db,
    const std::optional<DataSlice>& schema = std::nullopt,
    const std::optional<DataSlice>& item_schema = std::nullopt);

// Creates a slice of lists with given values. The dimension of the resulted
// slice will be one less than the dimension of the values. If `item_schema` is
// not provided, it will be taken from `values` or defaulted to OBJECT.
absl::StatusOr<DataSlice> CreateListsFromLastDimension(
    const std::shared_ptr<DataBag>& db, const DataSlice& values,
    const std::optional<DataSlice>& schema = std::nullopt,
    const std::optional<DataSlice>& item_schema = std::nullopt);

// Creates a list from values. If `values` dimension is more than one, the list
// will contain other lists. If `item_schema` is not provided, it will be
// taken from `values`.
absl::StatusOr<DataSlice> CreateNestedList(
    const std::shared_ptr<DataBag>& db, const DataSlice& values,
    const std::optional<DataSlice>& schema = std::nullopt,
    const std::optional<DataSlice>& item_schema = std::nullopt);

// Creates a DataSlice of lists with the provided shape. If `values` are
// provided, they will be appended to the lists after creation (that implies
// potential type casting and broadcasting). If `item_schema` is not provided,
// it will be taken from `values` or defaulted to OBJECT.
absl::StatusOr<DataSlice> CreateListShaped(
    const std::shared_ptr<DataBag>& db, DataSlice::JaggedShape shape,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema = std::nullopt,
    const std::optional<DataSlice>& item_schema = std::nullopt);

// Creates empty lists of the given shape_and_mask_from. If `values` are
// provided, they will be appended to the lists after creation (that implies
// potential type casting and broadcasting). If `item_schema` is not provided,
// it will be taken from `values` or defaulted to OBJECT.
absl::StatusOr<DataSlice> CreateListLike(
    const std::shared_ptr<DataBag>& db, const DataSlice& shape_and_mask_from,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema = std::nullopt,
    const std::optional<DataSlice>& item_schema = std::nullopt);

// Creates a NoFollow schema from `target_schema`. `target_schema` must be a
// valid schema slice. If `target_schema` is NoFollow, primitive, ITEMID or ANY
// schema, the error is returned.
//
// CreateNoFollowSchema is reversible with `ds.GetActualSchema()`.
absl::StatusOr<DataSlice> CreateNoFollowSchema(const DataSlice& target_schema);

// Returns a new DataSlice with same contents as `target`, but with NoFollow
// schema created from `target.GetSchema()` slice. NoFollow, Primitive, ITEMID
// and ANY slices are not accepted and the appropriate error is returned.
absl::StatusOr<DataSlice> NoFollow(const DataSlice& target);

}  // namespace koladata

#endif  // KOLADATA_OBJECT_FACTORIES_H_
