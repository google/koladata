# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Koda functions for creating various objects."""

from typing import Any

from arolla import arolla
from koladata.types import data_bag
from koladata.types import data_item as _  # pylint: disable=unused-import
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import schema_constants


bag = data_bag.DataBag.empty


def list_(
    items: Any | None = None,
    *,
    item_schema: data_slice.DataSlice | None = None,
    schema: data_slice.DataSlice | None = None,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None
) -> data_slice.DataSlice:
  """Creates list(s) by collapsing `items`.

  Returns an immutable list if `db` is not provided.

  If there is no argument, returns an empty Koda List.
  If the argument is a Python list, creates a nested Koda List.

  Examples:
  list() -> a single empty Koda List
  list([1, 2, 3]) -> Koda List with items 1, 2, 3
  list([[1, 2, 3], [4, 5]]) -> nested Koda List [[1, 2, 3], [4, 5]]
    # items are Koda lists.

  Args:
    items: The items to use. If not specified, an empty list of OBJECTs will be
      created.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where list(s) are created.

  Returns:
    The slice with list/lists.
  """
  if db is None:
    # TODO: Find a better way in order to avoid calling
    # `freeze_bag`. One alternative is to call `eval_op('list', ...)` however it
    # has different boxing rules.
    return bag().list(
        items=items, item_schema=item_schema, schema=schema, itemid=itemid,
    ).freeze_bag()
  return db.list(
      items=items, item_schema=item_schema, schema=schema, itemid=itemid,
  )


def list_like(
    shape_and_mask_from: data_slice.DataSlice,
    /,
    items: list[Any] | data_slice.DataSlice | None = None,
    *,
    item_schema: data_slice.DataSlice | None = None,
    schema: data_slice.DataSlice | None = None,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
) -> data_slice.DataSlice:
  """Creates new Koda lists with shape and sparsity of `shape_and_mask_from`.

  Returns immutable lists if `db` is not provided.

  Args:
    shape_and_mask_from: a DataSlice with the shape and sparsity for the
      desired lists.
    items: optional items to assign to the newly created lists. If not
      given, the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where lists are created.

  Returns:
    A DataSlice with the lists.
  """
  if db is None:
    # TODO: Find a better way in order to avoid calling
    # `freeze_bag`. One alternative is to call `eval_op('list_like', ...)`
    # however it has different boxing rules.
    return bag().list_like(
        shape_and_mask_from, items=items, item_schema=item_schema,
        schema=schema, itemid=itemid,
    ).freeze_bag()
  return db.list_like(
      shape_and_mask_from, items=items, item_schema=item_schema, schema=schema,
      itemid=itemid,
  )


def list_shaped(
    shape: jagged_shape.JaggedShape,
    /,
    items: list[Any] | data_slice.DataSlice | None = None,
    *,
    item_schema: data_slice.DataSlice | None = None,
    schema: data_slice.DataSlice | None = None,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
) -> data_slice.DataSlice:
  """Creates new Koda lists with the given shape.

  Returns immutable lists if `db` is not provided.

  Args:
    shape: the desired shape.
    items: optional items to assign to the newly created lists. If not
      given, the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where lists are created.

  Returns:
    A DataSlice with the lists.
  """
  if db is None:
    # TODO: Find a better way in order to avoid calling
    # `freeze_bag`. One alternative is to call `eval_op('list_shaped', ...)`
    # however it has different boxing rules.
    return bag().list_shaped(
        shape, items=items, item_schema=item_schema, schema=schema,
        itemid=itemid,
    ).freeze_bag()
  return db.list_shaped(
      shape, items=items, item_schema=item_schema, schema=schema, itemid=itemid,
  )


def list_shaped_as(
    shape_from: data_slice.DataSlice,
    /,
    items: list[Any] | data_slice.DataSlice | None = None,
    *,
    item_schema: data_slice.DataSlice | None = None,
    schema: data_slice.DataSlice | None = None,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
) -> data_slice.DataSlice:
  """Creates new Koda lists with shape of the given DataSlice.

  Returns immutable lists if `db` is not provided.

  Args:
    shape_from: mandatory DataSlice, whose shape the returned DataSlice will
      have.
    items: optional items to assign to the newly created lists. If not given,
      the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where lists are created.

  Returns:
    A DataSlice with the lists.
  """
  return list_shaped(
      shape_from.get_shape(),
      items=items,
      item_schema=item_schema,
      schema=schema,
      itemid=itemid,
      db=db,
  )


def dict_(
    items_or_keys: Any | None = None, values: Any | None = None,
    *,
    key_schema: data_slice.DataSlice | None = None,
    value_schema: data_slice.DataSlice | None = None,
    schema: data_slice.DataSlice | None = None,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None
) -> data_slice.DataSlice:
  """Creates a Koda dict.

  Returns an immutable dict if `db` is not provided.

  Acceptable arguments are:
    1) no argument: a single empty dict
    2) a Python dict whose keys are either primitives or DataItems and values
       are primitives, DataItems, Python list/dict which can be converted to a
       List/Dict DataItem, or a DataSlice which can folded into a List DataItem:
       a single dict
    3) two DataSlices/DataItems as keys and values: a DataSlice of dicts whose
       shape is the last N-1 dimensions of keys/values DataSlice

  Examples:
  dict() -> returns a single new dict
  dict({1: 2, 3: 4}) -> returns a single new dict
  dict({1: [1, 2]}) -> returns a single dict, mapping 1->List[1, 2]
  dict({1: kd.slice([1, 2])}) -> returns a single dict, mapping 1->List[1, 2]
  dict({db.uuobj(x=1, y=2): 3}) -> returns a single dict, mapping uuid->3
  dict(kd.slice([1, 2]), kd.slice([3, 4]))
    -> returns a dict ({1: 3, 2: 4})
  dict(kd.slice([[1], [2]]), kd.slice([3, 4]))
    -> returns a 1-D DataSlice that holds two dicts ({1: 3} and {2: 4})
  dict('key', 12) -> returns a single dict mapping 'key'->12

  Args:
    items_or_keys: a Python dict in case of items and a DataSlice in case of
      keys.
    values: a DataSlice. If provided, `items_or_keys` must be a DataSlice as
      keys.
    key_schema: the schema of the dict keys. If not specified, it will be
      deduced from keys or defaulted to OBJECT.
    value_schema: the schema of the dict values. If not specified, it will be
      deduced from values or defaulted to OBJECT.
    schema: The schema to use for the newly created Dict. If specified, then
        key_schema and value_schema must not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where dict(s) are created.

  Returns:
    A DataSlice with the dict.
  """
  if db is None:
    # TODO: Find a better way in order to avoid calling
    # `freeze_bag`. One alternative is to call `eval_op('dict', ...)` however it
    # has different boxing rules.
    return bag().dict(
        items_or_keys=items_or_keys,
        values=values,
        key_schema=key_schema,
        value_schema=value_schema,
        schema=schema,
        itemid=itemid,
    ).freeze_bag()
  return db.dict(
      items_or_keys=items_or_keys,
      values=values,
      key_schema=key_schema,
      value_schema=value_schema,
      schema=schema,
      itemid=itemid,
  )


def dict_like(
    shape_and_mask_from: data_slice.DataSlice,
    /,
    items_or_keys: Any | None = None,
    values: Any | None = None,
    *,
    key_schema: data_slice.DataSlice | None = None,
    value_schema: data_slice.DataSlice | None = None,
    schema: data_slice.DataSlice | None = None,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
) -> data_slice.DataSlice:
  """Creates new Koda dicts with shape and sparsity of `shape_and_mask_from`.

  Returns immutable dicts if `db` is not provided.

  If items_or_keys and values are not provided, creates empty dicts. Otherwise,
  the function assigns the given keys and values to the newly created dicts. So
  the keys and values must be either broadcastable to shape_and_mask_from
  shape, or one dimension higher.

  Args:
    shape_and_mask_from: a DataSlice with the shape and sparsity for the
      desired dicts.
    items_or_keys: either a Python dict (if `values` is None) or a DataSlice
      with keys. The Python dict case is supported only for scalar
      shape_and_mask_from.
    values: a DataSlice of values, when `items_or_keys` represents keys.
    key_schema: the schema of the dict keys. If not specified, it will be
      deduced from keys or defaulted to OBJECT.
    value_schema: the schema of the dict values. If not specified, it will be
      deduced from values or defaulted to OBJECT.
    schema: The schema to use for the newly created Dict. If specified, then
        key_schema and value_schema must not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where dicts are created.

  Returns:
    A DataSlice with the dicts.
  """
  if db is None:
    # TODO: Find a better way in order to avoid calling
    # `freeze_bag`. One alternative is to call `eval_op('dict_like', ...)`
    # however it has different boxing rules.
    return bag().dict_like(
        shape_and_mask_from,
        items_or_keys=items_or_keys,
        values=values,
        key_schema=key_schema,
        value_schema=value_schema,
        schema=schema,
        itemid=itemid,
    ).freeze_bag()
  return db.dict_like(
      shape_and_mask_from,
      items_or_keys=items_or_keys,
      values=values,
      key_schema=key_schema,
      value_schema=value_schema,
      schema=schema,
      itemid=itemid,
  )


def dict_shaped(
    shape: jagged_shape.JaggedShape,
    /,
    items_or_keys: Any | None = None,
    values: Any | None = None,
    key_schema: data_slice.DataSlice | None = None,
    value_schema: data_slice.DataSlice | None = None,
    schema: data_slice.DataSlice | None = None,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
) -> data_slice.DataSlice:
  """Creates new Koda dicts with the given shape.

  Returns immutable dicts if `db` is not provided.

  If items_or_keys and values are not provided, creates empty dicts. Otherwise,
  the function assigns the given keys and values to the newly created dicts. So
  the keys and values must be either broadcastable to `shape` or one dimension
  higher.

  Args:
    shape: the desired shape.
    items_or_keys: either a Python dict (if `values` is None) or a DataSlice
      with keys. The Python dict case is supported only for scalar shape.
    values: a DataSlice of values, when `items_or_keys` represents keys.
    key_schema: the schema of the dict keys. If not specified, it will be
      deduced from keys or defaulted to OBJECT.
    value_schema: the schema of the dict values. If not specified, it will be
      deduced from values or defaulted to OBJECT.
    schema: The schema to use for the newly created Dict. If specified, then
        key_schema and value_schema must not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: Optional DataBag where dicts are created.

  Returns:
    A DataSlice with the dicts.
  """
  if db is None:
    # TODO: Find a better way in order to avoid calling
    # `freeze_bag`. One alternative is to call `eval_op('dict_shaped', ...)`
    # however it has different boxing rules.
    return bag().dict_shaped(
        shape,
        items_or_keys=items_or_keys,
        values=values,
        key_schema=key_schema,
        value_schema=value_schema,
        schema=schema,
        itemid=itemid,
    ).freeze_bag()
  return db.dict_shaped(
      shape,
      items_or_keys=items_or_keys,
      values=values,
      key_schema=key_schema,
      value_schema=value_schema,
      schema=schema,
      itemid=itemid,
  )


def dict_shaped_as(
    shape_from: data_slice.DataSlice,
    /,
    items_or_keys: Any | None = None,
    values: Any | None = None,
    key_schema: data_slice.DataSlice | None = None,
    value_schema: data_slice.DataSlice | None = None,
    schema: data_slice.DataSlice | None = None,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
) -> data_slice.DataSlice:
  """Creates new Koda dicts with shape of the given DataSlice.

  Returns immutable dicts if `db` is not provided.

  If items_or_keys and values are not provided, creates empty dicts. Otherwise,
  the function assigns the given keys and values to the newly created dicts. So
  the keys and values must be either broadcastable to `shape` or one dimension
  higher.

  Args:
    shape_from: mandatory DataSlice, whose shape the returned DataSlice will
      have.
    items_or_keys: either a Python dict (if `values` is None) or a DataSlice
      with keys. The Python dict case is supported only for scalar shape.
    values: a DataSlice of values, when `items_or_keys` represents keys.
    key_schema: the schema of the dict keys. If not specified, it will be
      deduced from keys or defaulted to OBJECT.
    value_schema: the schema of the dict values. If not specified, it will be
      deduced from values or defaulted to OBJECT.
    schema: The schema to use for the newly created Dict. If specified, then
      key_schema and value_schema must not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: Optional DataBag where dicts are created.

  Returns:
    A DataSlice with the dicts.
  """
  return dict_shaped(
      shape_from.get_shape(),
      items_or_keys=items_or_keys,
      values=values,
      key_schema=key_schema,
      value_schema=value_schema,
      schema=schema,
      itemid=itemid,
      db=db,
  )


def new(
    arg: Any = arolla.unspecified(),
    /,
    *,
    schema: data_slice.DataSlice | str | None = None,
    update_schema: bool = False,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
    **attrs: Any,
) -> data_slice.DataSlice:
  """Creates Entities with given attrs.

  Returns an immutable Entity if `db` is not provided.

  Args:
    arg: optional Python object to be converted to an Entity.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
      Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
      schema instead. You can also pass schema='name' as a shortcut for
      schema=kd.named_schema('name').
    update_schema: if schema attribute is missing and the attribute is being set
      through `attrs`, schema is successfully updated.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
      itemid will only be set when the args is not a primitive or primitive
      slice if args present.
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
  """
  if db is None:
    # TODO: Find a better way in order to avoid calling
    # `freeze_bag`. One alternative is to call `eval_op('new', ...)` however it
    # has different boxing rules.
    return bag().new(
        arg, schema=schema, update_schema=update_schema, itemid=itemid, **attrs
    ).freeze_bag()
  return db.new(
      arg, schema=schema, update_schema=update_schema, itemid=itemid, **attrs
  )


def new_shaped(
    shape: jagged_shape.JaggedShape,
    /,
    *,
    schema: data_slice.DataSlice | str | None = None,
    update_schema: bool = False,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
    **attrs: Any,
) -> data_slice.DataSlice:
  """Creates new Entities with the given shape.

  Returns immutable Entities if `db` is not provided.

  Args:
    shape: JaggedShape that the returned DataSlice will have.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
      Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
      schema instead. You can also pass schema='name' as a shortcut for
      schema=kd.named_schema('name').
    update_schema: if schema attribute is missing and the attribute is being set
      through `attrs`, schema is successfully updated.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
  """
  if db is None:
    # TODO: Find a better way in order to avoid calling
    # `freeze_bag`. One alternative is to call `eval_op('new_shaped', ...)`
    # however it has different boxing rules.
    return bag().new_shaped(
        shape, schema=schema, update_schema=update_schema, itemid=itemid,
        **attrs
    ).freeze_bag()
  return db.new_shaped(
      shape, schema=schema, update_schema=update_schema, itemid=itemid, **attrs
  )


def new_shaped_as(
    shape_from: data_slice.DataSlice,
    /,
    *,
    schema: data_slice.DataSlice | str | None = None,
    update_schema: bool = False,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
    **attrs: Any,
) -> data_slice.DataSlice:
  """Creates new Koda entities with shape of the given DataSlice.

  Returns immutable Entities if `db` is not provided.

  Args:
    shape_from: DataSlice, whose shape the returned DataSlice will have.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
      Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
      schema instead. You can also pass schema='name' as a shortcut for
      schema=kd.named_schema('name').
    update_schema: if schema attribute is missing and the attribute is being set
      through `attrs`, schema is successfully updated.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
  """
  return new_shaped(
      shape_from.get_shape(),
      schema=schema,
      update_schema=update_schema,
      itemid=itemid,
      db=db,
      **attrs,
  )


def new_like(
    shape_and_mask_from: data_slice.DataSlice,
    /,
    *,
    schema: data_slice.DataSlice | str | None = None,
    update_schema: bool = False,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
    **attrs: Any,
) -> data_slice.DataSlice:
  """Creates new Entities with the shape and sparsity from shape_and_mask_from.

  Returns immutable Entities if `db` is not provided.

  Args:
    shape_and_mask_from: DataSlice, whose shape and sparsity the returned
      DataSlice will have.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
      Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
      schema instead. You can also pass schema='name' as a shortcut for
      schema=kd.named_schema('name').
    update_schema: if schema attribute is missing and the attribute is being set
      through `attrs`, schema is successfully updated.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
  """
  if db is None:
    # TODO: Find a better way in order to avoid calling
    # `freeze_bag`. One alternative is to call `eval_op('new_like', ...)`
    # however it has different boxing rules.
    return bag().new_like(
        shape_and_mask_from, schema=schema, update_schema=update_schema,
        itemid=itemid, **attrs
    ).freeze_bag()
  return db.new_like(
      shape_and_mask_from, schema=schema, update_schema=update_schema,
      itemid=itemid, **attrs
  )


def obj(
    arg: Any = arolla.unspecified(),
    /,
    *,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
    **attrs: Any
) -> data_slice.DataSlice:
  """Creates new Objects with an implicit stored schema.

  Returned DataSlice has OBJECT schema and is immutable if `db` is not provided.

  Args:
    arg: optional Python object to be converted to an Object.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
      itemid will only be set when the args is not a primitive or primitive
      slice if args presents.
    db: optional DataBag where object are created.
    **attrs: attrs to set on the returned object.

  Returns:
    data_slice.DataSlice with the given attrs and kd.OBJECT schema.
  """
  if db is None:
    return bag().obj(arg, itemid=itemid, **attrs).freeze_bag()
  return db.obj(arg, itemid=itemid, **attrs)


def container(
    *,
    db: data_bag.DataBag | None = None,
    **attrs: Any,
) -> data_slice.DataSlice:
  """Creates new Objects with an implicit stored schema.

  Returned DataSlice has OBJECT schema and mutable DataBag.

  Args:
    db: optional DataBag where object are created.
    **attrs: attrs to set on the returned object.

  Returns:
    data_slice.DataSlice with the given attrs and kd.OBJECT schema.
  """
  if db is None:
    db = bag()
  o = db.obj()
  o.set_attrs(**attrs)
  return o


def obj_shaped(
    shape: jagged_shape.JaggedShape,
    /,
    *,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
    **attrs: Any,
) -> data_slice.DataSlice:
  """Creates Objects with the given shape.

  Returned DataSlice has OBJECT schema and is immutable if `db` is not provided.

  Args:
    shape: JaggedShape that the returned DataSlice will have.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
  """
  if db is None:
    # TODO: Find a better way in order to avoid calling
    # `freeze_bag`. One alternative is to call `eval_op('obj_shaped', ...)`
    # however it has different boxing rules.
    return bag().obj_shaped(shape, itemid=itemid, **attrs).freeze_bag()
  return db.obj_shaped(shape, itemid=itemid, **attrs)


def obj_shaped_as(
    shape_from: data_slice.DataSlice,
    /,
    *,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
    **attrs: Any,
) -> data_slice.DataSlice:
  """Creates Objects with the shape of the given DataSlice.

  Returned DataSlice has OBJECT schema and is immutable if `db` is not provided.

  Args:
    shape_from: DataSlice, whose shape the returned DataSlice will have.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
  """
  return obj_shaped(shape_from.get_shape(), itemid=itemid, db=db, **attrs)


def obj_like(
    shape_and_mask_from: data_slice.DataSlice,
    /,
    *,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
    **attrs: Any,
) -> data_slice.DataSlice:
  """Creates Objects with shape and sparsity from shape_and_mask_from.

  Returned DataSlice has OBJECT schema and is immutable if `db` is not provided.

  Args:
    shape_and_mask_from: DataSlice, whose shape and sparsity the returned
      DataSlice will have.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
  """
  if db is None:
    # TODO: Find a better way in order to avoid calling
    # `freeze_bag`. One alternative is to call `eval_op('obj_shaped', ...)`
    # however it has different boxing rules.
    return bag().obj_like(
        shape_and_mask_from, itemid=itemid, **attrs
    ).freeze_bag()
  return db.obj_like(shape_and_mask_from, itemid=itemid, **attrs)


def uu(
    seed: str | None = None,
    *,
    schema: data_slice.DataSlice | None = None,
    update_schema: bool = False,
    db: data_bag.DataBag | None = None,
    **attrs: Any,
) -> data_slice.DataSlice:
  """Creates UuEntities with given attrs.

  Returns an immutable UU Entity if `db` is not provided.

  Args:
    seed: string to seed the uuid computation with.
    schema: optional DataSlice schema. If not specified, a UuSchema
      will be automatically created based on the schemas of the passed **attrs.
      Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
      schema instead.
    update_schema: if schema attribute is missing and the attribute is being set
      through `attrs`, schema is successfully updated.
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
  """
  if db is None:
    # TODO: Find a better way in order to avoid calling
    # `freeze_bag`. One alternative is to call `eval_op('uu', ...)` however it
    # has different boxing rules.
    return bag().uu(
        seed=seed, schema=schema, update_schema=update_schema, **attrs
    ).freeze_bag()
  return db.uu(seed=seed, schema=schema, update_schema=update_schema, **attrs)


def uuobj(
    seed: str | None = None,
    *,
    db: data_bag.DataBag | None = None,
    **attrs: Any,
) -> data_slice.DataSlice:
  """Creates object(s) whose ids are uuid(s) with the provided attributes.

  Returned DataSlice has OBJECT schema and is immutable if `db` is not provided.

  In order to create a different "Type" from the same arguments, use
  `seed` key with the desired value, e.g.

  kd.uuobj(seed='type_1', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

  and

  kd.uuobj(seed='type_2', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

  have different ids.

  Args:
    seed: (str) Allows different uuobj(s) to have different ids when created
      from the same inputs.
    db: optional DataBag where entities are created.
    **attrs: key-value pairs of object attributes where values are DataSlices
      or can be converted to DataSlices using kd.new / kd.obj.

  Returns:
    data_slice.DataSlice
  """
  if db is None:
    # TODO: Find a better way in order to avoid calling
    # `freeze_bag`. One alternative is to call `eval_op('uuobj', ...)` however
    # it has different boxing rules.
    return bag().uuobj(seed=seed, **attrs).freeze_bag()
  return db.uuobj(seed=seed, **attrs)


def empty_shaped(
    shape: data_slice.DataSlice,
    /,
    *,
    schema: data_slice.DataSlice = schema_constants.MASK,
    db: data_bag.DataBag | None = None,
) -> data_slice.DataSlice:
  """Creates a DataSlice of missing items with the given shape.

  If `schema` is an Entity schema and `db` is not provided, an empty Databag is
  created and attached to the resulting DataSlice and `schema` is adopted into
  the DataBag.

  Args:
    shape: Shape of the resulting DataSlice.
    schema: optional schema of the resulting DataSlice.
    db: optional DataBag to hold the schema if applicable.

  Returns:
    A DataSlice with the given shape.
  """
  return data_bag._empty_shaped(shape, schema, db)  # pylint: disable=protected-access


def empty_shaped_as(
    shape_from: data_slice.DataSlice,
    /,
    *,
    schema: data_slice.DataSlice = schema_constants.MASK,
    db: data_bag.DataBag | None = None,
) -> data_slice.DataSlice:
  """Creates a DataSlice of missing items with the shape of `shape_from`.

  If `schema` is an Entity schema and `db` is not provided, an empty Databag is
  created and attached to the resulting DataSlice and `schema` is adopted into
  the DataBag.

  Args:
    shape_from: used for the shape of the resulting DataSlice.
    schema: optional schema of the resulting DataSlice.
    db: optional DataBag to hold the schema if applicable.

  Returns:
    A DataSlice with the shape of the given DataSlice.
  """
  return empty_shaped(shape_from.get_shape(), schema=schema, db=db)


def implode(
    x: data_slice.DataSlice,
    /,
    ndim: int | data_slice.DataSlice = 1,
    itemid: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
) -> data_slice.DataSlice:
  """Implodes a Dataslice `x` a specified number of times.

  Returned lists are immutable if `db` is not provided.

  A single list "implosion" converts a rank-(K+1) DataSlice of T to a rank-K
  DataSlice of LIST[T], by folding the items in the last dimension of the
  original DataSlice into newly-created Lists.

  A single list implosion is equivalent to `kd.list(x, db)`.

  If `ndim` is set to a non-negative integer, implodes recursively `ndim` times.

  If `ndim` is set to a negative integer, implodes as many times as possible,
  until the result is a DataItem (i.e. a rank-0 DataSlice) containing a single
  nested List.

  The specified `db` is used to create any new Lists, and is the DataBag of the
  result DataSlice. If `db` is not specified, a new DataBag is created for this
  purpose.

  Args:
    x: the DataSlice to implode
    ndim: the number of implosion operations to perform
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where Lists are created from

  Returns:
    DataSlice of nested Lists
  """
  if db is None:
    return bag().implode(x, ndim, itemid).freeze_bag()
  return db.implode(x, ndim, itemid)


def concat_lists(
    *lists: data_slice.DataSlice,
    db: data_bag.DataBag | None = None
) -> data_slice.DataSlice:
  """Returns a DataSlice of Lists concatenated from the List items of `lists`.

  Returned lists are immutable if `db` is not provided.

  Each input DataSlice must contain only present List items, and the item
  schemas of each input must be compatible. Input DataSlices are aligned (see
  `kd.align`) automatically before concatenation.

  If `lists` is empty, this returns a single empty list with OBJECT item schema.

  The specified `db` is used to create the new concatenated lists, and is the
  DataBag used by the result DataSlice. If `db` is not specified, a new DataBag
  is created for this purpose.

  Args:
    *lists: the DataSlices of Lists to concatenate
    db: optional DataBag to populate with the result

  Returns:
    DataSlice of concatenated Lists
  """
  if db is None:
    # TODO: Find a better way in order to avoid calling
    # `freeze_bag`. One alternative is to call `eval_op('concat_lists', ...)`
    # however it has different boxing rules.
    return bag().concat_lists(*lists).freeze_bag()
  return db.concat_lists(*lists)
