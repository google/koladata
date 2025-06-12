# Copyright 2025 Google LLC
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

"""DataBag abstraction."""

from __future__ import annotations

import functools
from typing import Any, Iterable

from arolla import arolla
from koladata.expr import py_expr_eval_py_ext as _py_expr_eval_py_ext
from koladata.types import data_bag_py_ext as _data_bag_py_ext
from koladata.types import data_slice_py_ext as _data_slice_py_ext
from koladata.types import general_eager_ops as _general_eager_ops
from koladata.types import jagged_shape as _jagged_shape


DataBag = _data_bag_py_ext.DataBag
_eval_op = _py_expr_eval_py_ext.eval_op


### Implementation of the DataBag's additional functionality.
_DataSlice = _data_slice_py_ext.DataSlice


def _getitem(self: DataBag, sl: _DataSlice) -> _DataSlice:
  """Returns a DataSlice `sl` with `self` DataBag attached to it."""
  if not isinstance(sl, _DataSlice):
    raise TypeError(f'expected DataSlice, got {type(sl).__name__}')
  return sl.with_bag(self)


def _dict(
    self: DataBag,
    /,
    items_or_keys: dict[Any, Any] | _DataSlice | None = None,
    values: _DataSlice | None = None,
    *,
    key_schema: _DataSlice | None = None,
    value_schema: _DataSlice | None = None,
    schema: _DataSlice | None = None,
    itemid: _DataSlice | None = None,
) -> _DataSlice:  # pylint: disable=g-doc-args
  """Creates a Koda dict.

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
  dict(kd.slice([1, 2]), kd.slice([3, 4])) -> returns a dict, mapping 1->3 and
  2->4
  dict(kd.slice([[1], [2]]), kd.slice([3, 4])) -> returns two dicts, one
  mapping
    1->3 and another mapping 2->4
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
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting dicts.

  Returns:
    A DataSlice with the dict.
  """
  dict_shape = _jagged_shape.JaggedShape.from_edges()
  if itemid is not None:
    dict_shape = itemid.get_shape()
  elif (
      isinstance(items_or_keys, _DataSlice)
      and items_or_keys.get_shape().rank() != 0
  ):
    dict_shape = items_or_keys.get_shape()[:-1]
  return _dict_shaped(
      self,
      dict_shape,
      items_or_keys=items_or_keys,
      values=values,
      key_schema=key_schema,
      value_schema=value_schema,
      schema=schema,
      itemid=itemid,
  )


def _dict_like(
    self: DataBag,
    shape_and_mask_from: _DataSlice,
    /,
    items_or_keys: dict[Any, Any] | _DataSlice | None = None,
    values: _DataSlice | None = None,
    *,
    key_schema: _DataSlice | None = None,
    value_schema: _DataSlice | None = None,
    schema: _DataSlice | None = None,
    itemid: _DataSlice | None = None,
) -> _DataSlice:
  """Creates new Koda dicts with shape and sparsity of `shape_and_mask_from`.

  If items_or_keys and values are not provided, creates empty dicts. Otherwise,
  the function assigns the given keys and values to the newly created dicts. So
  the keys and values must be either broadcastable to shape_and_mask_from
  shape, or one dimension higher.

  Args:
    self: the DataBag.
    shape_and_mask_from: a DataSlice with the shape and sparsity for the desired
      dicts.
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
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting dicts.

  Returns:
    A DataSlice with the dicts.
  """
  return self._dict_like(  # pylint: disable=protected-access
      shape_and_mask_from,
      items_or_keys,
      values,
      key_schema,
      value_schema,
      schema,
      itemid,
  )


def _dict_shaped(
    self: DataBag,
    shape: _jagged_shape.JaggedShape,
    /,
    items_or_keys: dict[Any, Any] | _DataSlice | None = None,
    values: _DataSlice | None = None,
    *,
    key_schema: _DataSlice | None = None,
    value_schema: _DataSlice | None = None,
    schema: _DataSlice | None = None,
    itemid: _DataSlice | None = None,
) -> _DataSlice:
  """Creates new Koda dicts with the given shape.

  If items_or_keys and values are not provided, creates empty dicts. Otherwise,
  the function assigns the given keys and values to the newly created dicts. So
  the keys and values must be either broadcastable to `shape` or one dimension
  higher.

  Args:
    self: the DataBag.
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
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting dicts.

  Returns:
    A DataSlice with the dicts.
  """
  return self._dict_shaped(  # pylint: disable=protected-access
      shape, items_or_keys, values, key_schema, value_schema, schema, itemid
  )


def _list(
    self: DataBag,
    /,
    items: list[Any] | _DataSlice | None = None,
    *,
    item_schema: _DataSlice | None = None,
    schema: _DataSlice | None = None,
    itemid: _DataSlice | None = None,
) -> _DataSlice:  # pylint: disable=g-doc-args
  """Creates list(s) by collapsing `items`.

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

  Returns:
    A DataSlice with the list/lists.
  """
  return self._list(items, item_schema, schema, itemid)  # pylint: disable=protected-access


def _list_shaped(
    self: DataBag,
    shape: _jagged_shape.JaggedShape,
    /,
    items: list[Any] | _DataSlice | None = None,
    *,
    item_schema: _DataSlice | None = None,
    schema: _DataSlice | None = None,
    itemid: _DataSlice | None = None,
) -> _DataSlice:  # pylint: disable=g-doc-args
  """Creates new Koda lists with the given shape.

  Args:
    shape: the desired shape.
    items: optional items to assign to the newly created lists. If not given,
      the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

  Returns:
    A DataSlice with the lists.
  """
  return self._list_shaped(shape, items, item_schema, schema, itemid)  # pylint: disable=protected-access


def _list_like(
    self: DataBag,
    shape_and_mask_from: _DataSlice,
    /,
    items: list[Any] | _DataSlice | None = None,
    *,
    item_schema: _DataSlice | None = None,
    schema: _DataSlice | None = None,
    itemid: _DataSlice | None = None,
) -> _DataSlice:  # pylint: disable=g-doc-args
  """Creates new Koda lists with shape and sparsity of `shape_and_mask_from`.

  Args:
    shape_and_mask_from: a DataSlice with the shape and sparsity for the desired
      lists.
    items: optional items to assign to the newly created lists. If not given,
      the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

  Returns:
    A DataSlice with the lists.
  """
  return self._list_like(  # pylint: disable=protected-access
      shape_and_mask_from,
      items,
      item_schema,
      schema,
      itemid,
  )


def _implode(
    self: DataBag,
    x: _DataSlice,
    /,
    ndim: int | _DataSlice = 1,
    itemid: _DataSlice | None = None,
) -> _DataSlice:  # pylint: disable=g-doc-args
  """Implodes a Dataslice `x` a specified number of times.

  A single list "implosion" converts a rank-(K+1) DataSlice of T to a rank-K
  DataSlice of LIST[T], by folding the items in the last dimension of the
  original DataSlice into newly-created Lists.

  If `ndim` is set to a non-negative integer, implodes recursively `ndim` times.

  If `ndim` is set to a negative integer, implodes as many times as possible,
  until the result is a DataItem (i.e. a rank-0 DataSlice) containing a single
  nested List.

  The specified `db` is used to create any new Lists, and is the DataBag of the
  result DataSlice. If `db` is not specified, a new, empty DataBag is created
  for this purpose.

  Args:
    x: the DataSlice to implode
    ndim: the number of implosion operations to perform
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where Lists are created from

  Returns:
    DataSlice of nested Lists
  """
  if isinstance(ndim, _DataSlice):
    ndim = ndim.internal_as_py()
  return self._implode(x, ndim, itemid)  # pylint: disable=protected-access


def _concat_lists(self: DataBag, /, *lists: _DataSlice) -> _DataSlice:  # pylint: disable=g-doc-args
  """Returns a DataSlice of Lists concatenated from the List items of `lists`.

  Each input DataSlice must contain only present List items, and the item
  schemas of each input must be compatible. Input DataSlices are aligned (see
  `kd.align`) automatically before concatenation.

  If `lists` is empty, this returns a single empty list.

  The specified `db` is used to create the new concatenated lists, and is the
  DataBag used by the result DataSlice. If `db` is not specified, a new DataBag
  is created for this purpose.

  Args:
    *lists: the DataSlices of Lists to concatenate
    db: optional DataBag to populate with the result

  Returns:
    DataSlice of concatenated Lists
  """
  return self._concat_lists(*lists)  # pylint: disable=protected-access


def _freeze(self: DataBag) -> DataBag:
  """Returns a frozen DataBag equivalent to `self`."""
  return _eval_op('kd.freeze', self)


def _merge_inplace(
    self: DataBag,
    other_bags: DataBag | Iterable[DataBag],
    /,
    *,
    overwrite: bool = True,
    allow_data_conflicts: bool = True,
    allow_schema_conflicts: bool = False,
) -> DataBag:  # pylint: disable=g-doc-args
  """Copies all data from `other_bags` to this DataBag.

  Args:
    other_bags: Either a DataBag or a list of DataBags to merge into the current
      DataBag.
    overwrite: In case of conflicts, whether the new value (or the rightmost of
      the new values, if multiple) should be used instead of the old value. Note
      that this flag has no effect when allow_data_conflicts=False and
      allow_schema_conflicts=False. Note that db1.fork().inplace_merge(db2,
      overwrite=False) and db2.fork().inplace_merge(db1, overwrite=True) produce
      the same result.
    allow_data_conflicts: Whether we allow the same attribute to have different
      values in the bags being merged. When True, the overwrite= flag controls
      the behavior in case of a conflict. By default, both this flag and
      overwrite= are True, so we overwrite with the new values in case of a
      conflict.
    allow_schema_conflicts: Whether we allow the same attribute to have
      different types in an explicit schema. Note that setting this flag to True
      can be dangerous, as there might be some objects with the old schema that
      are not overwritten, and therefore will end up in an inconsistent state
      with their schema after the overwrite. When True, overwrite= flag controls
      the behavior in case of a conflict.

  Returns:
    self, so that multiple DataBag modifications can be chained.
  """
  if isinstance(other_bags, DataBag):
    other_bags = [other_bags]
  self._merge_inplace(  # pylint: disable=protected-access
      overwrite, allow_data_conflicts, allow_schema_conflicts, *other_bags
  )
  return self


def _enriched_bag(*dbs) -> DataBag:
  """Returns a merged DataBag with priority for DataBags earlier in the list."""
  return _eval_op('kd.enriched_bag', *dbs)


def _updated_bag(*dbs) -> DataBag:
  """Returns a merged DataBag with priority for DataBags later in the list."""
  return _eval_op('kd.updated_bag', *dbs)


class ContentsReprWrapper:

  def __init__(self, contents_repr: str):
    self._contents_repr = contents_repr

  def __repr__(self):
    return self._contents_repr


def _contents_repr(
    self: DataBag, /, *, triple_limit: int = 1000
) -> ContentsReprWrapper:
  """Returns a representation of the DataBag contents."""
  return ContentsReprWrapper(
      self._contents_repr(triple_limit=triple_limit)  # pylint: disable=protected-access
  )


def _data_triples_repr(
    self: DataBag, *, triple_limit: int = 1000
) -> ContentsReprWrapper:
  """Returns a representation of the DataBag contents, omitting schema triples."""
  return ContentsReprWrapper(
      self._data_triples_repr(triple_limit=triple_limit)  # pylint: disable=protected-access
  )


def _schema_triples_repr(
    self: DataBag, *, triple_limit: int = 1000
) -> ContentsReprWrapper:
  """Returns a representation of schema triples in the DataBag."""
  return ContentsReprWrapper(
      self._schema_triples_repr(  # pylint: disable=protected-access
          triple_limit=triple_limit
      )
  )


DataBag.__getitem__ = _getitem
DataBag.dict = _dict
DataBag.dict_like = _dict_like
DataBag.dict_shaped = _dict_shaped
DataBag.list = _list
DataBag.list_shaped = _list_shaped
DataBag.list_like = _list_like
DataBag.implode = _implode
DataBag.concat_lists = _concat_lists
DataBag.freeze = _freeze
DataBag.merge_inplace = _merge_inplace
DataBag.with_name = _general_eager_ops.with_name
DataBag.__lshift__ = _updated_bag
DataBag.__rshift__ = _enriched_bag
DataBag.contents_repr = _contents_repr
DataBag.data_triples_repr = _data_triples_repr
DataBag.schema_triples_repr = _schema_triples_repr


class NullDataBag(arolla.abc.QValue):
  """QValue specialization for null DataBag.

  In C++, this QValue is represented as a nullptr of DataBagPtr type.
  """

  def py_value(self) -> None:
    return None

  def with_name(self, name: str | arolla.types.Text) -> NullDataBag:
    return _general_eager_ops.with_name(self, name)


arolla.abc.register_qvalue_specialization(
    '::koladata::python::NullDataBag', NullDataBag
)


@functools.cache
def null_bag():
  """Returns an instance of a null DataBag."""
  return _eval_op('kd.get_bag', _DataSlice.from_vals(None))
