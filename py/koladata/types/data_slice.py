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

"""DataSlice abstraction."""

import dataclasses
import functools
from typing import Any
import warnings

from arolla import arolla
from koladata.expr import py_expr_eval_py_ext as _py_expr_eval_py_ext
from koladata.types import data_bag_py_ext as _data_bag_py_ext
from koladata.types import data_slice_py_ext as _data_slice_py_ext
from koladata.types import general_eager_ops
from koladata.types import jagged_shape
from koladata.types import operator_lookup


DataBag = _data_bag_py_ext.DataBag
DataSlice = _data_slice_py_ext.DataSlice


def _add_method(cls, method_name: str):
  """Returns a callable / decorator to put it as a cls's method.

  Adds methods only to the class it was called on (then it is also available in
  cls' subclasses.

  Example on how to define methods:

    @DataSlice.add_method('flatten')
    def _flatten(self, from, to) -> DataSlice:
      return arolla.abc.aux_eval_op(method_impl_lookup.flatten, self, from, to)

  Args:
    cls: DataSlice or its subclass.
    method_name: wraps the Python function as a method with `method_name`.
  """

  # TODO: Find a way to have a sensible docstring that DataSlice
  # method should have (e.g. exclude first argument as it is `self`).
  def wrap(op):
    """Wraps eager `op` into a DataSlice method `method_name`."""
    cls.internal_register_reserved_class_method_name(method_name)
    setattr(cls, method_name, op)
    return op

  return wrap


def register_reserved_class_method_names(cls):
  """Decorates `cls` such that it registers the names of all its methods.

  A convenience decorator, so that statements such as:

  `cls.internal_register_reserved_class_method_name('some_method')`

  do not need to be written explicitly.

  Example:

    @DataSlice.subclass
    class SubDataSlice(DataSlice):

      def some_method(self):
        ...

    # Causes `some_method` to be a forbiden Koda attribute that cannot be
    # accessed through `__getattr__` or `__setattr__`, but `get_attr` and
    # `set_attr` methods need to be used.

  Args:
    cls: DataSlice subclass

  Returns:
    cls
  """
  assert issubclass(cls, DataSlice)
  for attr_name in dir(cls):
    if not attr_name.startswith('_') and callable(getattr(cls, attr_name)):
      cls.internal_register_reserved_class_method_name(attr_name)
  return cls


# IPython/Colab try to access those attributes on any object and expect
# particular behavior from them, so we disallow accessing such attributes
# on a DataSlice via __getattr__.
RESERVED_ATTRIBUTES_WITHOUT_LEADING_UNDERSCORE = frozenset([
    'add_method',
    'getdoc',
    'trait_names',
])


def _init_data_slice_class():
  """Initializes DataSlice class."""
  if hasattr(DataSlice, 'add_method'):
    return
  # NOTE: `add_method` is available in subclasses too.
  DataSlice.add_method = classmethod(_add_method)
  for name in RESERVED_ATTRIBUTES_WITHOUT_LEADING_UNDERSCORE:
    DataSlice.internal_register_reserved_class_method_name(name)


### Implementation of the DataSlice's additional functionality.


_init_data_slice_class()
# NOTE: Using OperatorLookup supports caching of already looked-up operators and
# thus provides quicker access to operators, compared to looking up operators by
# their names.
_op_impl_lookup = operator_lookup.OperatorLookup()


##### DataSlice methods. #####

# TODO: Remove these aliases.
DataSlice.add_method('with_db')(DataSlice.with_bag)
DataSlice.add_method('no_db')(DataSlice.no_bag)


@DataSlice.add_method('maybe')
def _maybe(self, attr_name: str) -> DataSlice:
  # NOTE: Calling `get_attr`, instead of aux_eval_op, because it is implemented
  # in C Python.
  return self.get_attr(attr_name, None)


@DataSlice.add_method('has_attr')
def _has_attr(self, attr_name: str) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.has_attr, self, attr_name)


@DataSlice.add_method('reshape')
def _reshape(self, shape: jagged_shape.JaggedShape) -> DataSlice:
  if not isinstance(shape, jagged_shape.JaggedShape):
    raise TypeError(f'`shape` must be a JaggedShape, got: {shape}')
  return arolla.abc.aux_eval_op(_op_impl_lookup.reshape, self, shape)


@DataSlice.add_method('flatten')
def _flatten(
    self, from_dim: Any = 0, to_dim: Any = arolla.unspecified()
) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.flatten, self, from_dim, to_dim)


@DataSlice.add_method('add_dim')
def _add_dim(self, sizes: Any) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.add_dim, self, sizes)


@DataSlice.add_method('repeat')
def _repeat(self, sizes: Any) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.repeat, self, sizes)


@DataSlice.add_method('select')
def _select(self, fltr: Any) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.select, self, fltr)


@DataSlice.add_method('select_present')
def _select_present(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.select_present, self)


@DataSlice.add_method('select_items')
def _select_items(self, fltr: Any) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.select_items, self, fltr)


@DataSlice.add_method('select_keys')
def _select_keys(self, fltr: Any) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.select_keys, self, fltr)


@DataSlice.add_method('select_values')
def _select_values(self, fltr: Any) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.select_values, self, fltr)


@DataSlice.add_method('expand_to')
def _expand_to(
    self, target: Any, ndim: Any = arolla.unspecified()
) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.expand_to, self, target, ndim)


@DataSlice.add_method('list_size')
def _list_size(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.list_size, self)


@DataSlice.add_method('dict_size')
def _dict_size(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.dict_size, self)


@DataSlice.add_method('dict_update')
def _dict_update(self, *args, **kwargs) -> DataBag:
  return arolla.abc.aux_eval_op(
      _op_impl_lookup.dict_update, self, *args, **kwargs
  )


@DataSlice.add_method('with_dict_update')
def _with_dict_update(self, *args, **kwargs) -> DataSlice:
  return arolla.abc.aux_eval_op(
      _op_impl_lookup.with_dict_update, self, *args, **kwargs
  )


@DataSlice.add_method('follow')
def _follow(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.follow, self)


@DataSlice.add_method('extract')
def _extract(self, schema: DataSlice = arolla.unspecified()) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.extract, self, schema)


@DataSlice.add_method('clone')
def _clone(
    self,
    itemid: DataSlice = arolla.unspecified(),
    schema: DataSlice = arolla.unspecified(),
    **overrides: Any,
) -> DataSlice:
  # TODO: Replace with aux_eval_op when it supports handling the
  # hidden_seed.
  return _py_expr_eval_py_ext.eval_expr(
      _op_impl_lookup.clone(self, itemid=itemid, schema=schema, **overrides)
  )


@DataSlice.add_method('shallow_clone')
def _shallow_clone(
    self,
    itemid: DataSlice = arolla.unspecified(),
    schema: DataSlice = arolla.unspecified(),
    **overrides: Any,
) -> DataSlice:
  # TODO: Replace with aux_eval_op when it supports handling the
  # hidden_seed.
  return _py_expr_eval_py_ext.eval_expr(
      _op_impl_lookup.shallow_clone(
          self, itemid=itemid, schema=schema, **overrides
      )
  )


@DataSlice.add_method('deep_clone')
def _deep_clone(
    self, schema: DataSlice = arolla.unspecified(), **overrides: Any
) -> DataSlice:
  # TODO: Replace with aux_eval_op when it supports handling the
  # hidden_seed.
  return _py_expr_eval_py_ext.eval_expr(
      _op_impl_lookup.deep_clone(self, schema, **overrides)
  )


@DataSlice.add_method('deep_uuid')
def _deep_uuid(
    self,
    schema: DataSlice = arolla.unspecified(),
    *,
    seed: DataSlice = '',
) -> DataSlice:
  return arolla.abc.aux_eval_op(
      _op_impl_lookup.deep_uuid, self, schema, seed=seed
  )


@DataSlice.add_method('fork_bag')
# TODO: Remove this alias.
@DataSlice.add_method('fork_db')
def _fork_bag(self) -> DataSlice:
  if self.get_bag() is None:
    raise ValueError(
        'fork_bag expects the DataSlice to have a DataBag attached'
    )
  return self.with_bag(self.get_bag().fork(mutable=True))


@DataSlice.add_method('with_merged_bag')
def _with_merged_bag(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.with_merged_bag, self)


@DataSlice.add_method('enriched')
def _enriched(self, *db: DataBag) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.enriched, self, *db)


@DataSlice.add_method('updated')
def _updated(self, *db: DataBag) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.updated, self, *db)


@DataSlice.add_method('__neg__')
def _neg(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.neg, self)


@DataSlice.add_method('__pos__')
def _pos(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.pos, self)


DataSlice.add_method('with_name')(general_eager_ops.with_name)


@DataSlice.add_method('ref')
def _ref(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.ref, self)


@DataSlice.add_method('get_itemid')
def _get_itemid(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.get_itemid, self)


# TODO: Remove this alias.
@DataSlice.add_method('as_itemid')
def _as_itemid(self) -> DataSlice:
  warnings.warn(
      'as_itemid is deprecated. Use get_itemid instead.',
      RuntimeWarning,
  )
  return self.get_itemid()


@DataSlice.add_method('get_dtype')
def _get_dtype(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.get_dtype, self)


@DataSlice.add_method('get_obj_schema')
def _get_obj_schema(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.get_obj_schema, self)


@DataSlice.add_method('with_schema_from_obj')
def _with_schema_from_obj(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.with_schema_from_obj, self)


@DataSlice.add_method('get_ndim')
def _get_ndim(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.get_ndim, self)


@DataSlice.add_method('get_present_count')
def _get_present_count(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.count, self)


@DataSlice.add_method('get_size')
def _get_size(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.size, self)


@DataSlice.add_method('is_primitive')
def _is_primitive(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.is_primitive, self)


@DataSlice.add_method('get_item_schema')
def _get_item_schema(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.get_item_schema, self)


@DataSlice.add_method('get_key_schema')
def _get_key_schema(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.get_key_schema, self)


@DataSlice.add_method('get_value_schema')
def _get_value_schema(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.get_value_schema, self)


@DataSlice.add_method('stub')
def _stub(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.stub, self)


@DataSlice.add_method('attrs')
def _attrs(self, **attrs) -> DataBag:
  return arolla.abc.aux_eval_op(_op_impl_lookup.attrs, self, **attrs)


@DataSlice.add_method('with_attrs')
def _with_attrs(self, **attrs) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.with_attrs, self, **attrs)


@DataSlice.add_method('take')
def _take(self, indices: Any) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.take, self, indices)


@DataSlice.add_method('to_py')
def to_py(
    ds: DataSlice,
    max_depth: int = 2,
    obj_as_dict: bool = False,
    include_missing_attrs: bool = True,
) -> Any:
  """Returns a readable python object from a DataSlice.

  Attributes, lists, and dicts are recursively converted to Python objects.

  Args:
    ds: A DataSlice
    max_depth: Maximum depth for recursive printing. Each attribute, list, and
      dict increments the depth by 1. Use -1 for unlimited depth.
    obj_as_dict: Whether to convert objects to python dicts. By default objects
      are converted to automatically constructed 'Obj' dataclass instances.
    include_missing_attrs: whether to include attributes with None value in
      objects.
  """
  if max_depth >= 0:
    max_depth += ds.get_ndim()

  orig_bag = ds.get_bag()
  db = DataBag.empty()
  ds = db.implode(ds.no_bag(), -1)
  if orig_bag is not None:
    ds = ds.enriched(orig_bag)
  return _to_py_impl(ds, {}, 0, max_depth, obj_as_dict, include_missing_attrs)


@DataSlice.add_method('to_pytree')
def to_pytree(
    ds: DataSlice,
    max_depth: int = 2,
    include_missing_attrs: bool = True) -> Any:
  """Returns a readable python object from a DataSlice.

  Attributes, lists, and dicts are recursively converted to Python objects.
  Objects are converted to Python dicts.

  Same as kd.to_py(..., obj_as_dict=True)

  Args:
    ds: A DataSlice
    max_depth: Maximum depth for recursive printing. Each attribute, list, and
      dict increments the depth by 1. Use -1 for unlimited depth.
    include_missing_attrs: whether to include attributes with None value in
      objects.
  """
  return to_py(
      ds, max_depth=max_depth, obj_as_dict=True,
      include_missing_attrs=include_missing_attrs)


def _to_py_impl(
    ds: DataSlice,
    obj_id_to_python_obj: dict[DataSlice, Any],
    depth: int,
    max_depth: int,
    obj_as_dict: bool,
    include_missing_attrs: bool,
) -> Any:
  """Recursively converts a DataItem to a Python object."""
  assert ds.get_ndim() == 0

  existing = obj_id_to_python_obj.get(ds)
  if existing is not None:
    return existing

  if ds.is_primitive():
    return ds.internal_as_py()

  if ds.get_bag() is None:
    return {}

  schema = ds.get_schema()

  if schema.is_any_schema():
    raise ValueError(
        f'cannot convert a DataSlice with ANY schema to Python: {ds}'
    )

  if (
      max_depth >= 0 and depth >= max_depth
  ) or schema.is_itemid_schema():
    return ds

  is_list = ds.is_list()
  is_dict = ds.is_dict()

  # Remove special attributes
  attr_names = list(set(dir(ds)) - set(['__items__', '__keys__', '__values__']))
  assert not (attr_names and (is_list or is_dict))

  if attr_names and not obj_as_dict:
    obj_class = dataclasses.make_dataclass(
        'Obj',
        [
            (attr_name, Any, dataclasses.field(default=None))
            for attr_name in attr_names
        ],
        eq=False,
    )

    def eq(x, y):
      """Checks whether two dataclasses are equal ignoring types."""
      return dataclasses.is_dataclass(y) and dataclasses.asdict(
          x
      ) == dataclasses.asdict(y)

    obj_class.__eq__ = eq

    py_obj = obj_class()
  elif is_list:
    py_obj = []
  else:
    py_obj = {}

  obj_id_to_python_obj[ds] = py_obj

  attrs = {}
  next_depth = depth + 1
  for attr_name in attr_names:
    attr_ds = ds.get_attr(attr_name)
    attr_value = _to_py_impl(
        attr_ds, obj_id_to_python_obj, next_depth, max_depth, obj_as_dict,
        include_missing_attrs)
    if include_missing_attrs or attr_value is not None:
      attrs[attr_name] = attr_value

  if dataclasses.is_dataclass(py_obj):
    for name, value in attrs.items():
      setattr(py_obj, name, value)
  elif attrs and obj_as_dict and not is_dict and not is_list:
    py_obj.update(attrs)  # pytype: disable=attribute-error

  if is_list:
    list_values = py_obj
    assert isinstance(list_values, list)
    for child_ds in ds:
      list_values.append(
          _to_py_impl(
              child_ds, obj_id_to_python_obj, next_depth, max_depth,
              obj_as_dict, include_missing_attrs)
      )

  if is_dict:
    dict_values = py_obj
    for key in ds:
      value_ds = ds[key]
      dict_values[key.no_bag().internal_as_py()] = _to_py_impl(
          value_ds,
          obj_id_to_python_obj,
          next_depth,
          max_depth,
          obj_as_dict,
          include_missing_attrs,
      )

  return py_obj


##### DataSlice Magic methods. #####


@DataSlice.add_method('__add__')
def _add(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.add, self, other)


@DataSlice.add_method('__radd__')
def _radd(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.add, other, self)


@DataSlice.add_method('__sub__')
def _sub(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.subtract, self, other)


@DataSlice.add_method('__rsub__')
def _rsub(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.subtract, other, self)


@DataSlice.add_method('__mul__')
def _mul(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.multiply, self, other)


@DataSlice.add_method('__rmul__')
def _rmul(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.multiply, other, self)


@DataSlice.add_method('__truediv__')
def _truediv(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.divide, self, other)


@DataSlice.add_method('__rtruediv__')
def _rtruediv(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.divide, other, self)


@DataSlice.add_method('__floordiv__')
def _floordiv(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.floordiv, self, other)


@DataSlice.add_method('__rfloordiv__')
def _rfloordiv(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.floordiv, other, self)


@DataSlice.add_method('__mod__')
def _mod(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.mod, self, other)


@DataSlice.add_method('__rmod__')
def _rmod(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.mod, other, self)


@DataSlice.add_method('__pow__')
def _pow(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.pow, self, other)


@DataSlice.add_method('__rpow__')
def _rpow(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.pow, other, self)


@DataSlice.add_method('__and__')
def _and(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.apply_mask, self, other)


@DataSlice.add_method('__rand__')
def _rand(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.apply_mask, other, self)


@DataSlice.add_method('__eq__')
def _eq(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.equal, self, other)


@DataSlice.add_method('__ne__')
def _ne(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.not_equal, self, other)


@DataSlice.add_method('__gt__')
def _gt(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.greater, self, other)


@DataSlice.add_method('__ge__')
def _ge(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.greater_equal, self, other)


@DataSlice.add_method('__lt__')
def _lt(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.less, self, other)


@DataSlice.add_method('__le__')
def _le(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.less_equal, self, other)


@DataSlice.add_method('__or__')
def _or(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.coalesce, self, other)


@DataSlice.add_method('__ror__')
def _ror(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return arolla.abc.aux_eval_op(_op_impl_lookup.coalesce, other, self)


@DataSlice.add_method('__invert__')
def _invert(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.has_not, self)


class SlicingHelper:
  """Slicing helper for DataSlice.

  It is a syntactic sugar for kd.subslice. That is, kd.subslice(ds, *slices)
  is equivalent to ds.S[*slices]. For example,
    kd.subslice(x, 0) == x.S[0]
    kd.subslice(x, 0, 1, kd.item(0)) == x.S[0, 1, kd.item(0)]
    kd.subslice(x, slice(0, -1)) == x.S[0:-1]
    kd.subslice(x, slice(0, -1), slice(0, 1), slice(1, None))
      == x.S[0:-1, 0:1, 1:]
    kd.subslice(x, ..., slice(1, None)) == x.S[..., 1:]
    kd.subslice(x, slice(1, None)) == x.S[1:]

  Please see kd.subslice for more detailed explanations and examples.
  """

  def __init__(self, ds: DataSlice):
    self._ds = ds

  def __getitem__(self, s):
    slices = s if isinstance(s, tuple) else [s]
    return arolla.abc.aux_eval_op(_op_impl_lookup.subslice, self._ds, *slices)


class ListSlicingHelper:
  """ListSlicing helper for DataSlice.

  x.L on DataSlice returns a ListSlicingHelper, which treats the first dimension
  of DataSlice x as a a list.
  """

  def __init__(self, ds: DataSlice):
    ndim = ds.get_ndim().internal_as_py()
    if not ndim:
      raise ValueError('DataSlice must have at least one dimension to iterate.')
    self._ds = ds
    self._ndim = ndim

  @functools.cached_property
  def _imploded_ds(self) -> DataSlice:
    return DataBag.empty().implode(self._ds.no_bag(), -1)

  def __getitem__(self, s):
    return arolla.abc.aux_eval_op(
        _op_impl_lookup.explode, self._imploded_ds[s], self._ndim - 1
    ).with_bag(self._ds.get_bag())

  def __len__(self) -> int:
    return arolla.abc.aux_eval_op(
        getattr(_op_impl_lookup, 'shapes.dim_sizes'),
        arolla.abc.aux_eval_op(_op_impl_lookup.get_shape, self._ds),
        0,
    ).internal_as_py()[0]

  def __iter__(self):
    return (self[i] for i in range(len(self)))


# NOTE: we can create a decorator for adding property similar to add_method, if
# we need it for more properties.
DataSlice.internal_register_reserved_class_method_name('S')
DataSlice.S = property(SlicingHelper)
DataSlice.internal_register_reserved_class_method_name('L')
DataSlice.L = property(ListSlicingHelper)


@functools.cache
def unspecified():
  return DataSlice._unspecified()  # pylint: disable=protected-access
