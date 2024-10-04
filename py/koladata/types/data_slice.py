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

import functools
from typing import Any

from arolla import arolla
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
def _select(self, filter_ds: Any) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.select, self, filter_ds)


@DataSlice.add_method('select_present')
def _select_present(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.select_present, self)


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


@DataSlice.add_method('follow')
def _follow(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.follow, self)


@DataSlice.add_method('to_py')
def _to_py(self) -> DataSlice:
  return self.internal_as_py()


@DataSlice.add_method('clone')
def _clone(self, schema: DataSlice = arolla.unspecified()) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.clone, self, schema)


@DataSlice.add_method('extract')
def _extract(self, schema: DataSlice = arolla.unspecified()) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.extract, self, schema)


@DataSlice.add_method('deep_clone')
def _deep_clone(self, schema: DataSlice = arolla.unspecified()) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.deep_clone, self, schema)


@DataSlice.add_method('is_list')
def _is_list(self) -> DataSlice:
  return self.contains_only_lists()


@DataSlice.add_method('fork_db')
def _fork_db(self) -> DataSlice:
  if self.db is None:
    raise ValueError('fork_db expects the DataSlice to have a DataBag attached')
  return self.with_db(self.db.fork(mutable=True))


@DataSlice.add_method('freeze')
def _freeze(self) -> DataSlice:
  if self.db is None:
    raise ValueError('freeze expects the DataSlice to have a DataBag attached')
  return self.with_db(self.db.fork(mutable=False))


@DataSlice.add_method('enriched')
def _enriched(self, *db: DataBag) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.enriched, self, *db)


@DataSlice.add_method('updated')
def _updated(self, *db: DataBag) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.updated, self, *db)


DataSlice.add_method('with_name')(general_eager_ops.with_name)


@DataSlice.add_method('ref')
def _ref(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.ref, self)


@DataSlice.add_method('as_itemid')
def _as_itemid(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.as_itemid, self)


@DataSlice.add_method('get_obj_schema')
def _get_obj_schema(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.get_obj_schema, self)


@DataSlice.add_method('with_schema_from_obj')
def _with_schema_from_obj(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.with_schema_from_obj, self)


@DataSlice.add_method('get_ndim')
def _get_ndim(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.get_ndim, self)


@DataSlice.add_method('rank')
def _rank(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.rank, self)


@DataSlice.add_method('get_present_count')
def _get_present_count(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.count, self)


@DataSlice.add_method('get_size')
def _get_size(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.size, self)


@DataSlice.add_method('is_primitive')
def _is_primitive(self) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.is_primitive, self)


@DataSlice.add_method('attrs')
def _attrs(self, **attrs) -> DataBag:
  return arolla.abc.aux_eval_op(_op_impl_lookup.attrs, self, **attrs)


@DataSlice.add_method('with_attrs')
def _with_attrs(self, **attrs) -> DataSlice:
  return arolla.abc.aux_eval_op(_op_impl_lookup.with_attrs, self, **attrs)


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

  It is a syntactic sugar for kde.subslice. That is, kd.subslice(ds, *slices)
  is equivalent to ds.S[*slices].
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
    return DataBag.empty().implode(self._ds.no_db(), -1)

  def __getitem__(self, s):
    return arolla.abc.aux_eval_op(
        _op_impl_lookup.explode, self._imploded_ds[s], self._ndim - 1
    ).with_db(self._ds.db)

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
