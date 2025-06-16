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

"""DataSlice abstraction."""

import functools
from typing import Any
import warnings

from arolla import arolla
import koladata.base.py_conversions.dataclasses_util as _  # used in to_py.
from koladata.expr import py_expr_eval_py_ext as _py_expr_eval_py_ext
from koladata.types import data_bag_py_ext as _data_bag_py_ext
from koladata.types import data_slice_py_ext as _data_slice_py_ext
from koladata.types import general_eager_ops
from koladata.types import jagged_shape


DataBag = _data_bag_py_ext.DataBag
DataSlice = _data_slice_py_ext.DataSlice

_eval_op = _py_expr_eval_py_ext.eval_op


def _get_docstring(op_name: str) -> str | None:
  op = arolla.abc.lookup_operator(op_name)
  return op.getdoc()


def add_method(cls, method_name: str, docstring_from: str | None = None):
  """Returns a callable / decorator to put it as a cls's method.

  Adds methods only to the class it was called on (then it is also available in
  cls' subclasses).

  Example on how to define methods:

    @data_slice.add_method(data_slice.DataSlice, 'flatten')
    def _flatten(self, from, to) -> DataSlice:
      return _eval_op('kd.flatten', self, from, to)

  Args:
    cls: DataSlice or its subclass.
    method_name: wraps the Python function as a method with `method_name`.
    docstring_from: If not None, the docstring of the operator with this name
      will be used as the docstring of the method.
  """

  # TODO: Find a way to adjust the docstring to match the DataSlice
  # method signature, p.ex. exclude `x` argument or replace it with `self`).
  def wrap(op):
    """Wraps eager `op` into a DataSlice method `method_name`."""
    _data_slice_py_ext.internal_register_reserved_class_method_name(method_name)
    setattr(cls, method_name, op)
    if docstring_from is not None:
      op.getdoc = lambda: _get_docstring(docstring_from)
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
      _data_slice_py_ext.internal_register_reserved_class_method_name(attr_name)
  return cls


def get_reserved_attrs() -> frozenset[str]:
  """Returns a set of reserved attributes without leading underscore."""
  return _data_slice_py_ext.internal_get_reserved_attrs()


# IPython/Colab try to access those attributes on any object and expect
# particular behavior from them, so we disallow accessing such attributes
# on a DataSlice via __getattr__.
RESERVED_ATTRIBUTES_WITHOUT_LEADING_UNDERSCORE = frozenset([
    'getdoc',
    'trait_names',
])


def _init_data_slice_class():
  """Initializes DataSlice class."""
  if hasattr(DataSlice, 'getdoc'):
    return
  for name in RESERVED_ATTRIBUTES_WITHOUT_LEADING_UNDERSCORE:
    _data_slice_py_ext.internal_register_reserved_class_method_name(name)


### Implementation of the DataSlice's additional functionality.


_init_data_slice_class()


##### DataSlice methods. #####


@add_method(DataSlice, '__dir__')
def _dir(self) -> list[str]:
  """Returns the list of attrs accessible through `getattr(slice, my_attr)`."""
  attrs = []
  try:
    attrs = self.get_attr_names(intersection=True)
  except ValueError:
    pass
  # We only include those attributes that can be called through `slice.my_attr`.
  attrs = {
      attr for attr in attrs
      if _data_slice_py_ext.internal_is_compliant_attr_name(attr)
  }
  methods = set(super(DataSlice, self).__dir__())
  return sorted(methods | attrs)


@add_method(DataSlice, 'maybe', docstring_from='kd.maybe')
def _maybe(self, attr_name: str) -> DataSlice:
  # NOTE: Calling `get_attr`, instead of _eval_op, because it is implemented
  # in C Python.
  return self.get_attr(attr_name, None)


@add_method(DataSlice, 'has_attr', docstring_from='kd.has_attr')
def _has_attr(self, attr_name: str) -> DataSlice:
  return _eval_op('kd.has_attr', self, attr_name)


@add_method(DataSlice, 'reshape', docstring_from='kd.reshape')
def _reshape(self, shape: jagged_shape.JaggedShape) -> DataSlice:
  return _eval_op('kd.reshape', self, shape)


@add_method(DataSlice, 'reshape_as', docstring_from='kd.reshape_as')
def _reshape_as(self, shape_from: DataSlice) -> DataSlice:
  if not isinstance(shape_from, DataSlice):
    raise TypeError(f'`shape_from` must be a DataSlice, got: {shape_from}')
  return _eval_op('kd.reshape_as', self, shape_from)


@add_method(DataSlice, 'flatten', docstring_from='kd.flatten')
def _flatten(
    self,
    from_dim: int | DataSlice = DataSlice.from_vals(arolla.int64(0)),
    to_dim: Any = arolla.unspecified(),
) -> DataSlice:
  return _eval_op('kd.flatten', self, from_dim, to_dim)


@add_method(DataSlice, 'flatten_end', docstring_from='kd.flatten_end')
def _flatten_end(
    self,
    n_times: int | DataSlice = DataSlice.from_vals(arolla.int64(1)),
) -> DataSlice:
  return _eval_op('kd.flatten_end', self, n_times)


@add_method(DataSlice, 'repeat', docstring_from='kd.repeat')
def _repeat(self, sizes: Any) -> DataSlice:
  return _eval_op('kd.repeat', self, sizes)


@add_method(DataSlice, 'select', docstring_from='kd.select')
def _select(
    self, fltr: Any, expand_filter: bool | DataSlice = DataSlice.from_vals(True)
) -> DataSlice:
  return _eval_op('kd.select', self, fltr, expand_filter=expand_filter)


@add_method(DataSlice, 'select_present', docstring_from='kd.select_present')
def _select_present(self) -> DataSlice:
  return _eval_op('kd.select_present', self)


@add_method(DataSlice, 'select_items', docstring_from='kd.select_items')
def _select_items(self, fltr: Any) -> DataSlice:
  return _eval_op('kd.select_items', self, fltr)


@add_method(DataSlice, 'select_keys', docstring_from='kd.select_keys')
def _select_keys(self, fltr: Any) -> DataSlice:
  return _eval_op('kd.select_keys', self, fltr)


@add_method(DataSlice, 'select_values', docstring_from='kd.select_values')
def _select_values(self, fltr: Any) -> DataSlice:
  return _eval_op('kd.select_values', self, fltr)


@add_method(DataSlice, 'get_values', docstring_from='kd.get_values')
def _get_values(self, key_ds: Any = arolla.unspecified()) -> DataSlice:
  return _eval_op('kd.get_values', self, key_ds)


@add_method(DataSlice, 'expand_to', docstring_from='kd.expand_to')
def _expand_to(
    self, target: Any, ndim: Any = arolla.unspecified()
) -> DataSlice:
  return _eval_op('kd.expand_to', self, target, ndim)


@add_method(DataSlice, 'list_size', docstring_from='kd.list_size')
def _list_size(self) -> DataSlice:
  return _eval_op('kd.list_size', self)


@add_method(DataSlice, 'dict_size', docstring_from='kd.dict_size')
def _dict_size(self) -> DataSlice:
  return _eval_op('kd.dict_size', self)


@add_method(DataSlice, 'with_dict_update', docstring_from='kd.with_dict_update')
def _with_dict_update(
    self, keys: Any, values: Any = arolla.unspecified()
) -> DataSlice:
  return _eval_op('kd.with_dict_update', self, keys=keys, values=values)


@add_method(
    DataSlice,
    'with_list_append_update',
    docstring_from='kd.with_list_append_update',
)
def _with_list_append_update(self, append: Any) -> DataSlice:
  return _eval_op('kd.with_list_append_update', x=self, append=append)


@add_method(DataSlice, 'follow', docstring_from='kd.follow')
def _follow(self) -> DataSlice:
  return _eval_op('kd.follow', self)


@add_method(DataSlice, 'extract', docstring_from='kd.extract')
def _extract(self, schema: Any = arolla.unspecified()) -> DataSlice:
  return _eval_op('kd.extract', self, schema)


@add_method(DataSlice, 'extract_bag', docstring_from='kd.extract_bag')
def _extract_bag(self, schema: Any = arolla.unspecified()) -> DataBag:
  return _eval_op('kd.extract_bag', self, schema)


@add_method(DataSlice, 'clone', docstring_from='kd.clone')
def _clone(
    self,
    *,
    itemid: Any = arolla.unspecified(),
    schema: Any = arolla.unspecified(),
    **overrides: Any,
) -> DataSlice:
  return _py_expr_eval_py_ext.eval_op(
      'kd.clone', self, itemid=itemid, schema=schema, **overrides
  )


@add_method(DataSlice, 'shallow_clone', docstring_from='kd.shallow_clone')
def _shallow_clone(
    self,
    *,
    itemid: Any = arolla.unspecified(),
    schema: Any = arolla.unspecified(),
    **overrides: Any,
) -> DataSlice:
  return _py_expr_eval_py_ext.eval_op(
      'kd.shallow_clone', self, itemid=itemid, schema=schema, **overrides
  )


@add_method(DataSlice, 'deep_clone', docstring_from='kd.deep_clone')
def _deep_clone(
    self, schema: Any = arolla.unspecified(), **overrides: Any
) -> DataSlice:
  return _py_expr_eval_py_ext.eval_op(
      'kd.deep_clone', self, schema, **overrides
  )


@add_method(DataSlice, 'deep_uuid', docstring_from='kd.deep_uuid')
def _deep_uuid(
    self,
    schema: Any = arolla.unspecified(),
    *,
    seed: str | DataSlice = DataSlice.from_vals(''),
) -> DataSlice:
  return _eval_op('kd.deep_uuid', self, schema, seed=seed)


@add_method(DataSlice, 'fork_bag')
def _fork_bag(self) -> DataSlice:
  """Returns a copy of the DataSlice with a forked mutable DataBag."""
  if self.get_bag() is None:
    raise ValueError(
        'fork_bag expects the DataSlice to have a DataBag attached'
    )
  return self.with_bag(self.get_bag().fork(mutable=True))


@add_method(DataSlice, 'with_merged_bag', docstring_from='kd.with_merged_bag')
def _with_merged_bag(self) -> DataSlice:
  return _eval_op('kd.with_merged_bag', self)


@add_method(DataSlice, 'enriched', docstring_from='kd.enriched')
def _enriched(self, *bag: DataBag) -> DataSlice:
  return _eval_op('kd.enriched', self, *bag)


@add_method(DataSlice, 'updated', docstring_from='kd.updated')
def _updated(self, *bag: DataBag) -> DataSlice:
  return _eval_op('kd.updated', self, *bag)


@add_method(DataSlice, '__neg__', docstring_from='kd.math.neg')
def _neg(self) -> DataSlice:
  return _eval_op('kd.math.neg', self)


@add_method(DataSlice, '__pos__', docstring_from='kd.math.pos')
def _pos(self) -> DataSlice:
  return _eval_op('kd.math.pos', self)


add_method(DataSlice, 'with_name')(general_eager_ops.with_name)


@add_method(DataSlice, 'ref', docstring_from='kd.ref')
def _ref(self) -> DataSlice:
  return _eval_op('kd.ref', self)


@add_method(DataSlice, 'get_itemid', docstring_from='kd.get_itemid')
def _get_itemid(self) -> DataSlice:
  return _eval_op('kd.get_itemid', self)


@add_method(DataSlice, 'get_dtype', docstring_from='kd.get_dtype')
def _get_dtype(self) -> DataSlice:
  return _eval_op('kd.get_dtype', self)


@add_method(DataSlice, 'get_obj_schema', docstring_from='kd.get_obj_schema')
def _get_obj_schema(self) -> DataSlice:
  return _eval_op('kd.get_obj_schema', self)


@add_method(
    DataSlice, 'with_schema_from_obj', docstring_from='kd.with_schema_from_obj'
)
def _with_schema_from_obj(self) -> DataSlice:
  return _eval_op('kd.with_schema_from_obj', self)


@add_method(DataSlice, 'get_ndim', docstring_from='kd.get_ndim')
def _get_ndim(self) -> DataSlice:
  return _eval_op('kd.get_ndim', self)


@add_method(DataSlice, 'get_present_count', docstring_from='kd.count')
def _get_present_count(self) -> DataSlice:
  return _eval_op('kd.count', self)


@add_method(DataSlice, 'get_size', docstring_from='kd.size')
def _get_size(self) -> DataSlice:
  return _eval_op('kd.size', self)


@add_method(DataSlice, 'is_primitive', docstring_from='kd.is_primitive')
def _is_primitive(self) -> DataSlice:
  return _eval_op('kd.is_primitive', self)


@add_method(DataSlice, 'get_item_schema', docstring_from='kd.get_item_schema')
def _get_item_schema(self) -> DataSlice:
  return _eval_op('kd.get_item_schema', self)


@add_method(DataSlice, 'get_key_schema', docstring_from='kd.get_key_schema')
def _get_key_schema(self) -> DataSlice:
  return _eval_op('kd.get_key_schema', self)


@add_method(DataSlice, 'get_value_schema', docstring_from='kd.get_value_schema')
def _get_value_schema(self) -> DataSlice:
  return _eval_op('kd.get_value_schema', self)


@add_method(DataSlice, 'stub', docstring_from='kd.stub')
def _stub(self, attrs: DataSlice = DataSlice.from_vals([])) -> DataSlice:
  return _eval_op('kd.stub', self, attrs=attrs)


@add_method(DataSlice, 'with_attrs', docstring_from='kd.with_attrs')
def _with_attrs(
    self,
    *,
    overwrite_schema: bool | DataSlice = DataSlice.from_vals(False),
    **attrs,
) -> DataSlice:
  return _eval_op(
      'kd.with_attrs', self, overwrite_schema=overwrite_schema, **attrs
  )


@add_method(
    DataSlice, 'strict_with_attrs', docstring_from='kd.strict_with_attrs'
)
def _strict_with_attrs(
    self,
    **attrs,
) -> DataSlice:
  return _eval_op(
      'kd.strict_with_attrs',
      self,
      **attrs,
  )


@add_method(DataSlice, 'with_attr', docstring_from='kd.with_attr')
def _with_attr(
    self,
    attr_name: str | DataSlice,
    value: Any,
    overwrite_schema: bool | DataSlice = DataSlice.from_vals(False),
) -> DataSlice:
  return _eval_op(
      'kd.with_attr', self, attr_name, value, overwrite_schema=overwrite_schema
  )


@add_method(DataSlice, 'new')
def _new(self, **attrs):
  """Returns a new Entity with this Schema."""
  raise NotImplementedError(
      'only Schema can create new Entities using .new(...)'
  )


@add_method(DataSlice, 'take', docstring_from='kd.take')
def _take(self, indices: Any) -> DataSlice:
  return _eval_op('kd.take', self, indices)


@add_method(DataSlice, 'implode', docstring_from='kd.implode')
def _implode(
    self,
    ndim: int | DataSlice = DataSlice.from_vals(arolla.int64(1)),
    itemid: Any = arolla.unspecified(),
) -> DataSlice:
  return _eval_op('kd.implode', self, ndim, itemid)


@add_method(DataSlice, 'explode', docstring_from='kd.explode')
def _explode(
    self,
    ndim: int | DataSlice = DataSlice.from_vals(arolla.int64(1)),
) -> DataSlice:
  return _eval_op('kd.explode', self, ndim)


@add_method(DataSlice, 'to_py')
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
  return ds._to_py_impl(  # pylint: disable=protected-access
      max_depth, obj_as_dict, include_missing_attrs
  )


@add_method(DataSlice, 'to_pytree')
def to_pytree(
    ds: DataSlice, max_depth: int = 2, include_missing_attrs: bool = True
) -> Any:
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
  return ds._to_py_impl(  # pylint: disable=protected-access
      max_depth, True, include_missing_attrs
  )


@functools.cache
def _get_vis_module():
  """Returns the vis module if dependency exists, None otherwise."""
  try:
    # pylint: disable=g-import-not-at-top
    # pytype: disable=import-error
    from koladata.ext import vis
    # pylint: enable=g-import-not-at-top
    # pytype: enable=import-error
    return vis
  except ImportError:
    vis_path = 'koladata.ext.vis'
    warnings.warn(
        f'please include {vis_path} in your BUILD '
        'dependency or run the kernel with that dependency'
    )
  return None


@add_method(DataSlice, 'display')
def _display(
    self: DataSlice,
    options: Any | None = None,
) -> None:
  """Visualizes a DataSlice as an html widget.

  Args:
    self: The DataSlice to visualize.
    options: This should be a `koladata.ext.vis.DataSliceVisOptions`.
  """
  if (vis := _get_vis_module()) is not None:
    vis.visualize_slice(self, options=options)
  else:
    print(repr(self))


@add_method(DataSlice, 'get_sizes')
def get_sizes(self) -> DataSlice:
  """Returns a DataSlice of sizes of the DataSlice's shape."""
  return _eval_op('kd.shapes.get_sizes', self)

##### DataSlice Magic methods. #####


@add_method(DataSlice, '__add__')
def _add(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.math.add', self, other)


@add_method(DataSlice, '__radd__')
def _radd(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.math.add', other, self)


@add_method(DataSlice, '__sub__')
def _sub(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.math.subtract', self, other)


@add_method(DataSlice, '__rsub__')
def _rsub(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.math.subtract', other, self)


@add_method(DataSlice, '__mul__')
def _mul(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.math.multiply', self, other)


@add_method(DataSlice, '__rmul__')
def _rmul(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.math.multiply', other, self)


@add_method(DataSlice, '__truediv__')
def _truediv(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.math.divide', self, other)


@add_method(DataSlice, '__rtruediv__')
def _rtruediv(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.math.divide', other, self)


@add_method(DataSlice, '__floordiv__')
def _floordiv(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.math.floordiv', self, other)


@add_method(DataSlice, '__rfloordiv__')
def _rfloordiv(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.math.floordiv', other, self)


@add_method(DataSlice, '__mod__')
def _mod(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.math.mod', self, other)


@add_method(DataSlice, '__rmod__')
def _rmod(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.math.mod', other, self)


@add_method(DataSlice, '__pow__')
def _pow(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.math.pow', self, other)


@add_method(DataSlice, '__rpow__')
def _rpow(self, other):
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.math.pow', other, self)


@add_method(DataSlice, '__and__')
def _and(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.apply_mask', self, other)


@add_method(DataSlice, '__rand__')
def _rand(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.apply_mask', other, self)


@add_method(DataSlice, '__eq__')
def _eq(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.equal', self, other)


@add_method(DataSlice, '__ne__')
def _ne(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.not_equal', self, other)


@add_method(DataSlice, '__gt__')
def _gt(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.greater', self, other)


@add_method(DataSlice, '__ge__')
def _ge(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.greater_equal', self, other)


@add_method(DataSlice, '__lt__')
def _lt(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.less', self, other)


@add_method(DataSlice, '__le__')
def _le(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.less_equal', self, other)


@add_method(DataSlice, '__or__')
def _or(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.coalesce', self, other)


@add_method(DataSlice, '__ror__')
def _ror(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.coalesce', other, self)


@add_method(DataSlice, '__xor__')
def _xor(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.xor', self, other)


@add_method(DataSlice, '__rxor__')
def _rxor(self, other: Any) -> DataSlice:
  if isinstance(other, arolla.Expr):
    return NotImplemented
  return _eval_op('kd.xor', other, self)


@add_method(DataSlice, '__invert__')
def _invert(self) -> DataSlice:
  return _eval_op('kd.has_not', self)


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
    return _eval_op('kd.subslice', self._ds, *slices)


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
    return _eval_op(
        'kd.explode', self._imploded_ds[s], self._ndim - 1
    ).with_bag(self._ds.get_bag())

  def __len__(self) -> int:
    return _eval_op(
        'kd.shapes.dim_sizes', _eval_op('kd.get_shape', self._ds), 0
    ).internal_as_py()[0]

  def __iter__(self):
    return (self[i] for i in range(len(self)))


# NOTE: we can create a decorator for adding property similar to add_method, if
# we need it for more properties.
_data_slice_py_ext.internal_register_reserved_class_method_name('S')
DataSlice.S = property(SlicingHelper)
_data_slice_py_ext.internal_register_reserved_class_method_name('L')
DataSlice.L = property(ListSlicingHelper)


@functools.cache
def unspecified():
  return DataSlice._unspecified()  # pylint: disable=protected-access
