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

"""Definition of a DataSlice specific Expr View."""

import typing
from typing import Any

from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')


class BasicKodaView(arolla.abc.ExprView):
  """Basic ExprView applicable to all Koda types."""

  _basic_koda_view_tag = True

  def eval(
      self,
      self_input: Any = expr_eval.UNSPECIFIED_SELF_INPUT,
      /,
      **input_values: Any,
  ) -> arolla.AnyQValue:
    return expr_eval.eval(self, self_input, **input_values)

  def inputs(self) -> list[str]:
    return introspection.get_input_names(typing.cast(arolla.Expr, self))

  def with_name(self, name: str | arolla.types.Text) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.with_name', self, name)


class DataBagView(BasicKodaView):
  """ExprView for DataBags."""

  _data_bag_view_tag = True

  def __getitem__(self, x: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_item', self, x)


class SlicingHelper:
  """Slicing helper for DataSliceView.

  It is a syntactic sugar for kde.subslice. That is, kde.subslice(ds, *slices)
  is equivalent to ds.S[*slices].
  """

  def __init__(self, ds: 'DataSliceView'):
    self._ds = ds

  def __getitem__(self, s):
    slices = s if isinstance(s, tuple) else [s]
    return arolla.abc.aux_bind_op(
        'kde.core._subslice_for_slicing_helper', self._ds, *slices
    )


class ListSlicingHelper:
  """ListSlicing helper for DataSliceViews.

  x.L on DataSliceView returns a ListSlicingHelper, which treats the first
  dimension
  of DataSliceView x as a a list.
  """

  def __init__(self, ds: 'DataSliceView'):
    self._ds = ds

  def __getitem__(self, s):
    return arolla.abc.aux_bind_op('kde.core.subslice', self._ds, s, ...)


class DataSliceView(BasicKodaView):
  """ExprView for DataSlices."""

  _data_slice_view_tag = True

  def __getattr__(self, attr_name: str) -> arolla.Expr:
    if (
        attr_name.startswith('_')
        or attr_name
        in data_slice.RESERVED_ATTRIBUTES_WITHOUT_LEADING_UNDERSCORE
    ):
      raise AttributeError(attr_name)
    return arolla.abc.aux_bind_op('kde.get_attr', self, attr_name)

  def __getitem__(self, s):
    return arolla.abc.aux_bind_op('kde.get_item', self, s)

  @property
  def S(self) -> SlicingHelper:
    return SlicingHelper(self)

  @property
  def L(self) -> ListSlicingHelper:
    return ListSlicingHelper(self)

  def __add__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.add', self, other)

  def __radd__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.add', other, self)

  def __sub__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.subtract', self, other)

  def __rsub__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.subtract', other, self)

  def __mul__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.multiply', self, other)

  def __rmul__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.multiply', other, self)

  def __truediv__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.divide', self, other)

  def __rtruediv__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.divide', other, self)

  def __floordiv__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.floordiv', self, other)

  def __rfloordiv__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.floordiv', other, self)

  def __mod__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.mod', self, other)

  def __rmod__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.mod', other, self)

  def __pow__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.pow', self, other)

  def __rpow__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.pow', other, self)

  def __eq__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.equal', self, other)

  def __ne__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.not_equal', self, other)

  def __gt__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.greater', self, other)

  def __ge__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.greater_equal', self, other)

  def __lt__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.less', self, other)

  def __le__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.less_equal', self, other)

  def __and__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.apply_mask', self, other)

  def __rand__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.apply_mask', other, self)

  def __or__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.coalesce', self, other)

  def __ror__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.coalesce', other, self)

  def __invert__(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.has_not', self)

  def __call__(self, *args: Any, **kwargs: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.call', self, *args, **kwargs)

  def reshape(self, shape: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.reshape', self, shape)

  def flatten(
      self,
      from_dim: Any = data_slice.DataSlice.from_vals(0, schema_constants.INT64),
      to_dim: Any = arolla.unspecified(),
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.flatten', self, from_dim, to_dim)

  def add_dim(self, sizes: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.add_dim', self, sizes)

  def repeat(self, sizes: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.repeat', self, sizes)

  def select(self, filter_ds: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.select', self, filter_ds)

  def select_present(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.select_present', self)

  def expand_to(
      self, target: Any, ndim: Any = arolla.unspecified()
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.expand_to', self, target, ndim)

  def list_size(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.list_size', self)

  def dict_size(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.dict_size', self)

  def follow(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.follow', self)

  def ref(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.ref', self)

  def as_itemid(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.as_itemid', self)

  def as_any(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.as_any', self)

  def get_obj_schema(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_obj_schema', self)

  def with_schema_from_obj(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.with_schema_from_obj', self)

  def with_schema(self, schema: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.with_schema', self, schema)

  def get_shape(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_shape', self)

  def get_ndim(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_ndim', self)

  def rank(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.rank', self)

  def get_size(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.size', self)

  def has_attr(self, attr_name: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.has_attr', self, attr_name)

  def get_attr(
      self, attr_name: Any, default: Any = arolla.unspecified()
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_attr', self, attr_name, default)

  def maybe(self, attr_name: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.maybe', self, attr_name)

  def is_empty(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.is_empty', self)

  def get_keys(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_keys', self)

  def get_values(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_values', self)

  def with_db(self, db: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.with_db', self, db)

  @property
  def db(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_db', self)

  def no_db(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.no_db', self)

  def enriched(self, db: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.enriched', self, db)

  def updated(self, db: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.updated', self, db)

  def get_schema(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_schema', self)

  def get_present_count(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.count', self)


class KodaTupleView(BasicKodaView):
  """ExprView for tuples of Koda values, for operators returning tuples."""

  _koda_tuple_view_tag = True

  def __getitem__(self, index: int) -> arolla.Expr:
    return self._arolla_sequence_getitem_(index)

  # Support sequence contract, for tuple unpacking.
  def _arolla_sequence_getitem_(self, index: int) -> arolla.Expr:
    if index < 0 or index >= len(self.node_deps):
      raise IndexError('tuple index out of range')
    return arolla.M.core.get_nth(self, arolla.int64(index))


def has_basic_koda_view(node: arolla.Expr) -> bool:
  """Returns true iff the node has a basic koda view (only)."""
  return (
      hasattr(node, '_basic_koda_view_tag')
      and not hasattr(node, '_data_bag_view_tag')
      and not hasattr(node, '_data_slice_view_tag')
      and not hasattr(node, '_koda_tuple_view_tag')
  )


def has_data_bag_view(node: arolla.Expr) -> bool:
  """Returns true iff the node has a data bag view (only)."""
  return hasattr(node, '_data_bag_view_tag')


def has_data_slice_view(node: arolla.Expr) -> bool:
  """Returns true iff the node has a data slice view (only)."""
  return hasattr(node, '_data_slice_view_tag')


def has_koda_tuple_view(node: arolla.Expr) -> bool:
  """Returns true iff the node has a tuple view (only)."""
  return hasattr(node, '_koda_tuple_view_tag')


arolla.abc.set_expr_view_for_qtype(qtypes.DATA_SLICE, DataSliceView)
arolla.abc.set_expr_view_for_qtype(qtypes.DATA_BAG, DataBagView)
arolla.abc.set_expr_view_for_registered_operator(
    'koda_internal.input', DataSliceView
)
arolla.abc.set_expr_view_for_operator_family(
    '::koladata::expr::LiteralOperator', BasicKodaView
)
