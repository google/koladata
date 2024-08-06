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
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')


class BasicKodaView(arolla.abc.ExprView):
  """Basic ExprView applicable to all Koda types."""

  _basic_koda_view_tag = True

  def eval(self, **input_values: Any) -> arolla.AnyQValue:
    return expr_eval.eval(self, **input_values)

  def inputs(self) -> list[str]:
    return input_container.get_input_names(typing.cast(arolla.Expr, self), I)


class DataBagView(BasicKodaView):
  """ExprView for DataBags."""

  _data_bag_view_tag = True


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

  @property
  def S(self) -> SlicingHelper:
    return SlicingHelper(self)

  # TODO: Overload Python's magic methods as we add functionality
  # in operators.
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


class KodaMultipleReturnDataSliceTupleView(BasicKodaView):
  """ExprView for tuples of DataSlice, for operators returning multiple values.
  """

  _koda_multiple_return_data_slice_tuple_view_tag = True

  # Support sequence contract, for tuple unpacking.
  def _arolla_sequence_getitem_(self, index: int) -> arolla.Expr:
    if index < 0 or index >= len(self.node_deps):
      raise IndexError('tuple index out of range')
    return arolla.M.annotation.qtype(
        arolla.M.core.get_nth(self, arolla.int64(index)), qtypes.DATA_SLICE
    )


def has_basic_koda_view(node: arolla.Expr) -> bool:
  """Returns true iff the node has a basic koda view (only)."""
  return (
      hasattr(node, '_basic_koda_view_tag')
      and not hasattr(node, '_data_bag_view_tag')
      and not hasattr(node, '_data_slice_view_tag')
      and not hasattr(node, '_koda_multiple_return_data_slice_tuple_view_tag')
  )


def has_data_bag_view(node: arolla.Expr) -> bool:
  """Returns true iff the node has a data bag view (only)."""
  return hasattr(node, '_data_bag_view_tag')


def has_data_slice_view(node: arolla.Expr) -> bool:
  """Returns true iff the node has a data slice view (only)."""
  return hasattr(node, '_data_slice_view_tag')


def has_koda_multiple_return_data_slice_tuple_view(node: arolla.Expr) -> bool:
  """Returns true iff the node has a data slice tuple view (only)."""
  return hasattr(node, '_koda_multiple_return_data_slice_tuple_view_tag')


arolla.abc.set_expr_view_for_qtype(qtypes.DATA_SLICE, DataSliceView)
arolla.abc.set_expr_view_for_qtype(qtypes.DATA_BAG, DataBagView)
arolla.abc.set_expr_view_for_registered_operator(
    'koda_internal.input', DataSliceView
)
arolla.abc.set_expr_view_for_operator_family(
    '::koladata::expr::LiteralOperator', BasicKodaView
)
