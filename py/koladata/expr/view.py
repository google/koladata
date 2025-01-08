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
import warnings

from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.fstring import fstring
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')


class SlicingHelper:
  """Slicing helper for KodaView.

  It is a syntactic sugar for kde.subslice. That is, kde.subslice(ds, *slices)
  is equivalent to ds.S[*slices].
  """

  def __init__(self, ds: 'KodaView'):
    self._ds = ds

  def __getitem__(self, s):
    slices = s if isinstance(s, tuple) else [s]
    return arolla.abc.aux_bind_op(
        'kde.slices._subslice_for_slicing_helper', self._ds, *slices
    )


class ListSlicingHelper:
  """ListSlicing helper for KodaView.

  x.L on KodaView returns a ListSlicingHelper, which treats the first dimension
  of KodaView x as a a list.
  """

  def __init__(self, ds: 'KodaView'):
    self._ds = ds

  def __getitem__(self, s):
    return arolla.abc.aux_bind_op('kde.slices.subslice', self._ds, s, ...)


class KodaView(arolla.abc.ExprView):
  """ExprView applicable to all Koda types.

  See go/koda-expr-view for details.
  """

  _koda_view_tag = True

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

  def __getitem__(self, x: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('koda_internal.view.get_item', self, x)

  def freeze(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.freeze', self)

  def freeze_bag(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.freeze_bag', self)

  def __lshift__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.bags.updated', self, other)

  def __rshift__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.bags.enriched', self, other)

  def __getattr__(self, attr_name: str) -> arolla.Expr:
    if (
        attr_name.startswith('_')
        or attr_name
        in data_slice.RESERVED_ATTRIBUTES_WITHOUT_LEADING_UNDERSCORE
    ):
      raise AttributeError(attr_name)
    return arolla.abc.aux_bind_op('kde.get_attr', self, attr_name)

  def __format__(self, format_spec: str, /):
    return fstring.fstr_expr_placeholder(self, format_spec)

  @property
  def S(self) -> SlicingHelper:
    return SlicingHelper(self)

  @property
  def L(self) -> ListSlicingHelper:
    return ListSlicingHelper(self)

  def __add__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.core.add', self, other)

  def __radd__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.core.add', other, self)

  def __sub__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.math.subtract', self, other)

  def __rsub__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.math.subtract', other, self)

  def __mul__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.math.multiply', self, other)

  def __rmul__(self, other: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.math.multiply', other, self)

  def __truediv__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.math.divide', self, other)

  def __rtruediv__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.math.divide', other, self)

  def __floordiv__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.math.floordiv', self, other)

  def __rfloordiv__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.math.floordiv', other, self)

  def __mod__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.math.mod', self, other)

  def __rmod__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.math.mod', other, self)

  def __pow__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.math.pow', self, other)

  def __rpow__(self, other) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.math.pow', other, self)

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

  def __neg__(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.math.neg', self)

  def __pos__(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.math.pos', self)

  def __call__(
      self,
      *args: Any,
      return_type_as: Any = data_slice.DataSlice,
      **kwargs: Any,
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op(
        'kde.call', self, *args, return_type_as=return_type_as, **kwargs
    )

  def reshape(self, shape: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.reshape', self, shape)

  def reshape_as(self, shape_from: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.reshape_as', self, shape_from)

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

  def select(
      self, fltr: Any, expand_filter: Any = data_slice.DataSlice.from_vals(True)
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op(
        'kde.select', self, fltr, expand_filter=expand_filter
    )

  def select_present(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.select_present', self)

  def select_items(self, fltr: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.select_items', self, fltr)

  def select_keys(self, fltr: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.select_keys', self, fltr)

  def select_values(self, fltr: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.select_values', self, fltr)

  def expand_to(
      self, target: Any, ndim: Any = arolla.unspecified()
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.expand_to', self, target, ndim)

  def list_size(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.list_size', self)

  def dict_size(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.dict_size', self)

  def dict_update(
      self, keys: Any, values: Any = arolla.unspecified()
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op(
        'kde.dict_update', self, keys=keys, values=values
    )

  def with_dict_update(
      self, keys: Any, values: Any = arolla.unspecified()
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op(
        'kde.with_dict_update', self, keys=keys, values=values
    )

  def follow(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.follow', self)

  def ref(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.ref', self)

  def clone(
      self,
      *,
      itemid: Any = arolla.unspecified(),
      schema: Any = arolla.unspecified(),
      **overrides: Any,
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op(
        'kde.clone', self, itemid=itemid, schema=schema, **overrides
    )

  def shallow_clone(
      self,
      *,
      itemid: Any = arolla.unspecified(),
      schema: Any = arolla.unspecified(),
      **overrides: Any,
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op(
        'kde.shallow_clone', self, itemid=itemid, schema=schema, **overrides
    )

  def deep_clone(
      self, schema: Any = arolla.unspecified(), **overrides: Any
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.deep_clone', self, schema, **overrides)

  def deep_uuid(
      self,
      schema: Any = arolla.unspecified(),
      *,
      seed: Any = data_slice.DataSlice.from_vals(''),
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.deep_uuid', self, schema, seed=seed)

  def extract(self, schema: Any = arolla.unspecified()) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.extract', self, schema)

  def extract_bag(self, schema: Any = arolla.unspecified()) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.extract_bag', self, schema)

  def get_itemid(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_itemid', self)

  # TODO: Remove this alias.
  def as_itemid(self) -> arolla.Expr:
    warnings.warn(
        'as_itemid is deprecated. Use get_itemid instead.',
        RuntimeWarning,
    )
    return self.get_itemid()

  def as_any(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.as_any', self)

  def get_item_schema(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_item_schema', self)

  def get_key_schema(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_key_schema', self)

  def get_value_schema(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_value_schema', self)

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

  def get_dtype(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_dtype', self)

  def get_size(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.size', self)

  def has_attr(self, attr_name: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.has_attr', self, attr_name)

  def get_attr(
      self, attr_name: Any, default: Any = arolla.unspecified()
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_attr', self, attr_name, default)

  def stub(
      self, attrs: Any = data_slice.DataSlice.from_vals([])
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.stub', self, attrs=attrs)

  def with_attrs(
      self,
      *,
      update_schema: Any = data_slice.DataSlice.from_vals(False),
      **attrs: Any,
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op(
        'kde.with_attrs', self, update_schema=update_schema, **attrs
    )

  def with_attr(
      self,
      attr_name: Any,
      value: Any,
      update_schema: Any = data_slice.DataSlice.from_vals(False),
  ) -> arolla.Expr:
    return arolla.abc.aux_bind_op(
        'kde.with_attr', self, attr_name, value, update_schema=update_schema
    )

  def take(self, indices: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.take', self, indices)

  def maybe(self, attr_name: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.maybe', self, attr_name)

  def is_empty(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.is_empty', self)

  def is_entity(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.is_entity', self)

  def is_list(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.is_list', self)

  def is_dict(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.is_dict', self)

  def get_keys(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_keys', self)

  def get_values(self, key_ds: Any = arolla.unspecified()) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_values', self, key_ds=key_ds)

  def with_bag(self, bag: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.with_bag', self, bag)

  # TODO: Remove this alias.
  def with_db(self, bag: Any) -> arolla.Expr:
    return self.with_bag(bag)

  def get_bag(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_bag', self)

  # TODO: Remove this alias.
  @property
  def db(self) -> arolla.Expr:
    return self.get_bag()

  def no_bag(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.no_bag', self)

  # TODO: Remove this alias.
  def no_db(self) -> arolla.Expr:
    return self.no_bag()

  def with_merged_bag(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.with_merged_bag', self)

  def enriched(self, *bag: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.enriched', self, *bag)

  def updated(self, *bag: Any) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.updated', self, *bag)

  def get_schema(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.get_schema', self)

  def get_present_count(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.count', self)

  def is_primitive(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.is_primitive', self)

  def is_dict_schema(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.schema.is_dict_schema', self)

  def is_entity_schema(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.schema.is_entity_schema', self)

  def is_struct_schema(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.schema.is_struct_schema', self)

  def is_list_schema(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.schema.is_list_schema', self)

  def is_primitive_schema(self) -> arolla.Expr:
    return arolla.abc.aux_bind_op('kde.schema.is_primitive_schema', self)

  # Support sequence contract, for tuple unpacking.
  def _arolla_sequence_getitem_(self, index: int) -> arolla.Expr:
    if index < 0 or index >= len(self.node_deps):
      raise IndexError('tuple index out of range')
    return self[index]


def has_koda_view(node: arolla.Expr) -> bool:
  """Returns true iff the node has a koda view (only)."""
  return hasattr(node, '_koda_view_tag')


arolla.abc.set_expr_view_for_qtype(qtypes.DATA_SLICE, KodaView)
arolla.abc.set_expr_view_for_qtype(qtypes.DATA_BAG, KodaView)
arolla.abc.set_expr_view_for_registered_operator(
    'koda_internal.input', KodaView
)
# NOTE: This attaches a KodaView to all literals, including e.g. Arolla values.
# This is not ideal, but we want e.g. the `eval` method to be attached.
arolla.abc.set_expr_view_for_operator_family(
    '::koladata::expr::LiteralOperator', KodaView
)
