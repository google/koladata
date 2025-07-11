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

"""Definition of a DataSlice specific Expr View."""

import typing
from typing import Any

from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import source_location
from koladata.expr import tracing_mode
from koladata.fstring import fstring
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants
from koladata.util import kd_functools


I = input_container.InputContainer('I')


kd_functools.skip_file_from_functor_stack_trace(__file__)


def _raise_eager_only_method(method_name: str, class_name: str):
  raise ValueError(
      f'calling .{method_name}() on a {class_name} is not supported in'
      ' expr/tracing mode'
  )


def _aux_bind_op(op_name: str, *args, **kwargs):
  bound = arolla.abc.aux_bind_op(op_name, *args, **kwargs)
  # So far we only annotate expressions with source location in tracing mode,
  # because there are generally fewer expectations on the structure of exprs
  # embedded into the functors. kd.lazy operators are not modified and enable
  # users full control over the expr structure.
  if tracing_mode.is_tracing_enabled():
    return source_location.annotate_with_current_source_location(bound)
  else:
    return bound


class SlicingHelper:
  """Slicing helper for KodaView.

  It is a syntactic sugar for kd.subslice. That is, kd.subslice(ds, *slices)
  is equivalent to ds.S[*slices].
  """

  def __init__(self, ds: 'KodaView'):
    self._ds = ds

  def __getitem__(self, s):
    slices = s if isinstance(s, tuple) else [s]
    return _aux_bind_op(
        'kd.slices._subslice_for_slicing_helper', self._ds, *slices
    )


class ListSlicingHelper:
  """ListSlicing helper for KodaView.

  x.L on KodaView returns a ListSlicingHelper, which treats the first dimension
  of KodaView x as a a list.
  """

  def __init__(self, ds: 'KodaView'):
    self._ds = ds

  def __getitem__(self, s):
    return _aux_bind_op('kd.slices.subslice', self._ds, s, ...)


# List of operators that have the same arity as their resulting tuple.
# TODO: Remove this once we have proper support for tuple size
# detection.
_OPERATORS_ALLOWED_FOR_TUPLE_UNPACKING = frozenset([
    'kd.slices.align',
    'kd.align',
    'kd.tuples.tuple',
    'kd.tuples.slice',
    'kd.tuple',
    'test_make_tuple',
])


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
    # NOTE: Unlike other methods, we don't add source location here, because
    # there is nothing to evaluate.
    return arolla.abc.aux_bind_op('kd.with_name', self, name)

  def __getitem__(self, x: Any) -> arolla.Expr:
    return _aux_bind_op('koda_internal.view.get_item', self, x)

  def freeze(self) -> arolla.Expr:
    return _aux_bind_op('kd.freeze', self)

  def freeze_bag(self) -> arolla.Expr:
    return _aux_bind_op('kd.freeze_bag', self)

  def __lshift__(self, other) -> arolla.Expr:
    return _aux_bind_op('kd.bags.updated', self, other)

  def __rshift__(self, other) -> arolla.Expr:
    return _aux_bind_op('kd.bags.enriched', self, other)

  def __getattr__(self, attr_name: str) -> arolla.Expr:
    if (
        attr_name.startswith('_')
        or attr_name
        in data_slice.RESERVED_ATTRIBUTES_WITHOUT_LEADING_UNDERSCORE
    ):
      raise AttributeError(attr_name)
    return _aux_bind_op('kd.get_attr', self, attr_name)

  def __format__(self, format_spec: str, /):
    return fstring.fstr_expr_placeholder(self, format_spec)

  @property
  def S(self) -> SlicingHelper:
    return SlicingHelper(self)

  @property
  def L(self) -> ListSlicingHelper:
    return ListSlicingHelper(self)

  def __add__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.math.add', self, other)

  def __radd__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.math.add', other, self)

  def __sub__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.math.subtract', self, other)

  def __rsub__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.math.subtract', other, self)

  def __mul__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.math.multiply', self, other)

  def __rmul__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.math.multiply', other, self)

  def __truediv__(self, other) -> arolla.Expr:
    return _aux_bind_op('kd.math.divide', self, other)

  def __rtruediv__(self, other) -> arolla.Expr:
    return _aux_bind_op('kd.math.divide', other, self)

  def __floordiv__(self, other) -> arolla.Expr:
    return _aux_bind_op('kd.math.floordiv', self, other)

  def __rfloordiv__(self, other) -> arolla.Expr:
    return _aux_bind_op('kd.math.floordiv', other, self)

  def __mod__(self, other) -> arolla.Expr:
    return _aux_bind_op('kd.math.mod', self, other)

  def __rmod__(self, other) -> arolla.Expr:
    return _aux_bind_op('kd.math.mod', other, self)

  def __pow__(self, other) -> arolla.Expr:
    return _aux_bind_op('kd.math.pow', self, other)

  def __rpow__(self, other) -> arolla.Expr:
    return _aux_bind_op('kd.math.pow', other, self)

  def __eq__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.equal', self, other)

  def __ne__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.not_equal', self, other)

  def __gt__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.greater', self, other)

  def __ge__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.greater_equal', self, other)

  def __lt__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.less', self, other)

  def __le__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.less_equal', self, other)

  def __and__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.apply_mask', self, other)

  def __rand__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.apply_mask', other, self)

  def __or__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.coalesce', self, other)

  def __ror__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.coalesce', other, self)

  def __xor__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.xor', self, other)

  def __rxor__(self, other: Any) -> arolla.Expr:
    return _aux_bind_op('kd.xor', other, self)

  def __invert__(self) -> arolla.Expr:
    return _aux_bind_op('kd.has_not', self)

  def __neg__(self) -> arolla.Expr:
    return _aux_bind_op('kd.math.neg', self)

  def __pos__(self) -> arolla.Expr:
    return _aux_bind_op('kd.math.pos', self)

  def __call__(
      self,
      *args: Any,
      return_type_as: Any = data_slice.DataSlice,
      **kwargs: Any,
  ) -> arolla.Expr:
    return _aux_bind_op(
        'kd.call', self, *args, return_type_as=return_type_as, **kwargs
    )

  def reshape(self, shape: Any) -> arolla.Expr:
    return _aux_bind_op('kd.reshape', self, shape)

  def reshape_as(self, shape_from: Any) -> arolla.Expr:
    return _aux_bind_op('kd.reshape_as', self, shape_from)

  def flatten(
      self,
      from_dim: Any = data_slice.DataSlice.from_vals(0, schema_constants.INT64),
      to_dim: Any = arolla.unspecified(),
  ) -> arolla.Expr:
    return _aux_bind_op('kd.flatten', self, from_dim, to_dim)

  def flatten_end(
      self,
      n_times: Any = data_slice.DataSlice.from_vals(1, schema_constants.INT64),
  ) -> arolla.Expr:
    return _aux_bind_op('kd.flatten_end', self, n_times)

  def repeat(self, sizes: Any) -> arolla.Expr:
    return _aux_bind_op('kd.repeat', self, sizes)

  def select(
      self, fltr: Any, expand_filter: Any = data_slice.DataSlice.from_vals(True)
  ) -> arolla.Expr:
    return _aux_bind_op('kd.select', self, fltr, expand_filter=expand_filter)

  def select_present(self) -> arolla.Expr:
    return _aux_bind_op('kd.select_present', self)

  def select_items(self, fltr: Any) -> arolla.Expr:
    return _aux_bind_op('kd.select_items', self, fltr)

  def select_keys(self, fltr: Any) -> arolla.Expr:
    return _aux_bind_op('kd.select_keys', self, fltr)

  def select_values(self, fltr: Any) -> arolla.Expr:
    return _aux_bind_op('kd.select_values', self, fltr)

  def expand_to(
      self, target: Any, ndim: Any = arolla.unspecified()
  ) -> arolla.Expr:
    return _aux_bind_op('kd.expand_to', self, target, ndim)

  def list_size(self) -> arolla.Expr:
    return _aux_bind_op('kd.list_size', self)

  def dict_size(self) -> arolla.Expr:
    return _aux_bind_op('kd.dict_size', self)

  def with_dict_update(
      self, keys: Any, values: Any = arolla.unspecified()
  ) -> arolla.Expr:
    return _aux_bind_op('kd.with_dict_update', self, keys=keys, values=values)

  def with_list_append_update(self, append: Any) -> arolla.Expr:
    return _aux_bind_op('kd.with_list_append_update', self, append=append)

  def follow(self) -> arolla.Expr:
    return _aux_bind_op('kd.follow', self)

  def ref(self) -> arolla.Expr:
    return _aux_bind_op('kd.ref', self)

  def clone(
      self,
      *,
      itemid: Any = arolla.unspecified(),
      schema: Any = arolla.unspecified(),
      **overrides: Any,
  ) -> arolla.Expr:
    return _aux_bind_op(
        'kd.clone', self, itemid=itemid, schema=schema, **overrides
    )

  def shallow_clone(
      self,
      *,
      itemid: Any = arolla.unspecified(),
      schema: Any = arolla.unspecified(),
      **overrides: Any,
  ) -> arolla.Expr:
    return _aux_bind_op(
        'kd.shallow_clone', self, itemid=itemid, schema=schema, **overrides
    )

  def deep_clone(
      self, schema: Any = arolla.unspecified(), **overrides: Any
  ) -> arolla.Expr:
    return _aux_bind_op('kd.deep_clone', self, schema, **overrides)

  def deep_uuid(
      self,
      schema: Any = arolla.unspecified(),
      *,
      seed: Any = data_slice.DataSlice.from_vals(''),
  ) -> arolla.Expr:
    return _aux_bind_op('kd.deep_uuid', self, schema, seed=seed)

  def extract(self, schema: Any = arolla.unspecified()) -> arolla.Expr:
    return _aux_bind_op('kd.extract', self, schema)

  def extract_bag(self, schema: Any = arolla.unspecified()) -> arolla.Expr:
    return _aux_bind_op('kd.extract_bag', self, schema)

  def get_itemid(self) -> arolla.Expr:
    return _aux_bind_op('kd.get_itemid', self)

  def get_item_schema(self) -> arolla.Expr:
    return _aux_bind_op('kd.get_item_schema', self)

  def get_key_schema(self) -> arolla.Expr:
    return _aux_bind_op('kd.get_key_schema', self)

  def get_value_schema(self) -> arolla.Expr:
    return _aux_bind_op('kd.get_value_schema', self)

  def get_obj_schema(self) -> arolla.Expr:
    return _aux_bind_op('kd.get_obj_schema', self)

  def with_schema_from_obj(self) -> arolla.Expr:
    return _aux_bind_op('kd.with_schema_from_obj', self)

  def with_schema(self, schema: Any) -> arolla.Expr:
    return _aux_bind_op('kd.with_schema', self, schema)

  def get_shape(self) -> arolla.Expr:
    return _aux_bind_op('kd.get_shape', self)

  def get_ndim(self) -> arolla.Expr:
    return _aux_bind_op('kd.get_ndim', self)

  def get_dtype(self) -> arolla.Expr:
    return _aux_bind_op('kd.get_dtype', self)

  def get_size(self) -> arolla.Expr:
    return _aux_bind_op('kd.size', self)

  def get_sizes(self) -> arolla.Expr:
    return _aux_bind_op('kd.shapes.get_sizes', self)

  def has_attr(self, attr_name: Any) -> arolla.Expr:
    return _aux_bind_op('kd.has_attr', self, attr_name)

  def get_attr(
      self, attr_name: Any, default: Any = arolla.unspecified()
  ) -> arolla.Expr:
    return _aux_bind_op('kd.get_attr', self, attr_name, default)

  def stub(
      self, attrs: Any = data_slice.DataSlice.from_vals([])
  ) -> arolla.Expr:
    return _aux_bind_op('kd.stub', self, attrs=attrs)

  def with_attrs(
      self,
      *,
      overwrite_schema: Any = data_slice.DataSlice.from_vals(False),
      **attrs: Any,
  ) -> arolla.Expr:
    return _aux_bind_op(
        'kd.with_attrs', self, overwrite_schema=overwrite_schema, **attrs
    )

  def strict_with_attrs(
      self,
      **attrs: Any,
  ) -> arolla.Expr:
    return _aux_bind_op('kd.strict_with_attrs', self, **attrs)

  def with_attr(
      self,
      attr_name: Any,
      value: Any,
      overwrite_schema: Any = data_slice.DataSlice.from_vals(False),
  ) -> arolla.Expr:
    return _aux_bind_op(
        'kd.with_attr',
        self,
        attr_name,
        value,
        overwrite_schema=overwrite_schema,
    )

  def new(self, **attrs) -> arolla.Expr:
    return _aux_bind_op('kd.new', schema=self, **attrs)

  def take(self, indices: Any) -> arolla.Expr:
    return _aux_bind_op('kd.take', self, indices)

  def implode(
      self,
      ndim: Any = data_slice.DataSlice.from_vals(1, schema_constants.INT64),
      itemid: Any = arolla.unspecified(),
  ) -> arolla.Expr:
    return _aux_bind_op('kd.implode', self, ndim, itemid)

  def explode(
      self,
      ndim: Any = data_slice.DataSlice.from_vals(1, schema_constants.INT64),
  ) -> arolla.Expr:
    return _aux_bind_op('kd.explode', self, ndim)

  def maybe(self, attr_name: Any) -> arolla.Expr:
    return _aux_bind_op('kd.maybe', self, attr_name)

  def is_empty(self) -> arolla.Expr:
    return _aux_bind_op('kd.is_empty', self)

  def is_entity(self) -> arolla.Expr:
    return _aux_bind_op('kd.is_entity', self)

  def is_list(self) -> arolla.Expr:
    return _aux_bind_op('kd.is_list', self)

  def is_dict(self) -> arolla.Expr:
    return _aux_bind_op('kd.is_dict', self)

  def get_keys(self) -> arolla.Expr:
    return _aux_bind_op('kd.get_keys', self)

  def get_nofollowed_schema(self) -> arolla.Expr:
    return _aux_bind_op('kd.get_nofollowed_schema', self)

  def get_values(self, key_ds: Any = arolla.unspecified()) -> arolla.Expr:
    return _aux_bind_op('kd.get_values', self, key_ds=key_ds)

  def with_bag(self, bag: Any) -> arolla.Expr:
    return _aux_bind_op('kd.with_bag', self, bag)

  def get_bag(self) -> arolla.Expr:
    return _aux_bind_op('kd.get_bag', self)

  def no_bag(self) -> arolla.Expr:
    return _aux_bind_op('kd.no_bag', self)

  def with_merged_bag(self) -> arolla.Expr:
    return _aux_bind_op('kd.with_merged_bag', self)

  def enriched(self, *bag: Any) -> arolla.Expr:
    return _aux_bind_op('kd.enriched', self, *bag)

  def updated(self, *bag: Any) -> arolla.Expr:
    return _aux_bind_op('kd.updated', self, *bag)

  def get_schema(self) -> arolla.Expr:
    return _aux_bind_op('kd.get_schema', self)

  def get_present_count(self) -> arolla.Expr:
    return _aux_bind_op('kd.count', self)

  def is_primitive(self) -> arolla.Expr:
    return _aux_bind_op('kd.is_primitive', self)

  def is_dict_schema(self) -> arolla.Expr:
    return _aux_bind_op('kd.schema.is_dict_schema', self)

  def is_entity_schema(self) -> arolla.Expr:
    return _aux_bind_op('kd.schema.is_entity_schema', self)

  def is_struct_schema(self) -> arolla.Expr:
    return _aux_bind_op('kd.schema.is_struct_schema', self)

  def is_list_schema(self) -> arolla.Expr:
    return _aux_bind_op('kd.schema.is_list_schema', self)

  def is_primitive_schema(self) -> arolla.Expr:
    return _aux_bind_op('kd.schema.is_primitive_schema', self)

  # Support sequence contract, for tuple unpacking.
  def _arolla_sequence_getitem_(self, index: int) -> arolla.Expr:
    if self.qtype is not None and arolla.types.is_tuple_qtype(self.qtype):
      tuple_size = len(arolla.abc.get_field_qtypes(self.qtype))
    else:
      # Try guessing tuple size from the expression.
      # TODO: Use QType to determine tuple size instead.
      node = self
      while arolla.abc.is_annotation_operator(node.op):
        node = node.node_deps[0]
      if (
          node.op is None
          or not isinstance(node.op, arolla.types.RegisteredOperator)
          or node.op.display_name not in _OPERATORS_ALLOWED_FOR_TUPLE_UNPACKING
      ):
        raise ValueError(
            'tuple unpacking is only supported for nodes with known QType or '
            f'for a few selected operators, but got: {node}\n'
            'for a quick fix, consider using'
            ' arolla.M.annotation.QType(arolla.make_tuple_qtype(...)) to'
            ' explicitly specify the tuple qtype'
        )
      tuple_size = len(node.node_deps)

    if index < 0 or index >= tuple_size:
      raise IndexError('tuple index out of range')
    return self[index]

  # Eager-only DataSlice methods.
  def append(self, *args, **kwargs):  # pylint: disable=unused-argument
    raise ValueError(
        'calling .append() on a DataSlice is not supported in'
        ' expr/tracing mode; use .with_list_append_update() instead.'
    )

  def clear(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('clear', 'DataSlice')

  def display(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('display', 'DataSlice')

  def embed_schema(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('embed_schema', 'DataSlice')

  def fork_bag(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('fork_bag', 'DataSlice')

  def from_vals(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('from_vals', 'DataSlice')

  def get_attr_names(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('get_attr_names', 'DataSlice')

  def internal_as_arolla_value(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('internal_as_arolla_value', 'DataSlice')

  def internal_as_dense_array(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('internal_as_dense_array', 'DataSlice')

  def internal_as_py(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('internal_as_py', 'DataSlice')

  def internal_is_itemid_schema(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('internal_is_itemid_schema', 'DataSlice')

  def is_mutable(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('is_mutable', 'DataSlice/DataBag')

  def set_attr(self, *args, **kwargs):  # pylint: disable=unused-argument
    raise ValueError(
        'calling .set_attr() on a DataSlice is not supported in'
        ' expr/tracing mode; use .with_attr() instead.'
    )

  def set_attrs(self, *args, **kwargs):  # pylint: disable=unused-argument
    raise ValueError(
        'calling .set_attrs() on a DataSlice is not supported in'
        ' expr/tracing mode; use .with_attrs() instead.'
    )

  def set_schema(self, *args, **kwargs):  # pylint: disable=unused-argument
    raise ValueError(
        'calling .set_schema() on a DataSlice is not supported in'
        ' expr/tracing mode; use .with_schema() instead.'
    )

  def to_py(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('to_py', 'DataSlice')

  def to_pytree(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('to_pytree', 'DataSlice')

  def bind(self, **kwargs):
    return _aux_bind_op('kd.bind', self, **kwargs)

  # Eager-only ListItem methods
  def pop(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('pop', 'ListItem')

  # Eager-only DataBag methods.
  def adopt(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('adopt', 'DataBag')

  def adopt_stub(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('adopt_stub', 'DataBag')

  def concat_lists(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('concat_lists', 'DataBag')

  def contents_repr(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('contents_repr', 'DataBag')

  def data_triples_repr(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('data_triples_repr', 'DataBag')

  def dict(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('dict', 'DataBag')

  def dict_like(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('dict_like', 'DataBag')

  def dict_schema(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('dict_schema', 'DataBag')

  def dict_shaped(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('dict_shaped', 'DataBag')

  def empty(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('empty', 'DataBag')

  def fork(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('fork', 'DataBag')

  def get_approx_size(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('get_approx_size', 'DataBag')

  def list(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('list', 'DataBag')

  def list_like(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('list_like', 'DataBag')

  def list_schema(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('list_schema', 'DataBag')

  def list_shaped(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('list_shaped', 'DataBag')

  def merge_fallbacks(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('merge_fallbacks', 'DataBag')

  def merge_inplace(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('merge_inplace', 'DataBag')

  def named_schema(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('named_schema', 'DataBag')

  def new_like(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('new_like', 'DataBag')

  def new_schema(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('new_schema', 'DataBag')

  def new_shaped(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('new_shaped', 'DataBag')

  def obj(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('obj', 'DataBag')

  def obj_like(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('obj_like', 'DataBag')

  def obj_shaped(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('obj_shaped', 'DataBag')

  def schema_triples_repr(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('schema_triples_repr', 'DataBag')

  def uu(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('uu', 'DataBag')

  def uu_schema(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('uu_schema', 'DataBag')

  def uuobj(self, *args, **kwargs):  # pylint: disable=unused-argument
    _raise_eager_only_method('uuobj', 'DataBag')


def has_koda_view(node: arolla.Expr) -> bool:
  """Returns true iff the node has a koda view (only)."""
  return hasattr(node, '_koda_view_tag')


arolla.abc.set_expr_view_for_qtype(qtypes.DATA_SLICE, KodaView)
arolla.abc.set_expr_view_for_qtype(qtypes.DATA_BAG, KodaView)
arolla.abc.set_expr_view_for_qtype(qtypes.JAGGED_SHAPE, KodaView)
arolla.abc.set_expr_view_for_registered_operator(
    'koda_internal.input', KodaView
)
# NOTE: This attaches a KodaView to all literals, including e.g. Arolla values.
# This is not ideal, but we want e.g. the `eval` method to be attached.
arolla.abc.set_expr_view_for_operator_family(
    '::koladata::expr::LiteralOperator', KodaView
)
