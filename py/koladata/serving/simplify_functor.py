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

"""Tools for stripping source locations from traced functors."""

from arolla import arolla
from koladata import kd


_SOURCE_LOCATION_OP = arolla.abc.lookup_operator(
    'kd.annotation.source_location'
)


def _strip_source_locations(
    ds: kd.types.DataItem,
    already_stripped: dict[arolla.abc.Fingerprint, kd.types.DataItem],
) -> kd.types.DataItem:
  """Recursively strips source locations from a functor (implementation)."""

  def _strip_expr_source_locations(expr: arolla.Expr) -> arolla.Expr:
    """Strips source locations from the Expr."""

    def strip_node(node: arolla.Expr) -> arolla.Expr:
      if node.op == _SOURCE_LOCATION_OP:
        return node.node_deps[0]
      if node.is_literal and isinstance(node.qvalue, kd.types.DataSlice):
        return arolla.literal(
            _strip_source_locations(node.qvalue, already_stripped)
        )
      return node

    return arolla.abc.transform(expr, strip_node)

  def impl(ds: kd.types.DataItem) -> kd.types.DataItem:
    if not kd.is_item(ds):
      raise NotImplementedError(
          f'Non-scalar DataSlices are not supported: {ds}'
      )

    if ds.is_empty():
      return ds

    if ds.is_primitive():
      if kd.expr.is_packed_expr(ds):
        return kd.expr.pack_expr(
            _strip_expr_source_locations(kd.expr.unpack_expr(ds))
        )
      return ds

    if ds.is_list():
      list_items = [
          _strip_source_locations(x, already_stripped) for x in ds
      ]
      return kd.list(list_items, itemid=ds.get_itemid(), schema=ds.get_schema())

    if ds.is_dict():
      keys = ds.get_keys()
      values = ds.get_values()
      stripped_values = kd.slice(
          [_strip_source_locations(v, already_stripped) for v in values.L]
      )
      return kd.dict(
          keys,
          stripped_values,
          itemid=ds.get_itemid(),
          schema=ds.get_schema(),
      )

    if ds.is_primitive_schema() or ds.is_struct_schema():
      return ds

    assert kd.is_entity(ds), ds

    attrs = kd.get_attr_names(ds, intersection=False)
    attr_to_values = {
        attr: _strip_source_locations(
            kd.get_attr(ds, attr), already_stripped
        )
        for attr in attrs
    }

    if ds.get_schema() == kd.OBJECT:
      return kd.obj(**attr_to_values, itemid=ds.get_itemid())
    else:
      return kd.new(
          **attr_to_values, schema=ds.get_schema(), itemid=ds.get_itemid()
      )

  if ds.fingerprint in already_stripped:
    return already_stripped[ds.fingerprint]

  result = impl(ds)
  already_stripped[ds.fingerprint] = result
  return result


def strip_source_locations(fn: kd.types.DataItem) -> kd.types.DataItem:
  """Recursively strips source locations from a functor."""
  return _strip_source_locations(fn, dict())
