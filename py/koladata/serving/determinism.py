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

"""Tools for making traced functors deterministic."""

from arolla import arolla
from koladata import kd


_NON_DETERMINISTIC_TOKEN_OP = arolla.abc.lookup_operator(
    'koda_internal.non_deterministic'
)

_SOURCE_LOCATION_OP = arolla.abc.lookup_operator(
    'kd.annotation.source_location'
)


class Determinizer:
  """Makes traced functors deterministic.

  The class keeps mapping from non-uuids to frozen uuids, so it can convert
  several traced functors consistently, without breaking the
  interoperability between them.
  """

  def __init__(self, seed: str, strip_source_locations: bool = False):
    self._seed = seed
    self._frozen_non_deterministic_tokens = dict()
    self._frozen_ids = dict()
    self._schemas_being_validated = set()
    self._already_made_deterministic = dict()
    self._strip_source_locations = strip_source_locations

  def make_deterministic(self, ds: kd.types.DataItem) -> kd.types.DataItem:
    """Makes a DataItem (e.g. a functor) deterministic."""

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
              self._make_deterministic_expr(kd.expr.unpack_expr(ds))
          )
        # All other primitives are already deterministic.
        return ds

      if ds.is_list():
        self._assert_deterministic_schema(ds.get_schema())
        list_items = [self.make_deterministic(x) for x in ds]
        new_id = self._freeze_listid(ds.get_itemid())
        return kd.list(list_items, itemid=new_id, schema=ds.get_schema())

      if ds.is_dict():
        self._assert_deterministic_schema(ds.get_schema())
        keys = kd.sort(ds.get_keys())
        if not keys.is_primitive() or kd.expr.is_packed_expr(keys):
          raise NotImplementedError(
              f'Dicts with non-primitive or Expr keys are not supported: {ds}'
          )
        values = ds[keys]
        deterministic_values = kd.slice(
            [self.make_deterministic(v) for v in values.L]
        )
        new_id = self._freeze_dictid(ds.get_itemid())
        return kd.dict(
            keys, deterministic_values, itemid=new_id, schema=ds.get_schema()
        )

      if ds.is_primitive_schema() or ds.is_struct_schema():
        self._assert_deterministic_schema(ds)
        return ds

      # TODO: b/437034138 - Are there other cases not covered here?
      assert kd.is_entity(ds), ds

      new_id = self._freeze_itemid(ds.get_itemid())
      attrs = kd.get_attr_names(ds, intersection=False)
      attr_to_values = {
          attr: self.make_deterministic(kd.get_attr(ds, attr)) for attr in attrs
      }

      if ds.get_schema() == kd.OBJECT:
        return kd.obj(**attr_to_values, itemid=new_id)
      else:
        self._assert_deterministic_schema(ds.get_schema())
        return kd.new(**attr_to_values, schema=ds.get_schema(), itemid=new_id)

    if ds.fingerprint in self._already_made_deterministic:
      return self._already_made_deterministic[ds.fingerprint]

    result = impl(ds)
    self._already_made_deterministic[ds.fingerprint] = result
    return result

  def _freeze_non_deterministic_token(self, token):
    """Freezes an arolla.int64 non-deterministic token."""
    if token not in self._frozen_non_deterministic_tokens:
      int64_seed = kd.cityhash(self._seed, 0)
      new_token = arolla.int64(
          kd.cityhash(
              len(self._frozen_non_deterministic_tokens), int64_seed
          ).to_py()
      )
      self._frozen_non_deterministic_tokens[token] = new_token
    return self._frozen_non_deterministic_tokens[token]

  def _make_deterministic_expr(self, expr: arolla.Expr) -> arolla.Expr:
    """Freezes all the ItemId and non-deterministic tokens in the Expr."""

    def make_deterministic_node(node: arolla.Expr) -> arolla.Expr:
      if (
          node.op == _NON_DETERMINISTIC_TOKEN_OP
          and node.node_deps[1].is_literal
      ):
        return _NON_DETERMINISTIC_TOKEN_OP(
            node.node_deps[0],
            self._freeze_non_deterministic_token(node.node_deps[1].qvalue),
        )
      if self._strip_source_locations and node.op == _SOURCE_LOCATION_OP:
        return node.node_deps[0]
      if node.is_literal and isinstance(node.qvalue, kd.types.DataSlice):
        return arolla.literal(self.make_deterministic(node.qvalue))
      return node

    return arolla.abc.transform(expr, make_deterministic_node)

  def _freeze_itemid(self, itemid: kd.types.DataItem) -> kd.types.DataItem:
    if kd.ids.is_uuid(itemid):
      return itemid

    if itemid not in self._frozen_ids:
      self._frozen_ids[itemid] = kd.to_itemid(
          kd.uu(
              seed=self._seed,
              koladata_serving_determinism_id=len(self._frozen_ids),
          )
      )
    return self._frozen_ids[itemid]

  def _freeze_listid(self, itemid: kd.types.DataItem) -> kd.types.DataItem:
    if kd.ids.is_uuid(itemid):
      return itemid

    if itemid not in self._frozen_ids:
      self._frozen_ids[itemid] = kd.uuid_for_list(
          seed=self._seed, koladata_serving_determinism_id=len(self._frozen_ids)
      )
    return self._frozen_ids[itemid]

  def _freeze_dictid(self, itemid: kd.types.DataItem) -> kd.types.DataItem:
    if kd.ids.is_uuid(itemid):
      return itemid

    if itemid not in self._frozen_ids:
      self._frozen_ids[itemid] = kd.uuid_for_dict(
          seed=self._seed,
          koladata_serving_determinism_id=len(self._frozen_ids),
      )

    return self._frozen_ids[itemid]

  def _assert_deterministic_schema(self, schema: kd.types.DataItem):
    """Asserts that the schema is a primitive or a UUID."""
    # Don't fail on schema cycles.
    if schema in self._schemas_being_validated:
      return

    if schema.is_primitive_schema() or schema in (
        kd.OBJECT,
        kd.SCHEMA,
        kd.NONE,
        kd.ITEMID,
    ):
      return

    if not kd.ids.is_uuid(schema):
      raise ValueError(
          'All schemas used it the functors for serving must have uuids, got:'
          f' {schema}'
      )

    self._schemas_being_validated.add(schema)
    try:
      for attr_name in schema.get_attr_names(intersection=False):
        attr_schema = schema.get_attr(attr_name)
        self._assert_deterministic_schema(attr_schema)
    finally:
      self._schemas_being_validated.remove(schema)
