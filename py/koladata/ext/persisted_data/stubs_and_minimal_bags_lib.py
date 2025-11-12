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

"""Library for creating stubs and minimal bags.

A stub is essentially a minimal DataSlice. So this module provides functions
to create minimal slices and bags that are targeted for use by persisted
incremental dataslices. The PersistedIncrementalDataSliceManager keeps a schema
separately from the collection of data bags for the managed slice. So the bags
used in the collection do not have to carry much information about the schema -
only the bare minimum is kept.
"""

from koladata import kd
from koladata.ext.persisted_data import value_presence_util


def _check_no_non_primitive_metadata_attributes(
    schema: kd.types.DataItem,
) -> None:
  """Raises a ValueError if the schema has non-primitive metadata attributes."""
  try:
    metadata = schema.get_attr('__schema_metadata__')
  except ValueError:
    pass
  else:
    for attr_name in kd.dir(metadata):
      attr_value = metadata.get_attr(attr_name)
      if not attr_value.is_primitive():
        # We could in principle also support attr_values that are itemids, but
        # there is no use case for this now, and it complicates the code because
        # there is no kd.is_itemid() function.
        raise ValueError(
            f'schema {schema} has metadata attributes that are not primitives'
        )


def schema_stub(schema: kd.types.DataItem) -> kd.types.DataItem:
  """Returns a stub for the given schema item.

  The result is not simply schema.stub():
  * For kd.named_schema instances, the returned stub will keep the name.
  * It will keep any metadata associated with the schema.

  For the time being, the metadata's attribute values can only be primitives.

  Args:
    schema: the schema to stub.

  Raises:
    ValueError: if the schema has metadata with at least one attribute value
      that is not a primitive.

  Returns:
    A stub for the given schema with the additional info (if any).
  """
  _check_no_non_primitive_metadata_attributes(schema)
  schema_stub_bag = kd.bag()
  for attr_name in ['__schema_name__', '__schema_metadata__']:
    try:
      schema_stub_bag = kd.bags.updated(
          schema_stub_bag,
          kd.attrs(schema, **{attr_name: schema.get_attr(attr_name)}),
      )
    except ValueError:
      pass
  return schema.stub().with_bag(schema_stub_bag.merge_fallbacks())


def _check_is_acceptable_schema_slice(schema_ds: kd.types.DataSlice) -> None:
  """Raises a ValueError if the schema_ds has non-primitive metadata attributes.

  The check is performed recursively: non-primitive metadata attributes on
  sub-schemas are also not allowed.

  Args:
    schema_ds: a DataSlice with a kd.SCHEMA schema.

  Raises:
    ValueError: if the schema_ds has non-primitive metadata attributes.
  """
  seen_items = kd.mutable_bag().dict(key_schema=kd.ITEMID, value_schema=kd.MASK)

  def process(schema_item: kd.types.DataItem) -> None:
    try:
      itemid = schema_item.get_itemid()
    except ValueError:
      return
    if not kd.has(itemid):
      return
    if seen_items[itemid] == kd.present:
      return
    seen_items[itemid] = kd.present
    _check_no_non_primitive_metadata_attributes(schema_item)
    try:
      attr_names = kd.dir(schema_item)
    except ValueError:
      return
    else:
      for attr_name in attr_names:
        process(schema_item.get_attr(attr_name))

  for schema_item in schema_ds.flatten().L:
    process(schema_item)


def _stubby(
    ds: kd.types.DataSlice,
) -> kd.types.DataSlice:
  """Helper function to create a stub that is useful for minimal bags.

  The minimal bags are created by PersistedIncrementalDataSliceManager while it
  traverses an update DataSlice according to its schema. For the most part, the
  stubs that are used to create these minimal bags can be really minimal, in the
  sense that they are primitives or itemids without any additional information,
  not even about the schema (the schema is handled separately by using function
  `schema_stub()` above). So for the most part, this function simply returns
  ds.stub().no_bag(). However, there are two Koda schemas that can hide a world
  of complexity behind them and that are opaque in the sense that traversal
  based on the schema alone cannot capture the complexity in a step-by-step way.
  These schemas are kd.OBJECT and kd.SCHEMA. The use of kd.OBJECT is forbidden
  in persisted incremental dataslices. The use of kd.SCHEMA is allowed; it is
  handled by keeping the full complexity in a "super stub" that is simply
  ds.extract().

  Args:
    ds: the DataSlice to stub.

  Raises:
    ValueError: if ds is a kd.SCHEMA slice where some sub-schema has metadata
      with at least one attribute value that is not a primitive.

  Returns:
    The stub slice for the given DataSlice.
  """
  if ds.get_schema() == kd.SCHEMA:
    _check_is_acceptable_schema_slice(ds)
    return ds.extract()
  return ds.stub().no_bag()


def minimal_bag_associating_list_with_its_items(
    list_ds: kd.types.DataSlice,
) -> kd.types.DataBag | None:
  """Returns a minimal bag associating the given lists with their items.

  Args:
    list_ds: a DataSlice with a list schema.

  Returns:
    A minimal bag associating the given lists with their items, or None if
    list_ds has no list with information about its items.
  """
  # Dedup list itemids here with kd.unique() so that the call to
  # items_stub.implode(itemid=...) below won't complain about duplicate itemids.
  list_ds = kd.unique(list_ds.no_bag()).with_bag(list_ds.get_bag())
  mask = value_presence_util.has_info_about_list_items(list_ds)
  if mask.is_empty():
    return None
  items_stub = _stubby(list_ds[:])
  itemids = list_ds.get_itemid()
  return (items_stub.implode(itemid=itemids) & mask).extract_update()


def minimal_bag_associating_dict_with_its_keys_and_values(
    dict_ds: kd.types.DataSlice,
) -> kd.types.DataBag | None:
  """Returns a minimal bag associating the given dicts with their keys and values.

  Args:
    dict_ds: a DataSlice with a dict schema.

  Returns:
    A minimal bag associating the given dicts with their keys and values, or
    None if dict_ds has no key-value data.
  """
  # Dedup dict itemids here with kd.unique() so that the call to
  # kd.dict(..., itemid=...) below won't complain about duplicate itemids.
  dict_ds = kd.unique(dict_ds.no_bag()).with_bag(dict_ds.get_bag())
  keys = dict_ds.get_keys()
  if keys.is_empty():
    return None  # No keys means there is no key-value data.
  keys_stub = _stubby(keys)
  values_stub = _stubby(dict_ds.get_values())
  itemids = dict_ds.get_itemid()
  return kd.dict(keys_stub, values_stub, itemid=itemids).extract_update()


def minimal_bag_associating_entity_with_its_attr_value(
    entity_ds: kd.types.DataSlice,
    attr_name: str,
) -> kd.types.DataBag | None:
  """Returns a minimal bag associating the given entities with their attr values.

  Args:
    entity_ds: a DataSlice with an entity schema.
    attr_name: the name of an attribute of the entity schema.

  Returns:
    A minimal bag associating the given entities with their values for `attr`,
    or None if entity_ds has no entity with a present or removed value for the
    given attribute.
  """
  non_missing_values_mask = kd.has(
      entity_ds
  ) & ~value_presence_util.get_missing_mask(entity_ds, attr_name)
  entity_ds_to_update = entity_ds & non_missing_values_mask
  if entity_ds_to_update.is_empty():
    return None
  values_stub = _stubby(entity_ds_to_update.get_attr(attr_name))
  return kd.attrs(entity_ds_to_update, **{attr_name: values_stub})
