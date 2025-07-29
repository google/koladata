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

"""Functions for checking the presence of values in a DataSlice.

For example:
* Suppose we have a Koda entity e whose schema mentions an attribute attr. We
  would like to know whether e.attr is missing, present, or a removed value.
* A Koda LIST may or may not have information about the list items. We would
  like to distinguish between the two cases.

The functions in this module help answer these questions. Not only for
DataItems, but also for DataSlices.
"""

from koladata import kd

# Sentinel values that are present for various schemas. We will rely on sentinel
# values to distinguish between missing and removed values of attributes in the
# functions below. That is done by using a sentinel value in a fallback bag and
# checking for presence, so it is important that the sentinel values are all
# present. Presence is tested in value_presence_util_test.py.
_PRESENT_SENTINELS_LIST = [
    (kd.INT32, kd.int32(1)),
    (kd.INT64, kd.int64(1)),
    (kd.FLOAT32, kd.float32(1)),
    (kd.FLOAT64, kd.float64(1)),
    (kd.STRING, kd.item('')),
    (kd.BYTES, kd.item(b'')),
    (kd.BOOLEAN, kd.item(True, schema=kd.BOOLEAN)),
    (kd.MASK, kd.present),
    (kd.EXPR, kd.expr.pack_expr(kd.I.x)),
    (kd.ITEMID, kd.new_itemid()),
    (kd.SCHEMA, kd.INT32),
]
# Unfortunately Python dicts cannot use DataItems as keys and work properly.
# So we either have to use C++, where it is supported, or we use e.g. string
# keys, as we do here:
_PRESENT_SENTINELS = {str(schema): v for schema, v in _PRESENT_SENTINELS_LIST}


def _get_present_sentinel_value(schema: kd.types.DataItem) -> kd.types.DataItem:
  # OBJECT schemas are not supported in persisted incremental data slices:
  assert schema != kd.OBJECT
  # NONE schema is not inhabited by any present value:
  assert schema != kd.NONE
  if schema.is_entity_schema():
    return schema.new()
  if schema.is_list_schema():
    return kd.list(schema=schema)
  if schema.is_dict_schema():
    return kd.dict(schema=schema)
  return _PRESENT_SENTINELS[str(schema)]


def _check_has_entity_schema_with_attr(
    entity_ds: kd.types.DataSlice, attr_name: str
):
  schema = entity_ds.get_schema()
  if not schema.is_entity_schema():
    raise ValueError(f'{entity_ds} does not have an entity schema')
  if not schema.has_attr(attr_name):
    raise ValueError(
        f'the schema of {entity_ds} does not have an attribute named'
        f' "{attr_name}"'
    )


def get_present_mask(
    entity_ds: kd.types.DataSlice, attr_name: str
) -> kd.types.DataSlice:
  """Returns a MASK with kd.present wherever entity_ds.attr_name is present.

  I.e. the result will be kd.missing whenever ~kd.has(entity_ds) or
  entity_ds.attr_name is missing or removed.

  Args:
    entity_ds: a DataSlice with an entity schema.
    attr_name: the name of an attribute of the entity schema.

  Returns:
    A mask with the same shape as entity_ds.
  """
  _check_has_entity_schema_with_attr(entity_ds, attr_name)
  return kd.has(kd.maybe(entity_ds, attr_name))


def get_missing_mask(
    entity_ds: kd.types.DataSlice, attr_name: str
) -> kd.types.DataSlice:
  """Returns a MASK with kd.present wherever entity_ds.attr_name is missing.

  I.e. the result will be kd.missing whenever ~kd.has(entity_ds) or when
  entity_ds.attr_name is present or removed.

  Args:
    entity_ds: a DataSlice with an entity schema.
    attr_name: the name of an attribute of the entity schema.

  Returns:
    A mask with the same shape as entity_ds.
  """
  _check_has_entity_schema_with_attr(entity_ds, attr_name)
  attr_schema = entity_ds.get_schema().get_attr(attr_name)
  if attr_schema == kd.NONE:
    # The sentinel values used below are defined only for non-NONE schemas. So
    # we return early with the result when the schema is NONE.
    return kd.expand_to_shape(
        kd.slice([], schema=kd.MASK), entity_ds.get_shape()
    )
  present_mask = get_present_mask(entity_ds, attr_name)
  ds_with_missing_or_removed_attr = entity_ds & ~present_mask
  # ds_with_missing_or_removed_attr.get_attr(attr_name) returns only missing and
  # removed values.
  sentinel_value = _get_present_sentinel_value(attr_schema)
  fallback_bag = kd.attrs(
      ds_with_missing_or_removed_attr, **{attr_name: sentinel_value}
  )
  return get_present_mask(
      ds_with_missing_or_removed_attr.enriched(fallback_bag), attr_name
  )


def get_removed_mask(
    entity_ds: kd.types.DataSlice, attr_name: str
) -> kd.types.DataSlice:
  """Returns a MASK with kd.present wherever entity_ds.attr_name is removed.

  I.e. the result will be kd.missing whenever ~kd.has(entity_ds) or when
  entity_ds.attr_name is present or missing.

  Args:
    entity_ds: a DataSlice with an entity schema.
    attr_name: the name of an attribute of the entity schema.

  Returns:
    A mask with the same shape as entity_ds.
  """
  return (
      kd.has(entity_ds)
      & ~get_present_mask(entity_ds, attr_name)
      & ~get_missing_mask(entity_ds, attr_name)
  )


def has_info_about_list_items(
    list_ds: kd.types.DataSlice,
) -> kd.types.DataSlice:
  """Returns a MASK with kd.present wherever list_ds has info about its items.

  In particular, the result will be kd.present whenever a list is present and
  list_ds.get_bag() contains some information about its items.

  For example, suppose
  list_ds = kd.slice([kd.list([], item_schema=kd.INT32),
                      kd.list([5, 6]).no_bag()])
  has_info_about_list_items(list_ds)  # -> kd.slice([kd.present, kd.missing])

  Args:
    list_ds: a DataSlice with a list schema.

  Returns:
    A mask with the same shape as list_ds.
  """
  list_schema = list_ds.get_schema()
  if not list_schema.is_list_schema():
    raise ValueError(f'{list_ds} does not have a LIST schema')
  item_schema = list_schema.get_item_schema()
  if item_schema == kd.NONE:
    # In contrast with the situation with entity attributes above, we do not
    # need kd.has(sentinel_value) to hold here. The reason is that we will
    # append sentinel_value to the existing list items in a fallback bag, and we
    # will be able to observe a difference in the length of the list items (when
    # there is info about them) even when we append a missing value.
    sentinel_value = kd.item(None, schema=kd.NONE)
  else:
    sentinel_value = _get_present_sentinel_value(item_schema)
  fallback_bag = kd.list_append_update(list_ds, sentinel_value)
  return kd.has(list_ds) & (
      kd.lists.size(list_ds) == kd.lists.size(list_ds.enriched(fallback_bag))
  )
