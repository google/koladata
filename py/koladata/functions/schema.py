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

"""Koda functions for creating schemas."""

from koladata.types import data_bag
from koladata.types import data_item as _  # pylint: disable=unused-import
from koladata.types import data_slice


bag = data_bag.DataBag.empty


def new_schema(
    db: data_bag.DataBag | None = None, **attrs: data_slice.DataSlice
) -> data_slice.DataSlice:
  """Creates new schema in the given DataBag.

  Args:
    db: optional DataBag where the schema is created. If not provided, a new
      Databag is created.
    **attrs: attrs to set on the schema. Must be schemas.

  Returns:
    data_slice.DataSlice with the given attrs and kd.SCHEMA schema.
  """
  if db is None:
    db = bag()
  return db.new_schema(**attrs)


def list_schema(
    item_schema: data_slice.DataSlice, db: data_bag.DataBag | None = None
) -> data_slice.DataSlice:
  """Creates a list schema in the given DataBag.

  Args:
    item_schema: schema of the items in the list.
    db: optional DataBag where the schema is created. If not provided, a new
      Databag is created.

  Returns:
    data_slice.DataSlice representing a list schema.
  """
  if db is None:
    db = bag()
  return db.list_schema(item_schema)


def dict_schema(
    key_schema: data_slice.DataSlice,
    value_schema: data_slice.DataSlice,
    db: data_bag.DataBag | None = None,
) -> data_slice.DataSlice:
  """Creates a dict schema in the given DataBag.

  Args:
    key_schema: schema of the keys in the list.
    value_schema: schema of the values in the list.
    db: optional DataBag where the schema is created. If not provided, a new
      Databag is created.

  Returns:
    data_slice.DataSlice representing a dict schema.
  """
  if db is None:
    db = bag()
  return db.dict_schema(key_schema, value_schema)


def uu_schema(
    seed: str | None = None,
    db: data_bag.DataBag | None = None,
    **attrs: data_slice.DataSlice
) -> data_slice.DataSlice:
  """Creates a uu_schema in the given DataBag.

  Args:
    seed: optional string to seed the uuid computation with.
    db: optional DataBag where the schema is created. If not provided, a new
      Databag is created.
    **attrs: attrs to set on the schema. Must be schemas.

  Returns:
    data_slice.DataSlice with the given attrs and kd.SCHEMA schema.
  """
  if db is None:
    db = bag()
  return db.uu_schema(seed=seed, **attrs)
