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

"""Serialization functions for Kola Data."""

from arolla import arolla
from koladata.types import data_bag
from koladata.types import data_slice


def dumps(
    x: data_slice.DataSlice | data_bag.DataBag, /, *, riegeli_options: str = ''
) -> bytes:
  """Serializes a DataSlice or a DataBag.

  Due to current limitations of the underlying implementation, this can
  only serialize data slices with up to roughly 10**8 items.

  Args:
    x: DataSlice or DataBag to serialize.
    riegeli_options: A string with riegeli/records writer options. See
      https://github.com/google/riegeli/blob/master/doc/record_writer_options.md
        for details. If not provided, default options will be used.

  Returns:
    Serialized data.
  """
  return arolla.s11n.riegeli_dumps(x, riegeli_options=riegeli_options)


def loads(x: bytes) -> data_slice.DataSlice | data_bag.DataBag:
  """Deserializes a DataSlice or a DataBag."""
  result = arolla.s11n.riegeli_loads(x)
  if not isinstance(result, data_slice.DataSlice | data_bag.DataBag):
    raise ValueError('expected a DataSlice or a DataBag, got {type(result)}')
  return result


def loads_slice(x: bytes) -> data_slice.DataSlice:
  """Deserializes a DataSlice."""
  result = arolla.s11n.riegeli_loads(x)
  if not isinstance(result, data_slice.DataSlice):
    raise ValueError('expected a DataSlice, got {type(result)}')
  return result


def loads_bag(x: bytes) -> data_bag.DataBag:
  """Deserializes a DataBag."""
  result = arolla.s11n.riegeli_loads(x)
  if not isinstance(result, data_bag.DataBag):
    raise ValueError('expected a DataBag, got {type(result)}')
  return result
