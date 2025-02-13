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

  In case of a DataSlice, we try to use `x.extract()` to avoid serializing
  unnecessary DataBag data. If this is undesirable, consider serializing the
  DataBag directly.

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
  if not isinstance(x, data_slice.DataSlice | data_bag.DataBag):
    raise ValueError(f'expected a DataSlice or a DataBag, got {type(x)}')
  # NOTE(b/385272947): Consider moving this logic further down to support
  # extraction for literals inside of expressions. This is more tricky since we
  # want slices with the same bag to keep the same bag id, and some slices may
  # not contain the set of attributes used for the entire expression.
  if isinstance(x, data_slice.DataSlice):
    try:
      x = x.extract()
    except ValueError:
      pass  # no Bag, ...
  return arolla.s11n.riegeli_dumps(x, riegeli_options=riegeli_options)


def loads(x: bytes) -> data_slice.DataSlice | data_bag.DataBag:
  """Deserializes a DataSlice or a DataBag."""
  result = arolla.s11n.riegeli_loads(x)
  if not isinstance(result, data_slice.DataSlice | data_bag.DataBag):
    raise ValueError(f'expected a DataSlice or a DataBag, got {type(result)}')
  return result
