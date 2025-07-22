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

"""Serialization functions for Kola Data."""

from typing import Any

from arolla import arolla
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import extension_types as ext_types
from koladata.types import jagged_shape


def _isinstance_of_supported_type(x: Any) -> bool:
  if isinstance(
      x, data_slice.DataSlice | data_bag.DataBag | jagged_shape.JaggedShape
  ):
    return True
  return ext_types.is_koda_extension(x)


def _extract_keep_mutability(x: data_slice.DataSlice) -> data_slice.DataSlice:
  if x.get_bag() is None:
    return x
  x_extracted = x.extract()
  if x.get_bag().is_mutable():
    x_extracted = x_extracted.fork_bag()
  return x_extracted


def dumps(
    x: data_slice.DataSlice | data_bag.DataBag,
    /,
    *,
    riegeli_options: str | None = None,
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
        for details. If not provided, 'snappy' will be used.

  Returns:
    Serialized data.
  """
  if not _isinstance_of_supported_type(x):
    raise ValueError(
        'expected a DataSlice, DataBag, JaggedShape, '
        f'or an extension type derived from DataSlice, got {type(x)}'
    )
  # NOTE(b/385272947): Consider moving this logic further down to support
  # extraction for literals inside of expressions. This is more tricky since we
  # want slices with the same bag to keep the same bag id, and some slices may
  # not contain the set of attributes used for the entire expression.
  if isinstance(x, data_slice.DataSlice):
    x = _extract_keep_mutability(x)
  if ext_types.is_koda_extension(x):
    try:
      # Try to extract the DataSlice underlying the value of the extension type.
      x_ds = ext_types.unwrap(x)
      x_ds = _extract_keep_mutability(x_ds)
      x = ext_types.wrap(x_ds, x.qtype)
    except ValueError:
      pass  # no Bag, ...
  if riegeli_options is None:
    riegeli_options = 'snappy'
  return arolla.s11n.riegeli_dumps(x, riegeli_options=riegeli_options)


def loads(x: bytes) -> data_slice.DataSlice | data_bag.DataBag:
  """Deserializes a DataSlice or a DataBag."""
  result = arolla.s11n.riegeli_loads(x)
  if not _isinstance_of_supported_type(result):
    raise ValueError(
        'expected a DataSlice, DataBag, JaggedShape, '
        f'or an extension type derived from DataSlice, got {type(result)}'
    )
  return result
